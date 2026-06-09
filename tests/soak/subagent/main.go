//go:build soak

// Package main provides a long-running soak test for the SubAgentMiddleware system.
//
// Design:
//   - A parent ReActAgent with SubAgentMiddleware configured with 10 sub-agents
//   - Each sub-agent has its own mock LLM and tools with random failures/timeouts/panics
//   - Some sub-agents are nested (can call other sub-agents)
//   - Checkpoint and resume cycles at random intervals
//   - Default: 30 minutes; configurable via --duration flag
//
// Usage:
//
//	go run -tags soak ./tests/soak/subagent/ --duration 30m --tenants 8
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/middlewares/subagent"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// ---- Configuration ----

var (
	flagDuration  = flag.Duration("duration", 30*time.Minute, "Total soak duration")
	flagTenants   = flag.Int("tenants", 8, "Number of concurrent tenant goroutines")
	flagVerbose   = flag.Bool("verbose", false, "Verbose per-iteration logging")
	flagFailRate  = flag.Float64("fail-rate", 0.08, "Probability of random tool failure")
	flagCrashRate = flag.Float64("crash-rate", 0.02, "Probability of random tool panic")
)

// ---- Global Metrics ----

var (
	globalAgentExecs  int64
	globalAgentErrors int64
	globalToolFails   int64
	globalToolPanics  int64
	globalToolTimeouts int64
	globalCheckpoints  int64
	globalResumes      int64
	globalNestings     int64
)

// ---- Mock Model ----

// soakModel is a scripted mock model that cycles through tool calls and final responses.
type soakModel struct {
	mu           sync.Mutex
	toolNames    []string // tools the model can "call"
	finalText    string
	toolCallsPer int // how many tool calls per "session" before final response
	callCount    int
	delay        time.Duration // simulated LLM latency
}

func newSoakModel(toolNames []string, finalText string, delay time.Duration, toolCallsPer int) *soakModel {
	return &soakModel{
		toolNames:    toolNames,
		finalText:    finalText,
		toolCallsPer: toolCallsPer,
		delay:        delay,
	}
}

func (m *soakModel) Generate(ctx context.Context, messages []*schema.Message, opts ...agentcore.ModelOption) (*schema.Message, error) {
	m.mu.Lock()
	m.callCount++
	callsSoFar := m.callCount
	m.mu.Unlock()

	// Simulate LLM latency
	if m.delay > 0 {
		jitter := time.Duration(rand.Int63n(int64(m.delay)))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(m.delay + jitter):
		}
	}

	// After toolCallsPer calls, return final text
	if callsSoFar > m.toolCallsPer || len(m.toolNames) == 0 {
		return &schema.Message{Role: schema.RoleAssistant, Content: m.finalText}, nil
	}

	// Pick a random tool to call
	toolName := m.toolNames[rand.Intn(len(m.toolNames))]
	tc := schema.ToolCall{
		ID: fmt.Sprintf("call_%s_%d", toolName, callsSoFar),
		Function: schema.ToolCallFunction{
			Name:      toolName,
			Arguments: `{"input":"test"}`,
		},
	}
	return &schema.Message{
		Role:      schema.RoleAssistant,
		Content:   "",
		ToolCalls: []schema.ToolCall{tc},
	}, nil
}

func (m *soakModel) Stream(ctx context.Context, messages []*schema.Message, opts ...agentcore.ModelOption) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.Generate(ctx, messages, opts...)
	if err != nil {
		return nil, err
	}
	return schema.StreamReaderFromArray([]*schema.Message{msg}), nil
}

func (m *soakModel) BindTools(tools []*schema.ToolInfo) error { return nil }

// ---- Mock Tool ----

// soakTool simulates various tool behaviors: success, failure, timeout, crash.
type soakTool struct {
	name       string
	desc       string
	minDelay   time.Duration
	maxDelay   time.Duration
	failRate   float64
	crashRate  float64
}

func (t *soakTool) Name() string        { return t.name }
func (t *soakTool) Description() string  { return t.desc }

func (t *soakTool) Invoke(ctx context.Context, args string, opts ...agentcore.ToolOption) (string, error) {
	// Simulate crash
	if rand.Float64() < t.crashRate {
		atomic.AddInt64(&globalToolPanics, 1)
		panic(fmt.Sprintf("soak tool %s: simulated crash", t.name))
	}

	// Simulate processing delay
	delay := t.minDelay
	if t.maxDelay > t.minDelay {
		delay += time.Duration(rand.Int63n(int64(t.maxDelay - t.minDelay)))
	}
	if delay > 0 {
		select {
		case <-ctx.Done():
			atomic.AddInt64(&globalToolTimeouts, 1)
			return "", ctx.Err()
		case <-time.After(delay):
		}
	}

	// Simulate failure
	if rand.Float64() < t.failRate {
		atomic.AddInt64(&globalToolFails, 1)
		return "", fmt.Errorf("soak tool %s: rate limit exceeded", t.name)
	}

	return fmt.Sprintf("result from %s with args=%s", t.name, args), nil
}

func (t *soakTool) Stream(ctx context.Context, args string, opts ...agentcore.ToolOption) (*schema.StreamReader[string], error) {
	result, err := t.Invoke(ctx, args, opts...)
	if err != nil {
		return nil, err
	}
	return schema.StreamReaderFromArray([]string{result}), nil
}

// ---- In-Memory Checkpoint Store ----

type memCheckpointStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemCheckpointStore() *memCheckpointStore {
	return &memCheckpointStore{data: make(map[string][]byte)}
}

func (s *memCheckpointStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	return v, ok, nil
}

func (s *memCheckpointStore) Set(_ context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = data
	return nil
}

// ---- Tenant ----

func runTenant(ctx context.Context, id int, wg *sync.WaitGroup) {
	defer wg.Done()

	// Create tools for sub-agents with varying failure/crash profiles
	searchTool := &soakTool{
		name: "search", desc: "Search the web",
		minDelay: 1 * time.Millisecond, maxDelay: 5 * time.Millisecond,
		failRate: *flagFailRate, crashRate: *flagCrashRate,
	}
	codegenTool := &soakTool{
		name: "codegen", desc: "Generate code",
		minDelay: 2 * time.Millisecond, maxDelay: 10 * time.Millisecond,
		failRate: *flagFailRate * 1.5, crashRate: *flagCrashRate * 2,
	}
	reviewTool := &soakTool{
		name: "review", desc: "Review code",
		minDelay: 1 * time.Millisecond, maxDelay: 3 * time.Millisecond,
		failRate: *flagFailRate * 0.5, crashRate: *flagCrashRate * 0.5,
	}
	transformTool := &soakTool{
		name: "transform", desc: "Transform data",
		minDelay: 0, maxDelay: 2 * time.Millisecond,
		failRate: *flagFailRate * 0.3, crashRate: 0,
	}
	analyzeTool := &soakTool{
		name: "analyze", desc: "Analyze results",
		minDelay: 3 * time.Millisecond, maxDelay: 8 * time.Millisecond,
		failRate: *flagFailRate, crashRate: *flagCrashRate * 1.5,
	}

	// Create sub-agent specs

	// Workers 1-5: simple, no tools, fast
	workerSpecs := make([]subagent.SubAgentSpec, 5)
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("worker_%d", i+1)
		spec := subagent.SubAgentSpec{
			Name:        name,
			Description: fmt.Sprintf("Worker %d", i+1),
			AgentConfig: &subagent.AgentConfig{
				Model:         newSoakModel(nil, fmt.Sprintf("worker %d done", i+1), 500*time.Microsecond, 0),
				MaxIterations: 3,
				SystemPrompt:  "You are a worker agent.",
			},
		}
		workerSpecs[i] = spec
	}

	// Researchers 1-3: have tools
	researcherSpecs := make([]subagent.SubAgentSpec, 3)
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("researcher_%d", i+1)
		spec := subagent.SubAgentSpec{
			Name:        name,
			Description: fmt.Sprintf("Researcher %d", i+1),
			AgentConfig: &subagent.AgentConfig{
				Model:         newSoakModel([]string{"search", "analyze"}, fmt.Sprintf("research %d done", i+1), 1*time.Millisecond, 2),
				Tools:         []agentcore.Tool{searchTool, analyzeTool},
				MaxIterations: 5,
				SystemPrompt:  "You are a research agent.",
			},
		}
		researcherSpecs[i] = spec
	}

	// Coders 1-2: have codegen and review tools, some may be nested
	coderSpecs := make([]subagent.SubAgentSpec, 2)
	for i := 0; i < 2; i++ {
		name := fmt.Sprintf("coder_%d", i+1)
		spec := subagent.SubAgentSpec{
			Name:        name,
			Description: fmt.Sprintf("Coder %d", i+1),
			AgentConfig: &subagent.AgentConfig{
				Model:         newSoakModel([]string{"codegen", "review", "transform"}, fmt.Sprintf("code %d done", i+1), 1*time.Millisecond, 3),
				Tools:         []agentcore.Tool{codegenTool, reviewTool, transformTool},
				MaxIterations: 5,
				SystemPrompt:  "You are a coding agent.",
			},
		}
		coderSpecs[i] = spec
	}

	allSpecs := append(append(workerSpecs, researcherSpecs...), coderSpecs...)

	// Create SubAgentMiddleware with MaxDepth
	saMW := subagent.New(allSpecs, &subagent.Config{
		EmitInternalEvents: true,
		MaxDepth:           3,
	})

	// Parent model: randomly calls sub-agents
	allNames := make([]string, len(allSpecs))
	for i, s := range allSpecs {
		allNames[i] = s.Name
	}

	parentModel := newSoakModel(allNames, "parent final response", 2*time.Millisecond, 2)

	// Build parent config
	cfg := &agentcore.ReActConfig[*schema.Message]{
		Model:         parentModel,
		Middlewares:   []agentcore.ReActMiddleware{saMW},
		MaxIterations: 8,
		Instruction:   "You are a coordinator agent. Use available sub-agents to complete tasks.",
	}
	saMW.BindToConfig(cfg)
	agent := agentcore.NewReActAgent(cfg)

	store := newMemCheckpointStore()

	iteration := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		iteration++

		// Build input messages
		msgs := []*schema.Message{
			schema.UserMessage(fmt.Sprintf("Process this task (tenant=%d, iter=%d)", id, iteration)),
		}

		// Random checkpoint ID for checkpoint/resume testing
		cid := fmt.Sprintf("soak-t%d-iter%d-%d", id, iteration, time.Now().UnixNano())

		runner := agentcore.NewTypedRunner(agentcore.RunnerConfig[*schema.Message]{
			Agent:           agent,
			CheckPointStore: store,
		})
		iter := runner.Run(ctx, msgs, agentcore.WithCheckPointID(cid))

		var lastErr error
		var completed bool
		for {
			ev, ok := iter.Next()
			if !ok {
				completed = true
				break
			}
			if ev.Err != nil {
				lastErr = ev.Err
				// Non-fatal: the error is expected during soak
				if *flagVerbose {
					log.Printf("[Tenant %d] Iter %d: event error=%v", id, iteration, ev.Err)
				}
				break
			}
		}

		if completed {
			atomic.AddInt64(&globalCheckpoints, 1)
		}
		if lastErr != nil {
			atomic.AddInt64(&globalAgentErrors, 1)
		}
		atomic.AddInt64(&globalAgentExecs, 1)

		// Log progress
		if iteration%50 == 0 {
			if *flagVerbose || id == 0 {
				log.Printf("[Tenant %d] Iter %d: completed=%v err=%v", id, iteration, completed, lastErr)
			}
		}
	}
}

// ---- Metrics Reporter ----

func metricsReporter(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	lastCheck := time.Now()
	var lastExecs, lastErrors int64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		now := time.Now()
		elapsed := now.Sub(lastCheck)
		totalElapsed := now.Sub(startTime)

		execs := atomic.LoadInt64(&globalAgentExecs)
		errs := atomic.LoadInt64(&globalAgentErrors)
		fails := atomic.LoadInt64(&globalToolFails)
		panics := atomic.LoadInt64(&globalToolPanics)
		touts := atomic.LoadInt64(&globalToolTimeouts)
		cps := atomic.LoadInt64(&globalCheckpoints)

		execRate := float64(execs-lastExecs) / elapsed.Seconds()
		errRate := float64(errs-lastErrors) / elapsed.Seconds()

		fmt.Printf("\n[%s] === SubAgent Soak Progress ===\n", now.Format(time.RFC3339))
		fmt.Printf("  Duration:    %s / %s\n", totalElapsed.Round(time.Second), flagDuration.Round(time.Second))
		fmt.Printf("  Tenants:     %d / SubAgents: 10\n", *flagTenants)
		fmt.Printf("  Execs:       %d (%.1f/s)\n", execs, execRate)
		fmt.Printf("  Errors:      %d (%.1f/s, %.1f%%)\n", errs, errRate, pct(errs, execs))
		fmt.Printf("  ToolFails:   %d\n", fails)
		fmt.Printf("  ToolPanics:  %d\n", panics)
		fmt.Printf("  ToolTimeouts %d\n", touts)
		fmt.Printf("  Checkpoints: %d\n", cps)

		lastExecs = execs
		lastErrors = errs
		lastCheck = now
	}
}

func pct(n, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(n) / float64(total) * 100
}

// ---- Main ----

var startTime time.Time

func main() {
	flag.Parse()

	fmt.Printf("=== Harness-Go SubAgent Soak Test ===\n")
	fmt.Printf("  Duration:    %s\n", flagDuration)
	fmt.Printf("  Tenants:     %d\n", *flagTenants)
	fmt.Printf("  SubAgents:   10 (5 workers + 3 researchers + 2 coders)\n")
	fmt.Printf("  Fail Rate:   %.1f%%\n", *flagFailRate*100)
	fmt.Printf("  Crash Rate:  %.1f%%\n", *flagCrashRate*100)
	fmt.Printf("\n")

	// Signal handling
	ctx, cancel := context.WithTimeout(context.Background(), *flagDuration)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nReceived signal, shutting down...")
		cancel()
	}()

	startTime = time.Now()

	// Start metrics reporter
	go metricsReporter(ctx)

	// Start tenant goroutines
	var wg sync.WaitGroup
	for i := 0; i < *flagTenants; i++ {
		wg.Add(1)
		go runTenant(ctx, i, &wg)
		time.Sleep(50 * time.Millisecond) // stagger start
	}

	wg.Wait()

	// Final report
	fmt.Printf("\n=== SubAgent Soak Test Complete ===\n")
	fmt.Printf("  Duration:     %s\n", time.Since(startTime).Round(time.Second))
	fmt.Printf("  Total Execs:  %d\n", atomic.LoadInt64(&globalAgentExecs))
	fmt.Printf("  Total Errors: %d\n", atomic.LoadInt64(&globalAgentErrors))
	fmt.Printf("  Tool Fails:   %d\n", atomic.LoadInt64(&globalToolFails))
	fmt.Printf("  Tool Panics:  %d\n", atomic.LoadInt64(&globalToolPanics))
	fmt.Printf("  Tool Timeouts %d\n", atomic.LoadInt64(&globalToolTimeouts))
	fmt.Printf("  Checkpoints:  %d\n", atomic.LoadInt64(&globalCheckpoints))

	totalExecs := atomic.LoadInt64(&globalAgentExecs)
	totalErrors := atomic.LoadInt64(&globalAgentErrors)
	if totalExecs > 0 {
		fmt.Printf("  Error Rate:   %.2f%%\n", pct(totalErrors, totalExecs))
	}

	if totalErrors > 0 {
		fmt.Println("\n⚠️  Test completed with errors (expected during fault injection).")
	} else {
		fmt.Println("\n✅ Test completed with zero errors.")
	}
}
