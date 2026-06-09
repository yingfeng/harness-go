package agentcore

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// ToolsNodeConfig configures the tools node for a ReActAgent.
type ToolsNodeConfig struct {
	// Tools is the list of tools available for execution.
	Tools []Tool

	// ReturnDirectly specifies tool names that cause the agent to return immediately.
	ReturnDirectly map[string]bool

	// ToolCallMiddlewares are middleware wrappers applied during tool invocation (legacy path).
	ToolCallMiddlewares []ToolCallMiddleware

	// ToolInvokeMiddlewares are new-style middleware wrappers using ToolInvocationContext.
	// When set, they replace the legacy ToolCallMiddlewares path entirely.
	ToolInvokeMiddlewares []ToolInvokeMiddleware

	// EmitInternalEvents enables forwarding internal events from AgentTool children.
	EmitInternalEvents bool

	// LoopGuard prevents infinite loops by detecting repeated tool calls
	// with identical arguments or consecutive failures. If nil, no guard is applied.
	LoopGuard *LoopGuard
}

// ToolCallMiddleware wraps tool call endpoints with custom behavior.
type ToolCallMiddleware struct {
	// Invokable wraps synchronous tool calls. Nil means no wrapping.
	Invokable func(ctx context.Context, next InvokableToolEndpoint, tc *ToolContext) (InvokableToolEndpoint, error)
	// Streamable wraps streaming tool calls. Nil means no wrapping.
	Streamable func(ctx context.Context, next StreamableToolEndpoint, tc *ToolContext) (StreamableToolEndpoint, error)
	// EnhancedInvokable wraps enhanced synchronous tool calls. Nil means no wrapping.
	EnhancedInvokable func(ctx context.Context, next EnhancedInvokableToolEndpoint, tc *ToolContext) (EnhancedInvokableToolEndpoint, error)
	// EnhancedStreamable wraps enhanced streaming tool calls. Nil means no wrapping.
	EnhancedStreamable func(ctx context.Context, next EnhancedStreamableToolEndpoint, tc *ToolContext) (EnhancedStreamableToolEndpoint, error)
}

// ToolsNode handles tool extraction from model output, dispatching to tools,
// collecting results, and applying middleware chains.
type ToolsNode[M MessageType] struct {
	config  *ToolsNodeConfig
	toolMap  map[string]Tool
}

// NewToolsNode creates a new ToolsNode with the given configuration.
func NewToolsNode[M MessageType](cfg *ToolsNodeConfig) *ToolsNode[M] {
	tn := &ToolsNode[M]{config: cfg}
	tn.toolMap = make(map[string]Tool, len(cfg.Tools))
	for _, t := range cfg.Tools {
		tn.toolMap[t.Name()] = t
	}
	return tn
}

// Execute processes all tool calls found in the model response.
// It returns the list of tool result messages to append to state,
// and any agent action (e.g., return-directly) that should be handled.
//
// When multiple independent tool calls are present, Execute runs them concurrently
// using a bounded goroutine pool (default max concurrency = 10), reducing total
// latency from O(sum) to O(max). For a single tool call, no goroutine is spawned.
func (tn *ToolsNode[M]) Execute(ctx context.Context, resp M, state *TypedReActAgentState[M], _ interface{}) ([]M, *AgentAction, error) {
	toolCalls := extractToolCalls(resp)
	if len(toolCalls) == 0 {
		return nil, nil, nil
	}

	if len(toolCalls) == 1 {
		// Fast path: single tool call, no goroutine overhead.
		tc := toolCalls[0]
		var action *AgentAction
		if tn.config.ReturnDirectly != nil && tn.config.ReturnDirectly[tc.Function.Name] {
			action = NewExitAction()
		}
		toolMsg, err := tn.executeOne(ctx, tc)
		if err != nil {
			return nil, action, fmt.Errorf("tool '%s': %w", tc.Function.Name, err)
		}
		return []M{toolMsg}, action, nil
	}

	// Multi-tool path: plan execution batches by capability, then execute.
	batches := tn.planBatches(toolCalls)
	var action *AgentAction
	var mu sync.Mutex
	var firstErr error
	var results []M

	for _, batch := range batches {
		if batch.mode == batchParallel {
			// Execute parallel-safe tools concurrently.
			const maxConcurrency = 10
			sem := make(chan struct{}, maxConcurrency)
			parResults := make([]M, len(batch.calls))
			var wg sync.WaitGroup

			for i, tc := range batch.calls {
				if tn.config.ReturnDirectly != nil && tn.config.ReturnDirectly[tc.Function.Name] {
					mu.Lock()
					action = NewExitAction()
					mu.Unlock()
				}
				wg.Add(1)
				go func(idx int, call schema.ToolCall) {
					defer wg.Done()
					sem <- struct{}{}
					defer func() { <-sem }()
					msg, err := tn.executeOne(ctx, call)
					mu.Lock()
					defer mu.Unlock()
					if err != nil && firstErr == nil {
						firstErr = fmt.Errorf("tool '%s': %w", call.Function.Name, err)
						return
					}
					parResults[idx] = msg
				}(i, tc)
			}
			wg.Wait()
			for _, r := range parResults {
				if !isNilMessage(r) {
					results = append(results, r)
				}
			}
		} else {
			// Execute serial tools one by one.
			for _, tc := range batch.calls {
				if tn.config.ReturnDirectly != nil && tn.config.ReturnDirectly[tc.Function.Name] {
					action = NewExitAction()
				}
				msg, err := tn.executeOne(ctx, tc)
				if err != nil && firstErr == nil {
					firstErr = fmt.Errorf("tool '%s': %w", tc.Function.Name, err)
				}
				if !isNilMessage(msg) {
					results = append(results, msg)
				}
			}
		}
	}

	if firstErr != nil {
		return nil, action, firstErr
	}
	return results, action, nil
}

func (tn *ToolsNode[M]) executeOne(ctx context.Context, tc schema.ToolCall) (msg M, err error) {
	// Panic recovery: tool.Invoke may panic, catch and convert to tool result message.
	defer func() {
		if r := recover(); r != nil {
			msg = tn.makeToolMsg(fmt.Sprintf("Error: tool '%s' panicked: %v", tc.Function.Name, r), tc.ID)
			err = nil // do not propagate Go error; captured as tool result text
		}
	}()

	// LoopGuard: detect repeated calls with identical arguments.
	if lg := tn.getLoopGuard(); lg != nil {
		if err := lg.CheckSameArgs(tc.Function.Name, tc.Function.Arguments); err != nil {
			return tn.makeToolMsg(fmt.Sprintf("Error: %v", err), tc.ID), nil
		}
	}

	tool, ok := tn.toolMap[tc.Function.Name]
	if !ok {
		errMsg := fmt.Sprintf("tool '%s' not found", tc.Function.Name)
		return tn.makeToolMsg(errMsg, tc.ID), nil
	}

	// New-style ToolInvokeMiddleware path takes precedence.
	if len(tn.config.ToolInvokeMiddlewares) > 0 {
		return tn.executeWithNewChain(ctx, tc, tool)
	}

	// Legacy ToolCallMiddleware path.
	if et, ok := tool.(EnhancedTool); ok {
		return tn.executeEnhanced(ctx, tc, et)
	}
	return tn.executeStandard(ctx, tc, tool)
}

func (tn *ToolsNode[M]) executeWithNewChain(ctx context.Context, tc schema.ToolCall, tool Tool) (M, error) {
	args := &schema.ToolArgument{
		Name:      tc.Function.Name,
		Arguments: tc.Function.Arguments,
		CallID:    tc.ID,
	}

	ictx := &ToolInvocationContext{
		Name:      tc.Function.Name,
		CallID:    tc.ID,
		Arguments: args,
	}

	var invokeFn InvokeTool
	if et, ok := tool.(EnhancedTool); ok {
		invokeFn = EnhancedToolToInvokeFn(et)
	} else {
		invokeFn = ToolToInvokeFn(tool)
	}

	chained := ToolWrapperChain(invokeFn, tn.config.ToolInvokeMiddlewares...)
	result, err := chained(ctx, ictx)
	if err != nil {
		return tn.makeToolMsg(fmt.Sprintf("Error: %v", err), tc.ID), nil
	}

	content := result.Content
	if result.Error != "" && content == "" {
		content = fmt.Sprintf("Error: %s", result.Error)
	}
	return tn.makeToolMsg(content, tc.ID), nil
}

func (tn *ToolsNode[M]) executeStandard(ctx context.Context, tc schema.ToolCall, tool Tool) (M, error) {
	args := &schema.ToolArgument{
		Name:      tc.Function.Name,
		Arguments: tc.Function.Arguments,
		CallID:    tc.ID,
	}

	ep := func(ctx context.Context, args *schema.ToolArgument, opts ...ToolOption) (*schema.ToolResult, error) {
		result, err := tool.Invoke(ctx, args.Arguments, opts...)
		if err != nil {
			return &schema.ToolResult{Name: args.Name, Error: err.Error(), ToolCallID: args.CallID}, nil
		}
		return &schema.ToolResult{Name: args.Name, Content: result, ToolCallID: args.CallID}, nil
	}

	tCtx := &ToolContext{Name: tc.Function.Name, CallID: tc.ID}

	wrappedEp := ep
	for _, mw := range tn.config.ToolCallMiddlewares {
		if mw.Invokable == nil { continue }
		compatEp := wrappedEp
		var adaptErr error
		wrappedEp = func(ctx context.Context, args *schema.ToolArgument, opts ...ToolOption) (*schema.ToolResult, error) {
			stdEp := func(ctx context.Context, jsonArgs string, o ...ToolOption) (string, error) {
				r, e := compatEp(ctx, &schema.ToolArgument{Arguments: jsonArgs}, o...)
				if e != nil { return "", e }
				if r.Error != "" { return "", fmt.Errorf("tool error: %s", r.Error) }
				return r.Content, nil
			}
			var wrappedStd InvokableToolEndpoint = stdEp
			wrappedStd, adaptErr = mw.Invokable(ctx, wrappedStd, tCtx)
			if adaptErr != nil { return nil, adaptErr }
			result, err := wrappedStd(ctx, args.Arguments, opts...)
			if err != nil { return &schema.ToolResult{Name: args.Name, Error: err.Error()}, nil }
			return &schema.ToolResult{Name: args.Name, Content: result}, nil
		}
		if adaptErr != nil { return tn.makeToolMsg(fmt.Sprintf("middleware error: %v", adaptErr), tc.ID), adaptErr }
	}

	result, err := wrappedEp(ctx, args)
	if err != nil {
		return tn.makeToolMsg(fmt.Sprintf("Error: %v", err), tc.ID), nil
	}

	content := result.Content
	if result.Error != "" && content == "" {
		content = fmt.Sprintf("Error: %s", result.Error)
	}
	return tn.makeToolMsg(content, tc.ID), nil
}

func (tn *ToolsNode[M]) executeEnhanced(ctx context.Context, tc schema.ToolCall, et EnhancedTool) (M, error) {
	args := &schema.ToolArgument{
		Name:      tc.Function.Name,
		Arguments: tc.Function.Arguments,
		CallID:    tc.ID,
	}

	ep := func(ctx context.Context, args *schema.ToolArgument, opts ...ToolOption) (*schema.ToolResult, error) {
		return et.EnhancedInvoke(ctx, args, opts...)
	}

	tCtx := &ToolContext{Name: tc.Function.Name, CallID: tc.ID}

	for _, mw := range tn.config.ToolCallMiddlewares {
		if mw.EnhancedInvokable == nil { continue }
		var err error
		ep, err = mw.EnhancedInvokable(ctx, ep, tCtx)
		if err != nil { return tn.makeToolMsg(fmt.Sprintf("middleware error: %v", err), tc.ID), err }
	}

	result, err := ep(ctx, args)
	if err != nil {
		return tn.makeToolMsg(fmt.Sprintf("Error: %v", err), tc.ID), nil
	}

	content := result.Content
	if result.Error != "" && content == "" {
		content = fmt.Sprintf("Error: %s", result.Error)
	}
	return tn.makeToolMsg(content, tc.ID), nil
}

func (tn *ToolsNode[M]) makeToolMsg(content, callID string) M {
	msg := schema.ToolMessage(content, callID)
	return any(msg).(M)
}

// ---- Helper: convert tool results for event emission ----

func toolResultToEvent[M MessageType](msg M, roleName string) *TypedAgentEvent[M] {
	if m, ok := any(msg).(*schema.Message); ok {
		return any(typedEventFromMessage(m, nil, schema.RoleTool, roleName)).(*TypedAgentEvent[M])
	}
	return nil
}

// ---- JSON helpers ----

func parseToolArgs(argsJSON string, target any) error {
	if err := json.Unmarshal([]byte(argsJSON), target); err != nil {
		return fmt.Errorf("invalid tool arguments JSON: %w", err)
	}
	return nil
}

// ---- LoopGuard: detect repeated tool calls with same args ----

// LoopGuard prevents infinite loops where the model repeatedly calls a tool
// with identical parameters. It tracks consecutive same-args calls per tool.
type LoopGuard struct {
	mu        sync.Mutex
	sameArgs  map[string]int // key = toolName+"|"+argsHash
	failures  map[string]int // key = toolName
	maxSame   int
	maxFails  int
}

// NewLoopGuard creates a LoopGuard with the given thresholds.
func NewLoopGuard(maxSame, maxFails int) *LoopGuard {
	return &LoopGuard{
		sameArgs: make(map[string]int),
		failures: make(map[string]int),
		maxSame:  maxSame,
		maxFails: maxFails,
	}
}

// CheckSameArgs returns an error if the same tool+args pair is called too many times.
func (g *LoopGuard) CheckSameArgs(toolName, argsJSON string) error {
	if g == nil || g.maxSame <= 0 {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	hash := fmt.Sprintf("%s|%x", toolName, md5.Sum([]byte(argsJSON)))
	g.sameArgs[hash]++
	if g.sameArgs[hash] >= g.maxSame {
		return fmt.Errorf("loop guard: tool '%s' called %d times with identical arguments", toolName, g.sameArgs[hash])
	}
	return nil
}

// RecordFailure tracks consecutive failures for a tool.
// Returns an error if the failure limit is exceeded.
func (g *LoopGuard) RecordFailure(toolName string) error {
	if g == nil || g.maxFails <= 0 {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.failures[toolName]++
	cnt := g.failures[toolName]
	if cnt >= g.maxFails {
		return fmt.Errorf("loop guard: tool '%s' failed %d consecutive times", toolName, cnt)
	}
	return nil
}

// Reset clears tracking for a tool (called on success or different args).
func (g *LoopGuard) Reset(toolName string) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	// Remove all same-args entries for this tool
	for k := range g.sameArgs {
		if len(k) > len(toolName) && k[:len(toolName)] == toolName {
			delete(g.sameArgs, k)
		}
	}
	delete(g.failures, toolName)
}

// ---- Tool capability and batch planning ----

// toolCapFromTool returns the capability of a tool.
func toolCapFromTool(t Tool) ToolCapability {
	if ct, ok := t.(CapableTool); ok {
		return ct.Capability()
	}
	return ToolCapWritesFiles // default: conservative serial
}

// executionBatch represents a group of tool calls to execute together.
type executionBatch struct {
	mode  batchMode
	calls []schema.ToolCall
}

type batchMode int

const (
	batchParallel batchMode = iota
	batchSerial
)

// planBatches groups tool calls into parallel/serial batches based on capability.
// Read-only tools are grouped for parallel execution; others run serially.
func (tn *ToolsNode[M]) planBatches(tcs []schema.ToolCall) []executionBatch {
	var batches []executionBatch
	var currentParallel []schema.ToolCall

	flushParallel := func() {
		if len(currentParallel) > 0 {
			batches = append(batches, executionBatch{mode: batchParallel, calls: currentParallel})
			currentParallel = nil
		}
	}

	for _, tc := range tcs {
		tool, ok := tn.toolMap[tc.Function.Name]
		if !ok {
			// Unknown tool - treat as serial to be safe.
			flushParallel()
			batches = append(batches, executionBatch{mode: batchSerial, calls: []schema.ToolCall{tc}})
			continue
		}
		cap := toolCapFromTool(tool)
		if cap == ToolCapReadOnly {
			currentParallel = append(currentParallel, tc)
		} else {
			flushParallel()
			batches = append(batches, executionBatch{mode: batchSerial, calls: []schema.ToolCall{tc}})
		}
	}
	flushParallel()
	return batches
}

// ---- LoopGuard integration in executeOne ----

// getLoopGuard returns the LoopGuard from the ToolsNode if configured.
// It is stored on the ToolsNode to share state across invocation cycles.
func (tn *ToolsNode[M]) getLoopGuard() *LoopGuard {
	if tn.config != nil {
		return tn.config.LoopGuard
	}
	return nil
}

// clearLoopGuard writes the LoopGuard config back (no-op, config is shared by pointer).
func (tn *ToolsNode[M]) clearLoopGuard() {}
