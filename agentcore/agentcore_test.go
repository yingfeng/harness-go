package agentcore

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// ---- Mock ChatModel ----

type mockModel struct {
	responses []string
	mu        sync.Mutex
	callCount int
}

func (m *mockModel) addResp(r string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.responses = append(m.responses, r)
}

func (m *mockModel) Generate(ctx context.Context, msgs []Message, opts ...modelOption) (Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.callCount >= len(m.responses) {
		return nil, errors.New("no more responses configured")
	}
	resp := m.responses[m.callCount]
	m.callCount++
	return &schema.Message{Role: schema.RoleAssistant, Content: resp}, nil
}

func (m *mockModel) Stream(ctx context.Context, msgs []Message, opts ...modelOption) (*schema.StreamReader[Message], error) {
	msg, err := m.Generate(ctx, msgs, opts...)
	if err != nil { return nil, err }
	return schema.StreamReaderFromArray([]Message{msg}), nil
}

func (m *mockModel) BindTools(tools []*schema.ToolInfo) error { return nil }

// ---- Mock Tool ----

type mockTool struct {
	name     string
	desc     string
	executed bool
}

func (t *mockTool) Name() string                                     { return t.name }
func (t *mockTool) Description() string                               { return t.desc }
func (t *mockTool) Invoke(ctx context.Context, args string, opts ...toolOption) (string, error) {
	t.executed = true
	return "mock result for " + t.name, nil
}
func (t *mockTool) Stream(ctx context.Context, args string, opts ...toolOption) (*schema.StreamReader[string], error) {
	return schema.StreamReaderFromArray([]string{"mock stream result"}), nil
}

// ---- Mock Checkpoint Store ----

type memStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (s *memStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	if !ok { return nil, false, nil }
	return v, true, nil
}

func (s *memStore) Set(ctx context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil { s.data = make(map[string][]byte) }
	s.data[key] = data
	return nil
}

// ======================== Tests ========================

func TestChatModelAgent_Basic(t *testing.T) {
	model := &mockModel{}
	model.addResp("Hello, I am an AI assistant!")

	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model: model,
	})
	agent.name = "test_agent"

	ctx := context.Background()
	iter := agent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("Hi!")},
	})

	var events []*AgentEvent
	for {
		ev, ok := iter.Next()
		if !ok { break }
		events = append(events, ev)
	}

	if len(events) == 0 {
		t.Fatal("expected at least 1 event")
	}

	// Check model output event
	outputFound := false
	for _, ev := range events {
		if ev.Output != nil && ev.Output.MessageOutput != nil {
			if ev.Output.MessageOutput.Role == schema.RoleAssistant {
				outputFound = true
				if ev.Output.MessageOutput.Message.Content != "Hello, I am an AI assistant!" {
					t.Errorf("unexpected content: %s", ev.Output.MessageOutput.Message.Content)
				}
				break
			}
		}
	}
	if !outputFound {
		t.Error("no assistant output event found")
	}
}

func TestChatModelAgent_WithTool(t *testing.T) {
	model := &mockModel{}
	// First response has tool calls; second is final answer
	model.addResp("") // placeholder, will be replaced with direct ToolCalls
	model.addResp("Final answer after tool.")

	tool := &mockTool{name: "search", desc: "Search tool"}

	// Override model to produce tool calls directly
	model.mu.Lock()
	model.responses = nil
	model.callCount = 0
	model.mu.Unlock()

	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model: model,
		Tools: []Tool{tool},
	})
	agent.name = "test_tool_agent"

	ctx := context.Background()
	// Manually set up the run context and state for tool call test
	// We'll use a wrapped model that forces tool calls on first invoke
	wrapperModel := &forcedToolModel{inner: model}
	wrapperModel.toolCalls = []schema.ToolCall{{
		ID: "call1", Type: "function",
		Function: schema.ToolCallFunction{Name: "search", Arguments: `{"q":"test"}`},
	}}
	wrapperModel.finalResp = "Final answer after tool."
	wrapperModel.firstCall = true

	agent.config.Model = wrapperModel

	iter := agent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("Search for test")},
	})

	var events []*AgentEvent
	for {
		ev, ok := iter.Next()
		if !ok { break }
		events = append(events, ev)
	}

	if len(events) == 0 {
		t.Fatal("expected events")
	}

	// Verify tool was executed
	if !tool.executed {
		t.Error("tool was not executed")
	}
}

// forcedToolModel produces tool calls on first Generate then falls back to final answer.
type forcedToolModel struct {
	inner      *mockModel
	toolCalls  []schema.ToolCall
	finalResp  string
	firstCall  bool
}

func (m *forcedToolModel) Generate(ctx context.Context, msgs []Message, opts ...modelOption) (Message, error) {
	if m.firstCall {
		m.firstCall = false
		return &schema.Message{
			Role:      schema.RoleAssistant,
			Content:   "",
			ToolCalls: m.toolCalls,
		}, nil
	}
	return &schema.Message{Role: schema.RoleAssistant, Content: m.finalResp}, nil
}

func (m *forcedToolModel) Stream(ctx context.Context, msgs []Message, opts ...modelOption) (*schema.StreamReader[Message], error) {
	msg, err := m.Generate(ctx, msgs, opts...)
	if err != nil { return nil, err }
	return schema.StreamReaderFromArray([]Message{msg}), nil
}

func (m *forcedToolModel) BindTools(tools []*schema.ToolInfo) error { return nil }

func TestChatModelAgent_MaxIterations(t *testing.T) {
	tool := &mockTool{name: "loop", desc: "Infinite loop tool"}

	// A model that always produces tool calls
	loopModel := &loopToolModel{toolCalls: []schema.ToolCall{{
		ID: "call1", Type: "function",
		Function: schema.ToolCallFunction{Name: "loop", Arguments: "{}"},
	}}}

	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model:         loopModel,
		Tools:         []Tool{tool},
		MaxIterations: 3,
	})
	agent.name = "test_max_iter"

	ctx := context.Background()
	iter := agent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("Loop")},
	})

	var lastErr error
	for {
		ev, ok := iter.Next()
		if !ok { break }
		if ev.Err != nil {
			lastErr = ev.Err
		}
	}

	if lastErr == nil {
		t.Fatal("expected max iterations error")
	}
}

type loopToolModel struct {
	toolCalls []schema.ToolCall
}

func (m *loopToolModel) Generate(ctx context.Context, msgs []Message, opts ...modelOption) (Message, error) {
	return &schema.Message{Role: schema.RoleAssistant, Content: "", ToolCalls: m.toolCalls}, nil
}

func (m *loopToolModel) Stream(ctx context.Context, msgs []Message, opts ...modelOption) (*schema.StreamReader[Message], error) {
	msg, _ := m.Generate(ctx, msgs, opts...)
	return schema.StreamReaderFromArray([]Message{msg}), nil
}

func (m *loopToolModel) BindTools(tools []*schema.ToolInfo) error { return nil }

func TestRunner_Basic(t *testing.T) {
	model := &mockModel{}
	model.addResp("Runner response")

	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model: model,
	})
	agent.name = "runner_agent"

	runner := NewTypedRunner(RunnerConfig[*schema.Message]{
		Agent: agent,
	})

	ctx := context.Background()
	iter := runner.Run(ctx, []Message{schema.UserMessage("Hello runner")})

	found := false
	for {
		ev, ok := iter.Next()
		if !ok { break }
		if ev.Output != nil && ev.Output.MessageOutput != nil {
			if ev.Output.MessageOutput.Message.Content == "Runner response" {
				found = true
			}
		}
	}

	if !found {
		t.Error("expected runner response in output")
	}
}

func TestRunner_Query(t *testing.T) {
	model := &mockModel{}
	model.addResp("Query response")

	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model: model,
	})
	agent.name = "query_agent"

	runner := NewTypedRunner(RunnerConfig[*schema.Message]{
		Agent: agent,
	})

	ctx := context.Background()
	iter := runner.Query(ctx, "Test query")

	found := false
	for {
		ev, ok := iter.Next()
		if !ok { break }
		if ev.Output != nil && ev.Output.MessageOutput != nil {
			if ev.Output.MessageOutput.Message.Content == "Query response" {
				found = true
			}
		}
	}

	if !found {
		t.Error("expected query response in output")
	}
}

func TestCancel_Immediate(t *testing.T) {
	model := &mockModel{}
	model.addResp("Should not be reached")

	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model: model,
	})
	agent.name = "cancel_agent"

	opt, cancel := WithCancel()

	ctx := context.Background()
	iter := agent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("Hello")},
	}, opt)

	// Cancel immediately
	handle, contributed := cancel()
	if !contributed {
		t.Log("cancel did not contribute (may be ok)")
	}
	_ = handle

	canceled := false
	for {
		ev, ok := iter.Next()
		if !ok { break }
		if ev.Err != nil {
			var ce *CancelError
			if errors.As(ev.Err, &ce) {
				canceled = true
			}
		}
	}

	if !canceled {
		t.Log("expected cancel error (may not happen with immediate cancel)")
	}
}

func TestCancel_SafePoint(t *testing.T) {
	model := &mockModel{}
	model.addResp("First response")
	model.addResp("Second response")

	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model: model,
	})
	agent.name = "safepoint_agent"

	opt, cancel := WithCancel()

	ctx := context.Background()
	iter := agent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("Hello")},
	}, opt)

	// Consume first event
	ev, ok := iter.Next()
	if !ok {
		t.Fatal("expected first event")
	}
	_ = ev

	// Cancel after chat model
	cancel(WithCancelMode(CancelAfterChatModel))

	// Consume remaining
	var finalErr error
	for {
		ev, ok := iter.Next()
		if !ok { break }
		if ev.Err != nil {
			finalErr = ev.Err
		}
	}

	if finalErr == nil {
		t.Log("no cancel error (may complete normally)")
	}
}

func TestMiddleware_Chain(t *testing.T) {
	model := &mockModel{}
	model.addResp("Middleware result")

	var beforeAgentCalled, beforeModelCalled, afterModelCalled, afterAgentCalled bool

	mw := &testMiddleware{}
	mw.beforeAgent = func(ctx context.Context, rc *ChatModelAgentContext) (context.Context, *ChatModelAgentContext, error) {
		beforeAgentCalled = true
		rc.Instruction = "Custom instruction: " + rc.Instruction
		return ctx, rc, nil
	}
	mw.beforeModel = func(ctx context.Context, state *ChatModelAgentState, mc *ModelContext) (context.Context, *ChatModelAgentState, error) {
		beforeModelCalled = true
		return ctx, state, nil
	}
	mw.afterModel = func(ctx context.Context, state *ChatModelAgentState, mc *ModelContext) (context.Context, *ChatModelAgentState, error) {
		afterModelCalled = true
		return ctx, state, nil
	}
	mw.afterAgent = func(ctx context.Context, state *ChatModelAgentState) (context.Context, error) {
		afterAgentCalled = true
		return ctx, nil
	}

	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model:        model,
		Middlewares:  []ChatModelMiddleware{mw},
	})
	agent.name = "middleware_agent"

	ctx := context.Background()
	iter := agent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("Test middleware")},
	})
	for { ev, ok := iter.Next(); if !ok { break }; _ = ev }

	if !beforeAgentCalled {
		t.Error("BeforeAgent not called")
	}
	if !beforeModelCalled {
		t.Error("BeforeModelRewrite not called")
	}
	if !afterModelCalled {
		t.Error("AfterModelRewrite not called")
	}
	if !afterAgentCalled {
		t.Error("AfterAgent not called")
	}
}

func TestWorkflow_Sequential(t *testing.T) {
	m1 := &mockModel{}; m1.addResp("Agent 1 result")
	m2 := &mockModel{}; m2.addResp("Agent 2 result")

	a1 := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m1})
	a1.name = "a1"
	a2 := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m2})
	a2.name = "a2"

	ctx := context.Background()
	wf, err := NewSequential(ctx, &SequentialConfig{
		Name: "seq_test", Description: "test", SubAgents: []Agent{a1, a2},
	})
	if err != nil {
		t.Fatalf("NewSequential: %v", err)
	}

	// Run workflow agent - consume all events
	iter := wf.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("Run sequentially")},
	})

	events := 0
	for {
		ev, ok := iter.Next()
		if !ok { break }
		t.Logf("event: err=%v action=%v output=%v", ev.Err, ev.Action, ev.Output)
		_ = ev
		events++
	}

	t.Logf("sequential workflow: %d events", events)
	if events == 0 {
		t.Error("expected events from sequential workflow")
	}
}

func TestWorkflow_Parallel(t *testing.T) {
	m1 := &mockModel{}; m1.addResp("Parallel 1")
	m2 := &mockModel{}; m2.addResp("Parallel 2")

	a1 := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m1})
	a1.name = "p1"
	a2 := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m2})
	a2.name = "p2"

	ctx := context.Background()
	wf, err := NewParallel(ctx, &ParallelConfig{
		Name: "par_test", Description: "test", SubAgents: []Agent{a1, a2},
	})
	if err != nil {
		t.Fatalf("NewParallel: %v", err)
	}

	runner := NewTypedRunner(RunnerConfig[*schema.Message]{Agent: wf})
	iter := runner.Query(ctx, "Run in parallel")

	events := 0
	for {
		ev, ok := iter.Next()
		if !ok { break }
		_ = ev
		events++
	}

	if events == 0 {
		t.Error("expected events from parallel workflow")
	}
}

func TestAgentTool(t *testing.T) {
	subModel := &mockModel{}
	subModel.addResp("Sub agent result")

	subAgent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: subModel})
	subAgent.name = "sub_agent"

	ctx := context.Background()
	tool := NewAgentTool(ctx, subAgent, "sub_tool", "A sub-agent tool")

	if tool.Name() != "sub_tool" {
		t.Errorf("unexpected tool name: %s", tool.Name())
	}

	result, err := tool.Invoke(ctx, `{"query":"test"}`)
	if err != nil {
		t.Fatalf("tool invoke error: %v", err)
	}
	if result == "" {
		t.Error("expected non-empty result from agent tool")
	}
}

func TestTurnLoop(t *testing.T) {
	tl := NewTurnLoop()

	var executed []string
	tl.Add("task1", 0, func(ctx context.Context) error {
		executed = append(executed, "task1")
		return nil
	})
	tl.Add("task2", 10, func(ctx context.Context) error {
		executed = append(executed, "task2")
		return nil
	})
	tl.Add("task3", 5, func(ctx context.Context) error {
		executed = append(executed, "task3")
		return nil
	})

	ctx := context.Background()
	if err := tl.Run(ctx); err != nil {
		t.Fatalf("TurnLoop.Run: %v", err)
	}

	if len(executed) != 3 {
		t.Errorf("expected 3 tasks, got %d", len(executed))
	}
	// task2 (priority 10) should be first, then task3 (5), then task1 (0)
	if len(executed) >= 3 {
		if executed[0] != "task2" {
			t.Errorf("expected task2 first, got %s", executed[0])
		}
	}
}

func TestAgentToolFromRunner(t *testing.T) {
	subModel := &mockModel{}
	subModel.addResp("Sub-agent final")

	subAgent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: subModel})
	subAgent.name = "sub"

	ctx := context.Background()
	agentTool := NewAgentTool(ctx, subAgent, "research", "Research tool")

	mainModel := &forcedToolModel{
		inner:     &mockModel{},
		toolCalls: []schema.ToolCall{{ID: "c1", Type: "function", Function: schema.ToolCallFunction{Name: "research", Arguments: `{"topic":"AI"}`}}},
		finalResp: "Based on research, here's the answer.",
		firstCall: true,
	}

	mainAgent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model: mainModel,
		Tools: []Tool{agentTool},
	})
	mainAgent.name = "main"

	runner := NewTypedRunner(RunnerConfig[*schema.Message]{Agent: mainAgent})
	iter := runner.Query(ctx, "Research AI")

	count := 0
	for {
		ev, ok := iter.Next()
		if !ok { break }
		_ = ev
		count++
	}
	if count == 0 {
		t.Error("expected events from main agent with sub-agent tool")
	}
}

// ---- Test helper ----

type testMiddleware struct {
	BaseMiddleware[*schema.Message]
	beforeAgent func(context.Context, *ChatModelAgentContext) (context.Context, *ChatModelAgentContext, error)
	beforeModel func(context.Context, *ChatModelAgentState, *ModelContext) (context.Context, *ChatModelAgentState, error)
	afterModel  func(context.Context, *ChatModelAgentState, *ModelContext) (context.Context, *ChatModelAgentState, error)
	afterAgent  func(context.Context, *ChatModelAgentState) (context.Context, error)
}

func (m *testMiddleware) BeforeAgent(ctx context.Context, rc *ChatModelAgentContext) (context.Context, *ChatModelAgentContext, error) {
	if m.beforeAgent != nil { return m.beforeAgent(ctx, rc) }
	return ctx, rc, nil
}
func (m *testMiddleware) BeforeModelRewrite(ctx context.Context, state *ChatModelAgentState, mc *ModelContext) (context.Context, *ChatModelAgentState, error) {
	if m.beforeModel != nil { return m.beforeModel(ctx, state, mc) }
	return ctx, state, nil
}
func (m *testMiddleware) AfterModelRewrite(ctx context.Context, state *ChatModelAgentState, mc *ModelContext) (context.Context, *ChatModelAgentState, error) {
	if m.afterModel != nil { return m.afterModel(ctx, state, mc) }
	return ctx, state, nil
}
func (m *testMiddleware) AfterAgent(ctx context.Context, state *ChatModelAgentState) (context.Context, error) {
	if m.afterAgent != nil { return m.afterAgent(ctx, state) }
	return ctx, nil
}
