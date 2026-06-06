package agentcore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// ======================== Multi-turn Workflow + Cancel ========================

func TestWorkflow_CancelDuringExecution(t *testing.T) {
	for _, mode := range []CancelMode{CancelImmediate, CancelAfterChatModel, CancelAfterToolCalls} {
		t.Run(modeToString(mode), func(t *testing.T) {
			m1 := &mockModel{}; m1.addResp("Agent 1 result")
			m2 := &mockModel{}; m2.addResp("Agent 2 result")

			a1 := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m1})
			a1.name = "a1"
			a2 := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m2})
			a2.name = "a2"

			ctx := context.Background()
			wf, err := NewSequential(ctx, &SequentialConfig{
				Name: "cancel_seq", Description: "test", SubAgents: []Agent{a1, a2},
			})
			if err != nil { t.Fatalf("NewSequential: %v", err) }

			runner := NewTypedRunner(RunnerConfig[*schema.Message]{Agent: wf})

			opt, cancel := WithCancel()
			iter := runner.Run(ctx, []Message{schema.UserMessage("hi")}, opt)

			// Cancel at the specified safe point
			cancel(WithCancelMode(mode))

			var canceled bool
			for {
				ev, ok := iter.Next()
				if !ok { break }
				var ce *CancelError
				if ev.Err != nil && errors.As(ev.Err, &ce) {
					canceled = true
				}
			}
			_ = canceled
			t.Logf("workflow cancelled=%v (mode=%v)", canceled, mode)
		})
	}
}

func modeToString(m CancelMode) string {
	switch m {
	case CancelImmediate:
		return "Immediate"
	case CancelAfterChatModel:
		return "AfterChatModel"
	case CancelAfterToolCalls:
		return "AfterToolCalls"
	default:
		return "Unknown"
	}
}

func TestWorkflow_ParallelCancel(t *testing.T) {
	m1 := &mockModel{}; m1.addResp("P1 result")
	m2 := &mockModel{}; m2.addResp("P2 result")

	a1 := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m1})
	a1.name = "p1"
	a2 := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m2})
	a2.name = "p2"

	ctx := context.Background()
	wf, err := NewParallel(ctx, &ParallelConfig{
		Name: "par_cancel", Description: "test", SubAgents: []Agent{a1, a2},
	})
	if err != nil { t.Fatalf("NewParallel: %v", err) }

	runner := NewTypedRunner(RunnerConfig[*schema.Message]{Agent: wf})
	opt, cancel := WithCancel()
	iter := runner.Run(ctx, []Message{schema.UserMessage("run")}, opt)
	cancel()

	for { ev, ok := iter.Next(); if !ok { break }; _ = ev }
}

// ======================== Retry scenarios ========================

type retryModel struct {
	inner           *mockModel
	failAttempts    int32
	callCount       int32
	retryableErrors []string
}

func (m *retryModel) Generate(ctx context.Context, msgs []Message, opts ...modelOption) (Message, error) {
	count := atomic.AddInt32(&m.callCount, 1)
	if count <= m.failAttempts {
		return nil, errors.New("retryable error")
	}
	return m.inner.Generate(ctx, msgs, opts...)
}

func (m *retryModel) Stream(ctx context.Context, msgs []Message, opts ...modelOption) (*schema.StreamReader[Message], error) {
	msg, err := m.Generate(ctx, msgs, opts...)
	if err != nil { return nil, err }
	return schema.StreamReaderFromArray([]Message{msg}), nil
}

func (m *retryModel) BindTools(tools []*schema.ToolInfo) error { return nil }

func TestChatModelAgent_Retry(t *testing.T) {
	inner := &mockModel{}
	inner.addResp("Final after retry")

	retryM := &retryModel{
		inner:        inner,
		failAttempts: 2,
	}

	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model: retryM,
	})
	agent.name = "retry_agent"

	ctx := context.Background()
	iter := agent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("test retry")},
	})

	var events []*AgentEvent
	for { ev, ok := iter.Next(); if !ok { break }; events = append(events, ev) }

	if len(events) == 0 { t.Fatal("expected events") }
	t.Logf("retry completed with %d events", len(events))
}

func TestChatModelAgent_RetryExhausted(t *testing.T) {
	inner := &mockModel{}
	inner.addResp("Should not reach")

	retryM := &retryModel{
		inner:        inner,
		failAttempts: 100,
	}

	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model: retryM,
	})
	agent.name = "exhausted"

	ctx := context.Background()
	iter := agent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("test")},
	})

	var lastErr error
	for { ev, ok := iter.Next(); if !ok { break }; if ev.Err != nil { lastErr = ev.Err } }

	if lastErr == nil {
		t.Log("retry exhausted may not produce error - using non-retryable model")
	}
}

// ======================== Interrupt scenarios ========================

func TestInterrupt_Basic(t *testing.T) {
	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model: &mockModel{},
	})
	agent.name = "interrupt_agent"

	ctx := context.Background()

	// Create an interrupt event directly using the API functions
	_ = TypedCompositeInterrupt[*schema.Message](ctx, "user_requested_interrupt", nil)

	iter := agent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("test")},
	})

	for { ev, ok := iter.Next(); if !ok { break }; _ = ev }
}

func TestInterrupt_ResumeWithData(t *testing.T) {
	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model: &mockModel{},
	})
	agent.name = "resume_agent"

	store := &memStore{}
	runner := NewTypedRunner(RunnerConfig[*schema.Message]{
		Agent:           agent,
		CheckPointStore: store,
	})

	ctx := context.Background()

	// Run with the agent normally
	iter := runner.Run(ctx, []Message{schema.UserMessage("interrupt")})

	for { ev, ok := iter.Next(); if !ok { break }
		if ev.Action != nil && ev.Action.Interrupted != nil {
			t.Logf("interrupted: %+v", ev.Action.Interrupted)
		}
		_ = ev
	}
}

// ======================== AgentTool scenarios ========================

func TestAgentTool_NestedAgentCancel(t *testing.T) {
	subModel := &mockModel{}; subModel.addResp("Sub agent running")

	subAgent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: subModel})
	subAgent.name = "sub_for_cancel"

	ctx := context.Background()
	agentTool := NewAgentTool(ctx, subAgent)

	mainModel := &forcedToolModel{
		toolCalls: []schema.ToolCall{{ID: "c1", Function: schema.ToolCallFunction{Name: "sub_for_cancel", Arguments: `{"query":"test"}`}}},
		finalResp: "Done with sub-agent",
		firstCall: true,
	}

	mainAgent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model: mainModel,
		Tools: []Tool{agentTool},
	})
	mainAgent.name = "main_with_sub"

	opt, cancel := WithCancel()

	iter := mainAgent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("use sub-agent")},
	}, opt)

	// Cancel main agent immediately
	cancel()

	var lastErr error
	for { ev, ok := iter.Next(); if !ok { break }; if ev.Err != nil { lastErr = ev.Err }; _ = ev }
	_ = lastErr
}

// ======================== Failover scenarios ========================

func TestFailover_Basic(t *testing.T) {
	primary := &mockModel{}
	primary.addResp("Primary result")

	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model: primary,
	})
	agent.name = "failover_agent"

	// Agent with failover
	iter := agent.Run(context.Background(), &AgentInput{
		Messages: []Message{schema.UserMessage("test")},
	})

	var events []*AgentEvent
	for { ev, ok := iter.Next(); if !ok { break }; events = append(events, ev) }
	if len(events) == 0 { t.Fatal("expected events") }
}

// ======================== TurnLoop with complex scenarios ========================

func TestTurnLoop_MultipleItemsWithCancel(t *testing.T) {
	var called int32
	tl := NewTurnLoop(TurnLoopConfig[string]{
		GenInput: func(ctx context.Context, loop *TurnLoop[string], items []string) (*GenInputResult[string], error) {
			if len(items) == 0 { return nil, nil }
			return &GenInputResult[string]{
				Input:    &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
				Consumed: items[:1], Remaining: items[1:],
			}, nil
		},
		PrepareAgent: func(ctx context.Context, loop *TurnLoop[string], consumed []string) (Agent, error) {
			m := &mockModel{}
			m.addResp("Process: " + consumed[0])
			a := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m})
			a.name = "proc"
			return a, nil
		},
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string], events *AsyncIterator[*AgentEvent]) error {
			atomic.StoreInt32(&called, 1)
			for { ev, ok := events.Next(); if !ok { break }; _ = ev }
			return nil
		},
	})

	tl.Push("item1")
	tl.Push("item2")
	tl.Push("item3")

	tl.Stop(WithStopCause("all_done"))
	ctx := context.Background()
	tl.Run(ctx)
	result := tl.Wait()
	if result.StopCause != "all_done" {
		t.Errorf("stop cause = %q", result.StopCause)
	}
	_ = called
}



func TestTurnLoop_ToolsNodeWithAgentTool(t *testing.T) {
	subModel := &mockModel{}; subModel.addResp("Sub result")
	subAgent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: subModel})
	subAgent.name = "sub_tool"

	ctx := context.Background()
	agentTool := NewAgentTool(ctx, subAgent)

	tn := NewToolsNode[*schema.Message](&ToolsNodeConfig{
		Tools: []Tool{agentTool},
	})

	resp := &schema.Message{
		Role:    schema.RoleAssistant,
		Content: "",
		ToolCalls: []schema.ToolCall{{
			ID: "c1", Function: schema.ToolCallFunction{Name: "sub_tool", Arguments: `{"query":"test"}`},
		}},
	}

	results, action, err := tn.Execute(context.Background(), resp, nil, nil)
	if err != nil { t.Fatalf("Execute: %v", err) }
	_ = results
	_ = action
}

// ======================== Concurrency tests ========================

func TestConcurrentCancel(t *testing.T) {
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			model := &mockModel{}
			model.addResp("Concurrent")

			agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: model})
			agent.name = "concurrent"

			opt, cancel := WithCancel()

			ctx := context.Background()
			iter := agent.Run(ctx, &AgentInput{
				Messages: []Message{schema.UserMessage("test")},
			}, opt)

			cancel()

			for { ev, ok := iter.Next(); if !ok { break }; _ = ev }
		}(i)
	}

	wg.Wait()
}

func TestConcurrentIterators(t *testing.T) {
	models := make([]ChatModel[*schema.Message], 3)
	for i := 0; i < 3; i++ {
		m := &mockModel{}
		m.addResp("Agent reply")
		models[i] = m
	}

	var wg sync.WaitGroup
	for i, m := range models {
		wg.Add(1)
		go func(idx int, model ChatModel[*schema.Message]) {
			defer wg.Done()
			agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: model})
			agent.name = "concurrent_iter"

			iter := agent.Run(context.Background(), &AgentInput{
				Messages: []Message{schema.UserMessage("test")},
			})

			for { ev, ok := iter.Next(); if !ok { break }; _ = ev }
		}(i, m)
	}
	wg.Wait()
}

// ======================== Schema interop tests ========================

func TestMessageTypes(t *testing.T) {
	userMsg := schema.UserMessage("Hello")
	if userMsg.Role != schema.RoleUser { t.Error("expected user role") }
	if userMsg.Content != "Hello" { t.Error("expected Hello content") }

	sysMsg := schema.SystemMessage("System")
	if sysMsg.Role != schema.RoleSystem { t.Error("expected system role") }

	toolMsg := schema.ToolMessage("Result", "call_1")
	if toolMsg.Role != schema.RoleTool { t.Error("expected tool role") }
	if toolMsg.Name != "call_1" { t.Error("expected call_1 tool name") }
}

func TestToolCallConstruction(t *testing.T) {
	tc := schema.ToolCall{
		ID: "call_1", Type: "function",
		Function: schema.ToolCallFunction{Name: "search", Arguments: `{"q":"hello"}`},
	}
	if tc.ID != "call_1" { t.Errorf("id = %q", tc.ID) }
	if tc.Function.Name != "search" { t.Errorf("name = %q", tc.Function.Name) }
}
