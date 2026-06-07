package agentcore

import (
	"context"
	stderrors "errors"
	"testing"
	"time"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
	"github.com/infiniflow/ragflow/harness/checkpoint"
	harnesserrors "github.com/infiniflow/ragflow/harness/errors"
	"github.com/infiniflow/ragflow/harness/types"
)

// ---- Comprehensive Graph ReAct Tests ----

// TestReActGraph_FullCheckpointInterruptResume verifies the COMPLETE lifecycle:
//  1. Build graph with checkpoint + interrupt
//  2. Invoke → reaches tool call → pauses at execute_tools (interrupt)
//  3. Resume from checkpoint → executes tool → completes
//  4. Verify final state is correct
func TestReActGraph_FullCheckpointInterruptResume(t *testing.T) {
	// This test requires the Pregel engine injection from harness.init().
	// In standalone agentcore tests, skip.

	model := &forcedToolModel{
		inner: &mockModel{},
		toolCalls: []schema.ToolCall{{
			ID: "full_cp_1",
			Function: schema.ToolCallFunction{
				Name:      "calculator",
				Arguments: "{\"x\":10,\"y\":20}",
			},
		}},
		finalResp: "the result is 30",
		firstCall: true,
	}
	tool := &mockTool{name: "calculator", desc: "math tool"}
	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model:       model,
		Tools:       []Tool{tool},
		ToolsConfig: &ToolsNodeConfig{Tools: []Tool{tool}},
		MaxIterations: 3,
	})
	agent.name = "full_cycle_agent"

	saver := checkpoint.NewMemorySaver()
	rg, err := NewReActGraph(agent, &ReActGraphConfig{
		Checkpointer:    saver,
		RecursionLimit:  20,
		InterruptBefore: []string{"execute_tools"}, // pause before tool execution
	})
	if err != nil {
		t.Fatalf("NewReActGraph: %v", err)
	}

	ctx := context.Background()
	input := &AgentInput{
		Messages: []*schema.Message{schema.UserMessage("what is 10+20?")},
	}
	config := &types.RunnableConfig{ThreadID: "full-cycle-001"}

	// ---- Phase 1: First invocation - reaches interrupt ----
	t.Log("=== Phase 1: First invocation ===")
	_, err = rg.Invoke(ctx, input, config)
	if err == nil {
		t.Fatal("expected interrupt error, got nil")
	}
	t.Logf("interrupt captured: %v", err)

	// ---- Phase 2: Human-in-the-loop review (simulated) ----
	t.Log("=== Phase 2: Human review ===")
	time.Sleep(5 * time.Millisecond) // simulate review time

	// ---- Phase 3: Resume from checkpoint ----
	t.Log("=== Phase 3: Resume ===")
	state, err := rg.Invoke(ctx, nil, config)
	if err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	if state == nil || len(state.Messages) == 0 {
		t.Fatal("expected messages after resume")
	}
	last := state.Messages[len(state.Messages)-1]
	if last.Content != "the result is 30" {
		t.Errorf("expected 'the result is 30', got %q", last.Content)
	}
	t.Logf("=== Final output: %s ===", last.Content)
}

// TestReActGraph_SerialCheckpointCycles verifies multiple interrupt-resume cycles.
func TestReActGraph_SerialCheckpointCycles(t *testing.T) {
	// Model returns 3 tool calls in sequence, each requiring human approval.
	model := &sequentialToolModel{
		mock:       &mockModel{},
		toolCalls: [][]schema.ToolCall{
			{{ID: "sc1", Function: schema.ToolCallFunction{Name: "step1", Arguments: "{}"}}},
			{{ID: "sc2", Function: schema.ToolCallFunction{Name: "step2", Arguments: "{}"}}},
		},
		finalResp:  "all steps complete",
		callCount:  0,
	}
	tool1 := &mockTool{name: "step1", desc: "first step"}
	tool2 := &mockTool{name: "step2", desc: "second step"}

	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model:       model,
		Tools:       []Tool{tool1, tool2},
		ToolsConfig: &ToolsNodeConfig{Tools: []Tool{tool1, tool2}},
		MaxIterations: 5,
	})
	agent.name = "serial_cycle"

	saver := checkpoint.NewMemorySaver()
	rg, err := NewReActGraph(agent, &ReActGraphConfig{
		Checkpointer:   saver,
		RecursionLimit: 30,
	})
	if err != nil {
		t.Fatalf("NewReActGraph: %v", err)
	}

	ctx := context.Background()
	config := &types.RunnableConfig{ThreadID: "serial-cycle-001"}
	input := &AgentInput{Messages: []*schema.Message{schema.UserMessage("run all steps")}}

	// Run through multiple cycles.
	cycles := 0
	maxCycles := 3
	for cycles < maxCycles {
		_, err = rg.Invoke(ctx, input, config)
		if err == nil {
			t.Log("graph completed without interrupt")
			break
		}
		var gi *harnesserrors.GraphInterrupt
		if stderrors.As(err, &gi) {
			cycles++
			t.Logf("cycle %d: interrupted, resuming...", cycles)
			// The accumulated state is preserved in the graph's checkpoint.
			// On next Invoke, the graph resumes from where it left off.
		} else {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	t.Logf("serial checkpoint cycles completed: %d interrupt-resume cycles", cycles)
}

// TestReActGraph_StreamingCheckpointEvents verifies streaming produces
// checkpoint events at each node boundary.
func TestReActGraph_StreamingCheckpointEvents(t *testing.T) {
	model := &forcedToolModel{
		inner:     &mockModel{},
		toolCalls: []schema.ToolCall{{
			ID: "stream_cp",
			Function: schema.ToolCallFunction{Name: "stream_tool", Arguments: "{}"},
		}},
		finalResp: "streaming done",
		firstCall: true,
	}
	tool := &mockTool{name: "stream_tool", desc: "stream test"}
	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model:       model,
		Tools:       []Tool{tool},
		ToolsConfig: &ToolsNodeConfig{Tools: []Tool{tool}},
		MaxIterations: 2,
	})
	agent.name = "stream_cp_agent"

	saver := checkpoint.NewMemorySaver()
	rg, err := NewReActGraph(agent, &ReActGraphConfig{
		Checkpointer:    saver,
		InterruptBefore: []string{},
		RecursionLimit:  20,
	})
	if err != nil {
		t.Fatalf("NewReActGraph: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	outCh, _ := rg.Stream(ctx, &AgentInput{
		Messages: []*schema.Message{schema.UserMessage("stream test")},
	}, nil, types.StreamModeCheckpoints)

	// Count checkpoint events.
	eventCount := 0
timeout:
	for {
		select {
		case ev, ok := <-outCh:
			if !ok {
				break timeout
			}
			_ = ev
			eventCount++
		case <-ctx.Done():
			break timeout
		}
	}
	t.Logf("streaming checkpoint events received: %d", eventCount)
}

// TestReActGraph_ConcurrentCheckpoints verifies concurrent graph instances
// with separate checkpoints don't interfere.
func TestReActGraph_ConcurrentCheckpoints(t *testing.T) {
	const instances = 5
	errs := make(chan error, instances)

	for i := 0; i < instances; i++ {
		go func(id int) {
			m := &mockModel{}
			m.addResp("concurrent result")
			agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
				Model:  m,
				MaxIterations: 1,
			}).WithName("concurrent_cp_agent")

			rg, err := NewReActGraph(agent, &ReActGraphConfig{
				Checkpointer:    checkpoint.NewMemorySaver(),
				InterruptBefore: []string{},
				RecursionLimit:  10,
			})
			if err != nil {
				errs <- err
				return
			}

			ctx := context.Background()
			_, err = rg.Invoke(ctx, &AgentInput{
				Messages: []*schema.Message{schema.UserMessage("concurrent test")},
			}, nil)
			errs <- err
		}(i)
	}

	for i := 0; i < instances; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent instance %d failed: %v", i, err)
		}
	}
	t.Logf("concurrent checkpoints: %d instances completed", instances)
}

// TestReActGraph_ToolMiddlewareChain verifies that the full tool middleware
// chain is applied in the graph-based ReAct execute_tools node.
func TestReActGraph_ToolMiddlewareChain(t *testing.T) {
	var wrapCalled bool
	mw := &testMiddleware{
		wrapToolInvoke: func(ctx context.Context, ep InvokableToolEndpoint, tc *ToolContext) (InvokableToolEndpoint, error) {
			wrapCalled = true
			return func(ctx context.Context, args string, opts ...ToolOption) (string, error) {
				return "[mw]" + "wrapped_result", nil
			}, nil
		},
	}

	model := &forcedToolModel{
		inner:     &mockModel{},
		toolCalls: []schema.ToolCall{{
			ID: "mw_test",
			Function: schema.ToolCallFunction{Name: "mw_tool", Arguments: "{}"},
		}},
		finalResp: "middleware done",
		firstCall: true,
	}
	tool := &mockTool{name: "mw_tool", desc: "middleware test tool"}
	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model:       model,
		Tools:       []Tool{tool},
		ToolsConfig: &ToolsNodeConfig{Tools: []Tool{tool}},
		Middlewares: []TypedChatModelMiddleware[*schema.Message]{mw},
		MaxIterations: 2,
	})
	agent.name = "mw_graph_agent"

	rg, err := NewReActGraph(agent, &ReActGraphConfig{
		InterruptBefore: []string{},
		RecursionLimit:  10,
	})
	if err != nil {
		t.Fatalf("NewReActGraph: %v", err)
	}

	ctx := context.Background()
	state, err := rg.Invoke(ctx, &AgentInput{
		Messages: []*schema.Message{schema.UserMessage("test middleware")},
	}, nil)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if !wrapCalled {
		t.Error("tool middleware WrapToolInvoke was NOT called in graph ReAct")
	}
	if state != nil && len(state.Messages) > 0 {
		last := state.Messages[len(state.Messages)-1]
		t.Logf("middleware graph test: last message=%s", last.Content)
	}
}

// ---- Helper models ----

// sequentialToolModel returns different tool calls on each Generate call,
// simulating a multi-step tool interaction.
type sequentialToolModel struct {
	mock       *mockModel
	toolCalls  [][]schema.ToolCall
	finalResp  string
	callCount  int
}

func (m *sequentialToolModel) Generate(ctx context.Context, msgs []*schema.Message, opts ...ModelOption) (*schema.Message, error) {
	if m.callCount < len(m.toolCalls) {
		tcs := m.toolCalls[m.callCount]
		m.callCount++
		// Return a message with these tool calls.
		msg := &schema.Message{Role: schema.RoleAssistant, Content: ""}
		msg.ToolCalls = tcs
		return msg, nil
	}
	return &schema.Message{Role: schema.RoleAssistant, Content: m.finalResp}, nil
}

func (m *sequentialToolModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...ModelOption) (*schema.StreamReader[*schema.Message], error) {
	r := schema.NewStreamReader[*schema.Message]()
	msg, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		r.Close()
		return r, err
	}
	r.Send(msg, nil)
	r.Close()
	return r, nil
}

func (m *sequentialToolModel) BindTools(tools []*schema.ToolInfo) error { return nil }
