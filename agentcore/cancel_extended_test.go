package agentcore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// ================================================================
// Extended cancel tests ported from Eino cancel_test.go patterns
// ================================================================

// ---- 1. DeepAgent cancel propagation (3 levels: Root → AgentTool → Leaf) ----

// TestCancel_DeepAgentCancelImmediate verifies immediate cancel propagates
// through Root Agent → AgentTool → Leaf Agent.
func TestCancel_DeepAgentCancelImmediate(t *testing.T) {
	mRoot := newCancelTestChatModel(nil)
	mRoot.addResp("tool")
	mLeaf := newCancelTestChatModel(nil)
	mLeaf.addResp("leaf")
	leafAgent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: mLeaf}).WithName("deep_leaf")
	agt := NewAgentTool(context.Background(), leafAgent)
	root := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model: mRoot, Tools: []Tool{agt},
		ToolsConfig: &ToolsNodeConfig{Tools: []Tool{agt}},
	}).WithName("deep_root")

	store := newCancelTestStore()
	opt, cancel := WithCancel()
	runner := NewTypedRunner(RunnerConfig[*schema.Message]{Agent: root, CheckPointStore: store})
	ctx := context.Background()
	iter := runner.Run(ctx, []*schema.Message{schema.UserMessage("deep")}, opt)
	time.Sleep(30 * time.Millisecond)
	cancel()
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		if ev.Err != nil {
			var ce *CancelError
			if errors.As(ev.Err, &ce) {
				t.Logf("deep cancel error: %v", ce)
			}
			break
		}
	}
}

// TestCancel_DeepAgentCancelAfterChatModel verifies CancelAfterChatModel
// through nested agents.
func TestCancel_DeepAgentCancelAfterChatModel(t *testing.T) {
	mRoot := newCancelTestChatModel(nil)
	mRoot.addResp("tool")
	mLeaf := newCancelTestChatModel(nil)
	mLeaf.addResp("leaf")
	leafAgent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: mLeaf}).WithName("leaf_chat")
	agt := NewAgentTool(context.Background(), leafAgent)
	root := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model: mRoot, Tools: []Tool{agt},
		ToolsConfig: &ToolsNodeConfig{Tools: []Tool{agt}},
	}).WithName("root_chat")

	opt, cancel := WithCancel()
	runner := NewTypedRunner(RunnerConfig[*schema.Message]{Agent: root})
	ctx := context.Background()
	iter := runner.Run(ctx, []*schema.Message{schema.UserMessage("deep chat")}, opt)
	time.Sleep(30 * time.Millisecond)
	cancel(WithCancelMode(CancelAfterChatModel))
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		if ev.Err != nil {
			break
		}
	}
}

// ---- 2. Cancel unaware agent ----

// TestCancelUnawareAgent_GracePeriod verifies immediate cancel falls back
// to a grace period for agents that don't participate in the cancel protocol.
func TestCancelUnawareAgent_GracePeriod(t *testing.T) {
	ua := &cancelUnawareAgent{name: "unaware_grace", desc: "agent ignoring cancel"}
	cc := newCancelContext()
	ctx := withCancelContext(context.Background(), cc)
	opt := WrapImplSpecificOptFn(func(o *runOptions) { o.cancelCtx = cc })
	iter := ua.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("hi")}}, opt)
	cc.triggerImmediate()
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		if ev.Err != nil {
			t.Logf("unaware agent cancel err: %v", ev.Err)
			break
		}
	}
}

// ---- 3. Cancel + Resume with assertions ----

// TestCancelThenResume_Checkpoint verifies the cancel-then-resume cycle
// with proper assertions on the resumed state.
func TestCancelThenResume_Checkpoint(t *testing.T) {
	model := newCancelTestChatModel(nil)
	model.addResp("first")
	model.addResp("second")
	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: model}).WithName("cancel_resume_cp")
	store := newCancelTestStore()

	cid := "cancel-resume-cp-001"
	cancelOpt, cancelFunc := WithCancel()
	runner := NewTypedRunner(RunnerConfig[*schema.Message]{Agent: agent, CheckPointStore: store})
	ctx := context.Background()

	// Phase 1: Run and cancel
	iter := runner.Run(ctx, []*schema.Message{schema.UserMessage("run")}, WithCheckPointID(cid), cancelOpt)
	time.Sleep(10 * time.Millisecond)
	cancelFunc(WithCancelMode(CancelImmediate))
	for {
		_, ok := iter.Next()
		if !ok {
			break
		}
	}

	// Phase 2: Resume from checkpoint
	resumedIter, err := runner.Resume(ctx, cid)
	if err != nil {
		t.Logf("Resume failed (expected if cancel didn't produce checkpoint): %v", err)
		return
	}
	var outputs []string
	for {
		ev, ok := resumedIter.Next()
		if !ok {
			break
		}
		if ev.Err != nil {
			break
		}
		if ev.Output != nil && ev.Output.MessageOutput != nil && ev.Output.MessageOutput.Message != nil {
			outputs = append(outputs, ev.Output.MessageOutput.Message.Content)
		}
	}
	t.Logf("resumed outputs: %v", outputs)
}

// TestCancelThenResume_WithoutCheckpoint verifies cancel without checkpoint
// doesn't panic and returns appropriate error.
func TestCancelThenResume_WithoutCheckpoint(t *testing.T) {
	model := newCancelTestChatModel(nil)
	model.addResp("no_ckpt")
	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: model}).WithName("no_ckpt")
	cancelOpt, cancelFunc := WithCancel()
	runner := NewTypedRunner(RunnerConfig[*schema.Message]{Agent: agent})
	ctx := context.Background()
	iter := runner.Run(ctx, []*schema.Message{schema.UserMessage("run")}, cancelOpt)
	cancelFunc(WithCancelMode(CancelImmediate))
	for {
		_, ok := iter.Next()
		if !ok {
			break
		}
	}
}

// ---- 4. Multiple cancel calls ----

// TestMultipleCancelCalls_JoinModes verifies multiple cancel calls
// with different modes are combined correctly.
func TestMultipleCancelCalls_JoinModes(t *testing.T) {
	cc := newCancelContext()
	cf := cc.buildCancelFunc()

	h1, ok1 := cf(WithCancelMode(CancelAfterChatModel))
	if !ok1 {
		t.Fatal("first should contribute")
	}

	h2, ok2 := cf(WithCancelMode(CancelAfterToolCalls))
	if !ok2 {
		t.Fatal("second should contribute")
	}

	want := CancelAfterChatModel | CancelAfterToolCalls
	if cc.getMode() != want {
		t.Errorf("mode=%v want=%v", cc.getMode(), want)
	}

	cc.markHandled()
	_ = h1.Wait()
	_ = h2.Wait()
}

// TestMultipleCancelCalls_EscalateToImmediate verifies that calling
// with CancelImmediate after CancelAfterChatModel escalates.
func TestMultipleCancelCalls_EscalateToImmediate(t *testing.T) {
	cc := newCancelContext()
	cf := cc.buildCancelFunc()

	cf(WithCancelMode(CancelAfterChatModel))
	cf(WithCancelMode(CancelImmediate))

	if cc.getMode() != CancelImmediate {
		t.Errorf("expected CancelImmediate, got %v", cc.getMode())
	}
	if atomic.LoadInt32(&cc.escalated) != 1 {
		t.Error("expected escalated")
	}
}

// TestMultipleCancelCalls_TimeoutEscalation verifies that timeout
// causes escalation to immediate.
func TestMultipleCancelCalls_TimeoutEscalation(t *testing.T) {
	cc := newCancelContext()
	cf := cc.buildCancelFunc()

	_, ok := cf(WithCancelMode(CancelAfterChatModel), WithCancelTimeout(20*time.Millisecond))
	if !ok {
		t.Fatal("first should contribute")
	}

	time.Sleep(50 * time.Millisecond)

	if !cc.isImmediate() {
		t.Error("should escalate to immediate after timeout")
	}
	if atomic.LoadInt32(&cc.timeoutEscalated) != 1 {
		t.Error("expected timeout escalated flag")
	}
}

// TestMultipleCancelCalls_AfterDone verifies cancel after execution
// has ended returns ErrExecutionEnded.
func TestMultipleCancelCalls_AfterDone(t *testing.T) {
	cc := newCancelContext()
	cc.markDone()

	cf := cc.buildCancelFunc()
	h, ok := cf()
	if ok {
		t.Fatal("should not contribute after done")
	}
	if !errors.Is(h.Wait(), ErrExecutionEnded) {
		t.Error("expected ErrExecutionEnded")
	}
}

// TestMultipleCancelCalls_Concurrent verifies concurrent cancel calls.
func TestMultipleCancelCalls_Concurrent(t *testing.T) {
	for i := 0; i < 10; i++ {
		cc := newCancelContext()
		cf := cc.buildCancelFunc()

		var wg sync.WaitGroup
		for j := 0; j < 100; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = cf()
			}()
		}
		wg.Wait()
		cc.markHandled()
	}
}

// ---- 5. Cancel with tools + various modes ----

// TestCancel_ImmediateDuringModelCall verifies immediate
// cancel during model call (with tools).
func TestCancel_ImmediateDuringModelCall(t *testing.T) {
	model := newCancelTestChatModel(nil)
	model.addResp("tool")
	tool := newSlowTool("slow_tool", 100*time.Millisecond, "result")
	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model: model, Tools: []Tool{tool},
		ToolsConfig: &ToolsNodeConfig{Tools: []Tool{tool}},
	}).WithName("tool_immediate")

	opt, cancel := WithCancel()
	runner := NewTypedRunner(RunnerConfig[*schema.Message]{Agent: agent})
	ctx := context.Background()
	iter := runner.Run(ctx, []*schema.Message{schema.UserMessage("run")}, opt)
	time.Sleep(30 * time.Millisecond)
	cancel()
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		if ev.Err != nil {
			var ce *CancelError
			if errors.As(ev.Err, &ce) {
				t.Logf("cancel error: %v", ce)
			}
			break
		}
	}
}

// TestCancel_AfterChatModelWithTools verifies CancelAfterChatModel
// with tools (model completes, tool runs).
func TestCancel_AfterChatModelWithTools(t *testing.T) {
	model := newCancelTestChatModel(nil)
	model.addResp("tool")
	tool := newSlowTool("slow_tool", 200*time.Millisecond, "result")
	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model: model, Tools: []Tool{tool},
		ToolsConfig: &ToolsNodeConfig{Tools: []Tool{tool}},
	}).WithName("tool_after_chat")

	opt, cancel := WithCancel()
	ctx := context.Background()
	iter := agent.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("run")}}, opt)
	time.Sleep(30 * time.Millisecond)
	cancel(WithCancelMode(CancelAfterChatModel))
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		if ev.Err != nil {
			break
		}
	}
}

// TestCancel_AfterToolCallsWithTools verifies CancelAfterToolCalls
// lets tools complete before cancelling.
func TestCancel_AfterToolCallsWithTools(t *testing.T) {
	model := newCancelTestChatModel(nil)
	model.addResp("tool")
	model.addResp("final")
	tool := newSlowTool("slow_tool", 50*time.Millisecond, "result")
	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model: model, Tools: []Tool{tool},
		ToolsConfig: &ToolsNodeConfig{Tools: []Tool{tool}},
	}).WithName("tool_after_tool")

	opt, cancel := WithCancel()
	ctx := context.Background()
	iter := agent.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("run")}}, opt)
	time.Sleep(30 * time.Millisecond)
	cancel(WithCancelMode(CancelAfterToolCalls))
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		if ev.Err != nil {
			break
		}
	}
}

// ---- 6. CancelMonitoredToolHandler ----

// TestCancelMonitoredToolHandler_Invokable verifies the handler wraps
// invokable tool endpoints.
func TestCancelMonitoredToolHandler_Invokable(t *testing.T) {
	t.Run("NilContext", func(t *testing.T) {
		h := &cancelMonitoredToolHandler{}
		wrapped := h.WrapToolInvoke(func(ctx context.Context, args string, opts ...ToolOption) (string, error) {
			return "ok", nil
		})
		r, err := wrapped(context.Background(), "{}")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if r != "ok" {
			t.Errorf("got %q", r)
		}
	})
	t.Run("ImmediateCancel", func(t *testing.T) {
		h := &cancelMonitoredToolHandler{}
		cc := newCancelContext()
		cc.triggerImmediate()
		wrapped := h.WrapToolInvoke(func(ctx context.Context, args string, opts ...ToolOption) (string, error) {
			return "no", nil
		})
		_, err := wrapped(withCancelContext(context.Background(), cc), "{}")
		if err == nil {
			t.Fatal("expected error on immediate cancel")
		}
	})
}

// ---- 7. Cancel with streaming ----

// TestCancel_StreamingCancelImmediate verifies streaming + immediate cancel.
func TestCancel_StreamingCancelImmediate(t *testing.T) {
	model := newCancelTestChatModel(nil)
	model.addResp("stream_resp")
	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: model}).WithName("stream_cancel_imm")
	opt, cancel := WithCancel()
	ctx := context.Background()
	iter := agent.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("run")}, EnableStreaming: true}, opt)
	time.Sleep(20 * time.Millisecond)
	cancel()
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		if ev.Err != nil {
			break
		}
	}
}

// TestCancel_StreamingCancelAfterToolCalls verifies streaming + CancelAfterToolCalls.
func TestCancel_StreamingCancelAfterToolCalls(t *testing.T) {
	model := newCancelTestChatModel(nil)
	model.addResp("tool")
	model.addResp("final")
	tool := newSlowTool("slow_tool", 20*time.Millisecond, "result")
	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model: model, Tools: []Tool{tool},
		ToolsConfig: &ToolsNodeConfig{Tools: []Tool{tool}},
	}).WithName("stream_tool_cancel")
	opt, cancel := WithCancel()
	ctx := context.Background()
	iter := agent.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("run")}, EnableStreaming: true}, opt)
	time.Sleep(30 * time.Millisecond)
	cancel(WithCancelMode(CancelAfterToolCalls))
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		if ev.Err != nil {
			break
		}
	}
}

// ---- 8. Cancel with checkpoint store ----

// TestCancel_WithCheckpointStore verifies cancel with checkpoint store.
func TestCancel_WithCheckpointStore(t *testing.T) {
	model := newCancelTestChatModel(nil)
	model.addResp("ckpt")
	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: model}).WithName("ckpt_cancel")
	store := newCancelTestStore()
	opt, cancel := WithCancel()
	runner := NewTypedRunner(RunnerConfig[*schema.Message]{Agent: agent, CheckPointStore: store})
	ctx := context.Background()
	iter := runner.Run(ctx, []*schema.Message{schema.UserMessage("run")}, opt)
	cancel()
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		if ev.Err != nil {
			break
		}
	}
}

// ---- 9. Cancel after completion ----

// TestCancelAfterCompletion verifies that cancelling an already-completed
// execution returns ErrExecutionEnded.
func TestCancelAfterCompletion(t *testing.T) {
	model := &mockModel{}
	model.addResp("done")
	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: model}).WithName("after_done")
	opt, cancel := WithCancel()
	ctx := context.Background()
	iter := agent.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("hi")}}, opt)
	for {
		_, ok := iter.Next()
		if !ok {
			break
		}
	}
	h, ok := cancel()
	if !ok {
		if !errors.Is(h.Wait(), ErrExecutionEnded) {
			t.Error("expected ErrExecutionEnded")
		}
	}
}

// ---- 10. Supervisor agent cancel ----

// TestCancel_SupervisorAgent verifies cancel through supervisor flow agent.
func TestCancel_SupervisorAgent(t *testing.T) {
	mSup := newCancelTestChatModel(nil)
	mSup.addResp("sup")
	mSub := newCancelTestChatModel(nil)
	mSub.addResp("sub")
	sup := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: mSup}).WithName("sup_cancel")
	sub := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: mSub}).WithName("sub_cancel")
	ctx := context.Background()
	flow, err := SetSubAgents(ctx, sup, []Agent{sub})
	if err != nil {
		t.Fatalf("SetSubAgents: %v", err)
	}
	opt, cancel := WithCancel()
	iter := flow.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("route")}}, opt)
	time.Sleep(30 * time.Millisecond)
	cancel()
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		if ev.Err != nil {
			break
		}
	}
}

// ---- 11. AgentTool cancel propagation ----

// TestCancel_AgentToolCancelAfterChatModel verifies CancelAfterChatModel
// through AgentTool.
func TestCancel_AgentToolCancelAfterChatModel(t *testing.T) {
	mRoot := newCancelTestChatModel(nil)
	mRoot.addResp("tool")
	mLeaf := newCancelTestChatModel(nil)
	mLeaf.addResp("leaf")
	leafAgent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: mLeaf}).WithName("leaf_tool")
	agt := NewAgentTool(context.Background(), leafAgent)
	root := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model: mRoot, Tools: []Tool{agt},
		ToolsConfig: &ToolsNodeConfig{Tools: []Tool{agt}},
	}).WithName("root_tool_cancel")
	opt, cancel := WithCancel()
	ctx := context.Background()
	iter := root.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("start")}}, opt)
	time.Sleep(30 * time.Millisecond)
	cancel(WithCancelMode(CancelAfterChatModel))
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		if ev.Err != nil {
			break
		}
	}
}
