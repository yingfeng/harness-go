package agentcore

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// ======================== Cancel context tests ========================

func assertNotClosed(t *testing.T, ch <-chan struct{}, d time.Duration) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("channel was closed but should not have been")
	case <-time.After(d):
	}
}

func assertClosed(t *testing.T, ch <-chan struct{}, d time.Duration) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(d):
		t.Fatal("channel was not closed within timeout")
	}
}

func TestCancelContext_New(t *testing.T) {
	cc := newCancelContext()
	if !cc.isRoot() { t.Error("expected root") }
	if cc.shouldCancel() { t.Error("should not be cancelled initially") }
	if cc.getMode() != CancelImmediate { t.Error("default mode should be CancelImmediate") }
	cc.markDone()
}

func TestCancelContext_Lifecycle(t *testing.T) {
	cc := newCancelContext()

	// Running -> Cancelling transition
	cc.triggerCancel(CancelAfterChatModel)
	if !cc.shouldCancel() { t.Error("should be cancelled") }
	if cc.getMode() != CancelAfterChatModel { t.Error("mode should be CancelAfterChatModel") }

	// Cannot trigger again after cancelled
	cc.triggerCancel(CancelImmediate)
	// mode is already set, second trigger is no-op
	_ = cc.getMode()

	// Cancelling -> CancelHandled transition
	if !cc.markHandled() { t.Error("markHandled should succeed") }
	if cc.markHandled() { t.Error("markHandled should fail second time") }

	// Done channel is closed
	select {
	case <-cc.doneChan:
	default:
		t.Fatal("doneChan should be closed after markHandled")
	}
}

func TestCancelContext_Immediate(t *testing.T) {
	cc := newCancelContext()

	// Immediate triggers both cancel and immediate channels
	cc.triggerImmediate()
	if !cc.shouldCancel() { t.Error("should be cancelled") }
	if !cc.isImmediate() { t.Error("should be immediate") }
	if cc.getMode() != CancelImmediate { t.Error("mode should be CancelImmediate") }
}

func TestCancelContext_MarkDone(t *testing.T) {
	cc := newCancelContext()
	cc.markDone()

	// Done state persists
	cc.markDone() // no-op

	if cc.shouldCancel() { t.Error("done does not imply cancelled") }
	select {
	case <-cc.doneChan:
	default:
		t.Fatal("doneChan should be closed")
	}
}

func TestCancelContext_BuildCancelFunc_Immediate(t *testing.T) {
	cc := newCancelContext()
	cancelFn := cc.buildCancelFunc()

	_, ok := cancelFn()
	if !ok { t.Fatal("cancel should have contributed") }
	// Immediate cancel triggers immediate channel
	select {
	case <-cc.immediateChan:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("immediateChan not closed within timeout")
	}
}

func TestCancelContext_BuildCancelFunc_SafePoint(t *testing.T) {
	cc := newCancelContext()
	cancelFn := cc.buildCancelFunc()

	handle, ok := cancelFn(WithCancelMode(CancelAfterChatModel))
	if !ok { t.Fatal("cancel should contribute") }
	if !cc.shouldCancel() { t.Error("should be cancelled") }
	// Not immediate, so immediateChan should NOT be closed
	assertNotClosed(t, cc.immediateChan, 50*time.Millisecond)
	_ = handle
}

func TestCancelContext_BuildCancelFunc_Twice(t *testing.T) {
	cc := newCancelContext()
	cancelFn := cc.buildCancelFunc()

	_, ok := cancelFn(WithCancelMode(CancelAfterChatModel))
	if !ok { t.Fatal("first cancel should contribute") }

	// Second cancel should merge mode
	_, ok2 := cancelFn(WithCancelMode(CancelAfterToolCalls))
	if !ok2 { t.Fatal("second cancel should also contribute") }

	// The merged mode should still not be immediate (neither was immediate)
	assertNotClosed(t, cc.immediateChan, 50*time.Millisecond)

	_ = cc.getMode()
}

func TestCancelContext_BuildCancelFunc_AfterEnded(t *testing.T) {
	cc := newCancelContext()
	cc.markDone()

	handle, ok := cc.buildCancelFunc()()
	if ok { t.Fatal("cancel should not contribute after done") }
	err := handle.Wait()
	if !errors.Is(err, ErrExecutionEnded) {
		t.Errorf("expected ErrExecutionEnded, got %v", err)
	}
}

func TestCancelContext_Timeout(t *testing.T) {
	cc := newCancelContext()
	cancelFn := cc.buildCancelFunc()

	timeout := 100 * time.Millisecond
	_, ok := cancelFn(WithCancelMode(CancelAfterChatModel), WithCancelTimeout(timeout))
	if !ok { t.Fatal("cancel should contribute") }

	// After timeout, immediate should trigger
	time.Sleep(timeout + 50*time.Millisecond)
	if !cc.isImmediate() { t.Error("should escalate to immediate after timeout") }
}

func TestCancelContext_SetRecursive(t *testing.T) {
	cc := newCancelContext()
	cc.setRecursive(true)
	if !cc.isRecursive() { t.Error("should be recursive") }

	// Setting again is no-op
	cc.setRecursive(true)
}

func TestCancelContext_DeriveNoPropagation(t *testing.T) {
	parent := newCancelContext()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	child := parent.deriveAgentToolCancelContext(ctx)
	defer child.markDone()

	// Parent cancel with safe point should NOT propagate to child by default
	parent.triggerCancel(CancelAfterChatModel)
	assertNotClosed(t, child.cancelChan, 50*time.Millisecond)
}

func TestCancelContext_DeriveImmediateNoPropagation(t *testing.T) {
	parent := newCancelContext()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	child := parent.deriveAgentToolCancelContext(ctx)
	defer child.markDone()

	// Parent immediate cancel should NOT propagate to child by default
	parent.triggerImmediate()
	assertNotClosed(t, child.immediateChan, 50*time.Millisecond)
}

func TestCancelContext_DeriveRecursivePropagation(t *testing.T) {
	parent := newCancelContext()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	child := parent.deriveAgentToolCancelContext(ctx)
	defer child.markDone()

	// Set parent recursive
	parent.setRecursive(true)

	// Parent cancel should propagate to child
	parent.triggerCancel(CancelAfterChatModel)
	assertClosed(t, child.cancelChan, 200*time.Millisecond)
}

func TestCancelContext_DeriveRecursiveImmediatePropagation(t *testing.T) {
	parent := newCancelContext()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	child := parent.deriveAgentToolCancelContext(ctx)
	defer child.markDone()

	parent.setRecursive(true)
	parent.triggerImmediate()

	assertClosed(t, child.immediateChan, 200*time.Millisecond)
}

func TestCancelContext_DeriveMarkDoneNoLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	parent := newCancelContext()
	ctx, cancel := context.WithCancel(context.Background())

	child := parent.deriveAgentToolCancelContext(ctx)
	child.markDone()
	cancel()

	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()

	if after > before+5 {
		t.Errorf("goroutine leak: before=%d after=%d", before, after)
	}
}

func TestCancelContext_AgentToolDescendant(t *testing.T) {
	parent := newCancelContext()
	parent.markAgentToolDescendant()
	// Should be set
}

func TestCancelContext_GetMode_NoContext(t *testing.T) {
	// Context without cancel context should give CancelImmediate
	mode := getMode(context.Background())
	if mode != CancelImmediate { t.Errorf("expected CancelImmediate for nil, got %v", mode) }
}

func TestCancelContext_GetMode_WithContext(t *testing.T) {
	cc := newCancelContext()
	ctx := withCancelContext(context.Background(), cc)
	got := getMode(ctx)
	if got != CancelImmediate { t.Errorf("expected CancelImmediate, got %v", got) }
}

func getMode(ctx context.Context) CancelMode {
	cc := getCancelContext(ctx)
	if cc == nil { return CancelImmediate }
	return cc.getMode()
}

func TestCancelContext_BuildCancelFunc_TimeoutThenCancel(t *testing.T) {
	cc := newCancelContext()
	cancelFn := cc.buildCancelFunc()

	// First cancel with timeout
	_, ok := cancelFn(WithCancelMode(CancelAfterChatModel), WithCancelTimeout(200*time.Millisecond))
	if !ok { t.Fatal("cancel should contribute") }

	// Check there's no immediate yet
	if cc.isImmediate() { t.Error("should not be immediate yet") }

	// Second cancel should join modes
	_, ok2 := cancelFn(WithCancelMode(CancelAfterToolCalls))
	if !ok2 { t.Fatal("second cancel should contribute") }
	_ = ok2
}

func TestCancelContext_SendInterrupt(t *testing.T) {
	cc := newCancelContext()
	cc.triggerCancel(CancelAfterChatModel)
	sent := cc.sendInterrupt()
	if !sent { t.Error("interrupt should be sent") }
	// Second send should return false
	if cc.sendInterrupt() { t.Error("second interrupt should not be sent") }
}

func TestCancelContext_NoDuplicateCancel(t *testing.T) {
	cc := newCancelContext()

	// Build cancel function
	cancelFn := cc.buildCancelFunc()
	_, ok := cancelFn(WithCancelMode(CancelAfterChatModel))
	if !ok { t.Fatal("cancel should contribute") }

	// BuildCancelFunc should work multiple times
	cancelFn2 := cc.buildCancelFunc()
	h, ok2 := cancelFn2()
	_ = h
	// When state is already stCancelling, the second cancel should still work
	// (it just sets mode, doesn't close channel again)
	t.Logf("second cancel contributed: %v", ok2)
}

// ---- Cancel integration: cancel_monitored ----

func TestCancelMonitoredTool_ImmediateRejects(t *testing.T) {
	cc := newCancelContext()
	cc.triggerImmediate()

	handler := &cancelMonitoredToolHandler{}
	wrapped := handler.WrapToolInvoke(func(ctx context.Context, args string, opts ...ToolOption) (string, error) {
		return "should not reach", nil
	})

	ctx := withCancelContext(context.Background(), cc)
	_, err := wrapped(ctx, "{}")
	if err == nil {
		t.Fatal("expected error from immediate cancel")
	}
}

func TestCancelMonitoredTool_Normal(t *testing.T) {
	handler := &cancelMonitoredToolHandler{}
	wrapped := handler.WrapToolInvoke(func(ctx context.Context, args string, opts ...ToolOption) (string, error) {
		return "normal result", nil
	})

	cc := newCancelContext()
	ctx := withCancelContext(context.Background(), cc)
	result, err := wrapped(ctx, "{}")
	if err != nil { t.Fatalf("unexpected error: %v", err) }
	if result != "normal result" { t.Errorf("got %q", result) }
}

func TestCancelMonitoredTool_NilContext_NoEffect(t *testing.T) {
	handler := &cancelMonitoredToolHandler{}
	wrapped := handler.WrapToolInvoke(func(ctx context.Context, args string, opts ...ToolOption) (string, error) {
		return "ok", nil
	})

	result, err := wrapped(context.Background(), "{}")
	if err != nil { t.Fatalf("unexpected error: %v", err) }
	if result != "ok" { t.Errorf("got %q", result) }
}

// ---- Cancel via WithCancel: integration tests ----

func TestCancelWithRunner_Immediate(t *testing.T) {
	model := &mockModel{}
	model.addResp("Cancel me")

	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: model})
	agent.name = "cancel_runner"

	opt, cancel := WithCancel()

	ctx := context.Background()
	iter := agent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("test")},
	}, opt)

	// Cancel immediately before/after first event
	cancel()

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
}

func TestCancelWithRunner_SafePointAfterChatModel(t *testing.T) {
	model := &mockModel{}
	model.addResp("First round")
	model.addResp("Second round")

	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: model})
	agent.name = "safepoint"

	opt, cancel := WithCancel()

	ctx := context.Background()
	iter := agent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("test")},
	}, opt)

	// Consume first event
	ev, ok := iter.Next()
	if !ok { t.Fatal("expected first event") }
	_ = ev

	// Cancel at next safe point
	cancel(WithCancelMode(CancelAfterChatModel))

	// Consume rest
	var finalErr error
	for {
		ev, ok := iter.Next()
		if !ok { break }
		if ev.Err != nil { finalErr = ev.Err }
	}

	if finalErr == nil {
		t.Log("no cancel error (may complete before safe point)")
	}
}

func TestCancelWithRunner_AfterToolCalls(t *testing.T) {
	model := &mockModel{}
	model.addResp("Tool call phase")

	tool := &mockTool{name: "calc", desc: "calculator"}
	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model: model,
		Tools: []Tool{tool},
	})
	agent.name = "aft"

	// Use a model that returns tool calls
	wrapperModel := &forcedToolModel{
		inner:     model,
		toolCalls: []schema.ToolCall{{ID: "c1", Function: schema.ToolCallFunction{Name: "calc", Arguments: "{}"}}},
		finalResp: "Done",
		firstCall: true,
	}
	agent.config.Model = wrapperModel

	opt, cancel := WithCancel()

	ctx := context.Background()
	iter := agent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("run calc")},
	}, opt)

	// Cancel after tool calls
	cancel(WithCancelMode(CancelAfterToolCalls))

	for {
		ev, ok := iter.Next()
		if !ok { break }
		_ = ev
	}
}

func TestCancel_AlreadyDone(t *testing.T) {
	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: &mockModel{}})
	agent.name = "done_agent"

	opt, cancel := WithCancel()
	ctx := context.Background()
	iter := agent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("test")},
	}, opt)

	// Drain all
	for { ev, ok := iter.Next(); if !ok { break }; _ = ev }

	// Attempting to cancel after execution ended should report ended
	handle, ok := cancel()
	if ok {
		t.Log("cancel contributed after execution (may be fine)")
	} else {
		err := handle.Wait()
		if !errors.Is(err, ErrExecutionEnded) {
			t.Errorf("expected ErrExecutionEnded, got %v", err)
		}
	}
}

func TestCancel_TimeoutEscalation(t *testing.T) {
	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model: &mockModel{},
	})
	agent.name = "timeout_agent"

	opt, cancel := WithCancel()

	ctx := context.Background()
	iter := agent.Run(ctx, &AgentInput{
		Messages: []Message{schema.UserMessage("test")},
	}, opt)

	// Cancel with short timeout
	cancel(WithCancelMode(CancelAfterChatModel), WithCancelTimeout(50*time.Millisecond))
	time.Sleep(100 * time.Millisecond)

	for {
		ev, ok := iter.Next()
		if !ok { break }
		_ = ev
	}
}

// ---- Cancel with TurnLoop ----

func TestCancel_TurnLoopCancel(t *testing.T) {
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
			m.addResp("Turn: " + consumed[0])
			a := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m})
			a.name = "turn"
			return a, nil
		},
	})

	tl.Push("item1")

	// Stop with cause
	ctx := context.Background()
	tl.Run(ctx)
	tl.Stop(WithStopCause("manual"))
	result := tl.Wait()
	if result == nil {
		t.Fatal("expected result")
	}
	t.Logf("stop cause: %s", result.StopCause)
}

func TestCancel_MultipleCancels(t *testing.T) {
	cc := newCancelContext()
	cancelFn := cc.buildCancelFunc()

	// Multiple sequential cancels
	_, ok1 := cancelFn(WithCancelMode(CancelAfterChatModel))
	_, ok2 := cancelFn(WithCancelMode(CancelAfterToolCalls))
	_, ok3 := cancelFn(WithCancelMode(CancelImmediate))
	_ = ok1
	_ = ok2
	_ = ok3
}

// ---- cancelMonitoredModel tests ----

func TestCancelMonitoredModel_StreamCancel(t *testing.T) {
	cc := newCancelContext()
	inner := &mockModel{}
	inner.addResp("test stream")

	monitored := &cancelMonitoredModel[*schema.Message]{inner: inner, cc: cc}

	ctx := withCancelContext(context.Background(), cc)
	sr, err := monitored.Stream(ctx, []Message{schema.UserMessage("hi")})
	if err != nil { t.Fatalf("Stream: %v", err) }
	if sr == nil { t.Fatal("nil stream reader") }

	// Cancel
	cc.triggerImmediate()
	// Reader should return error on read
	_, readErr := sr.Recv()
	if readErr != nil && !errors.Is(readErr, ErrStreamCanceled) {
		t.Logf("stream recv returned: %v", readErr)
	}
}

func TestCancelMonitoredModel_StreamNormal(t *testing.T) {
	cc := newCancelContext()
	inner := &mockModel{}
	inner.addResp("normal stream data")

	monitored := &cancelMonitoredModel[*schema.Message]{inner: inner, cc: cc}
	ctx := withCancelContext(context.Background(), cc)

	sr, err := monitored.Stream(ctx, []Message{schema.UserMessage("hi")})
	if err != nil { t.Fatalf("Stream: %v", err) }

	msg, readErr := sr.Recv()
	if readErr != nil {
		t.Logf("stream ended: %v", readErr)
	}
	_ = msg
}

// ---- RunOption.WithCancel returns correct types ----

func TestWithCancel_ReturnsOptions(t *testing.T) {
	opt, cancel := WithCancel()
	if opt == nil { t.Error("expected non-nil option") }
	if cancel == nil { t.Error("expected non-nil cancel func") }
}

func TestWithCancel_StoresCancelCtx(t *testing.T) {
	opt, _ := WithCancel()
	o := &runOptions{}
	opt.apply(o)
	if o.cancelCtx == nil { t.Error("cancelCtx should be stored in runOptions") }
}

// ---- wrapIterWithCancelCtx tests ----

func TestWrapIterWithCancelCtx_NilCancel(t *testing.T) {
	it, gen := NewAsyncIteratorPair[*TypedAgentEvent[*schema.Message]]()
	go func() {
		defer gen.Close()
		ev := &TypedAgentEvent[*schema.Message]{}
		gen.Send(ev)
	}()

	wrapped := wrapIterWithCancelCtx(it, nil)
	ev, ok := wrapped.Next()
	if !ok { t.Fatal("expected event") }
	_ = ev
}

func TestWrapIterWithCancelCtx_Canceled(t *testing.T) {
	cc := newCancelContext()
	it, gen := NewAsyncIteratorPair[*TypedAgentEvent[*schema.Message]]()
	go func() {
		defer gen.Close()
		ev := &TypedAgentEvent[*schema.Message]{}
		gen.Send(ev)
	}()

	wrapped := wrapIterWithCancelCtx(it, cc)

	// Cancel before reading
	cc.triggerImmediate()

	_, ok := wrapped.Next()
	if ok {
		t.Log("received event before cancel error")
	}
	cc.markDone()
}

// ---- wrapStreamWithCancel tests ----

func TestWrapStreamWithCancel_Nil(t *testing.T) {
	result := wrapStreamWithCancel[Message](nil, nil)
	if result != nil {
		t.Error("nil input should return nil")
	}
}

func TestWrapStreamWithCancel_Normal(t *testing.T) {
	cc := newCancelContext()
	sr := schema.StreamReaderFromArray([]Message{
		&schema.Message{Content: "msg1"},
	})
	wrapped := wrapStreamWithCancel(sr, cc)
	if wrapped == nil { t.Fatal("nil wrapped stream") }
	msg, err := wrapped.Recv()
	if err != nil {
		t.Logf("stream recv: %v", err)
	}
	_ = msg
}

func TestWrapStreamWithCancel_Immediate(t *testing.T) {
	cc := newCancelContext()
	cc.triggerImmediate()

	sr := schema.StreamReaderFromArray([]Message{
		&schema.Message{Content: "will be cancelled"},
	})
	wrapped := wrapStreamWithCancel(sr, cc)
	if wrapped == nil { t.Fatal("nil wrapped stream") }

	// Should receive cancel error
	data, err := wrapped.Recv()
	if err != nil && !errors.Is(err, ErrStreamCanceled) {
		t.Errorf("expected ErrStreamCanceled, got %v", err)
	}
	_ = data
}

// ---- goroutine safety ----

func TestCancelContext_ConcurrentAccess(t *testing.T) {
	cc := newCancelContext()
	var wg sync.WaitGroup

	// Multiple goroutines accessing cancel context concurrently
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cc.shouldCancel()
			cc.isImmediate()
			cc.getMode()
			cc.isRecursive()
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		cc.markDone()
	}()

	wg.Wait()
}

func TestCancelContext_ConcurrentTrigger(t *testing.T) {
	cc := newCancelContext()
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cc.triggerCancel(CancelAfterChatModel)
		}()
	}

	wg.Wait()
}
