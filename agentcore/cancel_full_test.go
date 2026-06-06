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

func assertHasCancelError(t *testing.T, events []*AgentEvent) {
	t.Helper()
	for _, e := range events { var ce *CancelError; if e.Err != nil && errors.As(e.Err, &ce) { return } }
	t.Fatal("expected CancelError in events")
}

func drainAndAssertCancelError(t *testing.T, iter *AsyncIterator[*AgentEvent]) {
	t.Helper()
	for { ev, ok := iter.Next(); if !ok { break }; var ce *CancelError; if ev.Err != nil && errors.As(ev.Err, &ce) { return } }
	t.Fatal("expected CancelError in event stream")
}

func drainEventsAndAssertCancelError(t *testing.T, iter *AsyncIterator[*AgentEvent]) []*AgentEvent {
	t.Helper()
	var events []*AgentEvent; hasCancel := false
	for { ev, ok := iter.Next(); if !ok { break }; var ce *CancelError; if ev.Err != nil && errors.As(ev.Err, &ce) { hasCancel = true }; events = append(events, ev) }
	if !hasCancel { t.Fatal("expected CancelError in event stream") }
	return events
}

func drainEventsAndHasCancel(iter *AsyncIterator[*AgentEvent]) ([]*AgentEvent, bool) {
	var events []*AgentEvent; hasCancel := false
	for { e, ok := iter.Next(); if !ok { break }; events = append(events, e); var ce *CancelError; if e.Err != nil && errors.As(e.Err, &ce) { hasCancel = true } }
	return events, hasCancel
}

type cancelTestStore struct { m map[string][]byte; mu sync.Mutex }
func newCancelTestStore() *cancelTestStore { return &cancelTestStore{m: make(map[string][]byte)} }
func (s *cancelTestStore) Set(_ context.Context, key string, value []byte) error { s.mu.Lock(); defer s.mu.Unlock(); s.m[key] = value; return nil }
func (s *cancelTestStore) Get(_ context.Context, key string) ([]byte, bool, error) { s.mu.Lock(); defer s.mu.Unlock(); v, ok := s.m[key]; return v, ok, nil }


// ======================== CancelContext State Machine ========================

func TestCancelContext_Basics(t *testing.T) {
	cc := newCancelContext()
	if cc.shouldCancel() { t.Error("not cancelled initially") }
	cc.setMode(CancelImmediate); close(cc.cancelChan)
	if !cc.shouldCancel() { t.Error("should cancel after close") }
	if cc.getMode() != CancelImmediate { t.Error("mode") }
	_ = cc.markHandled()
}

func TestCancelContext_New(t *testing.T) {
	cc := newCancelContext()
	if !cc.isRoot() { t.Error("expected root") }
}

func TestCancelContext_Lifecycle(t *testing.T) {
	cc := newCancelContext()
	cc.triggerCancel(CancelAfterChatModel)
	if !cc.shouldCancel() { t.Error("should cancel") }
	if cc.getMode() != CancelAfterChatModel { t.Error("wrong mode") }
	if cc.isImmediate() { t.Error("should not be immediate") }
}

func TestCancelContext_Immediate(t *testing.T) {
	cc := newCancelContext()
	cc.triggerImmediate()
	if !cc.shouldCancel() { t.Error("should cancel") }
	if !cc.isImmediate() { t.Error("should be immediate") }
}

func TestCancelContext_MarkDone(t *testing.T) {
	cc := newCancelContext()
	cc.markDone()
	select { case <-cc.doneChan: default: t.Fatal("doneChan not closed") }
}

func TestCancelContext_MarkHandled(t *testing.T) {
	cc := newCancelContext()
	cc.triggerCancel(CancelAfterChatModel)
	if !cc.markHandled() { t.Error("first should succeed") }
	if cc.markHandled() { t.Error("second should fail") }
}

// ---- BuildCancelFunc ----

func TestBuildCancelFunc_Immediate(t *testing.T) {
	cc := newCancelContext()
	_, ok := cc.buildCancelFunc()()
	if !ok { t.Fatal("should contribute") }
	select { case <-cc.immediateChan: case <-time.After(100 * time.Millisecond): t.Fatal("immediate not triggered") }
}

func TestBuildCancelFunc_SafePoint(t *testing.T) {
	cc := newCancelContext()
	_, ok := cc.buildCancelFunc()(WithCancelMode(CancelAfterChatModel))
	if !ok { t.Fatal("should contribute") }
	if !cc.shouldCancel() { t.Error("should cancel") }
	select { case <-cc.immediateChan: t.Fatal("should NOT be immediate"); case <-time.After(50 * time.Millisecond): }
}

func TestBuildCancelFunc_AfterDone(t *testing.T) {
	cc := newCancelContext()
	cc.markDone()
	h, ok := cc.buildCancelFunc()()
	if ok { t.Fatal("should not contribute after done") }
	if !errors.Is(h.Wait(), ErrExecutionEnded) { t.Error("expected ErrExecutionEnded") }
}

func TestBuildCancelFunc_Twice(t *testing.T) {
	cc := newCancelContext()
	cf := cc.buildCancelFunc()
	h1, ok1 := cf(WithCancelMode(CancelAfterChatModel))
	h2, ok2 := cf(WithCancelMode(CancelAfterToolCalls))
	if !ok1 || !ok2 { t.Fatal("both should contribute") }
	want := CancelAfterChatModel | CancelAfterToolCalls
	if cc.getMode() != want { t.Errorf("mode=%v want=%v", cc.getMode(), want) }
	cc.markHandled(); _ = h1.Wait(); _ = h2.Wait()
}

func TestBuildCancelFunc_TimeoutEscalation(t *testing.T) {
	cc := newCancelContext()
	h, _ := cc.buildCancelFunc()(WithCancelMode(CancelAfterChatModel), WithCancelTimeout(30*time.Millisecond))
	time.Sleep(100 * time.Millisecond)
	if !cc.isImmediate() { t.Error("should escalate") }
	cancelErr := cc.createError()
	if !cancelErr.Info.Timeout { t.Error("expected timeout flag") }
	if !cancelErr.Info.Escalated { t.Error("expected escalated") }
	cc.markHandled()
	if !errors.Is(h.Wait(), ErrCancelTimeout) { t.Error("expected ErrCancelTimeout") }
}

func TestBuildCancelFunc_StateDoneUnderLock(t *testing.T) {
	for i := 0; i < 50; i++ {
		cc := newCancelContext()
		cf := cc.buildCancelFunc()
		cc.markDone()
		h, ok := cf()
		if ok { continue }
		if !errors.Is(h.Wait(), ErrExecutionEnded) { t.Error("expected ErrExecutionEnded") }
	}
}

func TestBuildCancelFunc_CASFailStateDone(t *testing.T) {
	for i := 0; i < 10; i++ {
		cc := newCancelContext()
		cf := cc.buildCancelFunc()
		var wg sync.WaitGroup
		for j := 0; j < 100; j++ {
			wg.Add(1)
			go func() { defer wg.Done(); _, _ = cf() }()
		}
		wg.Wait()
		cc.markHandled()
	}
}

// ---- DeriveAgentToolCancelContext ----

func TestDeriveAgentToolCancelContext(t *testing.T) {
	t.Run("Shallow/DoesNotPropagateSafePoint", func(t *testing.T) {
		parent := newCancelContext()
		ctx, cancel := context.WithCancel(context.Background()); defer cancel()
		child := parent.deriveAgentToolCancelContext(ctx); defer child.markDone()
		parent.triggerCancel(CancelAfterChatModel)
		select { case <-child.cancelChan: t.Fatal("propagated"); case <-time.After(50 * time.Millisecond): }
	})
	t.Run("Shallow/ImmediateDoesNotPropagate", func(t *testing.T) {
		parent := newCancelContext()
		ctx, cancel := context.WithCancel(context.Background()); defer cancel()
		child := parent.deriveAgentToolCancelContext(ctx); defer child.markDone()
		parent.triggerImmediate()
		select { case <-child.immediateChan: t.Fatal("propagated"); case <-time.After(50 * time.Millisecond): }
	})
	t.Run("Shallow/GrandchildNoPropagation", func(t *testing.T) {
		a := newCancelContext()
		ctx, cancel := context.WithCancel(context.Background()); defer cancel()
		b := a.deriveAgentToolCancelContext(ctx)
		c := b.deriveAgentToolCancelContext(ctx)
		t.Cleanup(func() { c.markDone(); b.markDone() })
		a.triggerCancel(CancelAfterChatModel)
		select { case <-b.cancelChan: t.Fatal("b"); case <-time.After(50 * time.Millisecond): }
		select { case <-c.cancelChan: t.Fatal("c"); case <-time.After(50 * time.Millisecond): }
	})
	t.Run("Shallow/GoroutineCleanup", func(t *testing.T) {
		before := goroutineCount()
		parent := newCancelContext()
		ctx, cancel := context.WithCancel(context.Background())
		child := parent.deriveAgentToolCancelContext(ctx)
		parent.triggerCancel(CancelAfterChatModel)
		time.Sleep(100 * time.Millisecond)
		child.markDone(); cancel()
		time.Sleep(200 * time.Millisecond)
		runtime.GC(); time.Sleep(50 * time.Millisecond)
		after := goroutineCount()
		if after > before+5 { t.Errorf("goroutine leak: %d -> %d", before, after) }
	})
	t.Run("Recursive/PropagatesSafePoint", func(t *testing.T) {
		parent, child, cleanup := setupParentChild(t); defer cleanup()
		parent.setRecursive(true)
		parent.triggerCancel(CancelAfterChatModel)
		select { case <-child.cancelChan: case <-time.After(1 * time.Second): t.Fatal("child not cancelled") }
		if !child.shouldCancel() { t.Error("child should cancel") }
	})
	t.Run("Recursive/ImmediatePropagates", func(t *testing.T) {
		parent, child, cleanup := setupParentChild(t); defer cleanup()
		parent.setRecursive(true)
		parent.triggerImmediate()
		select { case <-child.immediateChan: case <-time.After(1 * time.Second): t.Fatal("child not immediate") }
		if !child.isImmediate() { t.Error("child should be immediate") }
	})
	t.Run("Recursive/GrandchildPropagation", func(t *testing.T) {
		a := newCancelContext()
		ctx, cancel := context.WithCancel(context.Background()); defer cancel()
		b := a.deriveAgentToolCancelContext(ctx)
		c := b.deriveAgentToolCancelContext(ctx)
		t.Cleanup(func() { c.markDone(); b.markDone() })
		a.setRecursive(true)
		a.triggerCancel(CancelAfterChatModel)
		select { case <-b.cancelChan: case <-time.After(1 * time.Second): t.Fatal("B not cancelled") }
		select { case <-c.cancelChan: case <-time.After(1 * time.Second): t.Fatal("C not cancelled") }
	})
	t.Run("Escalation/EscalateFromNonRecursive", func(t *testing.T) {
		parent, child, cleanup := setupParentChild(t); defer cleanup()
		parent.triggerCancel(CancelAfterChatModel)
		select { case <-child.cancelChan: t.Fatal("should not propagate"); case <-time.After(50 * time.Millisecond): }
		parent.setRecursive(true)
		select { case <-child.cancelChan: case <-time.After(1 * time.Second): t.Fatal("child not cancelled") }
	})
}

func TestDeriveAgentToolCancelContext_Race(t *testing.T) {
	t.Run("SetRecursiveConcurrentWithCancelChan", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			parent := newCancelContext()
			ctx, cancel := context.WithCancel(context.Background())
			child := parent.deriveAgentToolCancelContext(ctx)
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); parent.setRecursive(true) }()
			go func() { defer wg.Done(); parent.triggerCancel(CancelAfterChatModel) }()
			wg.Wait()
			select { case <-child.cancelChan: case <-time.After(1 * time.Second): t.Fatal("child not cancelled") }
			child.markDone(); cancel()
		}
	})
	t.Run("ChildCompletesBeforeEscalation", func(t *testing.T) {
		parent := newCancelContext()
		ctx, cancel := context.WithCancel(context.Background()); defer cancel()
		child := parent.deriveAgentToolCancelContext(ctx)
		parent.triggerCancel(CancelAfterChatModel)
		time.Sleep(50 * time.Millisecond)
		child.markDone()
		time.Sleep(50 * time.Millisecond)
		parent.setRecursive(true)
		select { case <-child.cancelChan: t.Fatal("child done"); case <-time.After(50 * time.Millisecond): }
	})
	t.Run("MultipleChildren_PartialCompletion", func(t *testing.T) {
		parent := newCancelContext()
		ctx, cancel := context.WithCancel(context.Background()); defer cancel()
		child1 := parent.deriveAgentToolCancelContext(ctx)
		child2 := parent.deriveAgentToolCancelContext(ctx)
		parent.triggerCancel(CancelAfterChatModel)
		time.Sleep(50 * time.Millisecond)
		child1.markDone()
		parent.setRecursive(true)
		select { case <-child2.cancelChan: case <-time.After(1 * time.Second): t.Fatal("child2 not cancelled") }
		child2.markDone()
	})
}

// ---- sendInterrupt ----

func TestGraphInterruptFuncs_Parallel(t *testing.T) {
	cc := newCancelContext()
	if !cc.sendInterrupt() { t.Error("first should succeed") }
	if cc.sendInterrupt() { t.Error("second should fail") }
}

// ---- TestFilterCancelOption ----

func TestFilterCancelOption(t *testing.T) {
	opt, _ := WithCancel()
	result := filterCancelOption([]RunOption{opt})
	if len(result) != 0 { t.Error("cancel option should be filtered") }
}

// ---- TestWrapIterWithMarkDone ----

func TestWrapIterWithMarkDone(t *testing.T) {
	t.Run("CancelErrorIsWrapped", func(t *testing.T) {
		cc := newCancelContext()
		cc.triggerCancel(CancelAfterChatModel)
		it, gen := NewAsyncIteratorPair[*TypedAgentEvent[*schema.Message]]()
		go func() { defer gen.Close(); gen.Send(&TypedAgentEvent[*schema.Message]{}) }()
		wrapped := wrapIterWithCancelCtx(it, cc)
		_, ok := wrapped.Next()
		cc.markHandled(); _ = ok
	})
	t.Run("WithoutCancelContext", func(t *testing.T) {
		it, gen := NewAsyncIteratorPair[*TypedAgentEvent[*schema.Message]]()
		go func() { defer gen.Close(); gen.Send(&TypedAgentEvent[*schema.Message]{}) }()
		wrapped := wrapIterWithCancelCtx(it, nil)
		if _, ok := wrapped.Next(); !ok { t.Fatal("expected event") }
	})
}

// ---- TestHandleRunFuncError_AlreadyHandled_NoDuplicate ----

func TestHandleRunFuncError_AlreadyHandled_NoDuplicate(t *testing.T) {
	cc := newCancelContext()
	cc.triggerCancel(CancelAfterChatModel)
	if !cc.markHandled() { t.Fatal("first should succeed") }
	if cc.markHandled() { t.Fatal("second should fail") }
}

// ---- TestCancel_SafePointNeverFires ----

func TestCancel_SafePointNeverFires_ErrExecutionEnded(t *testing.T) {
	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: &mockModel{}})
	agent.name = "never"
	opt, cancel := WithCancel()
	ctx := context.Background()
	iter := agent.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("hi")}}, opt)
	cancel(WithCancelMode(CancelAfterChatModel))
	for { ev, ok := iter.Next(); if !ok { break }; _ = ev }
}

// ---- TestCancelContextKey ----

func TestCancelContextKey(t *testing.T) {
	cc := newCancelContext()
	ctx := withCancelContext(context.Background(), cc)
	got := getCancelContext(ctx)
	if got == nil { t.Fatal("expected cancelContext") }
	if v := getCancelContext(context.Background()); v != nil { t.Error("expected nil") }
}

// ---- Workflow Cancel Tests ----

func TestWithCancel_SequentialAgent(t *testing.T) {
	m1 := &mockModel{}; m1.addResp("A1")
	m2 := &mockModel{}; m2.addResp("A2")
	a1 := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m1}); a1.name = "s1"
	a2 := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m2}); a2.name = "s2"
	ctx := context.Background()
	wf, err := NewSequential(ctx, &SequentialConfig{Name: "seq", Description: "test", SubAgents: []Agent{a1, a2}})
	if err != nil { t.Fatalf("NewSequential: %v", err) }
	opt, cancel := WithCancel()
	iter := wf.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("run")}}, opt)
	cancel()
	for { ev, ok := iter.Next(); if !ok { break }; _ = ev }
}

func TestWithCancel_LoopAgent(t *testing.T) {
	m1 := &mockModel{}; m1.addResp("L1")
	m2 := &mockModel{}; m2.addResp("L2")
	a1 := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m1}); a1.name = "l1"
	a2 := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m2}); a2.name = "l2"
	ctx := context.Background()
	wf, err := NewLoop(ctx, &LoopConfig{Name: "loop", Description: "test", SubAgents: []Agent{a1, a2}, MaxIterations: 5})
	if err != nil { t.Fatalf("NewLoop: %v", err) }
	opt, cancel := WithCancel()
	iter := wf.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("run")}}, opt)
	cancel()
	for { ev, ok := iter.Next(); if !ok { break }; _ = ev }
}

func TestWithCancel_ParallelAgent(t *testing.T) {
	m1 := &mockModel{}; m1.addResp("P1")
	m2 := &mockModel{}; m2.addResp("P2")
	a1 := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m1}); a1.name = "p1"
	a2 := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m2}); a2.name = "p2"
	ctx := context.Background()
	wf, err := NewParallel(ctx, &ParallelConfig{Name: "par", Description: "test", SubAgents: []Agent{a1, a2}})
	if err != nil { t.Fatalf("NewParallel: %v", err) }
	opt, cancel := WithCancel()
	iter := wf.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("run")}}, opt)
	cancel()
	for { ev, ok := iter.Next(); if !ok { break }; _ = ev }
}

func TestCheckCancel_Sequential_BetweenSubAgents(t *testing.T) {
	m1 := &mockModel{}; m1.addResp("X1")
	m2 := &mockModel{}; m2.addResp("X2")
	a1 := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m1}); a1.name = "x1"
	a2 := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m2}); a2.name = "x2"
	ctx := context.Background()
	wf, _ := NewSequential(ctx, &SequentialConfig{Name: "chk", Description: "test", SubAgents: []Agent{a1, a2}})
	opt, cancel := WithCancel()
	iter := wf.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("run")}}, opt)
	cancel(WithCancelMode(CancelAfterChatModel))
	for { ev, ok := iter.Next(); if !ok { break }; _ = ev }
}

func TestCheckCancel_Loop_BetweenIterations(t *testing.T) {
	m1 := &mockModel{}; m1.addResp("Y1")
	m2 := &mockModel{}; m2.addResp("Y2")
	a1 := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m1}); a1.name = "y1"
	a2 := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m2}); a2.name = "y2"
	ctx := context.Background()
	wf, _ := NewLoop(ctx, &LoopConfig{Name: "chk_loop", Description: "test", SubAgents: []Agent{a1, a2}, MaxIterations: 5})
	opt, cancel := WithCancel()
	iter := wf.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("run")}}, opt)
	cancel(WithCancelMode(CancelAfterChatModel))
	for { ev, ok := iter.Next(); if !ok { break }; _ = ev }
}

func TestCheckCancel_Parallel_PreSpawn(t *testing.T) {
	m1 := &mockModel{}; m1.addResp("Z1")
	m2 := &mockModel{}; m2.addResp("Z2")
	a1 := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m1}); a1.name = "z1"
	a2 := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m2}); a2.name = "z2"
	ctx := context.Background()
	wf, _ := NewParallel(ctx, &ParallelConfig{Name: "chk_par", Description: "test", SubAgents: []Agent{a1, a2}})
	opt, cancel := WithCancel()
	iter := wf.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("run")}}, opt)
	cancel(WithCancelMode(CancelAfterChatModel))
	for { ev, ok := iter.Next(); if !ok { break }; _ = ev }
}

// ---- Cancel after completion ----

func TestWithCancel_AfterCompletion(t *testing.T) {
	model := &mockModel{}; model.addResp("done")
	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: model})
	agent.name = "after"
	opt, cancel := WithCancel()
	ctx := context.Background()
	iter := agent.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("hi")}}, opt)
	for { ev, ok := iter.Next(); if !ok { break }; _ = ev }
	h, ok := cancel()
	if !ok {
		if !errors.Is(h.Wait(), ErrExecutionEnded) { t.Error("expected ErrExecutionEnded") }
	}
}

func TestCancelMonitoredToolHandler_StreamableToolCall(t *testing.T) {
	t.Run("NilContext", func(t *testing.T) {
		handler := &cancelMonitoredToolHandler{}
		wrapped := handler.WrapToolInvoke(func(ctx context.Context, args string, opts ...ToolOption) (string, error) { return "ok", nil })
		r, err := wrapped(context.Background(), "{}")
		if err != nil { t.Fatalf("err: %v", err) }
		if r != "ok" { t.Errorf("got %q", r) }
	})
	t.Run("ImmediateCancel", func(t *testing.T) {
		handler := &cancelMonitoredToolHandler{}
		wrapped := handler.WrapToolInvoke(func(ctx context.Context, args string, opts ...ToolOption) (string, error) { return "no", nil })
		cc := newCancelContext(); cc.triggerImmediate()
		_, err := wrapped(withCancelContext(context.Background(), cc), "{}")
		if err == nil { t.Fatal("expected error") }
	})
}

// ---- Helpers ----

func goroutineCount() int { n := runtime.NumGoroutine(); return n }

func setupParentChild(t *testing.T) (parent, child *cancelContext, cleanup func()) {
	parent = newCancelContext()
	ctx, cancel := context.WithCancel(context.Background())
	child = parent.deriveAgentToolCancelContext(ctx)
	return parent, child, func() { child.markDone(); cancel() }
}
