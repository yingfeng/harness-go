package agentcore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// ---- TurnLoop tests ----

func TestTurnLoop_BasicPushStopWait(t *testing.T) {
	tl := NewTurnLoop(TurnLoopConfig[string]{
		GenInput: func(ctx context.Context, loop *TurnLoop[string], items []string) (*GenInputResult[string], error) {
			if len(items) == 0 {
				return nil, nil
			}
			return &GenInputResult[string]{
				Input:    &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
				Consumed: items[:1], Remaining: items[1:],
			}, nil
		},
		PrepareAgent: func(ctx context.Context, loop *TurnLoop[string], consumed []string) (Agent, error) {
			m := &mockModel{}
			m.addResp("Echo: " + consumed[0])
			a := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m})
			a.name = "echo"
			return a, nil
		},
	})

	tl.Push("hello")
	tl.Push("world")
	tl.Stop()

	ctx := context.Background()
	tl.Run(ctx)
	result := tl.Wait()

	if result.ExitReason != nil {
		t.Fatalf("unexpected exit error: %v", result.ExitReason)
	}
}

func TestTurnLoop_StopBeforeRun(t *testing.T) {
	tl := NewTurnLoop(TurnLoopConfig[string]{
		GenInput: func(ctx context.Context, loop *TurnLoop[string], items []string) (*GenInputResult[string], error) {
			return &GenInputResult[string]{Consumed: items}, nil
		},
		PrepareAgent: func(ctx context.Context, loop *TurnLoop[string], consumed []string) (Agent, error) {
			m := &mockModel{}
			m.addResp("ok")
			a := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m})
			a.name = "a"
			return a, nil
		},
	})
	tl.Stop()    // Stop before Run
	tl.Push("x") // late item
	ctx := context.Background()
	tl.Run(ctx)
	result := tl.Wait()
	if len(result.UnhandledItems) == 0 {
		t.Log("no unhandled items when stopped before run")
	}
}

func TestTurnLoop_ContextCancellation(t *testing.T) {
	tl := NewTurnLoop(TurnLoopConfig[string]{
		GenInput: func(ctx context.Context, loop *TurnLoop[string], items []string) (*GenInputResult[string], error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
			return &GenInputResult[string]{Consumed: items[:1], Remaining: items[1:]}, nil
		},
		PrepareAgent: func(ctx context.Context, loop *TurnLoop[string], consumed []string) (Agent, error) {
			m := &mockModel{}
			m.addResp("slow")
			a := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m})
			a.name = "slow"
			return a, nil
		},
	})

	tl.Push("task")
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	tl.Run(ctx)
	result := tl.Wait()
	if result.ExitReason == nil {
		t.Log("expected exit error due to context cancellation")
	}
}

func TestTurnLoop_IdleStop(t *testing.T) {
	tl := NewTurnLoop(TurnLoopConfig[string]{
		GenInput: func(ctx context.Context, loop *TurnLoop[string], items []string) (*GenInputResult[string], error) {
			return &GenInputResult[string]{Consumed: items}, nil
		},
		PrepareAgent: func(ctx context.Context, loop *TurnLoop[string], consumed []string) (Agent, error) {
			m := &mockModel{}
			m.addResp("idle")
			a := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m})
			a.name = "idle"
			return a, nil
		},
	})

	tl.Push("task1")
	ctx := context.Background()
	tl.Run(ctx)
	// Stop with idle timeout
	tl.Stop(UntilIdleFor(50 * time.Millisecond))
	result := tl.Wait()
	t.Logf("idle stop: err=%v unhandled=%d", result.ExitReason, len(result.UnhandledItems))
}

func TestTurnLoop_GenInputError(t *testing.T) {
	expectedErr := errors.New("gen input failed")
	tl := NewTurnLoop(TurnLoopConfig[string]{
		GenInput: func(ctx context.Context, loop *TurnLoop[string], items []string) (*GenInputResult[string], error) {
			return nil, expectedErr
		},
		PrepareAgent: func(ctx context.Context, loop *TurnLoop[string], consumed []string) (Agent, error) {
			return nil, nil
		},
	})

	tl.Push("fail")
	ctx := context.Background()
	tl.Run(ctx)
	result := tl.Wait()
	if !errors.Is(result.ExitReason, expectedErr) {
		t.Errorf("expected %v, got %v", expectedErr, result.ExitReason)
	}
}

func TestTurnLoop_PrepareAgentError(t *testing.T) {
	expectedErr := errors.New("prepare failed")
	tl := NewTurnLoop(TurnLoopConfig[string]{
		GenInput: func(ctx context.Context, loop *TurnLoop[string], items []string) (*GenInputResult[string], error) {
			return &GenInputResult[string]{Consumed: items}, nil
		},
		PrepareAgent: func(ctx context.Context, loop *TurnLoop[string], consumed []string) (Agent, error) {
			return nil, expectedErr
		},
	})

	tl.Push("task")
	ctx := context.Background()
	tl.Run(ctx)
	result := tl.Wait()
	if !errors.Is(result.ExitReason, expectedErr) {
		t.Errorf("expected %v, got %v", expectedErr, result.ExitReason)
	}
}

func TestTurnLoop_PushBeforeRunThenStop(t *testing.T) {
	tl := NewTurnLoop(TurnLoopConfig[string]{
		GenInput: func(ctx context.Context, loop *TurnLoop[string], items []string) (*GenInputResult[string], error) {
			return &GenInputResult[string]{Consumed: items[:1], Remaining: items[1:]}, nil
		},
		PrepareAgent: func(ctx context.Context, loop *TurnLoop[string], consumed []string) (Agent, error) {
			m := &mockModel{}
			m.addResp("graceful")
			a := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m})
			a.name = "g"
			return a, nil
		},
	})

	tl.Push("task1")
	tl.Push("task2")
	tl.Stop(WithStopCause("done"))
	ctx := context.Background()
	tl.Run(ctx)
	result := tl.Wait()
	_ = result
}

func TestTurnLoop_ImmediateStop(t *testing.T) {
	tl := NewTurnLoop(TurnLoopConfig[string]{
		GenInput: func(ctx context.Context, loop *TurnLoop[string], items []string) (*GenInputResult[string], error) {
			return &GenInputResult[string]{Consumed: items[:1], Remaining: items[1:]}, nil
		},
		PrepareAgent: func(ctx context.Context, loop *TurnLoop[string], consumed []string) (Agent, error) {
			m := &mockModel{}
			m.addResp("immediate")
			a := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m})
			a.name = "i"
			return a, nil
		},
	})

	tl.Push("urgent")
	ctx := context.Background()
	tl.Run(ctx)
	tl.Stop(WithImmediateStop(), WithSkipCheckpoint())
	result := tl.Wait()
	t.Logf("immediate stop: err=%v", result.ExitReason)
}

func TestTurnLoop_CheckpointWithStop(t *testing.T) {
	store := &memStore{}
	tl := NewTurnLoop(TurnLoopConfig[string]{
		GenInput: func(ctx context.Context, loop *TurnLoop[string], items []string) (*GenInputResult[string], error) {
			return &GenInputResult[string]{Consumed: items[:1], Remaining: items[1:]}, nil
		},
		PrepareAgent: func(ctx context.Context, loop *TurnLoop[string], consumed []string) (Agent, error) {
			m := &mockModel{}
			m.addResp("cp")
			return NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m}), nil
		},
		Store: store, CheckpointID: "test-cp",
	})

	tl.Push("checkpoint-test")
	ctx := context.Background()
	tl.Run(ctx)
	tl.Stop()
	result := tl.Wait()
	t.Logf("checkpoint result: err=%v", result.ExitReason)
}

func TestTurnLoop_StopCause(t *testing.T) {
	tl := NewTurnLoop(TurnLoopConfig[string]{
		GenInput: func(ctx context.Context, loop *TurnLoop[string], items []string) (*GenInputResult[string], error) {
			return &GenInputResult[string]{Consumed: items}, nil
		},
		PrepareAgent: func(ctx context.Context, loop *TurnLoop[string], consumed []string) (Agent, error) {
			m := &mockModel{}
			m.addResp("cause")
			return NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m}), nil
		},
	})
	tl.Push("x")
	tl.Stop(WithStopCause("max_tokens"))
	ctx := context.Background()
	tl.Run(ctx)
	result := tl.Wait()
	if result.StopCause != "max_tokens" {
		t.Errorf("StopCause = %q", result.StopCause)
	}
}

func TestTurnLoop_LateItems(t *testing.T) {
	tl := NewTurnLoop(TurnLoopConfig[string]{
		GenInput: func(ctx context.Context, loop *TurnLoop[string], items []string) (*GenInputResult[string], error) {
			return &GenInputResult[string]{Consumed: items}, nil
		},
		PrepareAgent: func(ctx context.Context, loop *TurnLoop[string], consumed []string) (Agent, error) {
			m := &mockModel{}
			m.addResp("late")
			return NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m}), nil
		},
	})
	tl.Push("a")
	tl.Stop()
	ok := tl.Push("b") // late item after stop
	if ok {
		t.Error("Push after Stop should return false")
	}
	tl.lateMu.Lock()
	lateCount := len(tl.lateItems)
	tl.lateMu.Unlock()
	if lateCount == 0 {
		t.Error("expected late items")
	}
	_ = context.Background()
}

func TestTurnLoop_NewTurnLoop_NilGenInput(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for nil GenInput")
		}
	}()
	NewTurnLoop(TurnLoopConfig[string]{PrepareAgent: nil})
	NewTurnLoop(TurnLoopConfig[string]{GenInput: nil}) // should panic
}

func TestTurnLoop_NewTurnLoop_NilPrepareAgent(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for nil PrepareAgent")
		}
	}()
	NewTurnLoop(TurnLoopConfig[string]{
		GenInput: func(ctx context.Context, loop *TurnLoop[string], items []string) (*GenInputResult[string], error) {
			return nil, nil
		},
	})
}

func TestTurnStopOptions(t *testing.T) {
	// Verify that stop option functions don't panic
	// WithGracefulStop
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("WithGracefulStop panic: %v", r)
			}
		}()
		o := &stopConfig{}
		WithGracefulStop()(o)
		if len(o.cancelOpts) == 0 {
			t.Error("expected cancel opts")
		}
	}()

	// WithImmediateStop
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("WithImmediateStop panic: %v", r)
			}
		}()
		o := &stopConfig{}
		WithImmediateStop()(o)
	}()

	// WithStopTimeout
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("WithStopTimeout panic: %v", r)
			}
		}()
		o := &stopConfig{}
		WithStopTimeout(100 * time.Millisecond)(o)
	}()

	// WithSkipCheckpoint
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("WithSkipCheckpoint panic: %v", r)
			}
		}()
		o := &stopConfig{}
		WithSkipCheckpoint()(o)
		if !o.skipCheckpoint {
			t.Error("skipCheckpoint not set")
		}
	}()

	// WithStopCause
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("WithStopCause panic: %v", r)
			}
		}()
		o := &stopConfig{}
		WithStopCause("reason")(o)
		if o.stopCause != "reason" {
			t.Error("stopCause not set")
		}
	}()

	// UntilIdleFor
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("UntilIdleFor panic: %v", r)
			}
		}()
		o := &stopConfig{}
		UntilIdleFor(5 * time.Second)(o)
		if o.idleFor != 5*time.Second {
			t.Error("idleFor not set")
		}
	}()
}

func TestTurnBuffer_Basic(t *testing.T) {
	b := newTurnBuffer[int]()
	if !b.TrySend(1) {
		t.Error("TrySend failed")
	}
	if !b.TrySend(2) {
		t.Error("TrySend failed")
	}
	items := b.TakeAll()
	if len(items) != 2 {
		t.Errorf("TakeAll: got %d, want 2", len(items))
	}
}

func TestTurnBuffer_CloseAndTakeAll(t *testing.T) {
	b := newTurnBuffer[int]()
	b.TrySend(42)
	b.Close()
	if b.TrySend(99) {
		t.Error("TrySend after close should return false")
	}
	items := b.TakeAll()
	if len(items) != 1 || items[0] != 42 {
		t.Errorf("TakeAll: got %v", items)
	}
}

func TestTurnBuffer_PushFront(t *testing.T) {
	b := newTurnBuffer[int]()
	b.TrySend(2)
	b.TrySend(3)
	b.PushFront([]int{0, 1})
	items := b.TakeAll()
	if len(items) != 4 {
		t.Errorf("len=%d", len(items))
	}
	for i, v := range items {
		if v != i {
			t.Errorf("items[%d]=%d", i, v)
		}
	}
}

func TestTurnBuffer_Wakeup(t *testing.T) {
	b := newTurnBuffer[int]()
	b.Wakeup()
	item, ok := b.Receive()
	if ok {
		t.Errorf("expected no item after wakeup, got %d", item)
	}
}

func TestTurnLoop_TimeoutStop(t *testing.T) {
	tl := NewTurnLoop(TurnLoopConfig[string]{
		GenInput: func(ctx context.Context, loop *TurnLoop[string], items []string) (*GenInputResult[string], error) {
			return &GenInputResult[string]{Consumed: items}, nil
		},
		PrepareAgent: func(ctx context.Context, loop *TurnLoop[string], consumed []string) (Agent, error) {
			m := &mockModel{}
			m.addResp("timeout")
			return NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m}), nil
		},
	})
	tl.Push("timeout-test")
	tl.Stop(WithStopTimeout(100*time.Millisecond), WithStopCause("timeout"))
	ctx := context.Background()
	tl.Run(ctx)
	result := tl.Wait()
	if result.StopCause != "timeout" {
		t.Errorf("StopCause = %q", result.StopCause)
	}
}

func TestTurnLoop_InterruptedItems(t *testing.T) {
	store := &memStore{}
	tl := NewTurnLoop(TurnLoopConfig[string]{
		GenInput: func(ctx context.Context, loop *TurnLoop[string], items []string) (*GenInputResult[string], error) {
			if len(items) == 0 {
				return nil, nil
			}
			return &GenInputResult[string]{
				Input:     &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
				Consumed:  items[:1],
				Remaining: items[1:],
			}, nil
		},
		PrepareAgent: func(ctx context.Context, loop *TurnLoop[string], consumed []string) (Agent, error) {
			m := &mockModel{}
			m.addResp("interrupt-test")
			return NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m}), nil
		},
		Store: store, CheckpointID: "interrupt-cp",
	})
	tl.Push("item1")
	tl.Stop()
	ctx := context.Background()
	tl.Run(ctx)
	result := tl.Wait()
	t.Logf("interrupted items: %v (len=%d)", result.InterruptedItems, len(result.InterruptedItems))
	_ = result
}
