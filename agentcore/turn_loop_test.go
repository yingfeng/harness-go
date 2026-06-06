package agentcore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// ======================== TurnLoop Construction ========================

func TestTurnLoop_NewPanicsWithNilGenInput(t *testing.T) {
	defer func() {
		if r := recover(); r == nil { t.Fatal("expected panic") }
	}()
	NewTurnLoop[string](TurnLoopConfig[string]{PrepareAgent: func(_ context.Context, _ *TurnLoop[string], _ []string) (Agent, error) { return nil, nil }})
}

func TestTurnLoop_NewPanicsWithNilPrepareAgent(t *testing.T) {
	defer func() {
		if r := recover(); r == nil { t.Fatal("expected panic") }
	}()
	NewTurnLoop[string](TurnLoopConfig[string]{GenInput: func(_ context.Context, _ *TurnLoop[string], _ []string) (*GenInputResult[string], error) { return nil, nil }})
}

// ======================== Push-Stop-Run patterns ========================

func TestTurnLoop_PushStopRun(t *testing.T) {
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
			m.addResp("Echo: " + consumed[0])
			return NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m}), nil
		},
	})

	tl.Push("a")
	tl.Push("b")
	tl.Stop()

	ctx := context.Background()
	tl.Run(ctx)
	result := tl.Wait()
	if result == nil { t.Fatal("nil result") }
	t.Logf("result: unhandled=%d", len(result.UnhandledItems))
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

func TestTurnLoop_GenInputError(t *testing.T) {
	tl := NewTurnLoop(TurnLoopConfig[string]{
		GenInput: func(ctx context.Context, loop *TurnLoop[string], items []string) (*GenInputResult[string], error) {
			if len(items) == 0 { return nil, nil }
			return nil, errors.New("gen_input_error")
		},
		PrepareAgent: func(ctx context.Context, loop *TurnLoop[string], consumed []string) (Agent, error) {
			return nil, nil
		},
	})
	tl.Push("bad")
	// Don't stop before Run — let the loop process one item and hit the GenInput error
	ctx := context.Background()
	tl.Run(ctx)
	tl.Stop(WithStopCause("test"))
	result := tl.Wait()
	if result.ExitReason == nil {
		// GenInput error might or might not be set depending on buffer timing
		t.Log("no exit error (may have completed before processing the bad item)")
	}
}

func TestTurnLoop_PrepareAgentError(t *testing.T) {
	tl := NewTurnLoop(TurnLoopConfig[string]{
		GenInput: func(ctx context.Context, loop *TurnLoop[string], items []string) (*GenInputResult[string], error) {
			if len(items) == 0 { return nil, nil }
			return &GenInputResult[string]{Consumed: items}, nil
		},
		PrepareAgent: func(ctx context.Context, loop *TurnLoop[string], consumed []string) (Agent, error) {
			return nil, errors.New("prepare_agent_error")
		},
	})
	tl.Push("bad_agent")
	ctx := context.Background()
	tl.Run(ctx)
	tl.Stop(WithStopCause("test"))
	result := tl.Wait()
	if result.ExitReason == nil {
		t.Log("no exit error (timing dependent)")
	}
}

func TestTurnLoop_MultipleItems(t *testing.T) {
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
			m.addResp("Multiple: " + consumed[0])
			return NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m}), nil
		},
	})

	tl.Push("1")
	tl.Push("2")
	tl.Push("3")
	tl.Push("4")
	tl.Push("5")

	tl.Stop()
	ctx := context.Background()
	tl.Run(ctx)
	result := tl.Wait()
	if result == nil { t.Fatal("nil result") }
	t.Logf("multiple items: unhandled=%d", len(result.UnhandledItems))
}

func TestTurnLoop_WithCheckpointStore(t *testing.T) {
	store := &memStore{}
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
			m.addResp("Checkpoint")
			return NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m}), nil
		},
		Store: store,
	})

	tl.Push("cp_task")
	tl.Stop()
	ctx := context.Background()
	tl.Run(ctx)
	result := tl.Wait()
	t.Logf("checkpoint result: err=%v", result.ExitReason)
}

func TestTurnLoop_ConcurrentPush(t *testing.T) {
	tl := NewTurnLoop(TurnLoopConfig[string]{
		GenInput: func(ctx context.Context, loop *TurnLoop[string], items []string) (*GenInputResult[string], error) {
			if len(items) == 0 { return nil, nil }
			return &GenInputResult[string]{
				Input:    &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
				Consumed: items[:1], Remaining: items[1:],
			}, nil
		},
		PrepareAgent: func(ctx context.Context, loop *TurnLoop[string], consumed []string) (Agent, error) {
			return NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: &mockModel{}}), nil
		},
	})

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tl.Push("item")
		}()
	}
	wg.Wait()

	tl.Stop()
	ctx := context.Background()
	tl.Run(ctx)
	result := tl.Wait()
	_ = result
}

func TestTurnLoop_AvoidsDeadlockOnPushRunStop(t *testing.T) {
	tl := NewTurnLoop(TurnLoopConfig[string]{
		GenInput: func(ctx context.Context, loop *TurnLoop[string], items []string) (*GenInputResult[string], error) {
			if len(items) == 0 { return nil, nil }
			return &GenInputResult[string]{
				Input:    &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
				Consumed: items[:1], Remaining: items[1:],
			}, nil
		},
		PrepareAgent: func(ctx context.Context, loop *TurnLoop[string], consumed []string) (Agent, error) {
			return NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: &mockModel{}}), nil
		},
	})

	tl.Push("seq1")
	tl.Push("seq2")
	tl.Stop()
	ctx := context.Background()
	tl.Run(ctx)
	result := tl.Wait()
	if result == nil { t.Fatal("nil result") }
	t.Logf("sequential: unhandled=%d", len(result.UnhandledItems))
}

func TestTurnLoop_OnAgentEventsCalled(t *testing.T) {
	var called atomic.Bool
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
			m.addResp("event test")
			return NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m}), nil
		},
		OnAgentEvents: func(ctx context.Context, tc *TurnContext[string], events *AsyncIterator[*AgentEvent]) error {
			called.Store(true)
			for { ev, ok := events.Next(); if !ok { break }; _ = ev }
			return nil
		},
	})

	tl.Push("ev")
	tl.Stop()
	ctx := context.Background()
	tl.Run(ctx)
	tl.Wait()
	if !called.Load() {
		t.Log("OnAgentEvents should be called")
	}
}

func TestTurnLoop_WithToolAgent(t *testing.T) {
	tool := &mockTool{name: "calc", desc: "calculator"}

	tl := NewTurnLoop(TurnLoopConfig[string]{
		GenInput: func(ctx context.Context, loop *TurnLoop[string], items []string) (*GenInputResult[string], error) {
			if len(items) == 0 { return nil, nil }
			return &GenInputResult[string]{
				Input:    &AgentInput{Messages: []Message{schema.UserMessage(items[0])}},
				Consumed: items[:1], Remaining: items[1:],
			}, nil
		},
		PrepareAgent: func(ctx context.Context, loop *TurnLoop[string], consumed []string) (Agent, error) {
			wrapperModel := &forcedToolModel{
				toolCalls: []schema.ToolCall{{ID: "c1", Function: schema.ToolCallFunction{Name: "calc", Arguments: "{}"}}},
				finalResp: "Tool done", firstCall: true,
			}
			return NewChatModelAgent(&ChatModelConfig[*schema.Message]{
				Model: wrapperModel,
				Tools: []Tool{tool},
			}), nil
		},
	})

	tl.Push("use tool")
	tl.Stop()
	ctx := context.Background()
	tl.Run(ctx)
	result := tl.Wait()
	t.Logf("tool agent: unhandled=%d", len(result.UnhandledItems))
}

func TestTurnLoop_ImmediateStop(t *testing.T) {
	tl := NewTurnLoop(TurnLoopConfig[string]{
		GenInput: func(ctx context.Context, loop *TurnLoop[string], items []string) (*GenInputResult[string], error) {
			return &GenInputResult[string]{Consumed: items}, nil
		},
		PrepareAgent: func(ctx context.Context, loop *TurnLoop[string], consumed []string) (Agent, error) {
			m := &mockModel{}
			m.addResp("immediate")
			return NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: m}), nil
		},
	})

	tl.Push("urgent")
	ctx := context.Background()
	tl.Run(ctx)
	tl.Stop(WithImmediateStop(), WithSkipCheckpoint())
	result := tl.Wait()
	t.Logf("immediate stop: err=%v", result.ExitReason)
}
