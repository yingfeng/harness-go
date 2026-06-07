package agentcore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// ---- TurnLoop core: struct, lifecycle, cleanup ----
//
// Configuration types (TurnLoopConfig, preemptController, stopController, etc.)
// are defined in turn_loop_config.go, turn_loop_preempt.go, and turn_loop_stop.go.
// Execution logic is split into:
//   - turn_loop_run.go     (planTurn, run, defaultTurnLoopOnAgentEvents)
//   - turn_loop_agent.go   (runAgentAndHandleEvents, watchPreempt, watchStop, setupBridgeStore)
//   - turn_loop_push.go    (Push, pushWithStrategy, pushWithConfig, appendLate)
//   - turn_loop_checkpoint.go (checkpoint serialization, tryLoadCheckpoint)

// TurnLoop executes agent turns in a push-based loop.
// See TurnLoopConfig for configuration details and TurnLoopState for results.
type TurnLoop[T any] struct {
	config TurnLoopConfig[T]

	buffer *turnBuffer[T]

	stopped int32
	started int32

	done chan struct{}

	result *TurnLoopState[T]

	runOnce sync.Once

	stopCtrl *stopController

	preemptCtrl *preemptController

	runErr error

	interruptedItems []T

	checkPointRunnerBytes []byte
	interruptContexts     []*InterruptCtx
	capturedCancelErr     *CancelError

	pendingResume *turnLoopPendingResume[T]

	loadCheckpointID string

	onAgentEvents func(ctx context.Context, tc *TurnContext[T], events *AsyncIterator[*AgentEvent]) error

	lateMu     sync.Mutex
	lateItems  []T
	lateSealed bool
}

// NewTurnLoop creates a new TurnLoop without starting it.
func NewTurnLoop[T any](cfg TurnLoopConfig[T]) *TurnLoop[T] {
	if cfg.GenInput == nil {
		panic("agentcore: NewTurnLoop: GenInput is required")
	}
	if cfg.PrepareAgent == nil {
		panic("agentcore: NewTurnLoop: PrepareAgent is required")
	}

	l := &TurnLoop[T]{
		config:      cfg,
		buffer:      newTurnBuffer[T](),
		done:        make(chan struct{}),
		stopCtrl:    newStopController(),
		preemptCtrl: newPreemptController(),
	}
	if cfg.OnAgentEvents != nil {
		l.onAgentEvents = cfg.OnAgentEvents
	} else {
		l.onAgentEvents = defaultTurnLoopOnAgentEvents[T]
	}
	return l
}

func (l *TurnLoop[T]) start(ctx context.Context) {
	l.runOnce.Do(func() {
		atomic.StoreInt32(&l.started, 1)
		go l.run(ctx)
	})
}

// Run starts the loop's processing goroutine. It is non-blocking.
func (l *TurnLoop[T]) Run(ctx context.Context) {
	l.start(ctx)
}

// Stop signals the loop to stop and returns immediately (non-blocking).
func (l *TurnLoop[T]) Stop(opts ...StopOption) {
	cfg := &stopConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.idleFor > 0 {
		cfg.agentCancelOpts = nil
	}

	decision := l.stopCtrl.requestStop(cfg)
	if decision.wakeIdle {
		l.buffer.Wakeup()
	}
	if decision.commit {
		l.finishStopCommit()
	}

	// If a stop timeout is configured, force-stop after the timeout
	if cfg.timeout != nil && *cfg.timeout > 0 {
		go func() {
			select {
			case <-time.After(*cfg.timeout):
				l.commitStop()
			case <-l.done:
			}
		}()
	}
}

func (l *TurnLoop[T]) commitStop() {
	if !l.stopCtrl.commit() {
		return
	}
	l.finishStopCommit()
}

func (l *TurnLoop[T]) finishStopCommit() {
	atomic.StoreInt32(&l.stopped, 1)
	l.buffer.Close()
}

// Wait blocks until the loop exits and returns the result.
func (l *TurnLoop[T]) Wait() *TurnLoopState[T] {
	<-l.done
	return l.result
}

func (l *TurnLoop[T]) cleanup(ctx context.Context) {
	atomic.StoreInt32(&l.stopped, 1)

	unhandled := l.buffer.TakeAll()
	checkpointID := l.config.CheckpointID
	isIdle := len(l.checkPointRunnerBytes) == 0 && len(unhandled) == 0 && len(l.interruptedItems) == 0

	exitCausedByStop := l.runErr == nil || errors.As(l.runErr, new(*CancelError)) || l.capturedCancelErr != nil
	businessInterrupt := errors.As(l.runErr, new(*InterruptError)) || l.interruptContexts != nil
	shouldSaveCheckpoint := l.config.Store != nil && checkpointID != "" &&
		((l.stopCtrl.isCommitted() && exitCausedByStop) || businessInterrupt) &&
		!isIdle && !l.stopCtrl.skipCheckpointEnabled()

	var checkpointed bool
	var checkpointErr error

	if shouldSaveCheckpoint {
		cp := &turnLoopCheckpoint[T]{
			RunnerCheckpoint: l.checkPointRunnerBytes,
			HasRunnerState:   len(l.checkPointRunnerBytes) > 0,
			UnhandledItems:   unhandled,
			CanceledItems:    l.interruptedItems,
		}
		checkpointed = true
		checkpointErr = l.saveTurnLoopCheckpoint(ctx, checkpointID, cp)
	} else if l.loadCheckpointID != "" {
		_ = l.deleteTurnLoopCheckpoint(ctx, l.loadCheckpointID)
	}

	var takeLateOnce sync.Once
	var takeLateResult []T

	l.result = &TurnLoopState[T]{
		ExitReason:          l.runErr,
		UnhandledItems:      unhandled,
		InterruptedItems:    l.interruptedItems,
		StopCause:           l.stopCtrl.cause(),
		CheckpointAttempted: checkpointed,
		CheckpointErr:       checkpointErr,
		TakeLateItems: func() []T {
			takeLateOnce.Do(func() {
				l.lateMu.Lock()
				takeLateResult = append([]T{}, l.lateItems...)
				l.lateSealed = true
				l.lateMu.Unlock()
			})
			return takeLateResult
		},
	}

	l.stopCtrl.closeForLoopExit()
	l.preemptCtrl.closeForLoopExit()
	l.buffer.Close()
	close(l.done)
}
