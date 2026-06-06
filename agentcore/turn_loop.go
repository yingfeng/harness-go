package agentcore

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// ---- TurnLoop core implementation ----
//
// Configuration types (TurnLoopConfig, preemptController, stopController, etc.)
// are defined in turn_loop_config.go, turn_loop_preempt.go, and turn_loop_stop.go.

func (l *TurnLoop[T]) planTurn(
	ctx context.Context,
	isResume bool,
	items []T,
	pr *turnLoopPendingResume[T],
) (*turnPlan[T], error) {
	if !isResume {
		result, err := l.config.GenInput(ctx, l, items)
		if err != nil {
			return nil, err
		}
		if result == nil {
			return nil, errors.New("GenInputResult is nil")
		}
		if result.Input == nil {
			return nil, errors.New("agent input is nil")
		}
		turnCtx := ctx
		if result.RunCtx != nil {
			turnCtx = result.RunCtx
		}
		return &turnPlan[T]{
			turnCtx:   turnCtx,
			remaining: result.Remaining,
			spec: &turnRunSpec[T]{
				runCtx:   result.RunCtx,
				input:    result.Input,
				runOpts:  result.RunOpts,
				consumed: result.Consumed,
			},
		}, nil
	}
	if pr == nil {
		return nil, errors.New("resume payload is nil")
	}
	if l.config.GenResume == nil {
		return nil, errors.New("GenResume is required for resume")
	}
	resumeResult, err := l.config.GenResume(ctx, l, pr.interrupted, pr.unhandled, pr.newItems)
	if err != nil {
		return nil, err
	}
	if resumeResult == nil {
		return nil, errors.New("GenResumeResult is nil")
	}
	turnCtx := ctx
	if resumeResult.RunCtx != nil {
		turnCtx = resumeResult.RunCtx
	}
	return &turnPlan[T]{
		turnCtx:   turnCtx,
		remaining: resumeResult.Remaining,
		spec: &turnRunSpec[T]{
			runCtx:       resumeResult.RunCtx,
			runOpts:      resumeResult.RunOpts,
			resumeParams: resumeResult.ResumeParams,
			isResume:     true,
			consumed:     resumeResult.Consumed,
			resumeBytes:  pr.resumeBytes,
		},
	}, nil
}

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

func (l *TurnLoop[T]) appendLate(item T) {
	l.lateMu.Lock()
	defer l.lateMu.Unlock()
	if l.lateSealed {
		panic("TurnLoop: Push called after TakeLateItems")
	}
	l.lateItems = append(l.lateItems, item)
}

func marshalTurnLoopCheckpoint[T any](c *turnLoopCheckpoint[T]) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func unmarshalTurnLoopCheckpoint[T any](data []byte) (*turnLoopCheckpoint[T], error) {
	var c turnLoopCheckpoint[T]
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

func (l *TurnLoop[T]) saveTurnLoopCheckpoint(ctx context.Context, checkPointID string, c *turnLoopCheckpoint[T]) error {
	if l.config.Store == nil {
		return errors.New("checkpoint store is nil")
	}
	data, err := marshalTurnLoopCheckpoint(c)
	if err != nil {
		return err
	}
	return l.config.Store.Set(ctx, checkPointID, data)
}

func (l *TurnLoop[T]) deleteTurnLoopCheckpoint(ctx context.Context, checkPointID string) error {
	if l.config.Store == nil {
		return nil
	}
	if deleter, ok := l.config.Store.(CheckPointDeleter); ok {
		return deleter.Delete(ctx, checkPointID)
	}
	return nil
}

func (l *TurnLoop[T]) tryLoadCheckpoint(ctx context.Context) error {
	checkPointID := l.config.CheckpointID
	if checkPointID == "" || l.config.Store == nil {
		return nil
	}

	l.loadCheckpointID = checkPointID

	data, existed, err := l.config.Store.Get(ctx, checkPointID)
	if err != nil {
		return fmt.Errorf("failed to load checkpoint[%s]: %w", checkPointID, err)
	}
	if !existed {
		return nil
	}

	var cp *turnLoopCheckpoint[T]
	if len(data) == 0 {
		return nil
	}
	cp, err = unmarshalTurnLoopCheckpoint[T](data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal checkpoint[%s]: %w", checkPointID, err)
	}

	newItems := l.buffer.TakeAll()

	if cp.HasRunnerState {
		if len(cp.RunnerCheckpoint) == 0 {
			l.buffer.PushFront(newItems)
			return fmt.Errorf("checkpoint[%s] has runner state but bytes are empty", checkPointID)
		}
		l.pendingResume = &turnLoopPendingResume[T]{
			interrupted: append([]T{}, cp.CanceledItems...),
			unhandled:   append([]T{}, cp.UnhandledItems...),
			newItems:    append([]T{}, newItems...),
			resumeBytes: append([]byte{}, cp.RunnerCheckpoint...),
		}
	} else {
		items := make([]T, 0, len(cp.UnhandledItems)+len(newItems))
		items = append(items, cp.UnhandledItems...)
		items = append(items, newItems...)
		l.buffer.PushFront(items)
	}

	return nil
}

func defaultTurnLoopOnAgentEvents[T any](_ context.Context, _ *TurnContext[T], events *AsyncIterator[*AgentEvent]) error {
	for {
		event, ok := events.Next()
		if !ok {
			break
		}
		if event.Err != nil {
			return event.Err
		}
	}
	return nil
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

// Push adds an item to the loop's buffer for processing.
// Returns false if the loop has stopped. When preemptive, returns an ack channel.
func (l *TurnLoop[T]) Push(item T, opts ...PushOption[T]) (bool, <-chan struct{}) {
	cfg := &pushConfig[T]{}
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.pushStrategy != nil {
		return l.pushWithStrategy(item, cfg)
	}

	return l.pushWithConfig(item, cfg)
}

// pushWithStrategy snapshots the current target turn while the strategy decides
// how to enqueue the item.
func (l *TurnLoop[T]) pushWithStrategy(item T, cfg *pushConfig[T]) (bool, <-chan struct{}) {
	strategy := cfg.pushStrategy

	snapshot := l.preemptCtrl.beginPush()
	defer l.preemptCtrl.endPush()

	runCtx := snapshot.ctx
	if runCtx == nil {
		runCtx = context.Background()
	}
	var tc *TurnContext[T]
	if snapshot.tc != nil {
		tc = snapshot.tc.(*TurnContext[T])
	}
	realOpts := strategy(runCtx, tc)
	cfg = &pushConfig[T]{}
	for _, opt := range realOpts {
		opt(cfg)
	}
	cfg.pushStrategy = nil

	if !cfg.preempt {
		if !l.buffer.TrySend(item) {
			l.appendLate(item)
			return false, nil
		}
		return true, nil
	}

	if atomic.LoadInt32(&l.stopped) != 0 {
		l.appendLate(item)
		return false, nil
	}

	if !l.buffer.TrySend(item) {
		l.appendLate(item)
		return false, nil
	}

	ack := make(chan struct{})
	if atomic.LoadInt32(&l.started) == 0 {
		close(ack)
		return true, ack
	}

	if cfg.preemptDelay > 0 {
		go func() {
			select {
			case <-time.After(cfg.preemptDelay):
				l.preemptCtrl.requestPreempt(snapshot, ack, cfg.agentCancelOpts...)
			case <-l.done:
				close(ack)
			}
		}()
	} else {
		l.preemptCtrl.requestPreempt(snapshot, ack, cfg.agentCancelOpts...)
	}
	return true, ack
}

func (l *TurnLoop[T]) pushWithConfig(item T, cfg *pushConfig[T]) (bool, <-chan struct{}) {
	if atomic.LoadInt32(&l.stopped) != 0 {
		l.appendLate(item)
		return false, nil
	}

	if cfg.preempt {
		snapshot := l.preemptCtrl.beginPush()
		defer l.preemptCtrl.endPush()

		if !l.buffer.TrySend(item) {
			l.appendLate(item)
			return false, nil
		}

		ack := make(chan struct{})
		if atomic.LoadInt32(&l.started) == 0 {
			close(ack)
			return true, ack
		}

		if cfg.preemptDelay > 0 {
			go func() {
				select {
				case <-time.After(cfg.preemptDelay):
					l.preemptCtrl.requestPreempt(snapshot, ack, cfg.agentCancelOpts...)
				case <-l.done:
					close(ack)
				}
			}()
		} else {
			l.preemptCtrl.requestPreempt(snapshot, ack, cfg.agentCancelOpts...)
		}
		return true, ack
	}

	if !l.buffer.TrySend(item) {
		l.appendLate(item)
		return false, nil
	}
	return true, nil
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

func (l *TurnLoop[T]) run(ctx context.Context) {
	defer l.cleanup(ctx)

	if err := l.tryLoadCheckpoint(ctx); err != nil {
		l.runErr = err
		return
	}

	// Monitor context cancellation: close the buffer so that a blocking
	// Receive() unblocks.
	go func() {
		select {
		case <-ctx.Done():
			l.buffer.Close()
		case <-l.done:
		}
	}()

	for {
		if l.stopCtrl.isCommitted() {
			return
		}

		isResume := false
		var pr *turnLoopPendingResume[T]
		var items []T
		var pushBack []T

		if l.pendingResume != nil {
			isResume = true
			pr = l.pendingResume
			l.pendingResume = nil

			l.preemptCtrl.waitForPushes()
			pr.newItems = append(pr.newItems, l.buffer.TakeAll()...)

			pushBack = make([]T, 0, len(pr.interrupted)+len(pr.unhandled)+len(pr.newItems))
			pushBack = append(pushBack, pr.interrupted...)
			pushBack = append(pushBack, pr.unhandled...)
			pushBack = append(pushBack, pr.newItems...)
		} else {
			var first T
			var ok bool

			if idleFor := l.stopCtrl.idleDuration(); idleFor > 0 {
				l.buffer.ClearWakeup()
				idleTimer := time.NewTimer(idleFor)
				cancelIdle := make(chan struct{})
				go func() {
					select {
					case <-idleTimer.C:
						l.commitStop()
					case <-cancelIdle:
					}
				}()

				first, ok = l.buffer.Receive()

				idleTimer.Stop()
				close(cancelIdle)

				if !ok && !l.buffer.IsClosed() {
					continue
				}
			} else {
				first, ok = l.buffer.Receive()
				if !ok && l.stopCtrl.idleDuration() > 0 {
					continue
				}
			}

			if !ok {
				if err := ctx.Err(); err != nil {
					l.runErr = err
				}
				return
			}

			if err := ctx.Err(); err != nil {
				l.buffer.PushFront([]T{first})
				l.runErr = err
				return
			}

			if l.stopCtrl.isCommitted() {
				l.buffer.PushFront([]T{first})
				return
			}

			l.preemptCtrl.waitForPushes()
			rest := l.buffer.TakeAll()
			items = append([]T{first}, rest...)
			pushBack = items
		}

		l.preemptCtrl.beginPlanningTurn()
		abortPlanning := func() {
			l.preemptCtrl.abortPlanningTurn().ack()
		}

		plan, err := l.planTurn(ctx, isResume, items, pr)
		if err != nil {
			abortPlanning()
			if len(pushBack) > 0 {
				l.buffer.PushFront(pushBack)
			}
			l.runErr = err
			return
		}

		if l.stopCtrl.isCommitted() {
			abortPlanning()
			if len(pushBack) > 0 {
				l.buffer.PushFront(pushBack)
			}
			return
		}

		agent, err := l.config.PrepareAgent(plan.turnCtx, l, plan.spec.consumed)
		if err != nil {
			abortPlanning()
			if len(pushBack) > 0 {
				l.buffer.PushFront(pushBack)
			}
			l.runErr = err
			return
		}

		if l.stopCtrl.isCommitted() {
			abortPlanning()
			if len(pushBack) > 0 {
				l.buffer.PushFront(pushBack)
			}
			return
		}

		l.buffer.PushFront(plan.remaining)

		runErr := l.runAgentAndHandleEvents(plan.turnCtx, agent, plan.spec)

		if runErr != nil {
			if l.capturedCancelErr != nil || l.interruptContexts != nil {
				l.interruptedItems = append([]T{}, plan.spec.consumed...)
			}
			l.runErr = runErr
			return
		}

		// Business interrupt: agent produced an Interrupted action
		if l.interruptContexts != nil {
			l.interruptedItems = append([]T{}, plan.spec.consumed...)
			l.runErr = &InterruptError{InterruptContexts: l.interruptContexts}
			return
		}
	}
}

type CheckPointDeleter interface {
	Delete(ctx context.Context, key string) error
}

func (l *TurnLoop[T]) setupBridgeStore(spec *turnRunSpec[T], runOpts []RunOption) ([]RunOption, *bridgeStore, error) {
	store := l.config.Store
	if store == nil && spec.isResume {
		return nil, nil, fmt.Errorf("failed to resume agent: checkpoint store is nil")
	}
	if store == nil {
		return runOpts, nil, nil
	}
	runOpts = append(runOpts, WithCheckPointID(bridgeCheckpointID))
	if spec.isResume {
		if len(spec.resumeBytes) == 0 {
			return nil, nil, fmt.Errorf("resume checkpoint is empty")
		}
		return runOpts, newResumeBridgeStore(bridgeCheckpointID, spec.resumeBytes), nil
	}
	return runOpts, newBridgeStore(), nil
}

// watchPreempt runs for the lifetime of a single active turn.
func (l *TurnLoop[T]) watchPreempt(done <-chan struct{}, agentCancelFunc AgentCancelFunc, preemptDone chan struct{}) {
	preemptDoneClosed := false
	for {
		select {
		case <-done:
			return
		case <-l.preemptCtrl.notify:
			req, ok := l.preemptCtrl.receivePreempt()
			if !ok {
				continue
			}
			_, contributed := agentCancelFunc(req.cancelOptions(time.Now())...)
			if contributed && !preemptDoneClosed {
				close(preemptDone)
				preemptDoneClosed = true
			}
			req.ack()
		}
	}
}

// watchStop runs for the lifetime of a single active turn.
func (l *TurnLoop[T]) watchStop(done <-chan struct{}, agentCancelFunc AgentCancelFunc, stoppedDone chan struct{}) {
	stoppedClosed := false

	submit := func(req *stopCancelRequest) {
		_, contributed := agentCancelFunc(req.cancelOptions(time.Now())...)
		if contributed && !stoppedClosed {
			close(stoppedDone)
			stoppedClosed = true
		}
	}

	for {
		if req, ok := l.stopCtrl.receiveCancel(); ok {
			submit(req)
			continue
		}

		select {
		case <-done:
			return
		case <-l.stopCtrl.notify:
		}
	}
}

func (l *TurnLoop[T]) runAgentAndHandleEvents(
	ctx context.Context,
	agent Agent,
	spec *turnRunSpec[T],
) error {
	l.interruptContexts = nil
	l.capturedCancelErr = nil
	l.checkPointRunnerBytes = nil

	var iter *AsyncIterator[*AgentEvent]

	runOpts, ms, err := l.setupBridgeStore(spec, spec.runOpts)
	if err != nil {
		l.preemptCtrl.abortPlanningTurn().ack()
		return err
	}
	store := l.config.Store
	cancelOpt, agentCancelFunc := WithCancel()
	runOpts = append(runOpts, cancelOpt)

	enableStreaming := false
	if spec.input != nil {
		enableStreaming = spec.input.EnableStreaming
	}
	runner := NewRunner(ctx, RunnerConfig[*schema.Message]{
		EnableStreaming: enableStreaming,
		Agent:           agent,
		CheckPointStore: ms,
	})

	preemptDone := make(chan struct{})
	stoppedDone := make(chan struct{})

	tc := &TurnContext[T]{
		Loop:      l,
		Consumed:  spec.consumed,
		Preempted: preemptDone,
		Stopped:   stoppedDone,
		StopCause: l.stopCtrl.cause,
	}
	l.preemptCtrl.beginActiveTurn(ctx, tc)
	l.stopCtrl.beginActiveTurn()
	defer func() {
		l.stopCtrl.endActiveTurn()
		l.preemptCtrl.endActiveTurn().ack()
	}()

	if spec.isResume {
		var err error
		if spec.resumeParams != nil {
			iter, err = runner.ResumeWithParams(ctx, bridgeCheckpointID, spec.resumeParams, runOpts...)
		} else {
			iter, err = runner.Resume(ctx, bridgeCheckpointID, runOpts...)
		}
		if err != nil {
			return fmt.Errorf("failed to resume agent: %w", err)
		}
	} else {
		iter = runner.Run(ctx, spec.input.Messages, runOpts...)
	}

	// Wrap iterator to capture framework-level signals (CancelError, InterruptContexts)
	srcIter := iter
	proxyIter, proxyGen := NewAsyncIteratorPair[*AgentEvent]()
	go func() {
		defer proxyGen.Close()
		for {
			event, ok := srcIter.Next()
			if !ok {
				break
			}
			if event != nil {
				if event.Err != nil {
					var cancelErr *CancelError
					if errors.As(event.Err, &cancelErr) {
						l.capturedCancelErr = cancelErr
					}
				}
				if event.Action != nil && event.Action.Interrupted != nil {
					l.interruptContexts = event.Action.Interrupted.InterruptContexts
				}
			}
			proxyGen.Send(event)
		}
	}()
	iter = proxyIter

	handleEvents := func() error {
		return l.onAgentEvents(ctx, tc, iter)
	}

	done := make(chan struct{})
	var handleErr error

	go func() {
		defer func() {
			panicErr := recover()
			if panicErr != nil {
				handleErr = fmt.Errorf("panic in OnAgentEvents: %v\n%s", panicErr, debug.Stack())
			}
			close(done)
		}()
		handleErr = handleEvents()
	}()
	go l.watchPreempt(done, agentCancelFunc, preemptDone)
	go l.watchStop(done, agentCancelFunc, stoppedDone)

	finalizeCheckpoint := func() error {
		if store != nil && ms != nil {
			data, ok, err := ms.Get(ctx, bridgeCheckpointID)
			if err != nil {
				return fmt.Errorf("failed to read runner checkpoint: %w", err)
			}
			if ok {
				l.checkPointRunnerBytes = append([]byte{}, data...)
			}
		}
		return nil
	}

	select {
	case <-done:
		select {
		case <-preemptDone:
			return nil
		default:
		}
		if err := finalizeCheckpoint(); err != nil {
			if handleErr != nil {
				handleErr = fmt.Errorf("%w; checkpoint error: %v", handleErr, err)
			} else {
				handleErr = err
			}
		}
		return l.applyFrameworkCapturedError(handleErr)
	case <-preemptDone:
		<-done
		return nil
	case <-stoppedDone:
		<-done
		if err := finalizeCheckpoint(); err != nil {
			if handleErr != nil {
				handleErr = fmt.Errorf("%w; checkpoint error: %v", handleErr, err)
			} else {
				handleErr = err
			}
		}
		return l.applyFrameworkCapturedError(handleErr)
	}
}

func (l *TurnLoop[T]) applyFrameworkCapturedError(handleErr error) error {
	if handleErr != nil {
		return handleErr
	}
	if l.capturedCancelErr != nil {
		return l.capturedCancelErr
	}
	return nil
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
