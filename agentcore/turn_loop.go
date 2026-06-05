package agentcore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// ---- TurnLoop: Push-based event loop for agent execution ----

// TurnLoopState holds the result when a TurnLoop exits.
type TurnLoopState[T any] struct {
	ExitReason       error
	UnhandledItems   []T
	InterruptedItems []T
	StopCause        string
	CheckpointSaved  bool
	CheckpointErr    error
}

// TurnContext provides per-turn context to callbacks.
type TurnContext[T any] struct {
	Loop     *TurnLoop[T]
	Consumed []T
	Stopped  <-chan struct{}
	StopCause func() string
}

// TurnLoopConfig configures a TurnLoop.
type TurnLoopConfig[T any] struct {
	// GenInput decides which items to consume vs defer. Required.
	GenInput func(ctx context.Context, loop *TurnLoop[T], items []T) (*GenInputResult[T], error)

	// PrepareAgent returns an Agent for the consumed items. Required.
	PrepareAgent func(ctx context.Context, loop *TurnLoop[T], consumed []T) (Agent, error)

	// OnAgentEvents handles events from the agent. Optional (default: drain).
	OnAgentEvents func(ctx context.Context, tc *TurnContext[T], events *AsyncIterator[*AgentEvent]) error

	// Store enables checkpoint persistence. Optional.
	Store CheckPointStore

	// CheckpointID identifies the checkpoint. Required if Store is set.
	CheckpointID string
}

// GenInputResult contains the result of GenInput processing.
type GenInputResult[T any] struct {
	Input     *AgentInput
	Consumed  []T
	Remaining []T
}

// StopOption configures a Stop call.
type StopOption func(*stopConfig)

type stopConfig struct {
	cancelOpts     []CancelOption
	skipCheckpoint bool
	stopCause      string
	idleFor        time.Duration
}

// WithGracefulStop cancels the current agent at the nearest safe point.
func WithGracefulStop() StopOption {
	return func(c *stopConfig) {
		c.cancelOpts = []CancelOption{
			WithCancelMode(CancelAfterChatModel | CancelAfterToolCalls),
			WithRecursiveCancel(),
		}
	}
}

// WithImmediateStop aborts the current agent immediately.
func WithImmediateStop() StopOption {
	return func(c *stopConfig) {
		c.cancelOpts = []CancelOption{WithRecursiveCancel()}
	}
}

// WithStopTimeout is like WithGracefulStop but escalates after timeout.
func WithStopTimeout(d time.Duration) StopOption {
	return func(c *stopConfig) {
		c.cancelOpts = []CancelOption{
			WithCancelMode(CancelAfterChatModel | CancelAfterToolCalls),
			WithRecursiveCancel(),
			WithCancelTimeout(d),
		}
	}
}

// WithSkipCheckpoint tells the loop not to persist a checkpoint on stop.
func WithSkipCheckpoint() StopOption {
	return func(c *stopConfig) { c.skipCheckpoint = true }
}

// WithStopCause attaches a reason to this stop.
func WithStopCause(cause string) StopOption {
	return func(c *stopConfig) { c.stopCause = cause }
}

// UntilIdleFor defers the stop until the loop has been idle for the duration.
func UntilIdleFor(d time.Duration) StopOption {
	return func(c *stopConfig) { c.idleFor = d }
}

// ---- TurnLoop ----

type TurnLoop[T any] struct {
	config TurnLoopConfig[T]

	buffer *turnBuffer[T]

	stopped int32
	started int32
	done    chan struct{}
	result  *TurnLoopState[T]

	runOnce sync.Once

	stopMu    sync.Mutex
	stopPhase int  // 0=open, 1=idleWaiting, 2=committed
	idleFor   time.Duration
	pendingStopCancel []CancelOption
	skipCheckpoint    bool
	stopCause         string
	stopNotify        chan struct{}

	onAgentEvents func(ctx context.Context, tc *TurnContext[T], events *AsyncIterator[*AgentEvent]) error

	interruptedItems        []T
	capturedCancelErr       *CancelError
	capturedInterruptCtxs   []*InterruptCtx

	lateMu    sync.Mutex
	lateItems []T
	lateDone  bool

	checkpointMu sync.Mutex
	runnerCheckpointBytes []byte
}

// NewTurnLoop creates a new TurnLoop. Call Run() to start processing.
func NewTurnLoop[T any](cfg TurnLoopConfig[T]) *TurnLoop[T] {
	if cfg.GenInput == nil { panic("TurnLoop: GenInput is required") }
	if cfg.PrepareAgent == nil { panic("TurnLoop: PrepareAgent is required") }

	l := &TurnLoop[T]{
		config:     cfg,
		buffer:     newTurnBuffer[T](),
		done:       make(chan struct{}),
		stopNotify: make(chan struct{}, 1),
	}
	if cfg.OnAgentEvents != nil {
		l.onAgentEvents = cfg.OnAgentEvents
	} else {
		l.onAgentEvents = func(_ context.Context, _ *TurnContext[T], events *AsyncIterator[*AgentEvent]) error {
			for { ev, ok := events.Next(); if !ok { break }; if ev.Err != nil { return ev.Err } }
			return nil
		}
	}
	return l
}

// Run starts the loop. Non-blocking; use Wait() to get the result.
func (l *TurnLoop[T]) Run(ctx context.Context) {
	l.runOnce.Do(func() {
		atomic.StoreInt32(&l.started, 1)
		go l.run(ctx)
	})
}

// Push adds an item to the buffer. Returns false if the loop has stopped.
func (l *TurnLoop[T]) Push(item T) bool {
	if atomic.LoadInt32(&l.stopped) != 0 {
		l.appendLate(item)
		return false
	}
	return l.buffer.TrySend(item)
}

// Stop signals the loop to stop. Without options, exits at turn boundary.
func (l *TurnLoop[T]) Stop(opts ...StopOption) {
	cfg := &stopConfig{}
	for _, o := range opts { o(cfg) }

	l.stopMu.Lock()
	defer l.stopMu.Unlock()

	if cfg.idleFor > 0 {
		l.idleFor = cfg.idleFor
		cfg.cancelOpts = nil // cancel opts are meaningless for idle stop
	}

	if cfg.stopCause != "" && l.stopCause == "" { l.stopCause = cfg.stopCause }
	if cfg.skipCheckpoint { l.skipCheckpoint = true }

	// Commit stop
	if l.stopPhase < 2 {
		l.stopPhase = 2
		if len(cfg.cancelOpts) > 0 {
			l.pendingStopCancel = cfg.cancelOpts
			select {
			case l.stopNotify <- struct{}{}:
			default:
			}
		}
		atomic.StoreInt32(&l.stopped, 1)
		l.buffer.Close()
	}
}

// Wait blocks until the loop exits and returns the result.
func (l *TurnLoop[T]) Wait() *TurnLoopState[T] {
	<-l.done
	return l.result
}

// ---- Internal: run loop ----

func (l *TurnLoop[T]) run(ctx context.Context) {
	l.result = &TurnLoopState[T]{} // initialize result
	defer l.cleanup(ctx)

	// Context cancellation monitor
	go func() {
		select {
		case <-ctx.Done():
			l.buffer.Close()
		case <-l.done:
		}
	}()

	for {
		if atomic.LoadInt32(&l.stopped) != 0 { return }

		// Idle-based stop
		if d := l.idleFor; d > 0 {
			first, ok := l.buffer.ReceiveTimeout(d)
			if !ok { // timed out — idle stop fires
				l.Stop()
				return
			}
			rest := l.buffer.TakeAll()
			items := append([]T{first}, rest...)
			if err := l.processTurn(ctx, items); err != nil { return }
			continue
		}

		first, ok := l.buffer.Receive()
		if !ok {
			if err := ctx.Err(); err != nil { l.result.ExitReason = err }
			return
		}
		if err := ctx.Err(); err != nil {
			l.result.ExitReason = err
			return
		}
		if atomic.LoadInt32(&l.stopped) != 0 { return }

		rest := l.buffer.TakeAll()
		items := append([]T{first}, rest...)
		if err := l.processTurn(ctx, items); err != nil { return }
	}
}

func (l *TurnLoop[T]) processTurn(ctx context.Context, items []T) error {
	if atomic.LoadInt32(&l.stopped) != 0 { return nil }

	// Plan the turn
	result, err := l.config.GenInput(ctx, l, items)
	if err != nil {
		l.result.ExitReason = err
		return err
	}
	if result == nil {
		return errors.New("GenInput returned nil result")
	}

	// Buffer remaining items for next turn
	if len(result.Remaining) > 0 {
		for _, item := range result.Remaining {
			l.buffer.TrySend(item)
		}
	}
	if atomic.LoadInt32(&l.stopped) != 0 { return nil }

	// Prepare agent
	agent, err := l.config.PrepareAgent(ctx, l, result.Consumed)
	if err != nil {
		l.result.ExitReason = err
		return err
	}
	if atomic.LoadInt32(&l.stopped) != 0 { return nil }

	// Build runner
	runnerOpt, cancelFunc := WithCancel()
	store := l.config.Store
	var runner *Runner
	if store != nil && l.config.CheckpointID != "" {
		runner = NewTypedRunner(RunnerConfig[*schema.Message]{
			Agent: agent, CheckPointStore: store,
		})
	} else {
		runner = NewTypedRunner(RunnerConfig[*schema.Message]{Agent: agent})
	}

	stopDone := make(chan struct{})
	tc := &TurnContext[T]{
		Loop: l, Consumed: result.Consumed,
		Stopped:  stopDone,
		StopCause: func() string { return l.stopCause },
	}

	// Start the agent
	var iter *AsyncIterator[*AgentEvent]
	if result.Input != nil {
		iter = runner.Run(ctx, result.Input.Messages, runnerOpt)
	} else {
		iter = runner.Run(ctx, []Message{schema.UserMessage("proceed")}, runnerOpt)
	}

	// Watch for stop/cancel during this turn
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-l.stopNotify:
				if opts := l.pendingStopCancel; len(opts) > 0 {
					cancelFunc(opts...)
					close(stopDone)
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	_ = cancelFunc // hold reference

	// Proxy events to capture framework signals
	srcIter := iter
	proxyIter, proxyGen := NewAsyncIteratorPair[*AgentEvent]()
	go func() {
		defer proxyGen.Close()
		for {
			ev, ok := srcIter.Next()
			if !ok { break }
			if ev != nil {
				if ev.Err != nil {
					var ce *CancelError
					if errors.As(ev.Err, &ce) { l.capturedCancelErr = ce }
				}
				if ev.Action != nil && ev.Action.Interrupted != nil {
					l.capturedInterruptCtxs = ev.Action.Interrupted.InterruptContexts
				}
			}
			proxyGen.Send(ev)
		}
	}()

	handleErr := l.onAgentEvents(ctx, tc, proxyIter)

	// Save checkpoint if needed
	if store != nil && l.config.CheckpointID != "" {
		if handleErr != nil {
			_, isCancel := handleErr.(*CancelError)
			_, isInterrupt := handleErr.(*InterruptError)
			if isCancel || isInterrupt || l.capturedCancelErr != nil || l.capturedInterruptCtxs != nil {
				l.interruptedItems = append([]T{}, result.Consumed...)
			}
		}
	}

	if handleErr != nil {
		// Exit the loop on error
		l.result.ExitReason = handleErr
		return handleErr
	}
	return nil
}

func (l *TurnLoop[T]) appendLate(item T) {
	l.lateMu.Lock()
	defer l.lateMu.Unlock()
	if !l.lateDone { l.lateItems = append(l.lateItems, item) }
}

func (l *TurnLoop[T]) cleanup(ctx context.Context) {
	atomic.StoreInt32(&l.stopped, 1)

	unhandled := l.buffer.TakeAll()

	l.lateMu.Lock()
	l.lateDone = true
	l.lateMu.Unlock()

	l.result = &TurnLoopState[T]{
		ExitReason:       l.result.ExitReason,
		UnhandledItems:   unhandled,
		InterruptedItems: l.interruptedItems,
		StopCause:        l.stopCause,
	}
	close(l.done)
}

// InterruptError signals a business interrupt during a turn.
type InterruptError struct {
	InterruptContexts []*InterruptCtx
}

func (e *InterruptError) Error() string {
	return fmt.Sprintf("agent interrupted: %d context(s)", len(e.InterruptContexts))
}

// ===== turnBuffer: thread-safe blocking buffer =====

type turnBuffer[T any] struct {
	mu      sync.Mutex
	cond    *sync.Cond
	buf     []T
	closed  bool
	wokenUp bool
}

func newTurnBuffer[T any]() *turnBuffer[T] {
	b := &turnBuffer[T]{}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *turnBuffer[T]) TrySend(item T) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed { return false }
	b.buf = append(b.buf, item)
	b.cond.Signal()
	return true
}

func (b *turnBuffer[T]) Receive() (T, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for len(b.buf) == 0 && !b.closed && !b.wokenUp {
		b.cond.Wait()
	}
	b.wokenUp = false
	if b.closed && len(b.buf) == 0 {
		var zero T
		return zero, false
	}
	if len(b.buf) == 0 {
		var zero T
		return zero, false
	}
	item := b.buf[0]
	b.buf = b.buf[1:]
	return item, true
}

func (b *turnBuffer[T]) ReceiveTimeout(d time.Duration) (T, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.buf) == 0 && !b.closed {
		b.mu.Unlock()
		select {
		case <-time.After(d):
			b.mu.Lock()
			if len(b.buf) == 0 {
				var zero T
				return zero, false
			}
		}
	}
	if b.closed && len(b.buf) == 0 {
		var zero T
		return zero, false
	}
	item := b.buf[0]
	b.buf = b.buf[1:]
	return item, true
}

func (b *turnBuffer[T]) TakeAll() []T {
	b.mu.Lock()
	defer b.mu.Unlock()
	items := b.buf
	b.buf = nil
	return items
}

func (b *turnBuffer[T]) PushFront(items []T) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(append([]T{}, items...), b.buf...)
	b.cond.Broadcast()
}

func (b *turnBuffer[T]) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed {
		b.closed = true
		b.cond.Broadcast()
	}
}

func (b *turnBuffer[T]) Wakeup() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.wokenUp = true
	b.cond.Signal()
}
