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

type stopPhase uint8

const (
	stopOpen stopPhase = iota
	stopIdleWaiting
	stopCommitted
)

type preemptTurnPhase uint8

const (
	preemptTurnIdle preemptTurnPhase = iota
	preemptTurnPlanning
	preemptTurnActive
)

func (p preemptTurnPhase) String() string {
	switch p {
	case preemptTurnIdle:
		return "idle"
	case preemptTurnPlanning:
		return "planning"
	case preemptTurnActive:
		return "active"
	default:
		return fmt.Sprintf("unknown(%d)", p)
	}
}

type preemptTurnSnapshot struct {
	hasTargetTurn bool
	turnID        uint64
	ctx           context.Context
	tc            any
}

type cancelRequestState struct {
	cfg             cancelConfig
	timeoutDeadline *time.Time
}

type preemptRequest struct {
	cancel   cancelRequestState
	ackChans []chan struct{}
}

func parseCancelOptions(opts ...CancelOption) cancelConfig {
	cfg := cancelConfig{Mode: CancelImmediate}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

func newCancelRequestState(opts []CancelOption, now time.Time) cancelRequestState {
	cfg := parseCancelOptions(opts...)
	var deadline *time.Time
	if cfg.Timeout != nil && *cfg.Timeout > 0 && cfg.Mode != CancelImmediate {
		d := now.Add(*cfg.Timeout)
		deadline = &d
	}
	cfg.Timeout = nil

	return cancelRequestState{
		cfg:             cfg,
		timeoutDeadline: deadline,
	}
}

func (s *cancelRequestState) merge(opts []CancelOption, now time.Time) {
	if opts == nil {
		return
	}

	next := newCancelRequestState(opts, now)
	if s.cfg.Mode == CancelImmediate || next.cfg.Mode == CancelImmediate {
		s.cfg.Mode = CancelImmediate
		s.timeoutDeadline = nil
	} else {
		s.cfg.Mode |= next.cfg.Mode
		if next.timeoutDeadline != nil {
			if s.timeoutDeadline == nil || next.timeoutDeadline.Before(*s.timeoutDeadline) {
				deadline := *next.timeoutDeadline
				s.timeoutDeadline = &deadline
			}
		}
	}
	if next.cfg.Recursive {
		s.cfg.Recursive = true
	}
}

func (s cancelRequestState) cancelOptions(now time.Time) []CancelOption {
	cfg := s.cfg
	if cfg.Mode != CancelImmediate && s.timeoutDeadline != nil {
		remaining := s.timeoutDeadline.Sub(now)
		if remaining <= 0 {
			cfg.Mode = CancelImmediate
			cfg.Timeout = nil
		} else {
			cfg.Timeout = &remaining
		}
	}

	opts := []CancelOption{WithCancelMode(cfg.Mode)}
	if cfg.Recursive {
		opts = append(opts, WithRecursiveCancel())
	}
	if cfg.Timeout != nil {
		opts = append(opts, WithCancelTimeout(*cfg.Timeout))
	}
	return opts
}

func newPreemptRequest(ack chan struct{}, opts []CancelOption, now time.Time) *preemptRequest {
	req := &preemptRequest{cancel: newCancelRequestState(opts, now)}
	if ack != nil {
		req.ackChans = append(req.ackChans, ack)
	}
	return req
}

func (r *preemptRequest) ack() {
	if r == nil {
		return
	}
	for _, ack := range r.ackChans {
		close(ack)
	}
	r.ackChans = nil
}

func (r *preemptRequest) merge(ack chan struct{}, opts []CancelOption, now time.Time) {
	if ack != nil {
		r.ackChans = append(r.ackChans, ack)
	}
	r.cancel.merge(opts, now)
}

func (r *preemptRequest) cancelOptions(now time.Time) []CancelOption {
	if r == nil {
		return nil
	}
	return r.cancel.cancelOptions(now)
}

// preemptController owns turn-targeted preempt requests and Push critical sections.
//
// Turn lifecycle:
//
//	idle ──beginPlanningTurn──▶ planning ──beginActiveTurn──▶ active ──endActiveTurn──▶ idle
//	                              │                                                      ▲
//	                              └────────abortPlanningTurn─────────────────────────────┘
//
// Push critical section (beginPush/endPush) overlaps with the turn lifecycle. The
// run loop calls waitForPushes before beginPlanningTurn to ensure no in-flight Push
// can observe stale turn state.
//
// Preempt request flow:
//   - Push captures a snapshot (turnID + hasTargetTurn) via beginPush.
//   - requestPreempt binds to the captured turnID; if the turn has moved on, the
//     request is resolved as a no-op.
//   - During active phase, receivePreempt transfers the pending request to the
//     watcher, which submits cancel and then acks.
type preemptController struct {
	mu   sync.Mutex
	cond *sync.Cond

	turnPhase     preemptTurnPhase
	turnID        uint64
	currentTC     any
	currentRunCtx context.Context

	pushInFlight int
	pending      *preemptRequest
	notify       chan struct{}
	closed       bool
}

func newPreemptController() *preemptController {
	c := &preemptController{notify: make(chan struct{}, 1)}
	c.cond = sync.NewCond(&c.mu)
	return c
}

func (c *preemptController) beginPlanningTurn() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.requirePhaseLocked(preemptTurnIdle, "beginPlanningTurn")
	c.requireNoPendingLocked("beginPlanningTurn")
	c.turnID++
	c.turnPhase = preemptTurnPlanning
	c.currentRunCtx = nil
	c.currentTC = nil
}

func (c *preemptController) abortPlanningTurn() *preemptRequest {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.requirePhaseLocked(preemptTurnPlanning, "abortPlanningTurn")
	c.turnPhase = preemptTurnIdle
	c.currentRunCtx = nil
	c.currentTC = nil
	req := c.pending
	c.pending = nil
	c.cond.Broadcast()
	return req
}

func (c *preemptController) beginActiveTurn(ctx context.Context, tc any) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.requirePhaseLocked(preemptTurnPlanning, "beginActiveTurn")
	c.turnPhase = preemptTurnActive
	c.currentRunCtx = ctx
	c.currentTC = tc
	if c.pending != nil {
		c.notifyWatcherLocked()
	}
}

func (c *preemptController) endActiveTurn() *preemptRequest {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.requirePhaseLocked(preemptTurnActive, "endActiveTurn")
	c.turnPhase = preemptTurnIdle
	c.currentRunCtx = nil
	c.currentTC = nil
	req := c.pending
	c.pending = nil
	c.cond.Broadcast()
	return req
}

func (c *preemptController) requirePhaseLocked(expected preemptTurnPhase, op string) {
	if c.turnPhase != expected {
		panic(fmt.Sprintf("adk: preemptController.%s called while turn phase is %s; expected %s", op, c.turnPhase, expected))
	}
}

func (c *preemptController) requireNoPendingLocked(op string) {
	if c.pending != nil {
		panic(fmt.Sprintf("adk: preemptController.%s called with stale pending preempt request", op))
	}
}

func (c *preemptController) beginPush() preemptTurnSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pushInFlight++
	return preemptTurnSnapshot{
		hasTargetTurn: c.turnPhase == preemptTurnPlanning || c.turnPhase == preemptTurnActive,
		turnID:        c.turnID,
		ctx:           c.currentRunCtx,
		tc:            c.currentTC,
	}
}

func (c *preemptController) endPush() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pushInFlight--
	if c.pushInFlight < 0 {
		panic("adk: preemptController.endPush called without matching beginPush")
	}
	c.cond.Broadcast()
}

func (c *preemptController) waitForPushes() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for c.pushInFlight > 0 {
		c.cond.Wait()
	}
}

func (c *preemptController) requestPreempt(target preemptTurnSnapshot, ack chan struct{}, opts ...CancelOption) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed || !target.hasTargetTurn || c.turnPhase == preemptTurnIdle || c.turnID != target.turnID {
		if ack != nil {
			close(ack)
		}
		return
	}

	now := time.Now()
	if c.pending == nil {
		c.pending = newPreemptRequest(ack, opts, now)
	} else {
		c.pending.merge(ack, opts, now)
	}
	if c.turnPhase == preemptTurnActive {
		c.notifyWatcherLocked()
	}
}

func (c *preemptController) receivePreempt() (*preemptRequest, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.turnPhase != preemptTurnActive || c.pending == nil {
		return nil, false
	}
	req := c.pending
	c.pending = nil
	return req, true
}

func (c *preemptController) closeForLoopExit() {
	c.mu.Lock()
	c.closed = true
	c.turnPhase = preemptTurnIdle
	c.currentRunCtx = nil
	c.currentTC = nil
	req := c.pending
	c.pending = nil
	select {
	case <-c.notify:
	default:
	}
	c.cond.Broadcast()
	c.mu.Unlock()

	req.ack()
}

func (c *preemptController) notifyWatcherLocked() {
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

type stopDecision struct {
	commit   bool
	wakeIdle bool
}

type stopCancelRequest struct {
	cancel cancelRequestState
}

func newStopCancelRequest(opts []CancelOption, now time.Time) *stopCancelRequest {
	return &stopCancelRequest{cancel: newCancelRequestState(opts, now)}
}

func (r *stopCancelRequest) merge(opts []CancelOption, now time.Time) {
	if r == nil {
		return
	}
	r.cancel.merge(opts, now)
}

func (r *stopCancelRequest) cancelOptions(now time.Time) []CancelOption {
	if r == nil {
		return nil
	}
	return r.cancel.cancelOptions(now)
}

// stopController owns global Stop state and optional active-turn cancel requests.
//
// Stop has two independent layers:
//   - terminal loop intent: committed Stop prevents future turns and closes the buffer;
//   - optional active-turn cancel: cancel-capable Stop calls create a pending request
//     consumed by the watcher if the current turn is still active.
//
// Unlike preempt, Stop is not bound to a turnID. It is global and terminal.
// A pending cancel request is consumed by the active turn or dropped when that
// turn ends before consumption.
type stopController struct {
	mu sync.Mutex

	phase stopPhase

	hasActiveCancelTarget bool
	pending               *stopCancelRequest
	notify                chan struct{}

	idleFor        time.Duration
	skipCheckpoint bool
	stopCause      string

	closed bool
}

func newStopController() *stopController {
	return &stopController{notify: make(chan struct{}, 1)}
}

func (c *stopController) requestStop(cfg *stopConfig) stopDecision {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return stopDecision{}
	}
	if cfg.skipCheckpoint {
		c.skipCheckpoint = true
	}
	if cfg.stopCause != "" && c.stopCause == "" {
		c.stopCause = cfg.stopCause
	}
	if cfg.idleFor > 0 {
		if c.phase != stopCommitted && c.idleFor == 0 {
			c.phase = stopIdleWaiting
			c.idleFor = cfg.idleFor
		}
		return stopDecision{wakeIdle: c.phase == stopIdleWaiting}
	}

	committed := c.commitLocked()
	if cfg.agentCancelOpts != nil {
		now := time.Now()
		if c.pending == nil {
			c.pending = newStopCancelRequest(cfg.agentCancelOpts, now)
		} else {
			c.pending.merge(cfg.agentCancelOpts, now)
		}
		if c.hasActiveCancelTarget {
			c.notifyWatcherLocked()
		}
	}
	return stopDecision{commit: committed}
}

func (c *stopController) commit() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.commitLocked()
}

func (c *stopController) commitLocked() bool {
	if c.closed || c.phase == stopCommitted {
		return false
	}
	c.phase = stopCommitted
	c.idleFor = 0
	return true
}

func (c *stopController) isCommitted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == stopCommitted
}

func (c *stopController) idleDuration() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase != stopIdleWaiting {
		return 0
	}
	return c.idleFor
}

func (c *stopController) skipCheckpointEnabled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.skipCheckpoint
}

func (c *stopController) cause() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stopCause
}

func (c *stopController) beginActiveTurn() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.hasActiveCancelTarget = true
	if c.pending != nil {
		c.notifyWatcherLocked()
	}
}

func (c *stopController) endActiveTurn() *stopCancelRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hasActiveCancelTarget = false
	req := c.pending
	c.pending = nil
	return req
}

func (c *stopController) receiveCancel() (*stopCancelRequest, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.hasActiveCancelTarget || c.pending == nil {
		return nil, false
	}
	req := c.pending
	c.pending = nil
	return req, true
}

func (c *stopController) closeForLoopExit() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.hasActiveCancelTarget = false
	c.pending = nil
	select {
	case <-c.notify:
	default:
	}
}

func (c *stopController) notifyWatcherLocked() {
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

// ---- TurnLoopConfig ----

// TurnLoopConfig is the configuration for creating a TurnLoop.
// M is hardcoded to *schema.Message internally, so the public API is TurnLoopConfig[T].
type TurnLoopConfig[T any] struct {
	// GenInput receives the TurnLoop instance and all buffered items, and decides what to process.
	// It returns which items to consume now vs keep for later turns.
	// The loop parameter allows calling Push() or Stop() directly from within the callback.
	// Required.
	GenInput func(ctx context.Context, loop *TurnLoop[T], items []T) (*GenInputResult[T], error)

	// GenResume is called at most once during Run(). When CheckpointID is
	// configured, Run() queries Store for the checkpoint:
	//   - If the checkpoint contains runner state (i.e. an agent was interrupted
	//     or canceled mid-turn), Run() calls GenResume to plan a resume turn.
	//   - Otherwise (no checkpoint, or between-turns checkpoint), GenResume is
	//     never called and the loop proceeds via GenInput.
	GenResume func(ctx context.Context, loop *TurnLoop[T], interruptedItems, unhandledItems, newItems []T) (*GenResumeResult[T], error)

	// PrepareAgent returns an Agent configured to handle the consumed items.
	// This callback should set up the agent with appropriate system prompt,
	// tools, and middlewares based on what items are being processed.
	// Called once per turn with the items that GenInput decided to consume.
	// The loop parameter allows calling Push() or Stop() directly from within the callback.
	// Required.
	PrepareAgent func(ctx context.Context, loop *TurnLoop[T], consumed []T) (Agent, error)

	// OnAgentEvents is called to handle events emitted by the agent.
	// The TurnContext provides per-turn info and control.
	//
	// Error handling: the returned error is only used when the callback itself
	// wants to abort the TurnLoop. The callback should NEVER propagate
	// CancelError — the framework handles it automatically:
	//   - Stop: the framework propagates CancelError as ExitReason, loop exits.
	//   - Preempt: the framework does not propagate CancelError; if the callback
	//     also returns nil, the loop continues with the next turn.
	//
	// Optional. If not provided, events are drained and the first error
	// (including CancelError from Stop) is returned as ExitReason.
	OnAgentEvents func(ctx context.Context, tc *TurnContext[T], events *AsyncIterator[*AgentEvent]) error

	// Store is the checkpoint store for persistence and resume. Optional.
	// When set together with CheckpointID, enables automatic checkpoint-based resume.
	// The TurnLoop always persists both runner checkpoint bytes and item bookkeeping
	// via gob encoding, so T must be gob-encodable when Store is used.
	Store CheckPointStore

	// CheckpointID, when set together with Store, enables automatic
	// checkpoint-based resume. On Run(), the TurnLoop queries Store for this ID.
	CheckpointID string
}

// GenInputResult contains the result of GenInput processing.
type GenInputResult[T any] struct {
	// RunCtx, if non-nil, overrides the context for this turn's execution
	// (PrepareAgent, agent run, OnAgentEvents).
	RunCtx context.Context

	// Input is the agent input to execute
	Input *AgentInput

	// RunOpts are the options for this agent run.
	RunOpts []RunOption

	// Consumed are the items selected for this turn.
	Consumed []T

	// Remaining are the items to keep in the buffer for a future turn.
	Remaining []T
}

// GenResumeResult contains the result of GenResume processing.
type GenResumeResult[T any] struct {
	// RunCtx, if non-nil, overrides the context for this resumed turn's execution.
	RunCtx context.Context

	// RunOpts are the options for this agent resume run.
	RunOpts []RunOption

	// ResumeParams are optional parameters for resuming an interrupted agent.
	ResumeParams *ResumeParams

	// Consumed are the items selected for this resumed turn.
	Consumed []T

	// Remaining are the items to keep in the buffer for a future turn.
	Remaining []T
}

type turnRunSpec[T any] struct {
	runCtx       context.Context
	input        *AgentInput
	runOpts      []RunOption
	resumeParams *ResumeParams
	isResume     bool
	consumed     []T
	resumeBytes  []byte
}

type turnPlan[T any] struct {
	turnCtx   context.Context
	remaining []T
	spec      *turnRunSpec[T]
}

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

// TurnLoopState is returned when TurnLoop exits, containing the exit reason
// and any items that were not processed.
type TurnLoopState[T any] struct {
	// ExitReason indicates why the loop exited.
	// nil means clean exit (Stop() was called without cancel options, or the
	// agent completed normally before Stop took effect).
	ExitReason error

	// UnhandledItems contains items that were buffered but not processed.
	UnhandledItems []T

	// InterruptedItems contains the items whose turn was interrupted.
	InterruptedItems []T

	// StopCause is the business-supplied reason passed via WithStopCause.
	StopCause string

	// CheckpointAttempted indicates whether a checkpoint save was attempted when the loop exited.
	CheckpointAttempted bool

	// CheckpointErr is the error from checkpoint save, if any.
	CheckpointErr error

	// TakeLateItems returns items that were pushed after the loop stopped
	// (i.e., Push returned false for these items). These items are NOT included
	// in the checkpoint.
	//
	// This function is idempotent: the first call computes and caches the result;
	// subsequent calls return the same slice.
	//
	// After TakeLateItems is called, any subsequent Push() will panic to
	// prevent items from being silently lost.
	//
	// It is safe to call TakeLateItems from any goroutine after Wait() returns.
	// If TakeLateItems is never called, late items are simply garbage collected.
	TakeLateItems func() []T
}

// TurnContext provides per-turn context to the OnAgentEvents callback.
type TurnContext[T any] struct {
	// Loop is the TurnLoop instance, allowing Push() or Stop() calls.
	Loop *TurnLoop[T]

	// Consumed contains items that triggered this agent execution.
	Consumed []T

	// Preempted is closed when a preempt signal fires for the current turn.
	Preempted <-chan struct{}

	// Stopped is closed when a Stop() call contributed to the CancelError for the
	// current turn.
	Stopped <-chan struct{}

	// StopCause returns the business-supplied reason from WithStopCause.
	StopCause func() string
}

// TurnLoop is a push-based event loop for agent execution.
// Users push items via Push() and the loop processes them through the agent.
//
// Create with NewTurnLoop, then start with Run:
//
//	loop := NewTurnLoop(cfg)
//	// pass loop to other components, push initial items, etc.
//	loop.Run(ctx)
//
// # Permissive API
//
// All methods are valid on a not-yet-running loop:
//   - Push: items are buffered and will be processed once Run is called.
//   - Stop: sets the stopped flag; a subsequent Run will exit immediately.
//   - Wait: blocks until Run is called AND the loop exits.
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

type turnLoopCheckpoint[T any] struct {
	RunnerCheckpoint []byte
	// HasRunnerState reports whether RunnerCheckpoint contains resumable runner state.
	HasRunnerState bool
	UnhandledItems []T
	CanceledItems  []T
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

type turnLoopPendingResume[T any] struct {
	interrupted []T
	unhandled   []T
	newItems    []T
	resumeBytes []byte
}

// SafePoint describes at which boundary the agent may be cancelled.
type SafePoint int

const (
	// AfterChatModel allows the agent to finish the current chat-model
	// call before being cancelled.
	AfterChatModel SafePoint = 1 << iota
	// AfterToolCalls allows the agent to finish the current tool-call round
	// before being cancelled.
	AfterToolCalls
	// AnySafePoint is shorthand for AfterChatModel | AfterToolCalls.
	AnySafePoint = AfterChatModel | AfterToolCalls
)

func (sp SafePoint) toCancelMode() CancelMode {
	var mode CancelMode
	if sp&AfterToolCalls != 0 {
		mode |= CancelAfterToolCalls
	}
	if sp&AfterChatModel != 0 {
		mode |= CancelAfterChatModel
	}
	return mode
}

type stopConfig struct {
	agentCancelOpts []CancelOption
	skipCheckpoint  bool
	stopCause       string
	idleFor         time.Duration
}

// StopOption is an option for Stop().
type StopOption func(*stopConfig)

// WithGraceful requests a graceful stop that waits at the nearest safe point
// (after tool calls or after a chat-model call) and propagates recursively to
// nested agents.
func WithGraceful() StopOption {
	return func(cfg *stopConfig) {
		cfg.agentCancelOpts = []CancelOption{
			WithCancelMode(CancelAfterChatModel | CancelAfterToolCalls),
			WithRecursiveCancel(),
		}
	}
}

// WithImmediate aborts the running agent turn as soon as possible.
func WithImmediate() StopOption {
	return func(cfg *stopConfig) {
		cfg.agentCancelOpts = []CancelOption{
			WithRecursiveCancel(),
		}
	}
}

// WithGracefulTimeout is like WithGraceful but adds a grace period.
func WithGracefulTimeout(gracePeriod time.Duration) StopOption {
	if gracePeriod <= 0 {
		panic("agentcore: WithGracefulTimeout: gracePeriod must be positive")
	}
	return func(cfg *stopConfig) {
		cfg.agentCancelOpts = []CancelOption{
			WithCancelMode(CancelAfterChatModel | CancelAfterToolCalls),
			WithRecursiveCancel(),
			WithCancelTimeout(gracePeriod),
		}
	}
}

// WithSkipCheckpoint tells the TurnLoop not to persist a checkpoint for this Stop call.
func WithSkipCheckpoint() StopOption {
	return func(cfg *stopConfig) {
		cfg.skipCheckpoint = true
	}
}

// WithStopCause attaches a business-supplied reason string to this Stop call.
func WithStopCause(cause string) StopOption {
	return func(cfg *stopConfig) {
		cfg.stopCause = cause
	}
}

// UntilIdleFor defers the stop until the TurnLoop has been continuously idle.
func UntilIdleFor(duration time.Duration) StopOption {
	if duration <= 0 {
		panic("agentcore: UntilIdleFor: duration must be positive")
	}
	return func(cfg *stopConfig) {
		cfg.idleFor = duration
	}
}

type pushConfig[T any] struct {
	preempt         bool
	preemptDelay    time.Duration
	agentCancelOpts []CancelOption
	pushStrategy    func(context.Context, *TurnContext[T]) []PushOption[T]
}

// PushOption is an option for Push().
type PushOption[T any] func(*pushConfig[T])

// WithPreempt signals that the current agent turn should be cancelled at the
// specified safePoint after pushing the new item.
func WithPreempt[T any](safePoint SafePoint) PushOption[T] {
	if safePoint == 0 {
		panic("agentcore: SafePoint must not be zero; use AfterToolCalls, AfterChatModel, or AnySafePoint")
	}
	return func(cfg *pushConfig[T]) {
		cfg.preempt = true
		cfg.agentCancelOpts = []CancelOption{
			WithCancelMode(safePoint.toCancelMode()),
		}
	}
}

// WithPreemptTimeout is like WithPreempt but adds a timeout.
func WithPreemptTimeout[T any](safePoint SafePoint, timeout time.Duration) PushOption[T] {
	if safePoint == 0 {
		panic("agentcore: SafePoint must not be zero; use AfterToolCalls, AfterChatModel, or AnySafePoint")
	}
	return func(cfg *pushConfig[T]) {
		cfg.preempt = true
		cfg.agentCancelOpts = []CancelOption{
			WithCancelMode(safePoint.toCancelMode()),
			WithCancelTimeout(timeout),
			WithRecursiveCancel(),
		}
	}
}

// WithPreemptDelay sets a delay duration before resolving a preemptive Push.
func WithPreemptDelay[T any](delay time.Duration) PushOption[T] {
	return func(cfg *pushConfig[T]) {
		cfg.preemptDelay = delay
	}
}

// WithPushStrategy provides dynamic push option resolution based on the current turn state.
func WithPushStrategy[T any](fn func(ctx context.Context, tc *TurnContext[T]) []PushOption[T]) PushOption[T] {
	return func(cfg *pushConfig[T]) {
		cfg.pushStrategy = fn
	}
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

const bridgeCheckpointID = "__adk_turnloop_bridge_cp__"

// bridgeStore is a minimal CheckPointStore used to bridge TurnLoop with Runner
// checkpoints without using the actual Store.
type bridgeStore struct {
	cpID string
	data []byte
	mu   sync.RWMutex
}

func newBridgeStore() *bridgeStore {
	return &bridgeStore{cpID: bridgeCheckpointID}
}

func newResumeBridgeStore(cpID string, data []byte) *bridgeStore {
	return &bridgeStore{cpID: cpID, data: append([]byte{}, data...)}
}

func (s *bridgeStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if key != s.cpID {
		return nil, false, nil
	}
	if len(s.data) == 0 {
		return nil, false, nil
	}
	return append([]byte{}, s.data...), true, nil
}

func (s *bridgeStore) Set(_ context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if key == s.cpID {
		s.data = append([]byte{}, data...)
	}
	return nil
}

func (s *bridgeStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if key == s.cpID {
		s.data = nil
	}
	return nil
}

var _ CheckPointStore = (*bridgeStore)(nil)
var _ CheckPointDeleter = (*bridgeStore)(nil)

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

// InterruptError signals a business interrupt during a turn.
type InterruptError struct {
	InterruptContexts []*InterruptCtx
}

func (e *InterruptError) Error() string {
	return fmt.Sprintf("agent interrupted: %d context(s)", len(e.InterruptContexts))
}

// ---- Deprecated aliases for backward compatibility ----

// WithImmediateStop is a deprecated alias for WithImmediate.
func WithImmediateStop() StopOption { return WithImmediate() }

// WithGracefulStop is a deprecated alias for WithGraceful.
func WithGracefulStop() StopOption { return WithGraceful() }

// WithStopTimeout is a deprecated alias for WithGracefulTimeout.
func WithStopTimeout(d time.Duration) StopOption { return WithGracefulTimeout(d) }
