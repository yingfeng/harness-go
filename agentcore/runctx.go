package agentcore

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

func init() {
	schema.RegisterType("_harness_event_wrap_entry", func() any { return &eventWrapEntry{} })
}

// eventWrapEntry wraps an event with metadata for checkpoint persistence.
type eventWrapEntry struct {
	Event     any
	Timestamp int64
}

// consumeStream checks if the wrapped event contains a streaming message and, if so,
// fully consumes the stream before checkpoint. This prevents partial data in checkpoints.
func (e *eventWrapEntry) consumeStream() {
	if e.Event == nil {
		return
	}
	ev, ok := e.Event.(*AgentEvent)
	if !ok || ev.Output == nil || ev.Output.MessageOutput == nil {
		return
	}
	mv := ev.Output.MessageOutput
	if !mv.IsStreaming || mv.MessageStream == nil {
		return
	}
	merged, err := schema.ConcatMessageStream(mv.MessageStream)
	if err == nil {
		mv.Message = merged
		mv.IsStreaming = false
		mv.MessageStream = nil
	}
}

func (e *eventWrapEntry) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(e.Timestamp); err != nil {
		return nil, err
	}
	if e.Event == nil {
		if err := enc.Encode(false); err != nil {
			return nil, err
		}
	} else {
		if err := enc.Encode(true); err != nil {
			return nil, err
		}
		typeName := reflect.TypeOf(e.Event).String()
		// Gob-registered types use their registered name; try direct encode first.
		if err := enc.Encode(&typeName); err != nil {
			return nil, err
		}
		if err := enc.Encode(e.Event); err != nil {
			return nil, fmt.Errorf("gob encode event (%s): %w", typeName, err)
		}
	}
	return buf.Bytes(), nil
}

func (e *eventWrapEntry) GobDecode(data []byte) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&e.Timestamp); err != nil {
		return err
	}
	var nonNil bool
	if err := dec.Decode(&nonNil); err != nil {
		return err
	}
	if nonNil {
		var typeName string
		if err := dec.Decode(&typeName); err != nil {
			return err
		}
		// Decode into generic interface{} — gob will reconstruct registered types.
		e.Event = new(any)
		if err := dec.Decode(e.Event); err != nil {
			return fmt.Errorf("gob decode event (%s): %w", typeName, err)
		}
		// Decode into interface{} wraps in a *any; unwrap.
		if p, ok := e.Event.(*any); ok {
			e.Event = *p
		}
	}
	return nil
}

// laneEvents holds per-lane event history for parallel workflows.
type laneEvents struct {
	Events []*eventWrapEntry
}

// runSession holds per-execution mutable state for an agent run.
type runSession struct {
	mu          sync.Mutex
	Values      map[string]any
	valuesMx    *sync.Mutex
	events      []*eventWrapEntry
	TypedEvents any // *[]*typedAgentEventWrapper[M] for AgenticMessage path (gob-encodable)
	LaneEvents  *laneEvents
}

func newRunSession() *runSession {
	return &runSession{Values: make(map[string]any), valuesMx: &sync.Mutex{}}
}

func (s *runSession) addEvent(event any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := &eventWrapEntry{Event: event, Timestamp: time.Now().UnixNano()}
	// Consume any streaming content before storing for checkpoint safety
	entry.consumeStream()
	s.events = append(s.events, entry)
}

func (s *runSession) getEvents() []any {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := make([]any, len(s.events))
	for i, e := range s.events {
		if e != nil {
			r[i] = e.Event
		}
	}
	return r
}

// runContext holds runtime metadata for an agent execution.
type runContext struct {
	RootInput any
	RunPath   []RunStep
	Session   *runSession
}

type runContextKey struct{}

func ctxWithNewTypedRunCtx[M MessageType](ctx context.Context, input *TypedAgentInput[M], sharedParentSession bool) context.Context {
	rc := &runContext{RootInput: input, RunPath: make([]RunStep, 0), Session: newRunSession()}
	return context.WithValue(ctx, runContextKey{}, rc)
}

func initRunCtx(ctx context.Context, agentName string, input *AgentInput) (context.Context, *runContext) {
	rc := getRunCtx(ctx)
	if rc == nil {
		rc = &runContext{RootInput: input, RunPath: make([]RunStep, 0), Session: newRunSession()}
		ctx = context.WithValue(ctx, runContextKey{}, rc)
	}
	rc.RunPath = append(rc.RunPath, RunStep{agentName: agentName})
	return ctx, rc
}

func getRunCtx(ctx context.Context) *runContext {
	if v := ctx.Value(runContextKey{}); v != nil {
		return v.(*runContext)
	}
	return nil
}

func setRunCtx(ctx context.Context, rc *runContext) context.Context {
	return context.WithValue(ctx, runContextKey{}, rc)
}

func forkRunCtx(ctx context.Context) context.Context {
	rc := getRunCtx(ctx)
	if rc == nil {
		return ctx
	}
	return context.WithValue(ctx, runContextKey{}, rc)
}

func updateRunPathOnly(ctx context.Context, steps ...string) context.Context {
	rc := getRunCtx(ctx)
	if rc == nil {
		return ctx
	}
	rc.RunPath = make([]RunStep, 0, len(steps))
	for _, s := range steps {
		rc.RunPath = append(rc.RunPath, RunStep{agentName: s})
	}
	return ctx
}

func joinRunCtxs(ctx context.Context, childCtxs ...context.Context) {
	parent := getRunCtx(ctx)
	if parent == nil { return }
	for _, cc := range childCtxs {
		child := getRunCtx(cc)
		if child == nil { continue }
		if child.Session == nil { continue }
		for _, ev := range child.Session.getEvents() {
			parent.Session.addEvent(ev)
		}
	}
}

func getSession(ctx context.Context) *runSession {
	if rc := getRunCtx(ctx); rc != nil {
		return rc.Session
	}
	return nil
}

func AddSessionValues(ctx context.Context, values map[string]any) {
	if rc := getRunCtx(ctx); rc != nil && rc.Session != nil && values != nil {
		for k, v := range values {
			rc.Session.Values[k] = v
		}
	}
}
