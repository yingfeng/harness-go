package agentcore

import (
	"context"
	"sync"
)

// runSession holds per-execution mutable state for an agent run.
type runSession struct {
	mu       sync.Mutex
	Values   map[string]any
	valuesMx *sync.Mutex
	events   []any
}

func newRunSession() *runSession {
	return &runSession{Values: make(map[string]any), valuesMx: &sync.Mutex{}}
}

func (s *runSession) addEvent(event any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
}

func (s *runSession) getEvents() []any {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := make([]any, len(s.events))
	copy(r, s.events)
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

func joinRunCtxs(ctx context.Context, childCtxs ...context.Context) {}

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
