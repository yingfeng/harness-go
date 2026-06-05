package agentcore

import "context"

// AgentCallbackInput is the input to the agent callback OnStart.
type AgentCallbackInput struct {
	Input      *AgentInput
	ResumeInfo *ResumeInfo
}

// AgentCallbackOutput is the output from the agent callback OnEnd.
type AgentCallbackOutput struct {
	Events *AsyncIterator[*AgentEvent]
}

type TypedAgentCallbackInput[M MessageType] struct {
	Input      *TypedAgentInput[M]
	ResumeInfo *ResumeInfo
}

type TypedAgentCallbackOutput[M MessageType] struct {
	Events *AsyncIterator[*TypedAgentEvent[M]]
}

// callbackHandler holds registered callback functions.
type callbackHandler struct {
	onStart func(ctx context.Context, input *AgentCallbackInput)
	onEnd   func(ctx context.Context, output *AgentCallbackOutput)
}

type callbackKey struct{}

func getCallbacks(ctx context.Context) []callbackHandler {
	if v := ctx.Value(callbackKey{}); v != nil {
		return v.([]callbackHandler)
	}
	return nil
}

func withCallbacks(ctx context.Context, cbs []callbackHandler) context.Context {
	if len(cbs) == 0 { return ctx }
	return context.WithValue(ctx, callbackKey{}, cbs)
}

func initAgentCallbacks(ctx context.Context, name, agentType string, opts ...RunOption) context.Context {
	o := getCommonOptions(nil, opts...)
	if len(o.callbacks) == 0 { return ctx }
	cbs := make([]callbackHandler, 0, len(o.callbacks))
	for _, cb := range o.callbacks {
		switch c := cb.(type) {
		case callbackHandler:
			cbs = append(cbs, c)
		}
	}
	return withCallbacks(ctx, cbs)
}

func initAgenticCallbacks(ctx context.Context, name, agentType string, opts ...RunOption) context.Context {
	return initAgentCallbacks(ctx, name, agentType, opts...)
}

func filterOptions(name string, opts []RunOption) []RunOption {
	// Remove callbacks not matching the given agent name from agentNames list
	o := getCommonOptions(nil, opts...)
	if len(o.agentNames) == 0 { return opts }

	var filtered []RunOption
	for _, opt := range opts {
		// Filter out AgentNames options that don't match
		if fn, ok := opt.(runOptFn); ok {
			tmp := &runOptions{}
			fn(tmp)
			if tmp.agentNames != nil {
				match := false
				for _, n := range tmp.agentNames {
					if n == name { match = true; break }
				}
				if !match { continue }
			}
		}
		filtered = append(filtered, opt)
	}
	return filtered
}

func filterCancelOption(opts []RunOption) []RunOption {
	// Remove cancel context options from sub-agent options
	// to avoid duplicate cancel handling
	var filtered []RunOption
	for _, opt := range opts {
		if fn, ok := opt.(runOptFn); ok {
			tmp := &runOptions{}
			fn(tmp)
			if tmp.cancelCtx != nil { continue }
		}
		filtered = append(filtered, opt)
	}
	if len(filtered) == len(opts) { return opts }
	return filtered
}

func filterCallbackHandlersForNestedAgents(name string, opts []RunOption) []RunOption {
	// Remove callback handlers that are scoped to specific agents
	o := getCommonOptions(nil, opts...)
	if len(o.agentNames) == 0 { return opts }

	var filtered []RunOption
	for _, opt := range opts {
		if fn, ok := opt.(runOptFn); ok {
			tmp := &runOptions{}
			fn(tmp)
			if tmp.agentNames != nil {
				match := false
				for _, n := range tmp.agentNames {
					if n == name { match = true; break }
				}
				if !match { continue }
			}
		}
		filtered = append(filtered, opt)
	}
	return filtered
}

func getAgentType(a Agent) string {
	if t, ok := a.(interface{ GetType() string }); ok {
		return t.GetType()
	}
	return "ChatModelAgent"
}

// ---- Run-local value helpers ----

func SetRunLocalValue(ctx context.Context, key string, val any) error {
	rc := getRunCtx(ctx)
	if rc == nil || rc.Session == nil {
		return errNotInAgentExec
	}
	rc.Session.Values[key] = val
	return nil
}

func GetRunLocalValue(ctx context.Context, key string) (any, bool, error) {
	rc := getRunCtx(ctx)
	if rc == nil || rc.Session == nil {
		return nil, false, errNotInAgentExec
	}
	v, ok := rc.Session.Values[key]
	return v, ok, nil
}

func DeleteRunLocalValue(ctx context.Context, key string) error {
	rc := getRunCtx(ctx)
	if rc == nil || rc.Session == nil {
		return errNotInAgentExec
	}
	delete(rc.Session.Values, key)
	return nil
}

func SendEvent(ctx context.Context, event *AgentEvent) error {
	ec := getChatModelExecCtx(ctx)
	if ec == nil || ec.generator == nil {
		return errNotInAgentExec
	}
	ec.send(event)
	return nil
}

func TypedSendEvent[M MessageType](ctx context.Context, event *TypedAgentEvent[M]) error {
	ec := getTypedChatModelExecCtx[M](ctx)
	if ec == nil || ec.generator == nil {
		return errNotInAgentExec
	}
	ec.send(event)
	return nil
}

type AgentExecError struct{ Message string }

func (e *AgentExecError) Error() string { return e.Message }

var errNotInAgentExec = &AgentExecError{Message: "must be called within ChatModelAgent Run/Resume"}
