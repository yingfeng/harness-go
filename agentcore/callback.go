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

func initAgentCallbacks(ctx context.Context, name, agentType string, opts ...RunOption) context.Context { return ctx }
func initAgenticCallbacks(ctx context.Context, name, agentType string, opts ...RunOption) context.Context { return ctx }
func filterOptions(name string, opts []RunOption) []RunOption        { return opts }
func filterCancelOption(opts []RunOption) []RunOption                 { return opts }
func filterCallbackHandlersForNestedAgents(name string, opts []RunOption) []RunOption { return opts }

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
