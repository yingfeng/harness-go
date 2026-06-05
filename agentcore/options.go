package agentcore

import "context"

// RunOption configures an agent run.
type RunOption interface{ apply(*runOptions) }

type runOptions struct {
	sessionValues        map[string]any
	sharedParentSession  bool
	checkPointID         *string
	cancelCtx            *cancelContext
	skipTransferMessages bool
	agentNames           []string
	callbacks            []any
}

type runOptFn func(*runOptions)

func (f runOptFn) apply(o *runOptions) { f(o) }

func WrapImplSpecificOptFn(fn func(*runOptions)) RunOption {
	return runOptFn(fn)
}

func getCommonOptions(o *runOptions, opts ...RunOption) *runOptions {
	if o == nil {
		o = &runOptions{}
	}
	for _, opt := range opts {
		if opt != nil {
			opt.apply(o)
		}
	}
	return o
}

func WithSessionValues(vals map[string]any) RunOption {
	return runOptFn(func(o *runOptions) { o.sessionValues = vals })
}
func WithCheckPointID(id string) RunOption {
	return runOptFn(func(o *runOptions) { o.checkPointID = &id })
}
func WithSkipTransferMessages() RunOption {
	return runOptFn(func(o *runOptions) { o.skipTransferMessages = true })
}
func WithCallbacks(cbs ...any) RunOption {
	return runOptFn(func(o *runOptions) { o.callbacks = cbs })
}
func WithAgentNames(names ...string) RunOption {
	return runOptFn(func(o *runOptions) { o.agentNames = names })
}
func WithSharedParentSession() RunOption {
	return runOptFn(func(o *runOptions) { o.sharedParentSession = true })
}

// ---- ChatModel-agent-specific options ----

type modelOption interface{ applyModel() }
type toolOption interface{ applyTool() }

func WithChatModelOptions(opts []modelOption) RunOption {
	return WrapImplSpecificOptFn(func(o *runOptions) {})
}
func WithToolOptions(opts []toolOption) RunOption {
	return WrapImplSpecificOptFn(func(o *runOptions) {})
}
func WithAgentToolOptions(agentName string, opts []RunOption) RunOption {
	return WrapImplSpecificOptFn(func(o *runOptions) {})
}
func WithHistoryModifier(fn func(context.Context, []Message) []Message) RunOption {
	return WrapImplSpecificOptFn(func(o *runOptions) {})
}

// ---- Cancel option ----

func WithCancel() (RunOption, AgentCancelFunc) {
	cc := newCancelContext()
	opt := WrapImplSpecificOptFn(func(o *runOptions) { o.cancelCtx = cc })
	return opt, cc.buildCancelFunc()
}
