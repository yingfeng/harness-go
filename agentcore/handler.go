package agentcore

import (
	"context"
	"github.com/infiniflow/ragflow/agent/agentcore/schema"
)

// ModelOption configures a model call.
type ModelOption interface{ applyModel() }

type modelOption = ModelOption

// ToolOption configures a tool call.
type ToolOption interface{ applyTool() }

type toolOption = ToolOption

// ---- ChatModel interface ----

type ChatModel[M MessageType] interface {
	Generate(ctx context.Context, messages []M, opts ...ModelOption) (M, error)
	Stream(ctx context.Context, messages []M, opts ...ModelOption) (*schema.StreamReader[M], error)
	BindTools(tools []*schema.ToolInfo) error
}

// ---- Tool interface ----

type Tool interface {
	Name() string
	Description() string
	Invoke(ctx context.Context, argumentsInJSON string, opts ...ToolOption) (string, error)
	Stream(ctx context.Context, argumentsInJSON string, opts ...ToolOption) (*schema.StreamReader[string], error)
}

// BaseTool provides a simple Tool implementation from a function.
type BaseTool struct {
	name    string
	desc    string
	invokeFn func(ctx context.Context, args string) (string, error)
}

func NewBaseTool(name, desc string, fn func(ctx context.Context, args string) (string, error)) *BaseTool {
	return &BaseTool{name: name, desc: desc, invokeFn: fn}
}
func (t *BaseTool) Name() string                                                 { return t.name }
func (t *BaseTool) Description() string                                           { return t.desc }
func (t *BaseTool) Invoke(ctx context.Context, args string, opts ...toolOption) (string, error)          { return t.invokeFn(ctx, args) }
func (t *BaseTool) Stream(ctx context.Context, args string, opts ...toolOption) (*schema.StreamReader[string], error) {
	return schema.StreamReaderFromArray([]string{""}), nil
}

// ---- Endpoint types ----

type InvokableToolEndpoint func(ctx context.Context, args string, opts ...toolOption) (string, error)
type StreamableToolEndpoint func(ctx context.Context, args string, opts ...toolOption) (*schema.StreamReader[string], error)

// ---- Model context ----

type TypedModelContext[M MessageType] struct {
	Tools               []*schema.ToolInfo
	ModelRetryConfig    *RetryConfig[M]
	ModelFailoverConfig *FailoverConfig[M]
	cancelCtx           *cancelContext
}

type ModelContext = TypedModelContext[*schema.Message]

type RetryConfig[M MessageType] struct{ MaxAttempts int }
type FailoverConfig[M MessageType] struct{ Models []ChatModel[M] }

// ---- Middleware interface ----

type TypedChatModelMiddleware[M MessageType] interface {
	BeforeAgent(ctx context.Context, rc *ChatModelAgentContext) (context.Context, *ChatModelAgentContext, error)
	AfterAgent(ctx context.Context, state *TypedChatModelAgentState[M]) (context.Context, error)
	BeforeModelRewrite(ctx context.Context, state *TypedChatModelAgentState[M], mc *TypedModelContext[M]) (context.Context, *TypedChatModelAgentState[M], error)
	AfterModelRewrite(ctx context.Context, state *TypedChatModelAgentState[M], mc *TypedModelContext[M]) (context.Context, *TypedChatModelAgentState[M], error)
	WrapToolInvoke(ctx context.Context, ep InvokableToolEndpoint, tc *ToolContext) (InvokableToolEndpoint, error)
	WrapToolStream(ctx context.Context, ep StreamableToolEndpoint, tc *ToolContext) (StreamableToolEndpoint, error)
	WrapModel(ctx context.Context, m ChatModel[M], mc *TypedModelContext[M]) (ChatModel[M], error)
}

type ChatModelMiddleware = TypedChatModelMiddleware[*schema.Message]

// BaseMiddleware provides no-op defaults for TypedChatModelMiddleware.
// Embed in custom middlewares to only override needed methods.
type BaseMiddleware[M MessageType] struct{}

func (b *BaseMiddleware[M]) BeforeAgent(ctx context.Context, rc *ChatModelAgentContext) (context.Context, *ChatModelAgentContext, error) { return ctx, rc, nil }
func (b *BaseMiddleware[M]) AfterAgent(ctx context.Context, state *TypedChatModelAgentState[M]) (context.Context, error) { return ctx, nil }
func (b *BaseMiddleware[M]) BeforeModelRewrite(ctx context.Context, state *TypedChatModelAgentState[M], mc *TypedModelContext[M]) (context.Context, *TypedChatModelAgentState[M], error) { return ctx, state, nil }
func (b *BaseMiddleware[M]) AfterModelRewrite(ctx context.Context, state *TypedChatModelAgentState[M], mc *TypedModelContext[M]) (context.Context, *TypedChatModelAgentState[M], error) { return ctx, state, nil }
func (b *BaseMiddleware[M]) WrapToolInvoke(_ context.Context, ep InvokableToolEndpoint, _ *ToolContext) (InvokableToolEndpoint, error) { return ep, nil }
func (b *BaseMiddleware[M]) WrapToolStream(_ context.Context, ep StreamableToolEndpoint, _ *ToolContext) (StreamableToolEndpoint, error) { return ep, nil }
func (b *BaseMiddleware[M]) WrapModel(_ context.Context, m ChatModel[M], _ *TypedModelContext[M]) (ChatModel[M], error) { return m, nil }
