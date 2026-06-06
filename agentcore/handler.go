package agentcore

import (
	"context"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// ---- Endpoint types for tool wrapping ---

// InvokableToolEndpoint is the function signature for invoking a tool synchronously.
type InvokableToolEndpoint func(ctx context.Context, args string, opts ...ToolOption) (string, error)

// StreamableToolEndpoint is the function signature for invoking a tool with streaming output.
type StreamableToolEndpoint func(ctx context.Context, args string, opts ...ToolOption) (*schema.StreamReader[string], error)

// EnhancedInvokableToolEndpoint is the function signature for invoking an enhanced tool synchronously.
// Enhanced tools return structured *schema.ToolResult instead of raw strings.
type EnhancedInvokableToolEndpoint func(ctx context.Context, args *schema.ToolArgument, opts ...ToolOption) (*schema.ToolResult, error)

// EnhancedStreamableToolEndpoint is the function signature for invoking an enhanced tool with streaming output.
type EnhancedStreamableToolEndpoint func(ctx context.Context, args *schema.ToolArgument, opts ...ToolOption) (*schema.StreamReader[*schema.ToolResult], error)

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

// Tool is the basic tool interface for synchronous and streaming invocation.
type Tool interface {
	Name() string
	Description() string
	Invoke(ctx context.Context, argumentsInJSON string, opts ...ToolOption) (string, error)
	Stream(ctx context.Context, argumentsInJSON string, opts ...ToolOption) (*schema.StreamReader[string], error)
}

// EnhancedTool is an optional interface that tools can implement to return
// structured *schema.ToolResult instead of raw strings.
// When a Tool also satisfies EnhancedTool, the framework will call the enhanced
// methods and route through WrapEnhancedInvokableToolCall / WrapEnhancedStreamableToolCall.
type EnhancedTool interface {
	Tool
	// EnhancedInvoke invokes the tool with structured argument and returns a structured result.
	EnhancedInvoke(ctx context.Context, args *schema.ToolArgument, opts ...ToolOption) (*schema.ToolResult, error)
	// EnhancedStream invokes the tool with streaming structured results.
	EnhancedStream(ctx context.Context, args *schema.ToolArgument, opts ...ToolOption) (*schema.StreamReader[*schema.ToolResult], error)
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

// ---- Model context ----

type TypedModelContext[M MessageType] struct {
	Tools               []*schema.ToolInfo
	DeferredToolInfos   []*schema.ToolInfo
	ModelRetryConfig    *TypedModelRetryConfig[M]
	ModelFailoverConfig *FailoverConfig[M]
	cancelCtx           *cancelContext
}

type ModelContext = TypedModelContext[*schema.Message]

// ---- Middleware interface ----
//
// TypedChatModelMiddleware[M MessageType] is the interface for agent middleware.
// Implement *BaseMiddleware[M] to get default no-op implementations, then override only what you need.
//
// Execution order (outermost to innermost wrapper chain):
// Model call lifecycle:
//  1. BeforeAgent (can modify instruction, tools, returnDirectly)
//  2. BeforeModelRewrite (can modify state before model call)
//  3. failover -> retry -> eventSender -> WrapModel -> model.Generate
//  4. AfterModelRewrite (can modify state after model call)
//  5. AfterAgent (final state after successful completion)
// Tool call lifecycle:
//  1. eventSenderToolWrapper (internal - sends tool result events)
//  2. WrapToolInvoke / WrapEnhancedInvokableToolCall (user wrappers)
//  3. Tool.Invoke

type TypedChatModelMiddleware[M MessageType] interface {
	BeforeAgent(ctx context.Context, rc *ChatModelAgentContext) (context.Context, *ChatModelAgentContext, error)
	AfterAgent(ctx context.Context, state *TypedChatModelAgentState[M]) (context.Context, error)
	BeforeModelRewrite(ctx context.Context, state *TypedChatModelAgentState[M], mc *TypedModelContext[M]) (context.Context, *TypedChatModelAgentState[M], error)
	AfterModelRewrite(ctx context.Context, state *TypedChatModelAgentState[M], mc *TypedModelContext[M]) (context.Context, *TypedChatModelAgentState[M], error)
	WrapToolInvoke(ctx context.Context, ep InvokableToolEndpoint, tc *ToolContext) (InvokableToolEndpoint, error)
	WrapToolStream(ctx context.Context, ep StreamableToolEndpoint, tc *ToolContext) (StreamableToolEndpoint, error)
	// Enhanced tool wrappers: called for tools that return structured *schema.ToolResult
	WrapEnhancedInvokableToolCall(ctx context.Context, ep EnhancedInvokableToolEndpoint, tc *ToolContext) (EnhancedInvokableToolEndpoint, error)
	WrapEnhancedStreamableToolCall(ctx context.Context, ep EnhancedStreamableToolEndpoint, tc *ToolContext) (EnhancedStreamableToolEndpoint, error)
	WrapModel(ctx context.Context, m ChatModel[M], mc *TypedModelContext[M]) (ChatModel[M], error)
}

type ChatModelMiddleware = TypedChatModelMiddleware[*schema.Message]

// Alias names for Eino ADK compatibility.
// These allow middlewares to use the same naming convention as Eino's ADK.
type (
	BeforeModelRewriteState[M MessageType] = TypedChatModelAgentState[M]
	AfterModelRewriteState[M MessageType]  = TypedChatModelAgentState[M]
)

// BaseMiddleware provides no-op defaults for TypedChatModelMiddleware.
// Embed in custom middlewares to only override needed methods.
type BaseMiddleware[M MessageType] struct{}

func (b *BaseMiddleware[M]) BeforeAgent(ctx context.Context, rc *ChatModelAgentContext) (context.Context, *ChatModelAgentContext, error) { return ctx, rc, nil }
func (b *BaseMiddleware[M]) AfterAgent(ctx context.Context, state *TypedChatModelAgentState[M]) (context.Context, error) { return ctx, nil }
func (b *BaseMiddleware[M]) BeforeModelRewrite(ctx context.Context, state *TypedChatModelAgentState[M], mc *TypedModelContext[M]) (context.Context, *TypedChatModelAgentState[M], error) { return ctx, state, nil }
func (b *BaseMiddleware[M]) AfterModelRewrite(ctx context.Context, state *TypedChatModelAgentState[M], mc *TypedModelContext[M]) (context.Context, *TypedChatModelAgentState[M], error) { return ctx, state, nil }
func (b *BaseMiddleware[M]) WrapToolInvoke(_ context.Context, ep InvokableToolEndpoint, _ *ToolContext) (InvokableToolEndpoint, error) { return ep, nil }
func (b *BaseMiddleware[M]) WrapToolStream(_ context.Context, ep StreamableToolEndpoint, _ *ToolContext) (StreamableToolEndpoint, error) { return ep, nil }
func (b *BaseMiddleware[M]) WrapEnhancedInvokableToolCall(_ context.Context, ep EnhancedInvokableToolEndpoint, _ *ToolContext) (EnhancedInvokableToolEndpoint, error) { return ep, nil }
func (b *BaseMiddleware[M]) WrapEnhancedStreamableToolCall(_ context.Context, ep EnhancedStreamableToolEndpoint, _ *ToolContext) (EnhancedStreamableToolEndpoint, error) { return ep, nil }
func (b *BaseMiddleware[M]) WrapModel(_ context.Context, m ChatModel[M], _ *TypedModelContext[M]) (ChatModel[M], error) { return m, nil }
