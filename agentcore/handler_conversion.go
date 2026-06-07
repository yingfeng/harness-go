package agentcore

import (
	"context"
)

// toolMiddlewareAdapter wraps a TypedReActMiddleware[M] as tool middleware.
// It delegates WrapToolInvoke/WrapToolStream/WrapEnhancedInvokableToolCall/WrapEnhancedStreamableToolCall
// from the middleware interface.
type toolMiddlewareAdapter[M MessageType] struct {
	mw TypedReActMiddleware[M]
}

func (a *toolMiddlewareAdapter[M]) WrapInvokableToolCall(ctx context.Context, ep InvokableToolEndpoint, tc *ToolContext) (InvokableToolEndpoint, error) {
	return a.mw.WrapToolInvoke(ctx, ep, tc)
}

func (a *toolMiddlewareAdapter[M]) WrapStreamableToolCall(ctx context.Context, ep StreamableToolEndpoint, tc *ToolContext) (StreamableToolEndpoint, error) {
	return a.mw.WrapToolStream(ctx, ep, tc)
}

func (a *toolMiddlewareAdapter[M]) WrapEnhancedInvokableToolCall(ctx context.Context, ep EnhancedInvokableToolEndpoint, tc *ToolContext) (EnhancedInvokableToolEndpoint, error) {
	return a.mw.WrapEnhancedInvokableToolCall(ctx, ep, tc)
}

func (a *toolMiddlewareAdapter[M]) WrapEnhancedStreamableToolCall(ctx context.Context, ep EnhancedStreamableToolEndpoint, tc *ToolContext) (EnhancedStreamableToolEndpoint, error) {
	return a.mw.WrapEnhancedStreamableToolCall(ctx, ep, tc)
}

// HandlersToToolMiddlewares converts middleware handlers to tool-level middleware adapters.
// The returned list can be used by ToolsNode to wrap tool endpoints.
func HandlersToToolMiddlewares[M MessageType](handlers []TypedReActMiddleware[M]) []*toolMiddlewareAdapter[M] {
	if len(handlers) == 0 {
		return nil
	}
	adapters := make([]*toolMiddlewareAdapter[M], 0, len(handlers))
	for _, h := range handlers {
		if h == nil {
			continue
		}
		adapters = append(adapters, &toolMiddlewareAdapter[M]{mw: h})
	}
	return adapters
}

// ToolMiddleware is the interface that tool-level middleware adapters satisfy.
// It wraps tool invocation endpoints.
type ToolMiddleware[M MessageType] interface {
	WrapInvokableToolCall(ctx context.Context, ep InvokableToolEndpoint, tc *ToolContext) (InvokableToolEndpoint, error)
	WrapStreamableToolCall(ctx context.Context, ep StreamableToolEndpoint, tc *ToolContext) (StreamableToolEndpoint, error)
	WrapEnhancedInvokableToolCall(ctx context.Context, ep EnhancedInvokableToolEndpoint, tc *ToolContext) (EnhancedInvokableToolEndpoint, error)
	WrapEnhancedStreamableToolCall(ctx context.Context, ep EnhancedStreamableToolEndpoint, tc *ToolContext) (EnhancedStreamableToolEndpoint, error)
}
