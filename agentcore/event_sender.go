package agentcore

import (
	"context"
	"io"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// ---- NewEventSenderModelWrapper creates a handler that sends model output events.
// Place this in the Handlers chain to control WHERE events are emitted:
// - Innermost position (last in Handlers list): events contain original (unmodified) model output
// - Outermost position (first in Handlers list): events contain fully processed output
//
// When detected in Handlers, the framework skips its built-in event sender to avoid duplicates.
func NewEventSenderModelWrapper[M MessageType]() *eventSenderModelHandler[M] {
	return &eventSenderModelHandler[M]{}
}

type eventSenderModelHandler[M MessageType] struct{}

func (h *eventSenderModelHandler[M]) WrapModel(ctx context.Context, m Model[M], mc *TypedModelContext[M]) (Model[M], error) {
	ec := getReActExecCtx[M](ctx)
	if ec == nil { return m, nil }
	return wrapModelWithEventSender(m, ec), nil
}

// All other middleware methods are no-op
func (h *eventSenderModelHandler[M]) BeforeAgent(ctx context.Context, rc *ReActAgentContext) (context.Context, *ReActAgentContext, error) { return ctx, rc, nil }
func (h *eventSenderModelHandler[M]) AfterAgent(ctx context.Context, state *TypedReActAgentState[M]) (context.Context, error) { return ctx, nil }
func (h *eventSenderModelHandler[M]) BeforeModelRewrite(ctx context.Context, state *TypedReActAgentState[M], mc *TypedModelContext[M]) (context.Context, *TypedReActAgentState[M], error) { return ctx, state, nil }
func (h *eventSenderModelHandler[M]) AfterModelRewrite(ctx context.Context, state *TypedReActAgentState[M], mc *TypedModelContext[M]) (context.Context, *TypedReActAgentState[M], error) { return ctx, state, nil }
func (h *eventSenderModelHandler[M]) WrapToolInvoke(_ context.Context, ep InvokableToolEndpoint, _ *ToolContext) (InvokableToolEndpoint, error) { return ep, nil }
func (h *eventSenderModelHandler[M]) WrapToolStream(_ context.Context, ep StreamableToolEndpoint, _ *ToolContext) (StreamableToolEndpoint, error) { return ep, nil }
func (h *eventSenderModelHandler[M]) WrapEnhancedInvokableToolCall(_ context.Context, ep EnhancedInvokableToolEndpoint, _ *ToolContext) (EnhancedInvokableToolEndpoint, error) { return ep, nil }
func (h *eventSenderModelHandler[M]) WrapEnhancedStreamableToolCall(_ context.Context, ep EnhancedStreamableToolEndpoint, _ *ToolContext) (EnhancedStreamableToolEndpoint, error) { return ep, nil }

// ---- NewEventSenderToolWrapper creates a handler that sends tool result events.
// Place this in the Handlers chain to control WHERE tool events are emitted.
func NewEventSenderToolWrapper[M MessageType]() *eventSenderToolHandler[M] {
	return &eventSenderToolHandler[M]{}
}

type eventSenderToolHandler[M MessageType] struct{}

func (h *eventSenderToolHandler[M]) WrapToolInvoke(ctx context.Context, next InvokableToolEndpoint, tc *ToolContext) (InvokableToolEndpoint, error) {
	ec := getReActExecCtx[M](ctx)
	if ec == nil { return next, nil }
	name := tc.Name
	callID := tc.CallID

	return func(ctx context.Context, args string, opts ...ToolOption) (string, error) {
		result, err := next(ctx, args, opts...)

		// Emit tool result event after all processing
		if ec != nil && ec.generator != nil {
			msg := schema.ToolMessage(result, callID)
			ev := typedEventFromMessage(any(msg).(M), nil, schema.RoleTool, name)
			ec.send(ev)
		}
		return result, err
	}, nil
}

func (h *eventSenderToolHandler[M]) WrapToolStream(ctx context.Context, next StreamableToolEndpoint, tc *ToolContext) (StreamableToolEndpoint, error) {
	ec := getReActExecCtx[M](ctx)
	if ec == nil { return next, nil }
	name := tc.Name
	callID := tc.CallID

	return func(ctx context.Context, args string, opts ...ToolOption) (*schema.StreamReader[string], error) {
		s, err := next(ctx, args, opts...)
		if err != nil { return s, err }

		// Wrap stream to collect and emit final result event
		r := schema.NewStreamReader[string]()
		go func() {
			defer r.Close()
			defer s.Close()
			var chunks []string
			for {
				c, e := s.Recv()
				if e == io.EOF { break }
				if e != nil { r.Send(c, e); return }
				chunks = append(chunks, c)
				r.Send(c, nil)
			}

			if ec != nil && ec.generator != nil && len(chunks) > 0 {
				content := ""
				for _, ch := range chunks { content += ch }
				msg := schema.ToolMessage(content, callID)
				ev := typedEventFromMessage(any(msg).(M), nil, schema.RoleTool, name)
				ec.send(ev)
			}
		}()
		return r, nil
	}, nil
}

func (h *eventSenderToolHandler[M]) WrapEnhancedInvokableToolCall(ctx context.Context, next EnhancedInvokableToolEndpoint, tc *ToolContext) (EnhancedInvokableToolEndpoint, error) {
	ec := getReActExecCtx[M](ctx)
	if ec == nil { return next, nil }
	name := tc.Name
	callID := tc.CallID

	return func(ctx context.Context, args *schema.ToolArgument, opts ...ToolOption) (*schema.ToolResult, error) {
		result, err := next(ctx, args, opts...)

		if ec != nil && ec.generator != nil && result != nil {
			// Check if the result has multimodal extra content (e.g., content blocks, file references)
			content := result.Content
			if content == "" { content = result.Error }

			msg := schema.ToolMessage(content, callID)
			msg.Name = name

			// If Extra contains content blocks or multimodal data, attach them via Extra
			if result.Extra != nil {
				if msg.Extra == nil {
					msg.Extra = make(map[string]any)
				}
				for k, v := range result.Extra {
					msg.Extra[k] = v
				}
			}
			ev := typedEventFromMessage(any(msg).(M), nil, schema.RoleTool, name)
			ec.send(ev)
		}
		return result, err
	}, nil
}

func (h *eventSenderToolHandler[M]) WrapEnhancedStreamableToolCall(ctx context.Context, next EnhancedStreamableToolEndpoint, tc *ToolContext) (EnhancedStreamableToolEndpoint, error) {
	ec := getReActExecCtx[M](ctx)
	if ec == nil { return next, nil }
	name := tc.Name
	callID := tc.CallID

	return func(ctx context.Context, args *schema.ToolArgument, opts ...ToolOption) (*schema.StreamReader[*schema.ToolResult], error) {
		s, err := next(ctx, args, opts...)
		if err != nil { return s, err }

		r := schema.NewStreamReader[*schema.ToolResult]()
		go func() {
			defer r.Close()
			defer s.Close()
			var results []*schema.ToolResult
			for {
				c, e := s.Recv()
				if e == io.EOF { break }
				if e != nil { r.Send(c, e); return }
				results = append(results, c)
				r.Send(c, nil)
			}

			if ec != nil && ec.generator != nil && len(results) > 0 {
				last := results[len(results)-1]
				content := last.Content
				if content == "" { content = last.Error }
				msg := schema.ToolMessage(content, callID)
				msg.Name = name

				// If Extra contains multimodal data, propagate to event message
				if last.Extra != nil {
					if msg.Extra == nil {
						msg.Extra = make(map[string]any)
					}
					for k, v := range last.Extra {
						msg.Extra[k] = v
					}
				}

				ev := typedEventFromMessage(any(msg).(M), nil, schema.RoleTool, name)
				ec.send(ev)
			}
		}()
		return r, nil
	}, nil
}

// No-op for remaining methods
func (h *eventSenderToolHandler[M]) BeforeAgent(ctx context.Context, rc *ReActAgentContext) (context.Context, *ReActAgentContext, error) { return ctx, rc, nil }
func (h *eventSenderToolHandler[M]) AfterAgent(ctx context.Context, state *TypedReActAgentState[M]) (context.Context, error) { return ctx, nil }
func (h *eventSenderToolHandler[M]) BeforeModelRewrite(ctx context.Context, state *TypedReActAgentState[M], mc *TypedModelContext[M]) (context.Context, *TypedReActAgentState[M], error) { return ctx, state, nil }
func (h *eventSenderToolHandler[M]) AfterModelRewrite(ctx context.Context, state *TypedReActAgentState[M], mc *TypedModelContext[M]) (context.Context, *TypedReActAgentState[M], error) { return ctx, state, nil }
func (h *eventSenderToolHandler[M]) WrapModel(ctx context.Context, m Model[M], mc *TypedModelContext[M]) (Model[M], error) { return m, nil }

// HasUserEventSenderToolWrapper checks if the handlers list contains a user-provided
// NewEventSenderToolWrapper. When present, the framework skips its internal default
// tool event sender to avoid duplicate events.
func HasUserEventSenderToolWrapper[M MessageType](handlers []TypedReActMiddleware[M]) bool {
	for _, h := range handlers {
		if _, ok := h.(*eventSenderToolHandler[M]); ok {
			return true
		}
	}
	return false
}

// HasUserEventSenderModelWrapper checks if the handlers list contains a user-provided
// NewEventSenderModelWrapper. When present, the framework skips its internal default
// model event sender to avoid duplicate events.
func HasUserEventSenderModelWrapper[M MessageType](handlers []TypedReActMiddleware[M]) bool {
	for _, h := range handlers {
		if _, ok := h.(*eventSenderModelHandler[M]); ok {
			return true
		}
	}
	return false
}

// ---- Tool event constructors (adapted from the ADK wrappers.go) ----
//
// These constructors create properly typed events for different tool invocation

// TypedToolInvokeEvent creates an event for a synchronous tool result.
func TypedToolInvokeEvent[M MessageType](result string, tc *ToolContext) *TypedAgentEvent[M] {
	msg := schema.ToolMessage(result, tc.CallID)
	return typedEventFromMessage(any(msg).(M), nil, schema.RoleTool, tc.Name)
}

// TypedToolStreamEvent creates an event for a streaming tool result.
func TypedToolStreamEvent[M MessageType](resultChunks []string, tc *ToolContext) *TypedAgentEvent[M] {
	content := ""
	for _, ch := range resultChunks {
		content += ch
	}
	msg := schema.ToolMessage(content, tc.CallID)
	return typedEventFromMessage(any(msg).(M), nil, schema.RoleTool, tc.Name)
}

// TypedEnhancedToolInvokeEvent creates an event for an enhanced tool result.
// Propagates Extra metadata for multimodal support.
func TypedEnhancedToolInvokeEvent[M MessageType](result *schema.ToolResult, tc *ToolContext) *TypedAgentEvent[M] {
	content := result.Content
	if content == "" {
		content = result.Error
	}
	msg := schema.ToolMessage(content, tc.CallID)
	msg.Name = tc.Name
	if result.Extra != nil {
		if msg.Extra == nil {
			msg.Extra = make(map[string]any, len(result.Extra))
		}
		for k, v := range result.Extra {
			msg.Extra[k] = v
		}
	}
	return typedEventFromMessage(any(msg).(M), nil, schema.RoleTool, tc.Name)
}

// TypedEnhancedToolStreamEvent creates an event for a streaming enhanced tool result.
// Propagates the last result's Extra metadata.
func TypedEnhancedToolStreamEvent[M MessageType](results []*schema.ToolResult, tc *ToolContext) *TypedAgentEvent[M] {
	if len(results) == 0 {
		return nil
	}
	last := results[len(results)-1]
	content := last.Content
	if content == "" {
		content = last.Error
	}
	msg := schema.ToolMessage(content, tc.CallID)
	msg.Name = tc.Name
	if last.Extra != nil {
		if msg.Extra == nil {
			msg.Extra = make(map[string]any, len(last.Extra))
		}
		for k, v := range last.Extra {
			msg.Extra[k] = v
		}
	}
	return typedEventFromMessage(any(msg).(M), nil, schema.RoleTool, tc.Name)
}
