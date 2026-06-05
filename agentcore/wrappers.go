package agentcore

import (
	"context"
	"io"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// EventSenderModelWrapper sends model output events through the agent's event stream.
type eventSenderModelWrapper[M MessageType] struct {
	inner  ChatModel[M]
	execCtx *chatModelExecCtx[M]
}

func wrapModelWithEventSender[M MessageType](inner ChatModel[M], ec *chatModelExecCtx[M]) ChatModel[M] {
	return &eventSenderModelWrapper[M]{inner: inner, execCtx: ec}
}

func (w *eventSenderModelWrapper[M]) Generate(ctx context.Context, msgs []M, opts ...modelOption) (M, error) {
	return w.inner.Generate(ctx, msgs, opts...)
}
func (w *eventSenderModelWrapper[M]) Stream(ctx context.Context, msgs []M, opts ...modelOption) (*schema.StreamReader[M], error) {
	s, err := w.inner.Stream(ctx, msgs, opts...)
	if err != nil { return nil, err }
	r := schema.NewStreamReader[M]()
	go func() {
		defer r.Close()
		var chunks []M
		for { c, err := s.Recv(); if err == io.EOF { break } else if err != nil { r.Send(c, err); return }
			chunks = append(chunks, c); r.Send(c, nil)
		}
		if len(chunks) > 0 {
			switch any(chunks[0]).(type) {
			case *schema.Message:
				ms := make([]*schema.Message, len(chunks))
				for i, c := range chunks { ms[i] = any(c).(*schema.Message) }
				if merged, err := schema.ConcatMessages(ms); err == nil {
					w.execCtx.send(typedModelOutputEvent(any(merged).(M), nil))
				}
			}
		}
	}()
	return r, nil
}
func (w *eventSenderModelWrapper[M]) BindTools(tools []*schema.ToolInfo) error { return w.inner.BindTools(tools) }

// BuildModelWrapperChain builds a chain of model wrappers around the base model.
func BuildModelWrapperChain[M MessageType](base ChatModel[M], ec *chatModelExecCtx[M], cfg *ChatModelConfig[M]) ChatModel[M] {
	model := base
	for _, mw := range cfg.Middlewares {
		if mw == nil { continue }
		mc := &TypedModelContext[M]{Tools: toolsToInfosTyped[M](cfg.Tools), ModelRetryConfig: cfg.RetryConfig, ModelFailoverConfig: cfg.FailoverConfig}
		wrapped, err := mw.WrapModel(context.Background(), model, mc)
		if err == nil && wrapped != nil { model = wrapped }
	}
	return model
}
