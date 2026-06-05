package agentcore

import (
	"context"
	"io"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// ---- EventSenderModelWrapper ----

type eventSenderModelWrapper[M MessageType] struct {
	inner   ChatModel[M]
	execCtx *chatModelExecCtx[M]
}

func wrapModelWithEventSender[M MessageType](inner ChatModel[M], ec *chatModelExecCtx[M]) ChatModel[M] {
	return &eventSenderModelWrapper[M]{inner: inner, execCtx: ec}
}

func (w *eventSenderModelWrapper[M]) Generate(ctx context.Context, msgs []M, opts ...ModelOption) (M, error) {
	if w.execCtx != nil && w.execCtx.suppressEventSend {
		return w.inner.Generate(ctx, msgs, opts...)
	}
	resp, err := w.inner.Generate(ctx, msgs, opts...)
	if err != nil { return resp, err }
	if w.execCtx != nil && w.execCtx.generator != nil && !isNilMessage(resp) {
		w.execCtx.send(typedModelOutputEvent(resp, nil))
	}
	return resp, nil
}

func (w *eventSenderModelWrapper[M]) Stream(ctx context.Context, msgs []M, opts ...ModelOption) (*schema.StreamReader[M], error) {
	s, err := w.inner.Stream(ctx, msgs, opts...)
	if err != nil { return nil, err }
	if w.execCtx != nil && w.execCtx.suppressEventSend {
		return s, nil
	}
	r := schema.NewStreamReader[M]()
	go func() {
		defer r.Close()
		defer s.Close()
		var chunks []M
		for {
			c, err := s.Recv()
			if err == io.EOF { break }
			if err != nil { r.Send(c, err); return }
			chunks = append(chunks, c)
			r.Send(c, nil)
		}
		if len(chunks) > 0 && w.execCtx != nil {
			if merged, e := mergeChunks(chunks); e == nil {
				w.execCtx.send(typedModelOutputEvent(merged, nil))
			}
		}
	}()
	return r, nil
}

func (w *eventSenderModelWrapper[M]) BindTools(tools []*schema.ToolInfo) error { return w.inner.BindTools(tools) }

// ---- CallbackInjectionModelWrapper (for tracing/monitoring) ----

type callbackModelWrapper[M MessageType] struct {
	inner ChatModel[M]
}

func (w *callbackModelWrapper[M]) Generate(ctx context.Context, msgs []M, opts ...ModelOption) (M, error) {
	return w.inner.Generate(ctx, msgs, opts...)
}
func (w *callbackModelWrapper[M]) Stream(ctx context.Context, msgs []M, opts ...ModelOption) (*schema.StreamReader[M], error) {
	return w.inner.Stream(ctx, msgs, opts...)
}
func (w *callbackModelWrapper[M]) BindTools(tools []*schema.ToolInfo) error { return w.inner.BindTools(tools) }

// ---- Model Wrapper Chain Builder ----

// BuildModelWrapperChain builds the complete wrapper chain:
//
//	base → failover → retry → eventSender → user wrappers → callback
//
// The chain is built from innermost (closest to model) to outermost.
func BuildModelWrapperChain[M MessageType](base ChatModel[M], ec *chatModelExecCtx[M], cfg *ChatModelConfig[M]) ChatModel[M] {
	model := base

	// 1. Event sender (innermost — first to see raw model output)
	model = wrapModelWithEventSender(model, ec)

	// 2. Retry (wraps event sender so retries replay the entire inner chain)
	if cfg.RetryConfig != nil {
		model = newTypedRetryModelWrapper(model, cfg.RetryConfig)
	}

	// 3. Failover (wraps retry so each failover attempt gets retry behavior)
	if cfg.FailoverConfig != nil && len(cfg.FailoverConfig.Models) > 0 {
		allModels := append([]ChatModel[M]{base}, cfg.FailoverConfig.Models...)
		model = newFailoverModel(allModels)
	}

	// 4. User middleware wrappers (outermost)
	for _, mw := range cfg.Middlewares {
		if mw == nil { continue }
		mc := &TypedModelContext[M]{
			Tools: toolsToInfosTyped[M](cfg.Tools),
			ModelRetryConfig:    cfg.RetryConfig,
			ModelFailoverConfig: cfg.FailoverConfig,
		}
		wrapped, err := mw.WrapModel(context.Background(), model, mc)
		if err == nil && wrapped != nil { model = wrapped }
	}

	// 5. Callback injection (outermost — wraps everything)
	model = &callbackModelWrapper[M]{inner: model}

	return model
}
