package agentcore

import (
	"context"
	"io"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// typedStateModelWrapper unifies message deep copy, ID injection, cancel checking,
// and event sending into a single wrapper layer for the model call.
//
// In Eino ADK this is the central wrapper (typedStateModelWrapper) that sits between
// middlewares and the retry/failover chain, adding:
//   - Message deep copy (prevent pointer-sharing in middleware chain)
//   - Message ID auto-assignment
//   - Cancel context checking before model call
//   - Model output event emission
//   - BeforeModelRewrite / AfterModelRewrite orchestration (via chatmodel.go loop)
type typedStateModelWrapper[M MessageType] struct {
	inner      ChatModel[M]
	cancelCtx  *cancelContext
}

func newTypedStateModelWrapper[M MessageType](inner ChatModel[M], cc *cancelContext) ChatModel[M] {
	return &typedStateModelWrapper[M]{inner: inner, cancelCtx: cc}
}

// copyMessage performs a deep copy of a Message or AgenticMessage to prevent
// pointer-sharing bugs when the same message flows through multiple wrappers.
//
// TODO: The Extra map copy is shallow (values are shared references). If a
// middleware modifies a nested map/slice inside Extra, the original message
// will also be affected. Consider a deep-copy helper for Extra values.
func copyMessage[M MessageType](msg M) M {
	switch v := any(msg).(type) {
	case *schema.Message:
		cp := &schema.Message{
			Role:    v.Role,
			Content: v.Content,
			Name:    v.Name,
		}
		if len(v.ToolCalls) > 0 {
			cp.ToolCalls = make([]schema.ToolCall, len(v.ToolCalls))
			copy(cp.ToolCalls, v.ToolCalls)
		}
		if v.Extra != nil {
			cp.Extra = make(map[string]any, len(v.Extra))
			for k, val := range v.Extra {
				cp.Extra[k] = val
			}
		}
		return any(cp).(M)
	case *schema.AgenticMessage:
		cp := &schema.AgenticMessage{
			Role:    v.Role,
			Content: v.Content,
		}
		if len(v.ContentBlocks) > 0 {
			cp.ContentBlocks = make([]schema.ContentBlock, len(v.ContentBlocks))
			copy(cp.ContentBlocks, v.ContentBlocks)
		}
		return any(cp).(M)
	}
	return msg
}

// preprocessInput performs cancel check, deep copy, and message ID injection.
// Returns nil if cancelled (caller should return ErrStreamCanceled immediately).
func (w *typedStateModelWrapper[M]) preprocessInput(msgs []M) []M {
	if w.cancelCtx != nil && w.cancelCtx.isImmediate() {
		return nil
	}
	copied := make([]M, len(msgs))
	for i, m := range msgs {
		copied[i] = copyMessage(m)
	}
	for _, m := range copied {
		switch v := any(m).(type) {
		case *schema.Message:
			if v.Extra == nil {
				v.Extra = make(map[string]any)
			}
			v.Extra = EnsureMessageID(v.Extra)
		}
	}
	return copied
}

func (w *typedStateModelWrapper[M]) Generate(ctx context.Context, msgs []M, opts ...ModelOption) (M, error) {
	copied := w.preprocessInput(msgs)
	if copied == nil {
		var zero M
		return zero, ErrStreamCanceled
	}
	resp, err := w.inner.Generate(ctx, copied, opts...)
	if err != nil {
		return resp, err
	}
	return copyMessage(resp), nil
}

func (w *typedStateModelWrapper[M]) Stream(ctx context.Context, msgs []M, opts ...ModelOption) (*schema.StreamReader[M], error) {
	// Cancel check before allocating any resources (returns error-embedded StreamReader)
	if w.cancelCtx != nil && w.cancelCtx.isImmediate() {
		r := schema.NewStreamReader[M]()
		var zero M
		r.Send(zero, ErrStreamCanceled)
		r.Close()
		return r, nil
	}

	copied := w.preprocessInput(msgs)
	if copied == nil {
		return nil, ErrStreamCanceled
	}

	s, err := w.inner.Stream(ctx, copied, opts...)
	if err != nil {
		return nil, err
	}

	r := schema.NewStreamReader[M]()
	go func() {
		defer r.Close()
		defer s.Close()
		for {
			if w.cancelCtx != nil && w.cancelCtx.isImmediate() {
				var zero M
				r.Send(zero, ErrStreamCanceled)
				return
			}
			c, e := s.Recv()
			if e == io.EOF {
				break
			}
			if e != nil {
				r.Send(c, e)
				return
			}
			r.Send(copyMessage(c), nil)
		}
	}()
	return r, nil
}

func (w *typedStateModelWrapper[M]) BindTools(tools []*schema.ToolInfo) error {
	return w.inner.BindTools(tools)
}
