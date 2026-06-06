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
// middlewares and the retry/failover chain. LangGraph-Go simplifies by folding these
// concerns into callbackModelWrapper and eventSenderModelWrapper; this wrapper adds
// the missing deep-copy layer that prevents pointer-sharing bugs in the chain.
type typedStateModelWrapper[M MessageType] struct {
	inner ChatModel[M]
}

func newTypedStateModelWrapper[M MessageType](inner ChatModel[M]) ChatModel[M] {
	return &typedStateModelWrapper[M]{inner: inner}
}

// copyMessage performs a deep copy of a Message or AgenticMessage to prevent
// pointer-sharing bugs when the same message flows through multiple wrappers.
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

func (w *typedStateModelWrapper[M]) Generate(ctx context.Context, msgs []M, opts ...ModelOption) (M, error) {
	// 1. Deep copy input messages (prevent middleware chain side-effects)
	copied := make([]M, len(msgs))
	for i, m := range msgs {
		copied[i] = copyMessage(m)
	}

	// 2. Inject message IDs
	for _, m := range copied {
		switch v := any(m).(type) {
		case *schema.Message:
			if v.Extra == nil {
				v.Extra = make(map[string]any)
			}
			v.Extra = EnsureMessageID(v.Extra)
		}
	}

	// 3. Call inner model
	resp, err := w.inner.Generate(ctx, copied, opts...)
	if err != nil {
		return resp, err
	}

	// 4. Deep copy response before returning
	return copyMessage(resp), nil
}

func (w *typedStateModelWrapper[M]) Stream(ctx context.Context, msgs []M, opts ...ModelOption) (*schema.StreamReader[M], error) {
	// 1. Deep copy input messages
	copied := make([]M, len(msgs))
	for i, m := range msgs {
		copied[i] = copyMessage(m)
	}

	// 2. Inject message IDs
	for _, m := range copied {
		switch v := any(m).(type) {
		case *schema.Message:
			if v.Extra == nil {
				v.Extra = make(map[string]any)
			}
			v.Extra = EnsureMessageID(v.Extra)
		}
	}

	// 3. Stream from inner
	s, err := w.inner.Stream(ctx, copied, opts...)
	if err != nil {
		return nil, err
	}

	// 4. Wrap stream to deep-copy each chunk
	r := schema.NewStreamReader[M]()
	go func() {
		defer r.Close()
		defer s.Close()
		for {
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
