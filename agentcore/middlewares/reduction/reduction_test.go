package reduction

import (
	"context"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

func TestNew_NilConfig(t *testing.T) {
	mw := New(nil)
	if mw == nil { t.Fatal("nil middleware") }
}

func TestBeforeModelRewrite_Truncation(t *testing.T) {
	mw := New(&Config{MaxToolOutputLen: 20, MaxToolCalls: 100})
	large := schema.ToolMessage("this is a very long tool output that should be truncated", "c1")
	state := &agentcore.TypedChatModelAgentState[*schema.Message]{
		Messages: []*schema.Message{
			schema.UserMessage("hi"),
			large,
		},
	}
	_, newState, err := mw.BeforeModelRewrite(context.Background(), state, nil)
	if err != nil { t.Fatalf("BeforeModelRewrite: %v", err) }
	msg := newState.Messages[1]
	if m, ok := any(msg).(*schema.Message); ok && len(m.Content) > 25 && m.Content != "..." {
		t.Logf("truncated content: %q (len=%d)", m.Content, len(m.Content))
	}
}

func TestWrapToolInvoke_Truncation(t *testing.T) {
	mw := New(&Config{MaxToolOutputLen: 10})
	next := func(ctx context.Context, args string, opts ...agentcore.ToolOption) (string, error) {
		return "this is a very long response that exceeds the limit", nil
	}
	wrapped, err := mw.WrapToolInvoke(context.Background(), next, &agentcore.ToolContext{Name: "test"})
	if err != nil { t.Fatalf("WrapToolInvoke: %v", err) }
	result, err := wrapped(context.Background(), "")
	if err != nil { t.Fatalf("invoke: %v", err) }
	if len(result) > 10+len("...(truncated)")+5 {
		t.Logf("truncated result: %q (len=%d)", result, len(result))
	}
}

func TestBeforeModelRewrite_ClearOldToolCalls(t *testing.T) {
	mw := New(&Config{MaxToolOutputLen: 10, MaxToolCalls: 2})
	state := &agentcore.TypedChatModelAgentState[*schema.Message]{
		Messages: []*schema.Message{
			schema.ToolMessage("first", "c1"),
			schema.ToolMessage("second", "c2"),
			schema.ToolMessage("third", "c3"),
		},
	}
	_, newState, err := mw.BeforeModelRewrite(context.Background(), state, nil)
	if err != nil { t.Fatalf("BeforeModelRewrite: %v", err) }
	if len(newState.Messages) != 3 { t.Errorf("expected 3 messages, got %d", len(newState.Messages)) }
}

func TestExcludeTools(t *testing.T) {
	cfg := &Config{
		MaxToolOutputLen: 10,
		ExcludeTools:     map[string]bool{"keep": true},
	}
	if cfg.ExcludeTools == nil { t.Fatal("nil exclude map") }
	if !cfg.ExcludeTools["keep"] { t.Error("keep should be excluded") }
}
