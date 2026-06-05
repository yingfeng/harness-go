package reduction

import (
	"context"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

func TestNew_NilConfig(t *testing.T) {
	mw := New[*schema.Message](nil)
	if mw == nil {
		t.Fatal("nil middleware")
	}
}

func TestBeforeModelRewrite_Truncation(t *testing.T) {
	mw := New[*schema.Message](&Config{MaxToolOutputLen: 20, MaxToolCalls: 100})

	longOutput := make([]byte, 100)
	for i := range longOutput { longOutput[i] = 'x' }

	state := &agentcore.TypedChatModelAgentState[*schema.Message]{
		Messages: []*schema.Message{
			schema.UserMessage("user msg"),
			schema.ToolMessage(string(longOutput), "call1"),
			schema.ToolMessage("short output", "call2"),
		},
	}
	mc := &agentcore.TypedModelContext[*schema.Message]{}

	ctx, newState, err := mw.BeforeModelRewrite(context.Background(), state, mc)
	if err != nil {
		t.Fatalf("BeforeModelRewrite: %v", err)
	}
	if ctx == nil {
		t.Fatal("nil ctx")
	}
	// First tool message should be truncated
	msg1 := newState.Messages[1]
	if len(msg1.Content) > len(longOutput)+len(truncSuffix) {
		t.Error("long tool output was not truncated")
	}
	// Second should remain unchanged
	msg2 := newState.Messages[2]
	if msg2.Content != "short output" {
		t.Errorf("short output changed: %s", msg2.Content)
	}
}

func TestBeforeModelRewrite_MaxCallsExceeded(t *testing.T) {
	mw := New[*schema.Message](&Config{MaxToolOutputLen: 1000, MaxToolCalls: 2})

	var msgs []*schema.Message
	msgs = append(msgs, schema.UserMessage("start"))
	for i := 0; i < 5; i++ {
		msgs = append(msgs, schema.ToolMessage("output "+string(rune('a'+i)), "call"))
	}

	state := &agentcore.TypedChatModelAgentState[*schema.Message]{Messages: msgs}
	mc := &agentcore.TypedModelContext[*schema.Message]{}

	_, maxState, err := mw.BeforeModelRewrite(context.Background(), state, mc)
	if err != nil {
		t.Fatalf("BeforeModelrewrite: %v", err)
	}
	// Messages after max calls should be replaced with "..."
	replacedCount := 0
	for i, m := range maxState.Messages {
		if m.Role == schema.RoleTool && m.Content == "..." {
			replacedCount++
			t.Logf("message %d replaced with ...", i)
		}
	}
	if replacedCount < 3 {
		t.Errorf("expected >= 3 replaced messages (5 total, max 2 allowed), got %d", replacedCount)
	}
}

func TestMiddleware_SatisfiesInterface(t *testing.T) {
	var _ agentcore.TypedChatModelMiddleware[*schema.Message] = New[*schema.Message](nil)
}
