package patchtoolcalls

import (
	"context"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

func TestBeforeModelRewrite_InsertsPlaceholders(t *testing.T) {
	mw := New[*schema.Message]()

	state := &agentcore.TypedChatModelAgentState[*schema.Message]{
		Messages: []*schema.Message{
			schema.UserMessage("hello"),
			{
				Role:      schema.RoleAssistant,
				Content:   "",
				ToolCalls: []schema.ToolCall{{ID: "tc1", Function: schema.ToolCallFunction{Name: "search", Arguments: "{}"}}},
			},
			// Missing tool result — next message is user, not tool
			schema.UserMessage("continue"),
		},
	}
	mc := &agentcore.TypedModelContext[*schema.Message]{}

	_, newState, err := mw.BeforeModelRewrite(context.Background(), state, mc)
	if err != nil {
		t.Fatalf("BeforeModelRewrite: %v", err)
	}

	// Should have inserted a placeholder tool message
	foundPlaceholder := false
	for _, m := range newState.Messages {
		if m.Role == schema.RoleTool && contains(m.Content, "not completed") {
			foundPlaceholder = true
		}
	}
	if !foundPlaceholder {
		t.Error("placeholder tool message not inserted for incomplete tool call")
	}
}

func TestBeforeModelRewrite_CompleteToolCall(t *testing.T) {
	mw := New[*schema.Message]()

	state := &agentcore.TypedChatModelAgentState[*schema.Message]{
		Messages: []*schema.Message{
			schema.UserMessage("hello"),
			{
				Role:      schema.RoleAssistant,
				Content:   "",
				ToolCalls: []schema.ToolCall{{ID: "tc1", Function: schema.ToolCallFunction{Name: "search", Arguments: "{}"}}},
			},
			schema.ToolMessage("search result", "tc1"), // proper tool result
		},
	}
	mc := &agentcore.TypedModelContext[*schema.Message]{}

	_, newState, err := mw.BeforeModelRewrite(context.Background(), state, mc)
	if err != nil {
		t.Fatalf("BeforeModelRewrite: %v", err)
	}
	// Should NOT insert placeholder — tool call already has result
	if len(newState.Messages) != 3 {
		t.Errorf("unexpected message count: %d (should stay 3)", len(newState.Messages))
	}
}

func contains(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub { return true }
	}
	return false
}
