package summarization

import (
	"context"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

type mockSummaryModel struct{}

func (m *mockSummaryModel) Generate(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.Message, error) {
	return &schema.Message{Role: schema.RoleAssistant, Content: "summary"}, nil
}
func (m *mockSummaryModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.StreamReader[*schema.Message], error) {
	return schema.StreamReaderFromArray([]*schema.Message{{Role: schema.RoleAssistant, Content: "summary"}}), nil
}
func (m *mockSummaryModel) BindTools(tools []*schema.ToolInfo) error { return nil }

func TestNew_NilConfig(t *testing.T) {
	model := &mockSummaryModel{}
	mw := New[*schema.Message](model, nil)
	if mw == nil {
		t.Fatal("nil middleware")
	}
}

func TestBeforeModelRewrite_UnderLimit(t *testing.T) {
	model := &mockSummaryModel{}
	mw := New[*schema.Message](model, &Config{MaxTokens: 100})

	state := &agentcore.TypedChatModelAgentState[*schema.Message]{
		Messages: []*schema.Message{
			schema.SystemMessage("system"),
			schema.UserMessage("short conversation"),
		},
	}
	mc := &agentcore.TypedModelContext[*schema.Message]{}

	_, newState, err := mw.BeforeModelRewrite(context.Background(), state, mc)
	if err != nil {
		t.Fatalf("BeforeModelRewrite: %v", err)
	}
	// Under limit — messages should be unchanged
	if len(newState.Messages) != 2 {
		t.Errorf("expected 2 messages under limit, got %d", len(newState.Messages))
	}
}

func TestBeforeModelRewrite_OverLimit(t *testing.T) {
	model := &mockSummaryModel{}
	mw := New[*schema.Message](model, &Config{MaxTokens: 3})

	var msgs []*schema.Message
	msgs = append(msgs, schema.SystemMessage("system"))
	for i := 0; i < 10; i++ {
		msgs = append(msgs, schema.UserMessage("message content"))
	}

	state := &agentcore.TypedChatModelAgentState[*schema.Message]{Messages: msgs}
	mc := &agentcore.TypedModelContext[*schema.Message]{}

	_, overState, err := mw.BeforeModelRewrite(context.Background(), state, mc)
	if err != nil {
		t.Fatalf("BeforeModelRewrite: %v", err)
	}
	// Over limit — should prepend summary + keep last 10
	if len(overState.Messages) <= 10 {
		t.Errorf("expected > 10 messages (summary prepended + last 10), got %d", len(overState.Messages))
	}
	firstMsg := overState.Messages[0]
	if firstMsg.Role != schema.RoleSystem {
		t.Error("first message should be summary system message")
	}
}

func TestMiddleware_SatisfiesInterface(t *testing.T) {
	model := &mockSummaryModel{}
	var _ agentcore.TypedChatModelMiddleware[*schema.Message] = New[*schema.Message](model, nil)
}
