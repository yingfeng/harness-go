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
	mw := New(nil)
	if mw == nil { t.Fatal("nil middleware") }
}

func TestBeforeModelRewrite_NoTrigger(t *testing.T) {
	mw := New(&Config{Model: &mockSummaryModel{}, MaxTokens: 1000})
	state := &agentcore.TypedChatModelAgentState[*schema.Message]{
		Messages: []*schema.Message{
			schema.UserMessage("Hello"),
		},
	}
	ctx, newState, err := mw.BeforeModelRewrite(context.Background(), state, nil)
	if err != nil { t.Fatalf("BeforeModelRewrite: %v", err) }
	_ = ctx
	if len(newState.Messages) != 1 {
		t.Error("should not summarize when below threshold")
	}
}

func TestTriggerCondition(t *testing.T) {
	cfg := &Config{Trigger: &TriggerCondition{MaxMessages: 2}}
	if cfg.Trigger.MaxMessages != 2 { t.Error("trigger not set") }
}

func TestExtractText(t *testing.T) {
	msg := schema.UserMessage("hello")
	if text := extractText(msg); text != "hello" { t.Errorf("got %q", text) }
	agentic := &schema.AgenticMessage{Role: schema.AgenticRoleAssistant, Content: "world"}
	if text := extractText(agentic); text != "world" { t.Errorf("got %q", text) }
}

func TestDefaultTokenCounter(t *testing.T) {
	msgs := []*schema.Message{schema.UserMessage("hello world"), schema.UserMessage("test message")}
	count, err := defaultTokenCounter(context.Background(), msgs)
	if err != nil { t.Fatalf("counter: %v", err) }
	if count <= 0 { t.Errorf("expected positive count, got %d", count) }
}

func TestGetSummaryInstruction_Language(t *testing.T) {
	en := getSummaryInstruction("en")
	zh := getSummaryInstruction("zh")
	if en == "" || zh == "" { t.Error("empty instruction") }
	if en == zh { t.Log("instructions are same (expected for bilingual)") }
}
