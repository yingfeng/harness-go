package deep

import (
	"context"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

type mockModel struct{}

func (m *mockModel) Generate(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.Message, error) {
	return &schema.Message{Role: schema.RoleAssistant, Content: "deep result"}, nil
}
func (m *mockModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.StreamReader[*schema.Message], error) {
	return schema.StreamReaderFromArray([]*schema.Message{{Role: schema.RoleAssistant, Content: "deep stream"}}), nil
}
func (m *mockModel) BindTools(tools []*schema.ToolInfo) error { return nil }

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Name != "deep_agent" {
		t.Errorf("expected name 'deep_agent', got %q", cfg.Name)
	}
	if cfg.MaxIterations != 20 {
		t.Errorf("expected MaxIterations 20, got %d", cfg.MaxIterations)
	}
}

func TestNewTyped_NilConfig(t *testing.T) {
	agent := NewTyped(nil)
	if agent == nil {
		t.Fatal("nil agent for nil config")
	}
}

func TestNewTyped_WithModel(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Model = &mockModel{}
	agent := NewTyped(cfg)
	if agent == nil { t.Fatal("nil agent") }
	name := agent.Name(context.Background())
	if name != "deep_agent" {
		t.Errorf("name = %q", name)
	}
}

func TestNew(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Model = &mockModel{}
	agent := New(cfg)
	if agent == nil { t.Fatal("nil agent") }
	_ = agent
}

func TestPrompt(t *testing.T) {
	prompt := Prompt()
	if prompt == "" {
		t.Error("empty prompt")
	}
}

func TestSelectPrompt(t *testing.T) {
	prompt := SelectPrompt("en")
	if prompt == "" {
		t.Error("empty select prompt")
	}
}
