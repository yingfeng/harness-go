package planexecute

import (
	"context"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

type mockModel struct{}

func (m *mockModel) Generate(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.Message, error) {
	return &schema.Message{Role: schema.RoleAssistant, Content: "plan result"}, nil
}
func (m *mockModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.StreamReader[*schema.Message], error) {
	return schema.StreamReaderFromArray([]*schema.Message{{Role: schema.RoleAssistant, Content: "plan stream"}}), nil
}
func (m *mockModel) BindTools(tools []*schema.ToolInfo) error { return nil }

func TestNewTyped_DefaultValues(t *testing.T) {
	cfg := &Config{Model: &mockModel{}}
	agent := NewTyped(cfg)
	if agent == nil { t.Fatal("nil agent") }
	name := agent.Name(context.Background())
	if name != "plan_execute_agent" {
		t.Errorf("expected name 'plan_execute_agent', got %q", name)
	}
}

func TestNewTyped_CustomName(t *testing.T) {
	cfg := &Config{Model: &mockModel{}, Name: "custom_planner"}
	agent := NewTyped(cfg)
	if agent == nil { t.Fatal("nil agent") }
	name := agent.Name(context.Background())
	if name != "custom_planner" {
		t.Errorf("name = %q", name)
	}
}

func TestNewTyped_MaxIterationsZero(t *testing.T) {
	cfg := &Config{Model: &mockModel{}, MaxIterations: 0}
	agent := NewTyped(cfg)
	if agent == nil { t.Fatal("nil agent") }
}

func TestNew(t *testing.T) {
	cfg := &Config{Model: &mockModel{}}
	agent := New(cfg)
	if agent == nil { t.Fatal("nil agent") }
}

func TestPrompt(t *testing.T) {
	prompt := Prompt()
	if prompt == "" {
		t.Error("empty prompt")
	}
}
