package planexecute

import (
	"context"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

func TestNewTyped_DefaultValues(t *testing.T) {
	model := &fakePEModel{}
	cfg := &Config{Model: model}
	a := NewTyped(cfg)
	if a == nil {
		t.Fatal("nil agent")
	}
	if a.Name(nil) != "plan_execute_agent" {
		t.Errorf("default name = %s", a.Name(nil))
	}
}

func TestNewTyped_CustomName(t *testing.T) {
	model := &fakePEModel{}
	cfg := &Config{Model: model, Name: "my_planner", MaxIterations: 99}
	a := NewTyped(cfg)
	name := a.Name(nil)
	if name != "my_planner" {
		t.Errorf("name = %s, want my_planner", name)
	}
}

func TestNewTyped_MaxIterationsZero(t *testing.T) {
	model := &fakePEModel{}
	cfg := &Config{Model: model, MaxIterations: 0} // should default to 15
	a := NewTyped(cfg)
	if a == nil {
		t.Fatal("nil agent with zero max iterations")
	}
}

func TestNew(t *testing.T) {
	model := &fakePEModel{}
	a := New(&Config{Model: model})
	if a == nil {
		t.Fatal("New() returned nil")
	}
}

func TestPrompt(t *testing.T) {
	p := Prompt()
	if p == "" {
		t.Error("Prompt empty")
	}
	if !contains(p, "plan-execute-replan") {
		t.Error("missing plan-execute pattern in prompt")
	}
}

type fakePEModel struct{}

func (m *fakePEModel) Generate(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.Message, error) {
	return &schema.Message{Role: schema.RoleAssistant, Content: "plan result"}, nil
}
func (m *fakePEModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.StreamReader[*schema.Message], error) {
	return schema.StreamReaderFromArray([]*schema.Message{{Content: "plan"}}), nil
}
func (m *fakePEModel) BindTools(tools []*schema.ToolInfo) error { return nil }

func contains(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub { return true }
	}
	return false
}
