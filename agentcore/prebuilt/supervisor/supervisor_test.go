package supervisor

import (
	"context"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Name != "supervisor" {
		t.Errorf("default name = %s", cfg.Name)
	}
}

func TestNew_RequiresModel(t *testing.T) {
	ctx := context.Background()
	_, err := New(ctx, &Config{})
	if err == nil {
		t.Error("expected error when Model is nil")
	}
}

func TestNew_WithModelAndAgents(t *testing.T) {
	ctx := context.Background()
	model := &mockSupervisorModel{}

	subAgent := agentcore.NewChatModelAgent(&agentcore.ChatModelConfig[*schema.Message]{
		Model:       model,
		Instruction: "You are a coder.",
	}).WithName("coder")

	flow, err := New(ctx, &Config{
		Model:  model,
		Name:   "my_supervisor",
		Agents: []AgentSpec{{Name: "coder", Description: "Writes code", Agent: subAgent}},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if flow == nil {
		t.Fatal("nil flow agent")
	}
}

func TestBuildAgentDescriptions(t *testing.T) {
	descs := buildAgentDescriptions([]AgentSpec{
		{Name: "researcher", Description: "Searches the web"},
		{Name: "writer", Description: "Writes articles"},
	})
	if !contains(descs, "researcher") || !contains(descs, "writer") {
		t.Errorf("bad descriptions: %s", descs)
	}
}

func TestBuildAgentDescriptions_Empty(t *testing.T) {
	descs := buildAgentDescriptions(nil)
	if descs != "" {
		t.Error("nil agents should produce empty description")
	}
}

func TestSystemPrompt(t *testing.T) {
	if systemPrompt == "" {
		t.Error("systemPrompt empty")
	}
	if !contains(systemPrompt, "supervisor") {
		t.Error("missing 'supervisor' in prompt")
	}
}

func TestNewWithRouter(t *testing.T) {
	ctx := context.Background()
	model := &mockSupervisorModel{}
	subAgent := agentcore.NewChatModelAgent(&agentcore.ChatModelConfig[*schema.Message]{Model: model}).WithName("sub")

	flow, err := NewWithRouter(ctx, model, []AgentSpec{{Name: "sub", Description: "Sub agent", Agent: subAgent}})
	if err != nil {
		t.Fatalf("NewWithRouter: %v", err)
	}
	if flow == nil {
		t.Fatal("nil from NewWithRouter")
	}
}

type mockSupervisorModel struct{}

func (m *mockSupervisorModel) Generate(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.Message, error) {
	return &schema.Message{Role: schema.RoleAssistant, Content: "routed to coder"}, nil
}
func (m *mockSupervisorModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.StreamReader[*schema.Message], error) {
	return schema.StreamReaderFromArray([]*schema.Message{{Content: "routed"}}), nil
}
func (m *mockSupervisorModel) BindTools(tools []*schema.ToolInfo) error { return nil }

func contains(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub { return true }
	}
	return false
}
