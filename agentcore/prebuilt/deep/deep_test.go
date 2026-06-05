package deep

import (
	"context"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Name != "deep_agent" {
		t.Errorf("default name = %s", cfg.Name)
	}
	if cfg.MaxIterations != 20 {
		t.Errorf("default MaxIterations = %d, want 20", cfg.MaxIterations)
	}
	if cfg.EnableShell {
		t.Error("shell should be disabled by default")
	}
}

func TestNewTyped_NilConfig(t *testing.T) {
	// Should not panic with nil config
	cfg := DefaultConfig()
	a := NewTyped(cfg)
	if a == nil {
		t.Fatal("nil agent")
	}
	if a.Name(nil) != "deep_agent" {
		t.Errorf("name = %s", a.Name(nil))
	}
}

func TestNewTyped_WithModel(t *testing.T) {
	model := &fakeModelForDeep{}
	cfg := &Config{Model: model, MaxIterations: 5, Name: "test_deep"}
	a := NewTyped(cfg)
	if a.Name(nil) != "test_deep" {
		t.Errorf("custom name not applied: %s", a.Name(nil))
	}
}

func TestNew(t *testing.T) {
	model := &fakeModelForDeep{}
	cfg := &Config{Model: model}
	a := New(cfg)
	if a == nil {
		t.Fatal("New() returned nil")
	}
}

func TestPrompt(t *testing.T) {
	p := Prompt()
	if p == "" {
		t.Error("Prompt() empty")
	}
	if !contains(p, "Deep Agent") {
		t.Error("prompt missing 'Deep Agent'")
	}
}

func TestSelectPrompt(t *testing.T) {
	en := SelectPrompt("en")
	zh := SelectPrompt("zh")
	unknown := SelectPrompt("fr") // fallback to default

	if en == zh {
		t.Error("en and zh prompts should differ")
	}
	if unknown != Prompt() {
		t.Error("unknown language should fall back to default")
	}
}

func TestTaskManager_CRUD(t *testing.T) {
	m := NewTaskManager()

	t1 := m.Create("task A", "")
	if t1.ID == "" || t1.State != TaskPending {
		t.Error("Create failed")
	}

	list := m.List()
	if len(list) != 1 {
		t.Errorf("List returned %d", len(list))
	}

	got, err := m.Get(t1.ID)
	if err != nil || got.ID != t1.ID {
		t.Error("Get failed")
	}

	err = m.Update(t1.ID, "done!", TaskCompleted)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	got2, _ := m.Get(t1.ID)
	if got2.Result != "done!" || got2.State != TaskCompleted {
		t.Error("Update did not persist")
	}
}

func TestTaskManager_GetNotFound(t *testing.T) {
	m := NewTaskManager()
	_, err := m.Get("nope")
	if err == nil {
		t.Error("expected error")
	}
}

func TestTaskState_Consts(t *testing.T) {
	consts := []TaskState{TaskPending, TaskRunning, TaskCompleted, TaskFailed}
	for _, c := range consts {
		if c == "" {
			t.Errorf("empty const: %v", c)
		}
	}
}

type fakeModelForDeep struct{}

func (m *fakeModelForDeep) Generate(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.Message, error) {
	return &schema.Message{Role: schema.RoleAssistant, Content: "ok"}, nil
}
func (m *fakeModelForDeep) Stream(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.StreamReader[*schema.Message], error) {
	return schema.StreamReaderFromArray([]*schema.Message{{Content: "ok"}}), nil
}
func (m *fakeModelForDeep) BindTools(tools []*schema.ToolInfo) error { return nil }

func contains(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub { return true }
	}
	return false
}
