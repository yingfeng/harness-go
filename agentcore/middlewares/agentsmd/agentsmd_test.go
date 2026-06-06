package agentsmd

import (
	"context"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

type testBackend struct {
	content string
}

func (b *testBackend) Read(path string) (string, error) { return b.content, nil }
func (b *testBackend) Exists(path string) bool           { return true }

func TestBeforeAgent_ContentInjection(t *testing.T) {
	backend := &testBackend{content: "# Available Agents\n- coder\n- reviewer"}
	mw := NewTyped[*schema.Message](&TypedConfig[*schema.Message]{
		Backend: backend,
		Files:   []string{"AGENTS.md"},
	})

	rc := &agentcore.ChatModelAgentContext{Instruction: "Help me."}
	_, newRc, err := mw.BeforeAgent(context.Background(), rc)
	if err != nil { t.Fatalf("BeforeAgent: %v", err) }
	if !contains(newRc.Instruction, "Available Agents") {
		t.Error("content not injected into instruction")
	}
}

func TestBeforeAgent_EmptyBackend(t *testing.T) {
	mw := NewTyped[*schema.Message](&TypedConfig[*schema.Message]{})
	rc := &agentcore.ChatModelAgentContext{Instruction: "Base"}
	_, newRc, _ := mw.BeforeAgent(context.Background(), rc)
	if newRc.Instruction != "Base" {
		t.Error("empty backend should not modify instruction")
	}
}

func TestBeforeAgent_NilConfig(t *testing.T) {
	mw := NewTyped[*schema.Message](nil)
	if mw == nil { t.Fatal("nil middleware") }
}

func contains(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub { return true }
	}
	return false
}
