package agentsmd

import (
	"context"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

func TestBeforeAgent_ContentInjection(t *testing.T) {
	mw := New[*schema.Message](&Config{
		Content: "# Available Agents\n- coder: writes code\n- reviewer: reviews code",
	})

	rc := &agentcore.ChatModelAgentContext{Instruction: "Help me."}
	_, newRc, err := mw.BeforeAgent(context.Background(), rc)
	if err != nil {
		t.Fatalf("BeforeAgent: %v", err)
	}
	if !contains(newRc.Instruction, "Available Agents") {
		t.Error("agents.md content not injected into instruction")
	}
}

func TestBeforeAgent_EmptyContent(t *testing.T) {
	mw := New[*schema.Message](&Config{})
	rc := &agentcore.ChatModelAgentContext{Instruction: "Base"}
	_, newRc, _ := mw.BeforeAgent(context.Background(), rc)
	if newRc.Instruction != "Base" {
		t.Error("empty content should not modify instruction")
	}
}

func contains(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub { return true }
	}
	return false
}
