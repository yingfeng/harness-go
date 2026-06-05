package skill

import (
	"context"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

func TestBeforeAgent_Injection(t *testing.T) {
	mw := New[*schema.Message](
		Config{Name: "coding", Content: "Always write tests"},
	)

	rc := &agentcore.ChatModelAgentContext{
		Instruction: "You are helpful.",
	}
	original := rc.Instruction

	_, newRc, err := mw.BeforeAgent(context.Background(), rc)
	if err != nil {
		t.Fatalf("BeforeAgent: %v", err)
	}
	if newRc.Instruction == original {
		t.Error("instruction should be modified by skill")
	}
	// Verify the appended content is present
	if !containsStr(newRc.Instruction, "Always write tests") {
		t.Error("skill content not found in instruction")
	}
}

func TestBeforeAgent_MultipleSkills(t *testing.T) {
	mw := New[*schema.Message](
		Config{Name: "skill-a", Content: "Content A"},
		Config{Name: "skill-b", Content: "Content B"},
	)

	rc := &agentcore.ChatModelAgentContext{Instruction: "Base"}
	_, newRc, _ := mw.BeforeAgent(context.Background(), rc)

	if !containsStr(newRc.Instruction, "Content A") || !containsStr(newRc.Instruction, "Content B") {
		t.Errorf("missing skill injection in: %s", newRc.Instruction)
	}
}

func TestExecMode_Consts(t *testing.T) {
	if ModeInline != 0 || ModeFork != 1 || ModeForkWithContext != 2 {
		t.Error("ExecMode constants unexpected values")
	}
}

func containsStr(s, sub string) bool { return len(s) >= len(sub) && searchStr(s, sub) }

func searchStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub { return true }
	}
	return false
}
