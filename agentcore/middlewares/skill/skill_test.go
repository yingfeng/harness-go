package skill

import (
	"context"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

func TestBeforeAgent_InlineSkill(t *testing.T) {
	mw := NewTyped[*schema.Message](&TypedConfig[*schema.Message]{
		Skills: []Config{
			{Name: "coding", Content: "Always write tests", ExecutionMode: ModeInline},
		},
	})
	rc := &agentcore.ChatModelAgentContext{Instruction: "You are helpful."}
	_, newRc, err := mw.BeforeAgent(context.Background(), rc)
	if err != nil { t.Fatalf("BeforeAgent: %v", err) }
	if !contains(newRc.Instruction, "Always write tests") {
		t.Error("skill content not injected")
	}
}

func TestBeforeAgent_ForkSkill(t *testing.T) {
	mw := NewTyped[*schema.Message](&TypedConfig[*schema.Message]{
		Skills: []Config{
			{Name: "search", Content: "Search skill", ExecutionMode: ModeFork},
		},
	})
	rc := &agentcore.ChatModelAgentContext{}
	_, newRc, err := mw.BeforeAgent(context.Background(), rc)
	if err != nil { t.Fatalf("BeforeAgent: %v", err) }
	found := false
	for _, t := range newRc.Tools {
		if t.Name() == "skill_search" { found = true; break }
	}
	if !found { t.Error("fork skill tool not added") }
}

func TestParseSkill_Frontmatter(t *testing.T) {
	content := "---\nname: test\nmodel: gpt-4\n---\nSkill body"
	cfg := parseSkill(content)
	if cfg == nil { t.Fatal("nil config") }
	if cfg.Name != "test" { t.Errorf("name=%q", cfg.Name) }
	if cfg.Model != "gpt-4" { t.Errorf("model=%q", cfg.Model) }
	if !contains(cfg.Content, "Skill body") { t.Error("body not parsed") }
}

func TestParseSkill_NoFrontmatter(t *testing.T) {
	cfg := parseSkill("Just content")
	if cfg == nil { t.Fatal("nil config") }
	if !contains(cfg.Content, "Just content") { t.Error("content not parsed") }
}

func TestBeforeAgent_NilConfig(t *testing.T) {
	mw := NewTyped[*schema.Message](nil)
	if mw == nil { t.Fatal("nil middleware") }
	rc := &agentcore.ChatModelAgentContext{}
	_, _, err := mw.BeforeAgent(context.Background(), rc)
	if err != nil { t.Fatalf("BeforeAgent: %v", err) }
}

func contains(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub { return true }
	}
	return false
}
