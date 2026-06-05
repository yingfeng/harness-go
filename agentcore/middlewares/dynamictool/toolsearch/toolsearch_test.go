package toolsearch

import (
	"context"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

type mockSearcher struct{ entries []ToolEntry }

func (s *mockSearcher) Search(query string) ([]ToolEntry, error) { return s.entries, nil }

func TestBeforeAgent_SetsToolSearchTool(t *testing.T) {
	searcher := &mockSearcher{
		entries: []ToolEntry{{Name: "calc", Description: "calculator"}},
	}
	mw := New[*schema.Message](searcher)

	rc := &agentcore.ChatModelAgentContext{}
	_, newRc, err := mw.BeforeAgent(context.Background(), rc)
	if err != nil {
		t.Fatalf("BeforeAgent: %v", err)
	}
	if newRc.ToolSearchTool == nil {
		t.Error("ToolSearchTool should be set when searcher is provided")
	}
	if newRc.ToolSearchTool.Name != "search_tools" {
		t.Errorf("tool name = %s, want search_tools", newRc.ToolSearchTool.Name)
	}
}

func TestBeforeAgent_NilSearcher(t *testing.T) {
	mw := New[*schema.Message](nil)
	rc := &agentcore.ChatModelAgentContext{}
	_, newRc, _ := mw.BeforeAgent(context.Background(), rc)
	if newRc.ToolSearchTool != nil {
		t.Error("ToolSearchTool should be nil when searcher is nil")
	}
}

func TestSearcherInterface(t *testing.T) {
	var _ Searcher = &mockSearcher{}
}
