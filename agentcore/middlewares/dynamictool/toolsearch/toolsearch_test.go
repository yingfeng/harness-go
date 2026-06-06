package toolsearch

import (
	"context"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

func TestNew_SmallToolset(t *testing.T) {
	// Small toolset: all tools passed directly
	mw := New[*schema.Message](&Config{
		AllTools: []agentcore.Tool{
			agentcore.NewBaseTool("tool_a", "Description A", nil),
			agentcore.NewBaseTool("tool_b", "Description B", nil),
		},
		SearchThreshold: 10,
	})
	rc := &agentcore.ChatModelAgentContext{}
	_, newRc, err := mw.BeforeAgent(context.Background(), rc)
	if err != nil { t.Fatalf("BeforeAgent: %v", err) }
	if len(newRc.Tools) != 2 {
		t.Errorf("expected 2 tools, got %d", len(newRc.Tools))
	}
}

func TestNew_LargeToolsetAddsSearch(t *testing.T) {
	// Large toolset: should add tool_search meta-tool
	tools := make([]agentcore.Tool, 0, 15)
	for i := 0; i < 15; i++ {
		tools = append(tools, agentcore.NewBaseTool(
			"tool_"+string(rune('a'+i)),
			"Description for tool_"+string(rune('a'+i)), nil))
	}
	mw := New[*schema.Message](&Config{
		AllTools:        tools,
		SearchThreshold: 10,
		MaxResults:      5,
	})
	rc := &agentcore.ChatModelAgentContext{}
	_, newRc, err := mw.BeforeAgent(context.Background(), rc)
	if err != nil { t.Fatalf("BeforeAgent: %v", err) }

	// Should have search tool + threshold/2 direct tools
	hasSearch := false
	for _, t := range newRc.Tools {
		if t.Name() == "tool_search" { hasSearch = true; break }
	}
	if !hasSearch { t.Error("expected tool_search meta-tool") }
}

func TestToolSearch_Function(t *testing.T) {
	tools := make([]agentcore.Tool, 0, 15)
	for i := 0; i < 15; i++ {
		tools = append(tools, agentcore.NewBaseTool(
			"calc_"+string(rune('a'+i)),
			"Calculator tool for math operations", nil))
	}
	mw := New[*schema.Message](&Config{
		AllTools:        tools,
		SearchThreshold: 5,
		MaxResults:      3,
	})
	rc := &agentcore.ChatModelAgentContext{}
	_, newRc, _ := mw.BeforeAgent(context.Background(), rc)
	for _, tool := range newRc.Tools {
		if tool.Name() == "tool_search" {
			result, err := tool.Invoke(context.Background(), "calc")
			if err != nil { t.Fatalf("tool_search: %v", err) }
			if result == "Please provide keywords to search." || result == "No tools found for: calc" {
				t.Errorf("expected tool results, got: %s", result)
			}
			return
		}
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := &Config{AllTools: []agentcore.Tool{agentcore.NewBaseTool("t", "d", nil)}}
	mw := New[*schema.Message](cfg)
	if mw == nil { t.Fatal("nil middleware") }
}
