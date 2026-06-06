package toolsearch

import (
	"context"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

func TestNew_SmallToolset(t *testing.T) {
	mw := New(&TypedConfig[*schema.Message]{
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
	tools := make([]agentcore.Tool, 15)
	for i := range tools {
		tools[i] = agentcore.NewBaseTool("tool_"+string(rune('a'+i)), "desc", nil)
	}
	mw := New(&TypedConfig[*schema.Message]{
		AllTools:        tools,
		SearchThreshold: 10,
		MaxResults:      5,
	})
	rc := &agentcore.ChatModelAgentContext{}
	_, newRc, err := mw.BeforeAgent(context.Background(), rc)
	if err != nil { t.Fatalf("BeforeAgent: %v", err) }
	hasSearch := false
	for _, t := range newRc.Tools {
		if t.Name() == "tool_search" { hasSearch = true; break }
	}
	if !hasSearch { t.Error("expected tool_search meta-tool") }
}

func TestToolSearch_Scoring(t *testing.T) {
	tools := make([]agentcore.Tool, 15)
	for i := range tools {
		tools[i] = agentcore.NewBaseTool("calc_"+string(rune('a'+i)), "Calculator tool", nil)
	}
	mw := New(&TypedConfig[*schema.Message]{
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

func TestSplitToolName(t *testing.T) {
	tests := []struct{ input string; expected []string }{
		{"read_file", []string{"read", "file"}},
		{"getUserData", []string{"get", "user", "data"}},
		{"mcp__tool", []string{"mcp", "tool"}},
		{"simple", []string{"simple"}},
	}
	for _, tt := range tests {
		result := splitToolName(tt.input)
		if len(result) != len(tt.expected) {
			t.Errorf("splitToolName(%q) = %v, want %v", tt.input, result, tt.expected)
		}
	}
}

func TestToolSearch_SelectSyntax(t *testing.T) {
	mw := New(&TypedConfig[*schema.Message]{
		AllTools: []agentcore.Tool{
			agentcore.NewBaseTool("tool1", "First tool", nil),
			agentcore.NewBaseTool("tool2", "Second tool", nil),
			agentcore.NewBaseTool("tool3", "Third tool", nil),
		},
		SearchThreshold: 1,
	})
	rc := &agentcore.ChatModelAgentContext{}
	_, newRc, _ := mw.BeforeAgent(context.Background(), rc)
	for _, tool := range newRc.Tools {
		if tool.Name() == "tool_search" {
			result, err := tool.Invoke(context.Background(), "select:tool1,tool3")
			if err != nil { t.Fatalf("tool_search: %v", err) }
			if result == "No selected tools found." {
				t.Error("expected selected tools")
			}
			return
		}
	}
}

func TestBeforeModelRewrite_DeferredMode(t *testing.T) {
	mw := New(&TypedConfig[*schema.Message]{
		AllTools: []agentcore.Tool{
			agentcore.NewBaseTool("tool_a", "Desc A", nil),
		},
		UseDeferred: true,
	})
	state := &agentcore.TypedChatModelAgentState[*schema.Message]{
		Messages: []*schema.Message{schema.UserMessage("test")},
	}
	_, newState, err := mw.BeforeModelRewrite(context.Background(), state, nil)
	if err != nil { t.Fatalf("BeforeModelRewrite: %v", err) }
	if len(newState.DeferredToolInfos) != 1 {
		t.Errorf("expected 1 deferred tool, got %d", len(newState.DeferredToolInfos))
	}
}
