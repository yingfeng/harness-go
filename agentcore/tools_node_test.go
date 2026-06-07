package agentcore

import (
	"context"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

func TestNewToolsNode(t *testing.T) {
	tool := &mockTool{name: "test", desc: "test tool"}
	tn := NewToolsNode[*schema.Message](&ToolsNodeConfig{
		Tools: []Tool{tool},
	})
	if tn == nil {
		t.Fatal("nil ToolsNode")
	}
	if len(tn.toolMap) != 1 {
		t.Error("tool map not populated")
	}
}

func TestToolsNode_Execute_NoToolCalls(t *testing.T) {
	tn := NewToolsNode[*schema.Message](&ToolsNodeConfig{})
	resp := &schema.Message{Role: schema.RoleAssistant, Content: "no tools here"}
	state := &TypedReActAgentState[*schema.Message]{}

	results, action, err := tn.Execute(context.Background(), resp, state, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if results != nil {
		t.Error("expected nil results when no tool calls")
	}
	if action != nil {
		t.Error("expected nil action")
	}
}

func TestToolsNode_Execute_ToolNotFound(t *testing.T) {
	tn := NewToolsNode[*schema.Message](&ToolsNodeConfig{
		Tools: []Tool{&mockTool{name: "existing", desc: ""}},
	})
	resp := &schema.Message{
		Role: schema.RoleAssistant,
		ToolCalls: []schema.ToolCall{
			{ID: "tc1", Function: schema.ToolCallFunction{Name: "missing_tool", Arguments: "{}"}},
		},
	}
	state := &TypedReActAgentState[*schema.Message]{}

	results, _, err := tn.Execute(context.Background(), resp, state, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result (error msg in tool response), got %d", len(results))
	}
}

func TestToolsNode_Execute_ReturnDirectly(t *testing.T) {
	exitTool := &mockTool{name: "exit_tool", desc: ""}

	tn := NewToolsNode[*schema.Message](&ToolsNodeConfig{
		Tools:         []Tool{exitTool},
		ReturnDirectly: map[string]bool{"exit_tool": true},
	})
	resp := &schema.Message{
		Role: schema.RoleAssistant,
		ToolCalls: []schema.ToolCall{
			{ID: "tc1", Function: schema.ToolCallFunction{Name: "exit_tool", Arguments: "{}"}},
		},
	}
	state := &TypedReActAgentState[*schema.Message]{}

	results, action, err := tn.Execute(context.Background(), resp, state, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !exitTool.executed {
		t.Error("tool was not executed")
	}
	if action == nil || !action.Exit {
		t.Error("expected Exit action for return-directly tool")
	}
	if len(results) != 1 {
		t.Error("expected 1 result even with return-directly")
	}
}

func TestParseToolArgs_ValidJSON(t *testing.T) {
	var in struct{ Name string `json:"name"` }
	err := parseToolArgs(`{"name":"test"}`, &in)
	if err != nil {
		t.Fatalf("parseToolArgs: %v", err)
	}
	if in.Name != "test" {
		t.Error("name not parsed")
	}
}

func TestParseToolArgs_InvalidJSON(t *testing.T) {
	err := parseToolArgs(`{invalid`, struct{}{})
	if err == nil {
		t.Error("expected parse error")
	}
}
