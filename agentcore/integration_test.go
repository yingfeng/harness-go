package agentcore

import (
	"context"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// TestChatModelAgent_ToolCallAndResponse_Integration verifies the full ReAct cycle:
// model returns tool call -> tool executes -> model returns final answer.
func TestChatModelAgent_ToolCallAndResponse_Integration(t *testing.T) {
	model := &forcedToolModel{
		inner:     &mockModel{},
		toolCalls: []schema.ToolCall{{ID: "call_1", Function: schema.ToolCallFunction{Name: "calculator", Arguments: "{\"x\":6,\"y\":7}"}}},
		finalResp: "the answer is 42",
		firstCall: true,
	}
	tool := &mockTool{name: "calculator", desc: "calculates things"}
	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model: model, Tools: []Tool{tool},
		ToolsConfig: &ToolsNodeConfig{Tools: []Tool{tool}},
	})
	agent.name = "calc_agent"
	ctx := context.Background()
	iter := agent.Run(ctx, &AgentInput{Messages: []Message{schema.UserMessage("what is 6*7?")}})
	var lastContent string
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		if ev.Err != nil {
			t.Fatalf("err: %v", ev.Err)
		}
		if ev.Output != nil && ev.Output.MessageOutput != nil && !ev.Output.MessageOutput.IsStreaming && ev.Output.MessageOutput.Message != nil {
			lastContent = ev.Output.MessageOutput.Message.Content
		}
	}
	if lastContent != "the answer is 42" {
		t.Errorf("expected 'the answer is 42', got %q", lastContent)
	}
}

// TestRunner_WithMockAgent runs a full cycle through the Runner with checkpoint.
func TestRunner_WithMockAgent(t *testing.T) {
	model := &mockModel{}
	model.addResp("hello world")
	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: model})
	agent.name = "runner_test"
	store := newCancelTestStore()
	runner := NewTypedRunner(RunnerConfig[*schema.Message]{Agent: agent, CheckPointStore: store})
	ctx := context.Background()
	iter := runner.Run(ctx, []*schema.Message{schema.UserMessage("say hi")})
	var found bool
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		if ev.Err != nil {
			t.Fatalf("err: %v", ev.Err)
		}
		if ev.Output != nil && ev.Output.MessageOutput != nil && !ev.Output.MessageOutput.IsStreaming && ev.Output.MessageOutput.Message != nil {
			if ev.Output.MessageOutput.Message.Content == "hello world" {
				found = true
			}
		}
	}
	if !found {
		t.Error("expected 'hello world' in output")
	}
}
