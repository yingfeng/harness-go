// Package subagents provides agents for the loop/reflection workflow.
package subagents

import (
	"context"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
	"github.com/infiniflow/ragflow/harness/examples/workflow"
)

// NewMainAgent creates the primary task-solving agent.
func NewMainAgent() agentcore.Agent {
	return agentcore.NewChatModelAgent[*schema.Message](&agentcore.ChatModelConfig[*schema.Message]{
		Model: workflow.MockModel("MainAgent"),
		Instruction: `You are the main agent responsible for solving the user's task.
Provide a comprehensive solution based on the given requirements.
Focus on delivering accurate and complete results.`,
	}).WithName("main_agent").WithDescription("Main agent that attempts to solve the user's task.")
}

// NewCritiqueAgent creates the critique agent that reviews and can exit the loop.
func NewCritiqueAgent() agentcore.Agent {
	// The critique agent has an "exit_and_summarize" tool that triggers BreakLoop.
	exitTool := agentcore.NewBaseTool("exit_and_summarize",
		"exit from the loop and provide a final summary response",
		func(ctx context.Context, args string) (string, error) {
			return "Loop execution completed. Summary: " + args, nil
		})

	return agentcore.NewChatModelAgent[*schema.Message](&agentcore.ChatModelConfig[*schema.Message]{
		Model: workflow.MockModel("CritiqueAgent"),
		Instruction: `You are a critique agent responsible for reviewing the main agent's work.
Analyze the provided solution for accuracy, completeness, and quality.
If you find issues or areas for improvement, provide specific feedback.
If the work is satisfactory, call the 'exit_and_summarize' tool and provide a final summary response.`,
		Tools:          []agentcore.Tool{exitTool},
		ReturnDirectly: map[string]bool{"exit_and_summarize": true},
	}).WithName("critique_agent").WithDescription("Critique agent that reviews and provides feedback.")
}
