// Package subagents provides planners and writers for the sequential workflow.
package subagents

import (
	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
	"github.com/infiniflow/ragflow/harness/examples/workflow"
)

// NewPlanAgent creates a chat model agent that generates a research plan.
func NewPlanAgent() agentcore.Agent {
	return agentcore.NewChatModelAgent[*schema.Message](&agentcore.ChatModelConfig[*schema.Message]{
		Model: workflow.MockModel("PlannerAgent"),
		Instruction: `You are an expert research planner.
Your goal is to create a comprehensive, step-by-step research plan for a given topic.
The plan should be logical, clear, and easy to follow.
The user will provide the research topic. Your output must ONLY be the research plan itself, without any conversational text, introductions, or summaries.`,
	}).WithName("PlannerAgent").WithDescription("Generates a research plan based on a topic.")
}

// NewWriterAgent creates a chat model agent that writes a report based on a plan.
func NewWriterAgent() agentcore.Agent {
	return agentcore.NewChatModelAgent[*schema.Message](&agentcore.ChatModelConfig[*schema.Message]{
		Model: workflow.MockModel("WriterAgent"),
		Instruction: `You are an expert academic writer.
You will be provided with a detailed research plan.
Your task is to expand on this plan to write a comprehensive, well-structured, and in-depth report.`,
	}).WithName("WriterAgent").WithDescription("Writes a report based on a research plan.")
}
