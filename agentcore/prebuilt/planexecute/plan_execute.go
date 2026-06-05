// Package planexecute provides Plan-Execute-Replan agent pattern.
package planexecute

import (
	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

type Config struct {
	Name, Description string
	Model             agentcore.ChatModel[*schema.Message]
	Tools             []agentcore.Tool
	MaxIterations     int
}

func NewTyped(cfg *Config) *agentcore.TypedChatModelAgent[*schema.Message] {
	if cfg.MaxIterations <= 0 { cfg.MaxIterations = 15 }
	if cfg.Name == "" { cfg.Name = "plan_execute_agent" }
	a := agentcore.NewChatModelAgent(&agentcore.ChatModelConfig[*schema.Message]{
		Model: cfg.Model, Tools: cfg.Tools, Instruction: prompt, MaxIterations: cfg.MaxIterations,
	})
	return a.WithName(cfg.Name).WithDescription(cfg.Description)
}

func New(cfg *Config) agentcore.Agent { return NewTyped(cfg) }
func Prompt() string                  { return prompt }

const prompt = `You are a plan-execute-replan agent.
1. Create a detailed plan to accomplish the task.
2. Execute the first step using tools.
3. Analyze the result and update the plan if needed.
4. Repeat until all steps are completed.`
