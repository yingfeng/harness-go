// Package deep provides DeepAgent — a depth-first task decomposition agent.
package deep

import (
	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

type Config struct {
	Name          string
	Description   string
	Model         agentcore.ChatModel[*schema.Message]
	Tools         []agentcore.Tool
	MaxIterations int
}

func NewTyped(cfg *Config) *agentcore.TypedChatModelAgent[*schema.Message] {
	if cfg.MaxIterations <= 0 { cfg.MaxIterations = 20 }
	if cfg.Name == "" { cfg.Name = "deep_agent" }
	a := agentcore.NewChatModelAgent(&agentcore.ChatModelConfig[*schema.Message]{
		Model: cfg.Model, Tools: cfg.Tools, Instruction: systemPrompt, MaxIterations: cfg.MaxIterations,
	})
	return a.WithName(cfg.Name).WithDescription(cfg.Description)
}

func New(cfg *Config) agentcore.Agent { return NewTyped(cfg) }
func Prompt() string                  { return systemPrompt }

const systemPrompt = `You are a coding and task-execution agent. Break down complex tasks into manageable steps.
Guidelines:
- Verify actions before executing
- Read files before editing
- Test changes when appropriate
- Track sub-tasks and their completion status`
