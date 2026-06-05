// Package supervisor provides the Supervisor multi-agent pattern.
// A central supervisor agent delegates tasks to specialized sub-agents
// and coordinates their execution.
package supervisor

import (
	"context"
	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

type Config struct {
	Name, Description string
	Model             agentcore.ChatModel[*schema.Message]
	SubAgents         []agentcore.Agent
	MaxIterations     int
}

func NewTyped(cfg *Config) *agentcore.TypedChatModelAgent[*schema.Message] {
	if cfg.MaxIterations <= 0 { cfg.MaxIterations = 20 }
	if cfg.Name == "" { cfg.Name = "supervisor" }
	a := agentcore.NewChatModelAgent(&agentcore.ChatModelConfig[*schema.Message]{
		Model: cfg.Model, Tools: buildTransferTools(cfg.SubAgents),
		Instruction: buildPrompt(cfg.SubAgents), MaxIterations: cfg.MaxIterations,
	})
	return a.WithName(cfg.Name).WithDescription(cfg.Description)
}

func New(cfg *Config) agentcore.Agent { return NewTyped(cfg) }

func buildTransferTools(subs []agentcore.Agent) []agentcore.Tool {
	tools := make([]agentcore.Tool, 0, len(subs))
	for _, s := range subs {
		s := s
		tools = append(tools, agentcore.NewBaseTool("transfer_to_"+s.Name(nil), "Transfer to agent: "+s.Description(nil),
			func(ctx context.Context, args string) (string, error) { return "Transferred to " + s.Name(nil), nil }))
	}
	return tools
}

func buildPrompt(subs []agentcore.Agent) string {
	p := "You are a supervisor coordinating multiple specialized agents.\n\nAvailable agents:\n"
	for _, s := range subs { p += "- " + s.Name(nil) + ": " + s.Description(nil) + "\n" }
	p += "\nAnalyze the user's request, decide which agent to delegate to, and coordinate their work."
	return p
}
