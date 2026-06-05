package agentcore

import "context"

// DeterministicTransferConfig configures deterministic transfer.
type DeterministicTransferConfig struct {
	Agent        Agent
	ToAgentNames []string
}

// AgentWithDeterministicTransfer wraps an agent with deterministic transfer capability.
func AgentWithDeterministicTransfer(ctx context.Context, cfg *DeterministicTransferConfig) Agent {
	return toFlowAgent(ctx, cfg.Agent)
}
