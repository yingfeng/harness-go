// Package agentsmd injects AGENTS.md file content into the model input.
// This is useful for providing context about available agents and their capabilities.
package agentsmd

import (
	"context"
	"github.com/infiniflow/ragflow/agent/agentcore"
)

type Config struct {
	Content string
}

type middleware[M agentcore.MessageType] struct {
	agentcore.BaseMiddleware[M]
	content string
}

func New[M agentcore.MessageType](cfg *Config) agentcore.TypedChatModelMiddleware[M] {
	return &middleware[M]{content: cfg.Content}
}

func (m *middleware[M]) BeforeAgent(ctx context.Context, rc *agentcore.ChatModelAgentContext) (context.Context, *agentcore.ChatModelAgentContext, error) {
	if m.content != "" {
		rc.Instruction = rc.Instruction + "\n\n" + m.content
	}
	return ctx, rc, nil
}
