// Package skill provides skill loading and execution middleware.
// Skills are pre-defined capabilities that can be loaded and used by agents.
package skill

import (
	"context"
	"github.com/infiniflow/ragflow/harness/agentcore"
)

type ExecMode int
const ( ModeInline ExecMode = iota; ModeFork; ModeForkWithContext )

type Config struct {
	Name           string
	Content        string
	ExecutionMode  ExecMode
}

type middleware[M agentcore.MessageType] struct {
	agentcore.BaseMiddleware[M]
	skills []Config
}

func New[M agentcore.MessageType](skills ...Config) agentcore.TypedChatModelMiddleware[M] {
	return &middleware[M]{skills: skills}
}

func (m *middleware[M]) BeforeAgent(ctx context.Context, rc *agentcore.ChatModelAgentContext) (context.Context, *agentcore.ChatModelAgentContext, error) {
	for _, s := range m.skills {
		rc.Instruction = rc.Instruction + "\n\n## Skill: " + s.Name + "\n" + s.Content
	}
	return ctx, rc, nil
}
