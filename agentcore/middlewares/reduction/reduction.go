// Package reduction provides tool output reduction middleware.
// It truncates and clears tool outputs that exceed size limits.
package reduction

import (
	"context"
	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

type Config struct {
	MaxToolOutputLen int
	MaxToolCalls     int
}

type middleware[M agentcore.MessageType] struct {
	agentcore.BaseMiddleware[M]
	cfg *Config
}

func New[M agentcore.MessageType](cfg *Config) agentcore.TypedChatModelMiddleware[M] {
	if cfg == nil { cfg = &Config{MaxToolOutputLen: 2000, MaxToolCalls: 20} }
	return &middleware[M]{cfg: cfg}
}

const truncSuffix = "\n...(truncated)"

func (mw *middleware[M]) BeforeModelRewrite(ctx context.Context, state *agentcore.TypedChatModelAgentState[M], mc *agentcore.TypedModelContext[M]) (context.Context, *agentcore.TypedChatModelAgentState[M], error) {
	toolCount := 0
	for _, msg := range state.Messages {
		if m, ok := any(msg).(*schema.Message); ok && m != nil && m.Role == schema.RoleTool {
			toolCount++
			if toolCount > mw.cfg.MaxToolCalls {
				m.Content = "..."
			} else if len(m.Content) > mw.cfg.MaxToolOutputLen {
				m.Content = m.Content[:mw.cfg.MaxToolOutputLen] + truncSuffix
			}
		}
	}
	return ctx, state, nil
}
