// Package reduction provides tool output reduction middleware.
// It truncates and clears tool outputs that exceed size limits.
package reduction

import (
	"context"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

type Config struct {
	MaxToolOutputLen int // Max characters per tool output before truncation
	MaxToolCalls     int // Max tool calls to keep; older ones are cleared
}

type middleware[M agentcore.MessageType] struct {
	agentcore.BaseMiddleware[M]
	cfg *Config
}

func New[M agentcore.MessageType](cfg *Config) agentcore.TypedChatModelMiddleware[M] {
	if cfg == nil { cfg = &Config{MaxToolOutputLen: 2000, MaxToolCalls: 20} }
	if cfg.MaxToolOutputLen <= 0 { cfg.MaxToolOutputLen = 2000 }
	if cfg.MaxToolCalls <= 0 { cfg.MaxToolCalls = 20 }
	return &middleware[M]{cfg: cfg}
}

const truncSuffix = "\n...(truncated)"

func (mw *middleware[M]) BeforeModelRewrite(ctx context.Context, state *agentcore.TypedChatModelAgentState[M], mc *agentcore.TypedModelContext[M]) (context.Context, *agentcore.TypedChatModelAgentState[M], error) {
	toolCount := 0
	for i, msg := range state.Messages {
		m, ok := any(msg).(*schema.Message)
		if !ok || m == nil || m.Role != schema.RoleTool { continue }

		toolCount++
		if toolCount > mw.cfg.MaxToolCalls {
			// Clear old tool content to save context
			m.Content = "..."
			m.Extra = nil
			continue
		}

		if len(m.Content) > mw.cfg.MaxToolOutputLen {
			// Truncate
			m.Content = m.Content[:mw.cfg.MaxToolOutputLen] + truncSuffix
			if m.Extra == nil { m.Extra = make(map[string]any) }
			m.Extra["truncated"] = true
		}

		state.Messages[i] = any(m).(M)
	}
	return ctx, state, nil
}

// FindLastToolMessages reverses the message list to find the last N+1 tool messages.
// Useful for downstream middlewares that need to know the boundary.
func FindLastToolMessages[M agentcore.MessageType](messages []M, maxToKeep int) int {
	toolCount := 0
	for i := len(messages) - 1; i >= 0; i-- {
		if m, ok := any(messages[i]).(*schema.Message); ok && m != nil && m.Role == schema.RoleTool {
			toolCount++
			if toolCount > maxToKeep { return i + 1 }
		}
	}
	return -1
}
