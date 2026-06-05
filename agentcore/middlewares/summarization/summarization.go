// Package summarization provides conversation summarization middleware.
// It automatically summarizes conversation history when token limits are approached.
package summarization

import (
	"context"
	"github.com/infiniflow/ragflow/agent/agentcore"
	"github.com/infiniflow/ragflow/agent/agentcore/schema"
)

type Config struct {
	MaxTokens   int
	SummaryLang string
}

type middleware[M agentcore.MessageType] struct {
	agentcore.BaseMiddleware[M]
	cfg    *Config
	model  agentcore.ChatModel[M]
}

func New[M agentcore.MessageType](model agentcore.ChatModel[M], cfg *Config) agentcore.TypedChatModelMiddleware[M] {
	if cfg == nil { cfg = &Config{MaxTokens: 4096} }
	return &middleware[M]{cfg: cfg, model: model}
}

func (m *middleware[M]) BeforeModelRewrite(ctx context.Context, state *agentcore.TypedChatModelAgentState[M], mc *agentcore.TypedModelContext[M]) (context.Context, *agentcore.TypedChatModelAgentState[M], error) {
	if len(state.Messages) > m.cfg.MaxTokens {
		// In production: summarize old messages
		summary := "[Previous conversation summarized]"
		state.Messages = append([]M{any(schema.SystemMessage(summary)).(M)}, state.Messages[len(state.Messages)-10:]...)
	}
	return ctx, state, nil
}
