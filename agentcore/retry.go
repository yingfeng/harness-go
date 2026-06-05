package agentcore

import (
	"context"
	"fmt"

	"github.com/infiniflow/ragflow/agent/agentcore/schema"
)

// retryableChatModel wraps a ChatModel with retry logic.
type retryableChatModel[M MessageType] struct {
	inner  ChatModel[M]
	config *RetryConfig[M]
}

func newRetryable[M MessageType](inner ChatModel[M], cfg *RetryConfig[M]) ChatModel[M] {
	if cfg == nil || cfg.MaxAttempts <= 1 { return inner }
	return &retryableChatModel[M]{inner: inner, config: cfg}
}

func (m *retryableChatModel[M]) Generate(ctx context.Context, input []M, opts ...modelOption) (M, error) {
	var lastErr error
	max := m.config.MaxAttempts
	if max <= 0 { max = 3 }
	for attempt := 0; attempt < max; attempt++ {
		r, err := m.inner.Generate(ctx, input, opts...)
		if err == nil { return r, nil }
		lastErr = err
	}
	var zero M
	return zero, fmt.Errorf("retry exhausted after %d attempts: %w", max, lastErr)
}

func (m *retryableChatModel[M]) Stream(ctx context.Context, input []M, opts ...modelOption) (*schema.StreamReader[M], error) {
	return m.inner.Stream(ctx, input, opts...)
}
func (m *retryableChatModel[M]) BindTools(tools []*schema.ToolInfo) error { return m.inner.BindTools(tools) }

func WithModelRetry[M MessageType](inner ChatModel[M], cfg *RetryConfig[M]) ChatModel[M] {
	return newRetryable(inner, cfg)
}
