package agentcore

import (
	"context"
	"fmt"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// failoverModel provides failover across multiple chat models.
type failoverModel[M MessageType] struct {
	models []ChatModel[M]
}

func newFailover[M MessageType](models []ChatModel[M]) ChatModel[M] {
	return &failoverModel[M]{models: models}
}

func (m *failoverModel[M]) Generate(ctx context.Context, input []M, opts ...modelOption) (M, error) {
	var lastErr error
	for i, model := range m.models {
		r, err := model.Generate(ctx, input, opts...)
		if err == nil { return r, nil }
		lastErr = err
		_ = i
	}
	var zero M
	return zero, fmt.Errorf("all models failed: %w", lastErr)
}

func (m *failoverModel[M]) Stream(ctx context.Context, input []M, opts ...modelOption) (*schema.StreamReader[M], error) {
	var lastErr error
	for _, model := range m.models {
		s, err := model.Stream(ctx, input, opts...)
		if err == nil { return s, nil }
		lastErr = err
	}
	return nil, fmt.Errorf("all models failed to stream: %w", lastErr)
}

func (m *failoverModel[M]) BindTools(tools []*schema.ToolInfo) error {
	for _, model := range m.models {
		if err := model.BindTools(tools); err != nil { return err }
	}
	return nil
}

func WithModelFailover[M MessageType](primary ChatModel[M], secondaries ...ChatModel[M]) ChatModel[M] {
	all := append([]ChatModel[M]{primary}, secondaries...)
	return newFailover(all)
}
