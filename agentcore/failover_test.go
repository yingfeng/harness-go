package agentcore

import (
	"context"
	"errors"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

func TestWithModelFailover_SingleModel(t *testing.T) {
	model := &mockModel{}
	wrapped := WithModelFailover(model)
	// WithModelFailover always wraps in failoverModel, even with single model
	if wrapped == nil {
		t.Fatal("nil wrapped model")
	}
	// Verify it still works (delegates to underlying model)
	model.addResp("ok")
	ctx := context.Background()
	msgs := []Message{schema.UserMessage("hi")}
	resp, err := wrapped.Generate(ctx, msgs)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if resp.Content != "ok" {
		t.Errorf("content = %s, want ok", resp.Content)
	}
}

func TestWithModelFailover_PrimarySucceeds(t *testing.T) {
	primary := &mockModel{}
	primary.addResp("from primary")

	fallback := &mockModel{}
	fallback.addResp("from fallback")

	wrapped := WithModelFailover(primary, fallback)
	ctx := context.Background()
	msgs := []Message{schema.UserMessage("hi")}

	resp, err := wrapped.Generate(ctx, msgs)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if resp.Content != "from primary" {
		t.Errorf("content = %s, want from primary", resp.Content)
	}
}

func TestWithModelFailover_FallsBack(t *testing.T) {
	primary := &failOnceModel{}
	fallback := &mockModel{}
	fallback.addResp("fallback result")

	wrapped := WithModelFailover(primary, fallback)
	ctx := context.Background()
	msgs := []Message{schema.UserMessage("failover test")}

	resp, err := wrapped.Generate(ctx, msgs)
	if err != nil {
		t.Fatalf("Generate after failover: %v", err)
	}
	if resp.Content != "fallback result" {
		t.Errorf("content = %s, want fallback result", resp.Content)
	}
}

func TestWithModelFailover_AllFail(t *testing.T) {
	primary := &alwaysFailModel{}
	secondary := &alwaysFailModel{}

	wrapped := WithModelFailover(primary, secondary)
	ctx := context.Background()
	_, err := wrapped.Generate(ctx, []Message{schema.UserMessage("")})
	if err == nil {
		t.Error("expected error when all models fail")
	}
}

type failOnceModel struct {
	failed bool
}

func (m *failOnceModel) Generate(_ context.Context, _ []Message, _ ...modelOption) (Message, error) {
	if !m.failed {
		m.failed = true
		return nil, errors.New("primary failure")
	}
	return &schema.Message{Content: "recovery"}, nil
}
func (m *failOnceModel) Stream(ctx context.Context, msgs []Message, opts ...modelOption) (*schema.StreamReader[Message], error) {
	msg, err := m.Generate(ctx, msgs, opts...)
	return schema.StreamReaderFromArray([]Message{msg}), err
}
func (m *failOnceModel) BindTools(tools []*schema.ToolInfo) error { return nil }
