package telemetry

import (
	"context"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

func TestNew(t *testing.T) {
	mw := New()
	if mw == nil {
		t.Fatal("expected non-nil middleware")
	}
	if !mw.cfg.EnableTracing {
		t.Error("expected tracing enabled by default")
	}
	if !mw.cfg.EnableMetrics {
		t.Error("expected metrics enabled by default")
	}
}

func TestNewWithOptions(t *testing.T) {
	mw := New(WithTracing(false), WithMetrics(false))
	if mw == nil {
		t.Fatal("expected non-nil middleware")
	}
	if mw.cfg.EnableTracing {
		t.Error("expected tracing disabled")
	}
	if mw.cfg.EnableMetrics {
		t.Error("expected metrics disabled")
	}
}

func TestMiddlewareImplementsInterface(t *testing.T) {
	mw := New()
	var _ agentcore.ReActMiddleware = mw
	_ = mw
}

func TestWrapToolInvokeNoTracer(t *testing.T) {
	// With a nil tracer (no provider), the middleware should pass through
	mw := New(WithTracing(true))

	invoked := false
	ep := func(ctx context.Context, args string, opts ...agentcore.ToolOption) (string, error) {
		invoked = true
		return "result", nil
	}

	wrapped, err := mw.WrapToolInvoke(context.Background(), ep, &agentcore.ToolContext{Name: "test_tool"})
	if err != nil {
		t.Fatalf("WrapToolInvoke failed: %v", err)
	}

	result, err := wrapped(context.Background(), "args")
	if err != nil {
		t.Fatalf("wrapped ep failed: %v", err)
	}
	if !invoked {
		t.Error("expected inner ep to be invoked")
	}
	if result != "result" {
		t.Errorf("expected 'result', got '%s'", result)
	}
}

func TestWrapModelNoTracer(t *testing.T) {
	mw := New()

	generated := false
	model := &mockModel{
		generateFn: func(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.Message, error) {
			generated = true
			return &schema.Message{Role: "assistant", Content: "ok"}, nil
		},
	}

	wrapped, err := mw.WrapModel(context.Background(), model, &agentcore.ModelContext{})
	if err != nil {
		t.Fatalf("WrapModel failed: %v", err)
	}

	result, err := wrapped.Generate(context.Background(), []*schema.Message{{Role: "user", Content: "hi"}})
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}
	if !generated {
		t.Error("expected inner model to be called")
	}
	if result.Content != "ok" {
		t.Errorf("expected 'ok', got '%s'", result.Content)
	}
}

func TestWrapEnhancedToolCallNoTracer(t *testing.T) {
	mw := New()

	invoked := false
	ep := func(ctx context.Context, args *schema.ToolArgument, opts ...agentcore.ToolOption) (*schema.ToolResult, error) {
		invoked = true
		return &schema.ToolResult{Content: "enhanced_result"}, nil
	}

	wrapped, err := mw.WrapEnhancedInvokableToolCall(context.Background(), ep, &agentcore.ToolContext{Name: "enh_tool"})
	if err != nil {
		t.Fatalf("WrapEnhancedInvokableToolCall failed: %v", err)
	}

	result, err := wrapped(context.Background(), &schema.ToolArgument{Name: "enh_tool"})
	if err != nil {
		t.Fatalf("wrapped ep failed: %v", err)
	}
	if !invoked {
		t.Error("expected inner ep to be invoked")
	}
	if result.Content != "enhanced_result" {
		t.Errorf("expected 'enhanced_result', got '%s'", result.Content)
	}
}

// mockModel is a minimal Model implementation for testing.
type mockModel struct {
	generateFn func(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.Message, error)
	streamFn   func(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.StreamReader[*schema.Message], error)
}

func (m *mockModel) Generate(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.Message, error) {
	if m.generateFn != nil {
		return m.generateFn(ctx, msgs, opts...)
	}
	return &schema.Message{Role: "assistant", Content: "mock"}, nil
}

func (m *mockModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.StreamReader[*schema.Message], error) {
	if m.streamFn != nil {
		return m.streamFn(ctx, msgs, opts...)
	}
	return schema.NewStreamReader[*schema.Message](), nil
}

func (m *mockModel) BindTools(tools []*schema.ToolInfo) error { return nil }
