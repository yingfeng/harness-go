package agentcore

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// ---- Handler/middleware lifecycle tests ----

func TestBaseMiddleware_AllMethods(t *testing.T) {
	var b BaseMiddleware[*schema.Message]
	rc := &ReActAgentContext{}
	s := NewReActAgentState([]*schema.Message{}, nil, 10)
	mc := &ModelContext{}

	ctx, rc2, err := b.BeforeAgent(context.Background(), rc)
	if err != nil {
		t.Fatalf("BeforeAgent: %v", err)
	}
	if rc2 == nil {
		t.Error("nil rc returned")
	}
	_ = ctx

	ctx, err = b.AfterAgent(context.Background(), s)
	if err != nil {
		t.Fatalf("AfterAgent: %v", err)
	}
	_ = ctx

	ctx, s2, err := b.BeforeModelRewrite(context.Background(), s, mc)
	if err != nil {
		t.Fatalf("BeforeModelRewrite: %v", err)
	}
	if s2 == nil {
		t.Error("nil state returned")
	}
	_ = ctx

	ctx, s3, err := b.AfterModelRewrite(context.Background(), s, mc)
	if err != nil {
		t.Fatalf("AfterModelRewrite: %v", err)
	}
	if s3 == nil {
		t.Error("nil state returned")
	}
	_ = ctx

	ep, err := b.WrapToolInvoke(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("WrapToolInvoke: %v", err)
	}
	if ep != nil {
		_ = ep
	}

	ep2, err := b.WrapToolStream(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("WrapToolStream: %v", err)
	}
	_ = ep2

	ep3, err := b.WrapEnhancedInvokableToolCall(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("WrapEnhancedInvokableToolCall: %v", err)
	}
	_ = ep3

	ep4, err := b.WrapEnhancedStreamableToolCall(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("WrapEnhancedStreamableToolCall: %v", err)
	}
	_ = ep4

	m, err := b.WrapModel(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("WrapModel: %v", err)
	}
	if m != nil {
		_ = m
	}
}

func TestMiddleware_BeforeAgentCanModifyInstruction(t *testing.T) {
	mw := &testMiddleware{}
	mw.beforeAgent = func(ctx context.Context, rc *ReActAgentContext) (context.Context, *ReActAgentContext, error) {
		rc.Instruction = "MODIFIED: " + rc.Instruction
		return ctx, rc, nil
	}
	model := &mockModel{}
	model.addResp("modified")
	agent := NewReActAgent(&ReActConfig[*schema.Message]{
		Model: model, Middlewares: []ReActMiddleware{mw},
	})
	agent.name = "mod_agent"
	iter := agent.Run(context.Background(), &AgentInput{
		Messages: []Message{schema.UserMessage("test")},
	})
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		_ = ev
	}
}

func TestMiddleware_BeforeModelRewriteCanModifyState(t *testing.T) {
	mw := &testMiddleware{}
	mw.beforeModel = func(ctx context.Context, state *ReActAgentState, mc *ModelContext) (context.Context, *ReActAgentState, error) {
		state.RemainingIterations = 1 // force stop after 1 iteration
		return ctx, state, nil
	}
	model := &mockModel{}
	model.addResp("bmr-test")
	agent := NewReActAgent(&ReActConfig[*schema.Message]{
		Model: model, Middlewares: []ReActMiddleware{mw},
	})
	agent.name = "bmr_agent"
	iter := agent.Run(context.Background(), &AgentInput{
		Messages: []Message{schema.UserMessage("test")},
	})
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		_ = ev
	}
}

func TestMiddleware_AfterModelRewriteModifiesState(t *testing.T) {
	mw := &testMiddleware{}
	mw.afterModel = func(ctx context.Context, state *ReActAgentState, mc *ModelContext) (context.Context, *ReActAgentState, error) {
		if len(state.Messages) > 0 {
			state.Messages[len(state.Messages)-1] = schema.ToolMessage("rewritten", "call_override")
		}
		return ctx, state, nil
	}
	model := &mockModel{}
	model.addResp("original")
	agent := NewReActAgent(&ReActConfig[*schema.Message]{
		Model: model, Middlewares: []ReActMiddleware{mw},
	})
	agent.name = "amr_agent"
	iter := agent.Run(context.Background(), &AgentInput{
		Messages: []Message{schema.UserMessage("test")},
	})
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		_ = ev
	}
}

func TestMiddleware_MultipleMiddlewares(t *testing.T) {
	var order []string
	mw1 := &testMiddleware{}
	mw1.beforeAgent = func(ctx context.Context, rc *ReActAgentContext) (context.Context, *ReActAgentContext, error) {
		order = append(order, "mw1.BeforeAgent")
		return ctx, rc, nil
	}
	mw2 := &testMiddleware{}
	mw2.beforeAgent = func(ctx context.Context, rc *ReActAgentContext) (context.Context, *ReActAgentContext, error) {
		order = append(order, "mw2.BeforeAgent")
		return ctx, rc, nil
	}
	model := &mockModel{}
	model.addResp("multi")
	agent := NewReActAgent(&ReActConfig[*schema.Message]{
		Model: model, Middlewares: []ReActMiddleware{mw1, mw2},
	})
	agent.name = "multi_mw"
	iter := agent.Run(context.Background(), &AgentInput{
		Messages: []Message{schema.UserMessage("test")},
	})
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		_ = ev
	}
	if len(order) != 2 {
		t.Errorf("expected 2 calls, got %d: %v", len(order), order)
	}
}

func TestMiddleware_BeforeAgentError(t *testing.T) {
	expectedErr := errors.New("before agent error")
	mw := &testMiddleware{}
	mw.beforeAgent = func(ctx context.Context, rc *ReActAgentContext) (context.Context, *ReActAgentContext, error) {
		return ctx, nil, expectedErr
	}
	model := &mockModel{}
	agent := NewReActAgent(&ReActConfig[*schema.Message]{
		Model: model, Middlewares: []ReActMiddleware{mw},
	})
	agent.name = "err_before"
	iter := agent.Run(context.Background(), &AgentInput{
		Messages: []Message{schema.UserMessage("test")},
	})
	var lastErr error
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		if ev.Err != nil {
			lastErr = ev.Err
		}
	}
	if lastErr == nil {
		t.Error("expected error from BeforeAgent middleware")
	}
}

func TestMiddleware_BeforeModelRewriteError(t *testing.T) {
	expectedErr := errors.New("before model error")
	mw := &testMiddleware{}
	mw.beforeModel = func(ctx context.Context, state *ReActAgentState, mc *ModelContext) (context.Context, *ReActAgentState, error) {
		return ctx, nil, expectedErr
	}
	model := &mockModel{}
	agent := NewReActAgent(&ReActConfig[*schema.Message]{
		Model: model, Middlewares: []ReActMiddleware{mw},
	})
	agent.name = "err_bmr"
	iter := agent.Run(context.Background(), &AgentInput{
		Messages: []Message{schema.UserMessage("test")},
	})
	var lastErr error
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		if ev.Err != nil {
			lastErr = ev.Err
		}
	}
	if lastErr == nil {
		t.Error("expected error from BeforeModelRewrite middleware")
	}
}

func TestMiddleware_AfterModelRewriteError(t *testing.T) {
	expectedErr := errors.New("after model error")
	mw := &testMiddleware{}
	mw.afterModel = func(ctx context.Context, state *ReActAgentState, mc *ModelContext) (context.Context, *ReActAgentState, error) {
		return ctx, nil, expectedErr
	}
	model := &mockModel{}
	agent := NewReActAgent(&ReActConfig[*schema.Message]{
		Model: model, Middlewares: []ReActMiddleware{mw},
	})
	agent.name = "err_amr"
	iter := agent.Run(context.Background(), &AgentInput{
		Messages: []Message{schema.UserMessage("test")},
	})
	var lastErr error
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		if ev.Err != nil {
			lastErr = ev.Err
		}
	}
	if lastErr == nil {
		t.Error("expected error from AfterModelRewrite middleware")
	}
}

func TestMiddleware_AfterAgentError(t *testing.T) {
	expectedErr := errors.New("after agent error")
	mw := &testMiddleware{}
	mw.afterAgent = func(ctx context.Context, state *ReActAgentState) (context.Context, error) {
		return ctx, expectedErr
	}
	model := &mockModel{}
	agent := NewReActAgent(&ReActConfig[*schema.Message]{
		Model: model, Middlewares: []ReActMiddleware{mw},
	})
	agent.name = "err_aa"
	iter := agent.Run(context.Background(), &AgentInput{
		Messages: []Message{schema.UserMessage("test")},
	})
	var lastErr error
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		if ev.Err != nil {
			lastErr = ev.Err
		}
	}
	if lastErr == nil {
		t.Error("expected error from AfterAgent middleware")
	}
}

func TestMiddleware_WrapModelReturnsError(t *testing.T) {
	expectedErr := errors.New("wrap model error")
	mw := &testMiddleware{}
	mw.wrapModel = func(ctx context.Context, m ChatModel[*schema.Message], mc *ModelContext) (ChatModel[*schema.Message], error) {
		return nil, expectedErr
	}
	model := &mockModel{}
	agent := NewReActAgent(&ReActConfig[*schema.Message]{
		Model: model, Middlewares: []ReActMiddleware{mw},
	})
	agent.name = "err_wm"
	iter := agent.Run(context.Background(), &AgentInput{
		Messages: []Message{schema.UserMessage("test")},
	})
	var lastErr error
	for {
		ev, ok := iter.Next()
		if !ok { break }
		if ev.Err != nil { lastErr = ev.Err }
	}
	if lastErr == nil {
		t.Error("expected error from WrapModel middleware")
	}
}

func TestMiddleware_WrapToolInvokeProxy(t *testing.T) {
	var called atomic.Bool
	mw := &testMiddleware{}
	mw.wrapToolInvoke = func(ctx context.Context, ep InvokableToolEndpoint, tc *ToolContext) (InvokableToolEndpoint, error) {
		called.Store(true)
		return func(ctx context.Context, args string, opts ...ToolOption) (string, error) {
			return "wrapped:" + tc.Name, nil
		}, nil
	}

	// Direct call without agent execution context
	ep := func(ctx context.Context, args string, opts ...ToolOption) (string, error) { return "raw", nil }
	wrapped, err := mw.WrapToolInvoke(context.Background(), ep, &ToolContext{Name: "test_tool"})
	if err != nil {
		t.Fatalf("WrapToolInvoke: %v", err)
	}
	result, err := wrapped(context.Background(), "{}")
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if result != "wrapped:test_tool" {
		t.Errorf("expected wrapped result, got %q", result)
	}
	if !called.Load() {
		t.Error("middleware WrapToolInvoke not called")
	}
}

func TestMiddleware_WrapToolInvokePassthrough(t *testing.T) {
	mw := &testMiddleware{}
	ep := func(ctx context.Context, args string, opts ...ToolOption) (string, error) { return "passthrough", nil }
	wrapped, err := mw.WrapToolInvoke(context.Background(), ep, &ToolContext{Name: "passthrough"})
	if err != nil {
		t.Fatalf("WrapToolInvoke: %v", err)
	}
	result, err := wrapped(context.Background(), "{}")
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if result != "passthrough" {
		t.Errorf("expected passthrough, got %q", result)
	}
}

func TestMiddleware_MultipleWrapToolInvocations(t *testing.T) {
	var log []string
	mw1 := &testMiddleware{}
	mw1.wrapToolInvoke = func(ctx context.Context, ep InvokableToolEndpoint, tc *ToolContext) (InvokableToolEndpoint, error) {
		log = append(log, "mw1")
		return ep, nil
	}
	mw2 := &testMiddleware{}
	mw2.wrapToolInvoke = func(ctx context.Context, ep InvokableToolEndpoint, tc *ToolContext) (InvokableToolEndpoint, error) {
		log = append(log, "mw2")
		return ep, nil
	}

	ep := func(ctx context.Context, args string, opts ...ToolOption) (string, error) { return "done", nil }

	// Manually chain: first mw2 wraps ep, then mw1 wraps the result
	wrapped1, _ := mw2.WrapToolInvoke(context.Background(), ep, &ToolContext{Name: "t"})
	wrapped2, _ := mw1.WrapToolInvoke(context.Background(), wrapped1, &ToolContext{Name: "t"})
	result, err := wrapped2(context.Background(), "{}")
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if result != "done" {
		t.Errorf("result = %q", result)
	}
	if len(log) != 2 {
		t.Errorf("expected 2 middleware calls, got %d: %v", len(log), log)
	}
}

// ---- Tool integration with middleware chain ----

func TestMiddleware_WrapToolInlineExecution(t *testing.T) {
	var wrapped bool
	mw := &testMiddleware{}
	mw.wrapToolInvoke = func(ctx context.Context, ep InvokableToolEndpoint, tc *ToolContext) (InvokableToolEndpoint, error) {
		wrapped = true
		return func(ctx context.Context, args string, opts ...ToolOption) (string, error) {
			r, err := ep(ctx, args, opts...)
			return "[" + r + "]", err
		}, nil
	}

	tool := &mockTool{name: "greet", desc: "Greeting tool"}
	// Instead of running full agent, test directly
	tCtx := &ToolContext{Name: "greet", CallID: "c1"}
	ep := func(ctx context.Context, args string, opts ...ToolOption) (string, error) {
		return tool.Invoke(ctx, args)
	}
	wrappedEp, err := mw.WrapToolInvoke(context.Background(), ep, tCtx)
	if err != nil {
		t.Fatalf("WrapToolInvoke: %v", err)
	}
	result, err := wrappedEp(context.Background(), `{"name":"world"}`)
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if !wrapped {
		t.Error("middleware not called")
	}
	if result != "[mock result for greet]" {
		t.Errorf("result = %q", result)
	}
}

func TestMiddleware_WrapModelPassthrough(t *testing.T) {
	mw := &testMiddleware{}
	model := &mockModel{}
	model.addResp("passthrough-result")
	mc := &ModelContext{}
	wrapped, err := mw.WrapModel(context.Background(), model, mc)
	if err != nil {
		t.Fatalf("WrapModel: %v", err)
	}
	resp, err := wrapped.Generate(context.Background(), []*schema.Message{{Role: schema.RoleUser, Content: "test"}})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if resp.Content != "passthrough-result" {
		t.Errorf("expected 'passthrough-result', got %q", resp.Content)
	}
}

func TestMiddleware_WrapEnhancedToolCall(t *testing.T) {
	mw := &testMiddleware{}
	var called bool
	mw.wrapEnhancedInvoke = func(ctx context.Context, ep EnhancedInvokableToolEndpoint, tc *ToolContext) (EnhancedInvokableToolEndpoint, error) {
		called = true
		return ep, nil
	}
	ep := EnhancedInvokableToolEndpoint(func(ctx context.Context, args *schema.ToolArgument, opts ...ToolOption) (*schema.ToolResult, error) {
		return &schema.ToolResult{Content: "enhanced result"}, nil
	})
	wrapped, err := mw.WrapEnhancedInvokableToolCall(context.Background(), ep, &ToolContext{Name: "enhanced_tool"})
	if err != nil {
		t.Fatalf("WrapEnhancedInvokableToolCall: %v", err)
	}
	result, err := wrapped(context.Background(), &schema.ToolArgument{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if result.Content != "enhanced result" {
		t.Errorf("expected 'enhanced result', got %q", result.Content)
	}
	if !called {
		t.Error("middleware not called")
	}
}

func TestMiddleware_WrapEnhancedStreamableToolCall(t *testing.T) {
	mw := &testMiddleware{}
	var called bool
	mw.wrapEnhancedStream = func(ctx context.Context, ep EnhancedStreamableToolEndpoint, tc *ToolContext) (EnhancedStreamableToolEndpoint, error) {
		called = true
		return ep, nil
	}
	ep := EnhancedStreamableToolEndpoint(func(ctx context.Context, args *schema.ToolArgument, opts ...ToolOption) (*schema.StreamReader[*schema.ToolResult], error) {
		return schema.StreamReaderFromArray([]*schema.ToolResult{{Content: "streamed"}}), nil
	})
	wrapped, err := mw.WrapEnhancedStreamableToolCall(context.Background(), ep, &ToolContext{Name: "stream_tool"})
	if err != nil {
		t.Fatalf("WrapEnhancedStreamableToolCall: %v", err)
	}
	sr, err := wrapped(context.Background(), &schema.ToolArgument{})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	result, err := sr.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if result.Content != "streamed" {
		t.Errorf("expected 'streamed', got %q", result.Content)
	}
	if !called {
		t.Error("middleware not called")
	}
}
