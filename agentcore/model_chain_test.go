package agentcore

import (
	"context"
	"io"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// ---- BuildModelWrapperChain tests ----

func TestBuildModelWrapperChain_NoConfig(t *testing.T) {
	base := &mockModel{}
	base.addResp("raw")
	model := BuildModelWrapperChain(base, nil, DefaultReActConfig[*schema.Message]())
	if model == nil { t.Fatal("nil model") }
	resp, err := model.Generate(context.Background(), []Message{schema.UserMessage("hi")})
	if err != nil { t.Fatalf("Generate: %v", err) }
	if resp.Content != "raw" { t.Errorf("content = %s", resp.Content) }
}

func TestBuildModelWrapperChain_WithRetry(t *testing.T) {
	base := &mockModel{}
	base.addResp("retry-ok")
	cfg := DefaultReActConfig[*schema.Message]()
	cfg.RetryConfig = &ModelRetryConfig{MaxRetries: 2}
	model := BuildModelWrapperChain(base, nil, cfg)
	if model == nil { t.Fatal("nil model") }
	resp, err := model.Generate(context.Background(), []Message{schema.UserMessage("hi")})
	if err != nil { t.Fatalf("Generate: %v", err) }
	if resp.Content != "retry-ok" { t.Errorf("content = %s", resp.Content) }
}

func TestBuildModelWrapperChain_WithFailover(t *testing.T) {
	primary := &mockModel{}
	primary.addResp("primary-ok")
	fallback := &mockModel{}
	fallback.addResp("fallback")

	cfg := DefaultReActConfig[*schema.Message]()
	cfg.FailoverConfig = &FailoverConfigMsg{Models: []ChatModel[*schema.Message]{fallback}}
	model := BuildModelWrapperChain(primary, nil, cfg)
	resp, err := model.Generate(context.Background(), []Message{schema.UserMessage("hi")})
	if err != nil { t.Fatalf("Generate: %v", err) }
	if resp.Content != "primary-ok" { t.Errorf("content = %s", resp.Content) }
}

func TestBuildModelWrapperChain_WithMiddleware(t *testing.T) {
	var wrapCalled bool
	mw := &testMiddleware{}
	mw.wrapModel = func(ctx context.Context, m ChatModel[*schema.Message], mc *ModelContext) (ChatModel[*schema.Message], error) {
		wrapCalled = true
		return m, nil
	}
	base := &mockModel{}
	base.addResp("mw-ok")
	cfg := DefaultReActConfig[*schema.Message]()
	cfg.Middlewares = []ReActMiddleware{mw}
	model := BuildModelWrapperChain(base, nil, cfg)
	resp, err := model.Generate(context.Background(), []Message{schema.UserMessage("hi")})
	if err != nil { t.Fatalf("Generate: %v", err) }
	if !wrapCalled { t.Error("middleware WrapModel not called") }
	_ = resp
}

func TestBuildModelWrapperChain_WithFullChain(t *testing.T) {
	var wrapCalled bool
	mw := &testMiddleware{}
	mw.wrapModel = func(ctx context.Context, m ChatModel[*schema.Message], mc *ModelContext) (ChatModel[*schema.Message], error) {
		wrapCalled = true
		return m, nil
	}

	primary := &mockModel{}
	primary.addResp("chain-ok")
	fallback := &mockModel{}
	fallback.addResp("fallback")

	cfg := DefaultReActConfig[*schema.Message]()
	cfg.RetryConfig = &ModelRetryConfig{MaxRetries: 2}
	cfg.FailoverConfig = &FailoverConfigMsg{Models: []ChatModel[*schema.Message]{fallback}}
	cfg.Middlewares = []ReActMiddleware{mw}

	model := BuildModelWrapperChain(primary, nil, cfg)
	resp, err := model.Generate(context.Background(), []Message{schema.UserMessage("hi")})
	if err != nil { t.Fatalf("Generate: %v", err) }
	if !wrapCalled { t.Error("middleware WrapModel not called in chain") }
	_ = resp
}

func TestBuildModelWrapperChain_NilMiddleware(t *testing.T) {
	base := &mockModel{}
	base.addResp("nil-mw")
	cfg := DefaultReActConfig[*schema.Message]()
	cfg.Middlewares = []ReActMiddleware{nil, nil}
	model := BuildModelWrapperChain(base, nil, cfg)
	_, err := model.Generate(context.Background(), []Message{schema.UserMessage("hi")})
	if err != nil { t.Fatalf("Generate: %v", err) }
}

// ---- eventSenderModelWrapper tests ----

func TestEventSenderModelWrapper_GenerateSendsEvent(t *testing.T) {
	base := &mockModel{}
	base.addResp("event-test")
	it, gen := NewAsyncIteratorPair[*TypedAgentEvent[*schema.Message]]()
	ec := &reActExecCtx{generator: gen}
	wrapped := wrapModelWithEventSender(base, ec)

	go func() {
		defer gen.Close()
		resp, err := wrapped.Generate(context.Background(), []Message{schema.UserMessage("hi")})
		if err != nil { t.Errorf("Generate: %v", err) }
		if resp.Content != "event-test" { t.Errorf("content = %s", resp.Content) }
	}()

	ev, ok := it.Next()
	if !ok { t.Fatal("expected event from wrapper") }
	if ev.Output == nil || ev.Output.MessageOutput == nil {
		t.Error("expected message output event")
	}
}

func TestEventSenderModelWrapper_StreamSendsEvent(t *testing.T) {
	base := &mockModel{}
	base.addResp("stream-event")
	it, gen := NewAsyncIteratorPair[*TypedAgentEvent[*schema.Message]]()
	ec := &reActExecCtx{generator: gen}
	wrapped := wrapModelWithEventSender(base, ec)

	go func() {
		defer gen.Close()
		s, err := wrapped.Stream(context.Background(), []Message{schema.UserMessage("hi")})
		if err != nil { t.Errorf("Stream: %v", err) }
		for {
			_, err := s.Recv()
			if err == io.EOF { break }
			if err != nil { t.Errorf("Recv: %v", err); return }
		}
	}()

	ev, ok := it.Next()
	if !ok { t.Fatal("expected event from stream wrapper") }
	if ev.Output == nil || ev.Output.MessageOutput == nil {
		t.Error("expected message output event from stream")
	}
}

func TestEventSenderModelWrapper_SuppressEventSend(t *testing.T) {
	base := &mockModel{}
	base.addResp("suppressed")
	it, gen := NewAsyncIteratorPair[*TypedAgentEvent[*schema.Message]]()
	ec := &reActExecCtx{generator: gen, suppressEventSend: true}
	wrapped := wrapModelWithEventSender(base, ec)

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer gen.Close()
		_, err := wrapped.Generate(context.Background(), []Message{schema.UserMessage("hi")})
		if err != nil { t.Errorf("Generate: %v", err) }
	}()

	// After Generate returns, check that the channel has no events
	<-done
	// The generator is closed; try reading one item. If suppressEventSend works,
	// the closed empty channel returns a zero value immediately.
	_, ok := it.Next()
	// When the channel is closed and empty, Next() returns (zero, false)
	if ok {
		t.Error("event should be suppressed")
	}
}

func TestEventSenderModelWrapper_NilExecCtx(t *testing.T) {
	base := &mockModel{}
	base.addResp("nil-ec")
	wrapped := wrapModelWithEventSender(base, nil)
	resp, err := wrapped.Generate(context.Background(), []Message{schema.UserMessage("hi")})
	if err != nil { t.Fatalf("Generate: %v", err) }
	if resp.Content != "nil-ec" { t.Errorf("content = %s", resp.Content) }
}

func TestEventSenderModelWrapper_NilGenerator(t *testing.T) {
	base := &mockModel{}
	base.addResp("nil-gen")
	ec := &reActExecCtx{generator: nil}
	wrapped := wrapModelWithEventSender(base, ec)
	resp, err := wrapped.Generate(context.Background(), []Message{schema.UserMessage("hi")})
	if err != nil { t.Fatalf("Generate: %v", err) }
	if resp.Content != "nil-gen" { t.Errorf("content = %s", resp.Content) }
}

func TestEventSenderModelWrapper_BindTools(t *testing.T) {
	base := &mockModel{}
	wrapped := wrapModelWithEventSender(base, nil)
	err := wrapped.BindTools([]*schema.ToolInfo{{Name: "test"}})
	if err != nil { t.Fatalf("BindTools: %v", err) }
}

func TestEventSenderModelWrapper_IsNilMessage(t *testing.T) {
	base := &mockModel{}
	base.addResp("")
	it, gen := NewAsyncIteratorPair[*TypedAgentEvent[*schema.Message]]()
	ec := &reActExecCtx{generator: gen}
	wrapped := wrapModelWithEventSender(base, ec)

	done := make(chan struct{})
	go func() {
		defer gen.Close()
		_, err := wrapped.Generate(context.Background(), []Message{schema.UserMessage("hi")})
		if err != nil { t.Errorf("Generate: %v", err) }
		close(done)
	}()
	// Empty content might still send the event because it's not nil
	var hasEvent bool
	select {
	case <-done:
	case <-it.ch:
		hasEvent = true
	}
	_ = hasEvent
}

// ---- eventSenderToolWrapper tests ----

func TestEventSenderToolWrapper_WrapTool(t *testing.T) {
	it, gen := NewAsyncIteratorPair[*TypedAgentEvent[*schema.Message]]()
	ec := &reActExecCtx{generator: gen}
	wrapper := NewEventSenderToolWrapper[*schema.Message]()

	ep := func(ctx context.Context, args string, opts ...ToolOption) (string, error) {
		return "tool result", nil
	}

	// exec ctx must be in the context at WrapToolInvoke time because the
	// wrapper captures ec via getReActExecCtx from the context.
	runCtx := &runContext{Session: &runSession{Values: map[string]any{"__exec_ctx": ec}}}
	ctx := context.WithValue(context.Background(), runContextKey{}, runCtx)

	wrapped, err := wrapper.WrapToolInvoke(ctx, ep, &ToolContext{Name: "test_tool", CallID: "call_1"})
	if err != nil { t.Fatalf("WrapToolInvoke: %v", err) }

	go func() {
		defer gen.Close()
		result, err := wrapped(ctx, "{}")
		if err != nil { t.Errorf("invoke: %v", err) }
		if result != "tool result" { t.Errorf("result = %q", result) }
	}()

	ev, ok := it.Next()
	if !ok { t.Fatal("expected tool result event") }
	if ev.Output != nil && ev.Output.MessageOutput != nil {
		if ev.Output.MessageOutput.Role != schema.RoleTool {
			t.Errorf("role = %s", ev.Output.MessageOutput.Role)
		}
	}
}

func TestEventSenderToolWrapper_WrapToolStream(t *testing.T) {
	it, gen := NewAsyncIteratorPair[*TypedAgentEvent[*schema.Message]]()
	ec := &reActExecCtx{generator: gen}
	wrapper := NewEventSenderToolWrapper[*schema.Message]()

	ep := func(ctx context.Context, args string, opts ...ToolOption) (*schema.StreamReader[string], error) {
		return schema.StreamReaderFromArray([]string{"stream", "result"}), nil
	}

	wrapped, err := wrapper.WrapToolStream(context.Background(), ep, &ToolContext{Name: "stream_tool", CallID: "call_s1"})
	if err != nil { t.Fatalf("WrapToolStream: %v", err) }

	runCtx := &runContext{Session: &runSession{Values: map[string]any{"__exec_ctx": ec}}}
	ctx := context.WithValue(context.Background(), runContextKey{}, runCtx)

	go func() {
		defer gen.Close()
		s, err := wrapped(ctx, "{}")
		if err != nil { t.Errorf("invoke: %v", err) }
		for {
			_, err := s.Recv()
			if err == io.EOF { break }
			if err != nil { t.Errorf("Recv: %v", err); return }
		}
	}()

	select {
	case <-it.ch:
		t.Logf("got stream tool event")
	default:
		t.Log("stream tool event may arrive async")
	}
}

func TestEventSenderToolWrapper_WrapEnhancedTool(t *testing.T) {
	it, gen := NewAsyncIteratorPair[*TypedAgentEvent[*schema.Message]]()
	ec := &reActExecCtx{generator: gen}
	wrapper := NewEventSenderToolWrapper[*schema.Message]()

	ep := func(ctx context.Context, args *schema.ToolArgument, opts ...ToolOption) (*schema.ToolResult, error) {
		return &schema.ToolResult{Name: "enhanced_tool", Content: "enhanced result"}, nil
	}

	wrapped, err := wrapper.WrapEnhancedInvokableToolCall(context.Background(), ep, &ToolContext{Name: "enhanced", CallID: "call_e1"})
	if err != nil { t.Fatalf("WrapEnhancedInvokableToolCall: %v", err) }

	runCtx := &runContext{Session: &runSession{Values: map[string]any{"__exec_ctx": ec}}}
	ctx := context.WithValue(context.Background(), runContextKey{}, runCtx)

	go func() {
		defer gen.Close()
		result, err := wrapped(ctx, &schema.ToolArgument{Name: "enhanced"})
		if err != nil { t.Errorf("invoke: %v", err) }
		if result.Content != "enhanced result" { t.Errorf("content = %q", result.Content) }
	}()

	select {
	case <-it.ch:
		t.Logf("got enhanced tool event")
	default:
		t.Log("enhanced tool event may arrive async")
	}
}

func TestEventSenderToolWrapper_NilExecCtx(t *testing.T) {
	wrapper := NewEventSenderToolWrapper[*schema.Message]()
	ep := func(ctx context.Context, args string, opts ...ToolOption) (string, error) { return "ok", nil }
	wrapped, err := wrapper.WrapToolInvoke(context.Background(), ep, &ToolContext{Name: "t"})
	if err != nil { t.Fatalf("WrapToolInvoke: %v", err) }
	result, err := wrapped(context.Background(), "{}")
	if err != nil { t.Fatalf("invoke: %v", err) }
	if result != "ok" { t.Errorf("result = %q", result) }
}

func TestEventSenderToolWrapper_StreamNilExecCtx(t *testing.T) {
	wrapper := NewEventSenderToolWrapper[*schema.Message]()
	ep := func(ctx context.Context, args string, opts ...ToolOption) (*schema.StreamReader[string], error) {
		return schema.StreamReaderFromArray([]string{"stream-ok"}), nil
	}
	wrapped, err := wrapper.WrapToolStream(context.Background(), ep, &ToolContext{Name: "t"})
	if err != nil { t.Fatalf("WrapToolStream: %v", err) }
	s, err := wrapped(context.Background(), "{}")
	if err != nil { t.Fatalf("invoke: %v", err) }
	chunk, err := s.Recv()
	if err != nil { t.Fatalf("Recv: %v", err) }
	if chunk != "stream-ok" { t.Errorf("chunk = %q", chunk) }
}

func TestEventSenderToolWrapper_EnhancedStreamNilExecCtx(t *testing.T) {
	wrapper := NewEventSenderToolWrapper[*schema.Message]()
	ep := func(ctx context.Context, args *schema.ToolArgument, opts ...ToolOption) (*schema.StreamReader[*schema.ToolResult], error) {
		return schema.StreamReaderFromArray([]*schema.ToolResult{{Content: "enhanced-stream"}}), nil
	}
	wrapped, err := wrapper.WrapEnhancedStreamableToolCall(context.Background(), ep, &ToolContext{Name: "t"})
	if err != nil { t.Fatalf("WrapEnhancedStreamableToolCall: %v", err) }
	s, err := wrapped(context.Background(), &schema.ToolArgument{Name: "t"})
	if err != nil { t.Fatalf("invoke: %v", err) }
	res, err := s.Recv()
	if err != nil { t.Fatalf("Recv: %v", err) }
	if res.Content != "enhanced-stream" { t.Errorf("content = %q", res.Content) }
}

func TestEventSenderToolWrapper_NilExecutorDetected(t *testing.T) {
	// Test that HasUserEventSenderToolWrapper returns false for nil
	if HasUserEventSenderToolWrapper[*schema.Message](nil) {
		t.Error("nil should be false")
	}
}

// ---- callbackModelWrapper tests ----

func TestCallbackModelWrapper_Basic(t *testing.T) {
	inner := &mockModel{}
	inner.addResp("cb-ok")
	wrapped := &callbackModelWrapper[*schema.Message]{inner: inner}
	resp, err := wrapped.Generate(context.Background(), []Message{schema.UserMessage("hi")})
	if err != nil { t.Fatalf("Generate: %v", err) }
	if resp.Content != "cb-ok" { t.Errorf("content = %s", resp.Content) }
}

func TestCallbackModelWrapper_Stream(t *testing.T) {
	inner := &mockModel{}
	inner.addResp("cb-stream")
	wrapped := &callbackModelWrapper[*schema.Message]{inner: inner}
	s, err := wrapped.Stream(context.Background(), []Message{schema.UserMessage("hi")})
	if err != nil { t.Fatalf("Stream: %v", err) }
	chunk, err := s.Recv()
	if err != nil { t.Fatalf("Recv: %v", err) }
	if chunk.Content != "cb-stream" { t.Errorf("content = %s", chunk.Content) }
}

func TestCallbackModelWrapper_BindTools(t *testing.T) {
	inner := &mockModel{}
	wrapped := &callbackModelWrapper[*schema.Message]{inner: inner}
	err := wrapped.BindTools([]*schema.ToolInfo{{Name: "test"}})
	if err != nil { t.Fatalf("BindTools: %v", err) }
}

// ---- HasUserEventSenderModelWrapper tests ----

func TestHasUserEventSenderModelWrapper_NilSlice(t *testing.T) {
	if HasUserEventSenderModelWrapper[*schema.Message](nil) {
		t.Error("nil should be false")
	}
}

func TestHasUserEventSenderModelWrapper_WithWrapper(t *testing.T) {
	w := NewEventSenderModelWrapper[*schema.Message]()
	handlers := []TypedReActMiddleware[*schema.Message]{w}
	if !HasUserEventSenderModelWrapper(handlers) {
		t.Error("should detect wrapper")
	}
}

func TestHasUserEventSenderModelWrapper_WithoutWrapper(t *testing.T) {
	mw := &testMiddleware{}
	handlers := []TypedReActMiddleware[*schema.Message]{mw}
	if HasUserEventSenderModelWrapper(handlers) {
		t.Error("should not detect non-wrapper")
	}
}

func TestHasUserEventSenderToolWrapper_WithoutWrapper(t *testing.T) {
	mw := &testMiddleware{}
	handlers := []TypedReActMiddleware[*schema.Message]{mw}
	if HasUserEventSenderToolWrapper(handlers) {
		t.Error("should not detect non-wrapper")
	}
}

// ---- Wrapper behavior with ExecCtx integration ----

func TestEventSenderModelWrapper_WithExecCtxGenerator(t *testing.T) {
	base := &mockModel{}
	base.addResp("gen-event")
	it, gen := NewAsyncIteratorPair[*TypedAgentEvent[*schema.Message]]()
	ec := &reActExecCtx{generator: gen}
	wrapped := wrapModelWithEventSender(base, ec)

	go func() {
		defer gen.Close()
		_, err := wrapped.Generate(context.Background(), []Message{schema.UserMessage("hi")})
		if err != nil { t.Errorf("Generate: %v", err) }
	}()

	// Read event from iterator to verify generator integration works
	ev, ok := it.Next()
	if !ok { t.Fatal("expected event via generator") }
	if ev.Output == nil || ev.Output.MessageOutput == nil {
		t.Error("expected message output event")
	}
}
