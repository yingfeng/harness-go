package agentcore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// ---- Mock ChatModel ----

type mockModel struct {
	responses   []string
	mu          sync.Mutex
	callCount   int
	shouldFail  bool
}

func (m *mockModel) addResp(r string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.responses = append(m.responses, r)
}

func (m *mockModel) Generate(ctx context.Context, msgs []Message, opts ...modelOption) (Message, error) {
	if m.shouldFail {
		return nil, errors.New("mock model failed")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.callCount >= len(m.responses) {
		return nil, errors.New("no more responses configured")
	}
	resp := m.responses[m.callCount]
	m.callCount++
	return &schema.Message{Role: schema.RoleAssistant, Content: resp}, nil
}

func (m *mockModel) Stream(ctx context.Context, msgs []Message, opts ...modelOption) (*schema.StreamReader[Message], error) {
	msg, err := m.Generate(ctx, msgs, opts...)
	if err != nil { return nil, err }
	return schema.StreamReaderFromArray([]Message{msg}), nil
}

func (m *mockModel) BindTools(tools []*schema.ToolInfo) error { return nil }

// ---- Mock Tool ----

type mockTool struct {
	name     string
	desc     string
	executed bool
	mu       sync.Mutex
}

func (t *mockTool) Name() string                                     { return t.name }
func (t *mockTool) Description() string                               { return t.desc }
func (t *mockTool) Invoke(ctx context.Context, args string, opts ...toolOption) (string, error) {
	t.mu.Lock()
	t.executed = true
	t.mu.Unlock()
	return "mock result for " + t.name, nil
}
func (t *mockTool) Stream(ctx context.Context, args string, opts ...toolOption) (*schema.StreamReader[string], error) {
	return schema.StreamReaderFromArray([]string{"mock stream result"}), nil
}

// ---- Mock Checkpoint Store ----

type memStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (s *memStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	if !ok { return nil, false, nil }
	return v, true, nil
}

func (s *memStore) Set(ctx context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil { s.data = make(map[string][]byte) }
	s.data[key] = data
	return nil
}

// ---- forcedToolModel: produces tool calls on first Generate then falls back ----

type forcedToolModel struct {
	inner      *mockModel
	toolCalls  []schema.ToolCall
	finalResp  string
	firstCall  bool
}

func (m *forcedToolModel) Generate(ctx context.Context, msgs []Message, opts ...modelOption) (Message, error) {
	if m.firstCall {
		m.firstCall = false
		return &schema.Message{
			Role:      schema.RoleAssistant,
			Content:   "",
			ToolCalls: m.toolCalls,
		}, nil
	}
	return &schema.Message{Role: schema.RoleAssistant, Content: m.finalResp}, nil
}

func (m *forcedToolModel) Stream(ctx context.Context, msgs []Message, opts ...modelOption) (*schema.StreamReader[Message], error) {
	msg, _ := m.Generate(ctx, msgs, opts...)
	return schema.StreamReaderFromArray([]Message{msg}), nil
}

func (m *forcedToolModel) BindTools(tools []*schema.ToolInfo) error { return nil }

// ---- loopToolModel: always produces tool calls ----

type loopToolModel struct {
	toolCalls []schema.ToolCall
}

func (m *loopToolModel) Generate(ctx context.Context, msgs []Message, opts ...modelOption) (Message, error) {
	return &schema.Message{Role: schema.RoleAssistant, Content: "", ToolCalls: m.toolCalls}, nil
}

func (m *loopToolModel) Stream(ctx context.Context, msgs []Message, opts ...modelOption) (*schema.StreamReader[Message], error) {
	msg, _ := m.Generate(ctx, msgs, opts...)
	return schema.StreamReaderFromArray([]Message{msg}), nil
}

func (m *loopToolModel) BindTools(tools []*schema.ToolInfo) error { return nil }

// ---- testMiddleware: pluggable middleware for testing ----

type testMiddleware struct {
	BaseMiddleware[*schema.Message]
	beforeAgent        func(context.Context, *ChatModelAgentContext) (context.Context, *ChatModelAgentContext, error)
	beforeModel        func(context.Context, *ChatModelAgentState, *ModelContext) (context.Context, *ChatModelAgentState, error)
	afterModel         func(context.Context, *ChatModelAgentState, *ModelContext) (context.Context, *ChatModelAgentState, error)
	afterAgent         func(context.Context, *ChatModelAgentState) (context.Context, error)
	wrapModel          func(context.Context, ChatModel[*schema.Message], *ModelContext) (ChatModel[*schema.Message], error)
	wrapToolInvoke     func(context.Context, InvokableToolEndpoint, *ToolContext) (InvokableToolEndpoint, error)
	wrapToolStream     func(context.Context, StreamableToolEndpoint, *ToolContext) (StreamableToolEndpoint, error)
	wrapEnhancedInvoke   func(context.Context, EnhancedInvokableToolEndpoint, *ToolContext) (EnhancedInvokableToolEndpoint, error)
	wrapEnhancedStream   func(context.Context, EnhancedStreamableToolEndpoint, *ToolContext) (EnhancedStreamableToolEndpoint, error)
}

func (m *testMiddleware) BeforeAgent(ctx context.Context, rc *ChatModelAgentContext) (context.Context, *ChatModelAgentContext, error) {
	if m.beforeAgent != nil { return m.beforeAgent(ctx, rc) }
	return ctx, rc, nil
}
func (m *testMiddleware) BeforeModelRewrite(ctx context.Context, state *ChatModelAgentState, mc *ModelContext) (context.Context, *ChatModelAgentState, error) {
	if m.beforeModel != nil { return m.beforeModel(ctx, state, mc) }
	return ctx, state, nil
}
func (m *testMiddleware) AfterModelRewrite(ctx context.Context, state *ChatModelAgentState, mc *ModelContext) (context.Context, *ChatModelAgentState, error) {
	if m.afterModel != nil { return m.afterModel(ctx, state, mc) }
	return ctx, state, nil
}
func (m *testMiddleware) AfterAgent(ctx context.Context, state *ChatModelAgentState) (context.Context, error) {
	if m.afterAgent != nil { return m.afterAgent(ctx, state) }
	return ctx, nil
}
func (m *testMiddleware) WrapModel(ctx context.Context, c ChatModel[*schema.Message], mc *ModelContext) (ChatModel[*schema.Message], error) {
	if m.wrapModel != nil { return m.wrapModel(ctx, c, mc) }
	return c, nil
}
func (m *testMiddleware) WrapToolInvoke(ctx context.Context, ep InvokableToolEndpoint, tc *ToolContext) (InvokableToolEndpoint, error) {
	if m.wrapToolInvoke != nil { return m.wrapToolInvoke(ctx, ep, tc) }
	return ep, nil
}
func (m *testMiddleware) WrapToolStream(ctx context.Context, ep StreamableToolEndpoint, tc *ToolContext) (StreamableToolEndpoint, error) {
	if m.wrapToolStream != nil { return m.wrapToolStream(ctx, ep, tc) }
	return ep, nil
}
func (m *testMiddleware) WrapEnhancedInvokableToolCall(ctx context.Context, ep EnhancedInvokableToolEndpoint, tc *ToolContext) (EnhancedInvokableToolEndpoint, error) {
	if m.wrapEnhancedInvoke != nil { return m.wrapEnhancedInvoke(ctx, ep, tc) }
	return m.BaseMiddleware.WrapEnhancedInvokableToolCall(ctx, ep, tc)
}
func (m *testMiddleware) WrapEnhancedStreamableToolCall(ctx context.Context, ep EnhancedStreamableToolEndpoint, tc *ToolContext) (EnhancedStreamableToolEndpoint, error) {
	if m.wrapEnhancedStream != nil { return m.wrapEnhancedStream(ctx, ep, tc) }
	return m.BaseMiddleware.WrapEnhancedStreamableToolCall(ctx, ep, tc)
}

// ---- cancelTestChatModel: delayable model that responds to ctx.Done() ----

type cancelTestChatModel struct {
	delayNs     int64
	response    *schema.Message
	startedChan chan struct{}
	doneChan    chan struct{}
}

func newCancelTestChatModel(resp *schema.Message) *cancelTestChatModel {
	return &cancelTestChatModel{
		response:    resp,
		startedChan: make(chan struct{}, 1),
		doneChan:    make(chan struct{}, 1),
	}
}

func (m *cancelTestChatModel) getDelay() time.Duration {
	return time.Duration(atomic.LoadInt64(&m.delayNs))
}
func (m *cancelTestChatModel) setDelay(d time.Duration) {
	atomic.StoreInt64(&m.delayNs, int64(d))
}
func (m *cancelTestChatModel) Generate(ctx context.Context, msgs []Message, opts ...modelOption) (Message, error) {
	select {
	case m.startedChan <- struct{}{}:
	default:
	}
	select {
	case <-time.After(m.getDelay()):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case m.doneChan <- struct{}{}:
	default:
	}
	return m.response, nil
}
func (m *cancelTestChatModel) Stream(ctx context.Context, msgs []Message, opts ...modelOption) (*schema.StreamReader[Message], error) {
	select {
	case m.startedChan <- struct{}{}:
	default:
	}
	select {
	case <-time.After(m.getDelay()):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case m.doneChan <- struct{}{}:
	default:
	}
	return schema.StreamReaderFromArray([]Message{m.response}), nil
}
func (m *cancelTestChatModel) BindTools(tools []*schema.ToolInfo) error { return nil }

// ---- slowTool: tool with configurable delay ----

type slowTool struct {
	name        string
	delay       time.Duration
	result      string
	callCount   int32
	startedChan chan struct{}
}

func newSlowTool(name string, delay time.Duration, result string) *slowTool {
	return &slowTool{
		name:        name,
		delay:       delay,
		result:      result,
		startedChan: make(chan struct{}, 10),
	}
}
func (t *slowTool) Name() string                                                         { return t.name }
func (t *slowTool) Description() string                                                   { return "slow tool: " + t.name }
func (t *slowTool) Invoke(ctx context.Context, args string, opts ...ToolOption) (string, error) {
	atomic.AddInt32(&t.callCount, 1)
	select {
	case t.startedChan <- struct{}{}:
	default:
	}
	select {
	case <-time.After(t.delay):
	case <-ctx.Done():
		return "", ctx.Err()
	}
	return t.result, nil
}
func (t *slowTool) Stream(ctx context.Context, args string, opts ...ToolOption) (*schema.StreamReader[string], error) {
	return schema.StreamReaderFromArray([]string{t.result}), nil
}
