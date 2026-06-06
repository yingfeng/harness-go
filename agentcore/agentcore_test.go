package agentcore

import (
	"context"
	"errors"
	"sync"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// ---- Mock ChatModel ----

type mockModel struct {
	responses []string
	mu        sync.Mutex
	callCount int
}

func (m *mockModel) addResp(r string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.responses = append(m.responses, r)
}

func (m *mockModel) Generate(ctx context.Context, msgs []Message, opts ...modelOption) (Message, error) {
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
	beforeAgent    func(context.Context, *ChatModelAgentContext) (context.Context, *ChatModelAgentContext, error)
	beforeModel    func(context.Context, *ChatModelAgentState, *ModelContext) (context.Context, *ChatModelAgentState, error)
	afterModel     func(context.Context, *ChatModelAgentState, *ModelContext) (context.Context, *ChatModelAgentState, error)
	afterAgent     func(context.Context, *ChatModelAgentState) (context.Context, error)
	wrapModel      func(context.Context, ChatModel[*schema.Message], *ModelContext) (ChatModel[*schema.Message], error)
	wrapToolInvoke func(context.Context, InvokableToolEndpoint, *ToolContext) (InvokableToolEndpoint, error)
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
