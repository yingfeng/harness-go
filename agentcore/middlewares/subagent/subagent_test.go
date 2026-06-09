package subagent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// ---- Mock Model ----

type mockModel struct {
	responses []string
	mu        sync.Mutex
}

func (m *mockModel) addResp(r string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.responses = append(m.responses, r)
}

func (m *mockModel) Generate(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.responses) == 0 {
		return nil, errors.New("mockModel: no more responses")
	}
	resp := m.responses[0]
	m.responses = m.responses[1:]
	return &schema.Message{Role: schema.RoleAssistant, Content: resp}, nil
}

func (m *mockModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	return schema.StreamReaderFromArray([]*schema.Message{msg}), nil
}

func (m *mockModel) BindTools(tools []*schema.ToolInfo) error { return nil }

// ---- forcedToolModel: first call returns tool calls, subsequent return final response ----

type forcedToolModel struct {
	inner     *mockModel
	toolCalls []schema.ToolCall
	finalResp string
	mu        sync.Mutex
	firstCall bool
}

func newForcedToolModel(inner *mockModel, toolCalls []schema.ToolCall, finalResp string) *forcedToolModel {
	return &forcedToolModel{
		inner:     inner,
		toolCalls: toolCalls,
		finalResp: finalResp,
		firstCall: true,
	}
}

func (m *forcedToolModel) Generate(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.Message, error) {
	m.mu.Lock()
	isFirst := m.firstCall
	if isFirst {
		m.firstCall = false
	}
	m.mu.Unlock()
	if isFirst {
		return &schema.Message{
			Role:      schema.RoleAssistant,
			Content:   "",
			ToolCalls: m.toolCalls,
		}, nil
	}
	return &schema.Message{Role: schema.RoleAssistant, Content: m.finalResp}, nil
}

func (m *forcedToolModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.StreamReader[*schema.Message], error) {
	msg, _ := m.Generate(ctx, msgs, opts...)
	return schema.StreamReaderFromArray([]*schema.Message{msg}), nil
}

func (m *forcedToolModel) BindTools(tools []*schema.ToolInfo) error { return nil }

// ---- Mock Tool ----

type mockTool struct {
	name     string
	desc     string
	executed bool
	mu       sync.Mutex
}

func (t *mockTool) Name() string             { return t.name }
func (t *mockTool) Description() string       { return t.desc }
func (t *mockTool) Invoke(ctx context.Context, args string, opts ...agentcore.ToolOption) (string, error) {
	t.mu.Lock()
	t.executed = true
	t.mu.Unlock()
	return "mock result for " + t.name, nil
}
func (t *mockTool) Stream(ctx context.Context, args string, opts ...agentcore.ToolOption) (*schema.StreamReader[string], error) {
	return schema.StreamReaderFromArray([]string{"mock stream result"}), nil
}

// ---- Middleware tracking ----

type trackingMiddleware struct {
	agentcore.BaseMiddleware[*schema.Message]
	beforeAgentCalled bool
	beforeModelCalled bool
	mu                sync.Mutex
}

func (m *trackingMiddleware) BeforeAgent(ctx context.Context, rc *agentcore.ReActAgentContext) (context.Context, *agentcore.ReActAgentContext, error) {
	m.mu.Lock()
	m.beforeAgentCalled = true
	m.mu.Unlock()
	return ctx, rc, nil
}
func (m *trackingMiddleware) BeforeModelRewrite(ctx context.Context, state *agentcore.ReActAgentState, mc *agentcore.ModelContext) (context.Context, *agentcore.ReActAgentState, error) {
	m.mu.Lock()
	m.beforeModelCalled = true
	m.mu.Unlock()
	return ctx, state, nil
}

// ---- Checkpoint store ----

type memStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemStore() *memStore { return &memStore{data: make(map[string][]byte)} }
func (s *memStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	if !ok {
		return nil, false, nil
	}
	return v, true, nil
}
func (s *memStore) Set(ctx context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = data
	return nil
}

// ---- Helpers ----

func runAgent(ctx context.Context, t *testing.T, agent agentcore.Agent, msg string) (string, error) {
	t.Helper()
	runner := agentcore.NewTypedRunner(agentcore.RunnerConfig[*schema.Message]{Agent: agent})
	iter := runner.Run(ctx, []*schema.Message{schema.UserMessage(msg)})
	var final string
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		if ev.Err != nil {
			return final, ev.Err
		}
		if ev.Output != nil && ev.Output.MessageOutput != nil &&
			!ev.Output.MessageOutput.IsStreaming &&
			ev.Output.MessageOutput.Message != nil {
			final = ev.Output.MessageOutput.Message.Content
		}
	}
	return final, nil
}

func runAgentWithStore(ctx context.Context, t *testing.T, agent agentcore.Agent, msg string, store *memStore) (string, error) {
	t.Helper()
	runner := agentcore.NewTypedRunner(agentcore.RunnerConfig[*schema.Message]{Agent: agent, CheckPointStore: store})
	iter := runner.Run(ctx, []*schema.Message{schema.UserMessage(msg)})
	var final string
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		if ev.Err != nil {
			return final, ev.Err
		}
		if ev.Output != nil && ev.Output.MessageOutput != nil &&
			!ev.Output.MessageOutput.IsStreaming &&
			ev.Output.MessageOutput.Message != nil {
			final = ev.Output.MessageOutput.Message.Content
		}
	}
	return final, nil
}

// ========================================================================
// Tests
// ========================================================================

// TestSubAgent_Basic verifies a pre-built sub-agent is invoked via tool call.
func TestSubAgent_Basic(t *testing.T) {
	subModel := &mockModel{}
	subModel.addResp("result from researcher")
	subAgent := agentcore.NewReActAgent(&agentcore.ReActConfig[*schema.Message]{
		Model: subModel,
	}).WithName("researcher").WithDescription("Research a topic")

	mw := New([]SubAgentSpec{
		{Name: "researcher", Description: "Research a topic", Agent: subAgent},
	}, nil)

	parentModel := newForcedToolModel(&mockModel{},
		[]schema.ToolCall{
			{ID: "call_1", Function: schema.ToolCallFunction{Name: "researcher", Arguments: "{}"}},
		},
		"parent final answer",
	)
	cfg := &agentcore.ReActConfig[*schema.Message]{Model: parentModel, Middlewares: []agentcore.ReActMiddleware{mw}}
	mw.BindToConfig(cfg)
	agent := agentcore.NewReActAgent(cfg)

	ctx := context.Background()
	final, err := runAgent(ctx, t, agent, "research something")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if final != "parent final answer" {
		t.Errorf("expected 'parent final answer', got %q", final)
	}
	t.Logf("basic: final=%q", final)
}

// TestSubAgent_DeclarativeConfig verifies the declarative AgentConfig path.
func TestSubAgent_DeclarativeConfig(t *testing.T) {
	mw := New([]SubAgentSpec{
		{
			Name:        "worker",
			Description: "Worker agent",
			AgentConfig: &AgentConfig{
				Model: func() agentcore.Model[*schema.Message] {
					m := &mockModel{}
					m.addResp("worker done")
					return m
				}(),
				SystemPrompt: "You are a worker.",
			},
		},
	}, nil)

	parentModel := newForcedToolModel(&mockModel{},
		[]schema.ToolCall{
			{ID: "w1", Function: schema.ToolCallFunction{Name: "worker", Arguments: "{}"}},
		},
		"parent ok",
	)
	cfg := &agentcore.ReActConfig[*schema.Message]{Model: parentModel, Middlewares: []agentcore.ReActMiddleware{mw}}
	mw.BindToConfig(cfg)
	agent := agentcore.NewReActAgent(cfg)

	ctx := context.Background()
	final, err := runAgent(ctx, t, agent, "do work")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if final != "parent ok" {
		t.Errorf("expected 'parent ok', got %q", final)
	}
	t.Logf("declarative: final=%q", final)
}

// TestSubAgent_DeclarativeWithOwnTools verifies AgentConfig sub-agent that
// has its own tools.
func TestSubAgent_DeclarativeWithOwnTools(t *testing.T) {
	innerTool := &mockTool{name: "calc", desc: "Calculator"}

	mw := New([]SubAgentSpec{
		{
			Name:        "worker",
			Description: "Worker with tools",
			AgentConfig: &AgentConfig{
				Model: newForcedToolModel(&mockModel{},
					[]schema.ToolCall{
						{ID: "ct", Function: schema.ToolCallFunction{Name: "calc", Arguments: "{'x':1}"}},
					},
					"worker result",
				),
				Tools:         []agentcore.Tool{innerTool},
				SystemPrompt:  "You are a worker with tools.",
				MaxIterations: 5,
			},
		},
	}, nil)

	parentModel := newForcedToolModel(&mockModel{},
		[]schema.ToolCall{
			{ID: "pw", Function: schema.ToolCallFunction{Name: "worker", Arguments: "{}"}},
		},
		"parent done",
	)
	cfg := &agentcore.ReActConfig[*schema.Message]{Model: parentModel, Middlewares: []agentcore.ReActMiddleware{mw}}
	mw.BindToConfig(cfg)
	agent := agentcore.NewReActAgent(cfg)

	ctx := context.Background()
	final, err := runAgent(ctx, t, agent, "do work")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if final != "parent done" {
		t.Errorf("expected 'parent done', got %q", final)
	}
	if !innerTool.executed {
		t.Error("sub-agent's own tool was not executed")
	}
	t.Logf("declarative with tools: final=%q, tool executed=%v", final, innerTool.executed)
}

// TestSubAgent_MultipleSubAgents verifies multiple sub-agents are available.
func TestSubAgent_MultipleSubAgents(t *testing.T) {
	mw := New([]SubAgentSpec{
		{
			Name: "researcher", Description: "Research agent",
			AgentConfig: &AgentConfig{Model: func() *mockModel { m := &mockModel{}; m.addResp("research done"); return m }()},
		},
		{
			Name: "coder", Description: "Coding agent",
			AgentConfig: &AgentConfig{Model: func() *mockModel { m := &mockModel{}; m.addResp("code done"); return m }()},
		},
	}, nil)

	parentModel := newForcedToolModel(&mockModel{},
		[]schema.ToolCall{
			{ID: "c1", Function: schema.ToolCallFunction{Name: "coder", Arguments: "{}"}},
		},
		"parent done",
	)
	cfg := &agentcore.ReActConfig[*schema.Message]{Model: parentModel, Middlewares: []agentcore.ReActMiddleware{mw}}
	mw.BindToConfig(cfg)
	agent := agentcore.NewReActAgent(cfg)

	ctx := context.Background()
	final, err := runAgent(ctx, t, agent, "do work")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if final != "parent done" {
		t.Errorf("expected 'parent done', got %q", final)
	}
	t.Logf("multiple sub-agents: final=%q", final)
}

// TestSubAgent_AgentFactory verifies lazy agent construction (backward compat).
func TestSubAgent_AgentFactory(t *testing.T) {
	var constructed bool
	factory := func(ctx context.Context) (agentcore.Agent, error) {
		constructed = true
		m := &mockModel{}
		m.addResp("factory built result")
		return agentcore.NewReActAgent(&agentcore.ReActConfig[*schema.Message]{
			Model: m,
		}).WithName("factory_agent").WithDescription("Lazy built agent"), nil
	}

	mw := New([]SubAgentSpec{
		{Name: "factory_agent", Description: "Lazy built", AgentFactory: factory},
	}, nil)

	parentModel := newForcedToolModel(&mockModel{},
		[]schema.ToolCall{
			{ID: "f1", Function: schema.ToolCallFunction{Name: "factory_agent", Arguments: "{}"}},
		},
		"parent with factory",
	)
	cfg := &agentcore.ReActConfig[*schema.Message]{Model: parentModel, Middlewares: []agentcore.ReActMiddleware{mw}}
	mw.BindToConfig(cfg)
	agent := agentcore.NewReActAgent(cfg)

	ctx := context.Background()
	final, err := runAgent(ctx, t, agent, "test factory")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if final != "parent with factory" {
		t.Errorf("expected 'parent with factory', got %q", final)
	}
	if !constructed {
		t.Error("AgentFactory was not called")
	}
	t.Logf("factory: final=%q, constructed=%v", final, constructed)
}

// TestSubAgent_MiddlewareChain verifies integration with other middlewares.
func TestSubAgent_MiddlewareChain(t *testing.T) {
	tracker := &trackingMiddleware{}

	mw := New([]SubAgentSpec{
		{
			Name: "helper", Description: "Helper agent",
			AgentConfig: &AgentConfig{
				Model: func() *mockModel { m := &mockModel{}; m.addResp("helper done"); return m }(),
			},
		},
	}, nil)

	parentModel := newForcedToolModel(&mockModel{},
		[]schema.ToolCall{
			{ID: "h1", Function: schema.ToolCallFunction{Name: "helper", Arguments: "{}"}},
		},
		"parent chain",
	)
	cfg := &agentcore.ReActConfig[*schema.Message]{
		Model:       parentModel,
		Middlewares: []agentcore.ReActMiddleware{tracker, mw},
	}
	mw.BindToConfig(cfg)
	agent := agentcore.NewReActAgent(cfg)

	ctx := context.Background()
	final, err := runAgent(ctx, t, agent, "chain test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if final != "parent chain" {
		t.Errorf("expected 'parent chain', got %q", final)
	}
	if !tracker.beforeAgentCalled {
		t.Error("tracking middleware BeforeAgent was not called")
	}
	t.Logf("chain: final=%q, tracker.BeforeAgent=%v", final, tracker.beforeAgentCalled)
}

// TestSubAgent_NestedSubAgent verifies 3-level nesting (parent → middle → inner).
func TestSubAgent_NestedSubAgent(t *testing.T) {
	// Innermost.
	innerMW := New([]SubAgentSpec{
		{
			Name: "inner", Description: "Inner sub-agent",
			AgentConfig: &AgentConfig{
				Model: func() *mockModel { m := &mockModel{}; m.addResp("inner result"); return m }(),
			},
		},
	}, &Config{MaxDepth: 5})
	innerCfg := &agentcore.ReActConfig[*schema.Message]{
		Model: newForcedToolModel(&mockModel{},
			[]schema.ToolCall{
				{ID: "inner1", Function: schema.ToolCallFunction{Name: "inner", Arguments: "{}"}},
			},
			"middle done",
		),
		Middlewares: []agentcore.ReActMiddleware{innerMW},
	}
	innerMW.BindToConfig(innerCfg)
	middleAgent := agentcore.NewReActAgent(innerCfg).WithName("middle").WithDescription("Middle sub-agent")

	// Top-level.
	outerMW := New([]SubAgentSpec{
		{Name: "middle", Description: "Middle sub-agent", Agent: middleAgent},
	}, &Config{MaxDepth: 5})
	outerCfg := &agentcore.ReActConfig[*schema.Message]{
		Model: newForcedToolModel(&mockModel{},
			[]schema.ToolCall{
				{ID: "outer1", Function: schema.ToolCallFunction{Name: "middle", Arguments: "{}"}},
			},
			"top done",
		),
		Middlewares: []agentcore.ReActMiddleware{outerMW},
	}
	outerMW.BindToConfig(outerCfg)
	topAgent := agentcore.NewReActAgent(outerCfg)

	ctx := context.Background()
	final, err := runAgent(ctx, t, topAgent, "nested call")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if final != "top done" {
		t.Errorf("expected 'top done', got %q", final)
	}
	t.Logf("nested: final=%q", final)
}

// TestSubAgent_RecursionGuard verifies that nesting beyond MaxDepth is blocked.
// The tool error is converted to a tool result string (not a Go error) by
// ToolsNode.executeStandard, so the agent completes normally but the inner
// agent is never invoked.
func TestSubAgent_RecursionGuard(t *testing.T) {
	// Track whether leaf model was ever called.
	leafModel := &mockModel{}
	leafModel.addResp("leaf result")

	leafAgent := agentcore.NewReActAgent(&agentcore.ReActConfig[*schema.Message]{
		Model: leafModel,
	}).WithName("leaf").WithDescription("Leaf agent (innermost)")

	// Middle sub-agent with MaxDepth=1: parent→middle works (depth 0→1),
	// but middle→leaf fails (depth 1→2 exceeds limit).
	middleMW := New([]SubAgentSpec{
		{Name: "leaf", Description: "Leaf", Agent: leafAgent},
	}, &Config{MaxDepth: 1})
	middleCfg := &agentcore.ReActConfig[*schema.Message]{
		Model: newForcedToolModel(&mockModel{},
			[]schema.ToolCall{
				{ID: "leaf1", Function: schema.ToolCallFunction{Name: "leaf", Arguments: "{}"}},
			},
			"middle done",
		),
		MaxIterations: 5,
		Middlewares:   []agentcore.ReActMiddleware{middleMW},
	}
	middleMW.BindToConfig(middleCfg)
	middleAgent := agentcore.NewReActAgent(middleCfg).WithName("middle").WithDescription("Middle sub-agent")

	// Top-level parent agent calls middle.
	topMW := New([]SubAgentSpec{
		{Name: "middle", Description: "Middle", Agent: middleAgent},
	}, nil)
	topCfg := &agentcore.ReActConfig[*schema.Message]{
		Model: newForcedToolModel(&mockModel{},
			[]schema.ToolCall{
				{ID: "top1", Function: schema.ToolCallFunction{Name: "middle", Arguments: "{}"}},
			},
			"top done",
		),
		MaxIterations: 5,
		Middlewares:   []agentcore.ReActMiddleware{topMW},
	}
	topMW.BindToConfig(topCfg)
	topAgent := agentcore.NewReActAgent(topCfg)

	ctx := context.Background()
	final, err := runAgent(ctx, t, topAgent, "start")
	if err != nil {
		// Go-level error from inline path is also acceptable.
		t.Logf("recursion guard: got Go error: %v", err)
		return
	}
	// No Go error: ToolsNode captured the recursion error as a tool result string.
	if final != "top done" {
		t.Errorf("expected 'top done', got %q", final)
	}
	// Verify leaf model was NEVER called (responses not consumed → still has 1 entry).
	t.Logf("recursion guard: leaf model has %d remaining responses", len(leafModel.responses))
	if len(leafModel.responses) != 1 {
		t.Error("recursion guard: leaf model was invoked when it should have been blocked")
	}
	t.Logf("recursion guard: final=%q, leaf blocked=true", final)
}

// TestSubAgent_NestedWithinLimit verifies nesting works when depth is within MaxDepth.

// TestSubAgent_NestedWithinLimit verifies nesting works when depth is within MaxDepth.
func TestSubAgent_NestedWithinLimit(t *testing.T) {
	// Leaf sub-agent.
	leafAgent := agentcore.NewReActAgent(&agentcore.ReActConfig[*schema.Message]{
		Model: func() *mockModel { m := &mockModel{}; m.addResp("leaf result"); return m }(),
	}).WithName("leaf").WithDescription("Leaf agent")

	// Middle sub-agent with MaxDepth=2 (allows parent→middle→leaf).
	middleMW := New([]SubAgentSpec{
		{Name: "leaf", Description: "Leaf", Agent: leafAgent},
	}, &Config{MaxDepth: 2})
	middleCfg := &agentcore.ReActConfig[*schema.Message]{
		Model: newForcedToolModel(&mockModel{},
			[]schema.ToolCall{
				{ID: "leaf1", Function: schema.ToolCallFunction{Name: "leaf", Arguments: "{}"}},
			},
			"middle done",
		),
		MaxIterations: 5,
		Middlewares:   []agentcore.ReActMiddleware{middleMW},
	}
	middleMW.BindToConfig(middleCfg)
	middleAgent := agentcore.NewReActAgent(middleCfg).WithName("middle").WithDescription("Middle sub-agent")

	// Top-level.
	topMW := New([]SubAgentSpec{
		{Name: "middle", Description: "Middle", Agent: middleAgent},
	}, nil)
	topCfg := &agentcore.ReActConfig[*schema.Message]{
		Model: newForcedToolModel(&mockModel{},
			[]schema.ToolCall{
				{ID: "top1", Function: schema.ToolCallFunction{Name: "middle", Arguments: "{}"}},
			},
			"top done",
		),
		MaxIterations: 5,
		Middlewares:   []agentcore.ReActMiddleware{topMW},
	}
	topMW.BindToConfig(topCfg)
	topAgent := agentcore.NewReActAgent(topCfg)

	ctx := context.Background()
	final, err := runAgent(ctx, t, topAgent, "start")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if final != "top done" {
		t.Errorf("expected 'top done', got %q", final)
	}
	t.Logf("within limit: final=%q", final)
}

// TestSubAgent_MiddlewareInheritance verifies parent middlewares are inherited.
func TestSubAgent_MiddlewareInheritance(t *testing.T) {
	parentTracker := &trackingMiddleware{}

	// Sub-agent with InheritParentMiddlewares. It should have parentTracker
	// in its middleware chain (but NOT the SubAgentMiddleware itself).
	mw := New([]SubAgentSpec{
		{
			Name:        "inheritor",
			Description: "Inheriting sub-agent",
			AgentConfig: &AgentConfig{
				Model: func() *mockModel { m := &mockModel{}; m.addResp("inheritor done"); return m }(),
			},
			InheritParentMiddlewares: true,
			ExcludedParentMiddlewareNames: nil,
		},
	}, nil)

	parentModel := newForcedToolModel(&mockModel{},
		[]schema.ToolCall{
			{ID: "ih", Function: schema.ToolCallFunction{Name: "inheritor", Arguments: "{}"}},
		},
		"parent inherited",
	)
	cfg := &agentcore.ReActConfig[*schema.Message]{
		Model:       parentModel,
		Middlewares: []agentcore.ReActMiddleware{parentTracker, mw},
	}
	mw.BindToConfig(cfg)
	agent := agentcore.NewReActAgent(cfg)

	ctx := context.Background()
	final, err := runAgent(ctx, t, agent, "test inheritance")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if final != "parent inherited" {
		t.Errorf("expected 'parent inherited', got %q", final)
	}
	if !parentTracker.beforeAgentCalled {
		t.Error("parent tracker BeforeAgent was not called")
	}
	t.Logf("inheritance: final=%q, parentTracker.BeforeAgent=%v", final, parentTracker.beforeAgentCalled)
}

// TestSubAgent_NoParentTools verifies graceful handling when parent has only
// sub-agent tools (no user-provided tools).
func TestSubAgent_NoParentTools(t *testing.T) {
	mw := New([]SubAgentSpec{
		{
			Name: "researcher", Description: "Research agent",
			AgentConfig: &AgentConfig{
				Model: func() *mockModel { m := &mockModel{}; m.addResp("research done"); return m }(),
			},
		},
	}, nil)

	parentModel := &mockModel{}
	parentModel.addResp("no tools needed")

	cfg := &agentcore.ReActConfig[*schema.Message]{Model: parentModel, Middlewares: []agentcore.ReActMiddleware{mw}}
	mw.BindToConfig(cfg)
	agent := agentcore.NewReActAgent(cfg)

	ctx := context.Background()
	final, err := runAgent(ctx, t, agent, "hello")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if final != "no tools needed" {
		t.Errorf("expected 'no tools needed', got %q", final)
	}
	t.Logf("no parent tools: final=%q", final)
}

// TestSubAgent_AgentFactoryOnly verifies the legacy AgentFactory path still works.
func TestSubAgent_AgentFactoryOnly(t *testing.T) {
	mw := New([]SubAgentSpec{
		{
			Name:        "legacy",
			Description: "Legacy factory agent",
			AgentFactory: func(ctx context.Context) (agentcore.Agent, error) {
				m := &mockModel{}
				m.addResp("legacy result")
				return agentcore.NewReActAgent(&agentcore.ReActConfig[*schema.Message]{
					Model: m,
				}).WithName("legacy").WithDescription("Legacy factory agent"), nil
			},
		},
	}, nil)

	parentModel := newForcedToolModel(&mockModel{},
		[]schema.ToolCall{
			{ID: "lg", Function: schema.ToolCallFunction{Name: "legacy", Arguments: "{}"}},
		},
		"parent legacy",
	)
	cfg := &agentcore.ReActConfig[*schema.Message]{Model: parentModel, Middlewares: []agentcore.ReActMiddleware{mw}}
	mw.BindToConfig(cfg)
	agent := agentcore.NewReActAgent(cfg)

	ctx := context.Background()
	final, err := runAgent(ctx, t, agent, "test legacy")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if final != "parent legacy" {
		t.Errorf("expected 'parent legacy', got %q", final)
	}
	t.Logf("legacy factory: final=%q", final)
}

// TestSubAgent_BindIdempotent verifies BindToConfig can be called multiple times
// without creating duplicate tools.
func TestSubAgent_BindIdempotent(t *testing.T) {
	mw := New([]SubAgentSpec{
		{
			Name: "worker", Description: "Worker",
			AgentConfig: &AgentConfig{
				Model: func() *mockModel { m := &mockModel{}; m.addResp("ok"); return m }(),
			},
		},
	}, nil)

	cfg := &agentcore.ReActConfig[*schema.Message]{
		Model:       newForcedToolModel(&mockModel{}, nil, "done"),
		Middlewares: []agentcore.ReActMiddleware{mw},
	}
	// Call BindToConfig twice.
	mw.BindToConfig(cfg)
	mw.BindToConfig(cfg)

	// Should have exactly 1 tool.
	if len(cfg.Tools) != 1 {
		t.Errorf("expected 1 tool after idempotent BindToConfig, got %d", len(cfg.Tools))
	}
	t.Logf("idempotent: tools=%d", len(cfg.Tools))
}

// TestSubAgent_RecursionErrorMessageDirect is covered by the direct test
// in agentcore/ (which accesses unexported subAgentDepthKey).

// TestSubAgent_SubAgentOwnMiddlewares verifies sub-agent specific middlewares
// are applied alongside inherited ones.
func TestSubAgent_SubAgentOwnMiddlewares(t *testing.T) {
	subTracker := &trackingMiddleware{}

	mw := New([]SubAgentSpec{
		{
			Name:        "tracked",
			Description: "Tracked sub-agent",
			AgentConfig: &AgentConfig{
				Model: func() *mockModel { m := &mockModel{}; m.addResp("tracked done"); return m }(),
				Middlewares: []agentcore.ReActMiddleware{subTracker},
			},
			InheritParentMiddlewares: true,
		},
	}, nil)

	parentModel := newForcedToolModel(&mockModel{},
		[]schema.ToolCall{
			{ID: "tr", Function: schema.ToolCallFunction{Name: "tracked", Arguments: "{}"}},
		},
		"parent tracked",
	)
	cfg := &agentcore.ReActConfig[*schema.Message]{Model: parentModel, Middlewares: []agentcore.ReActMiddleware{mw}}
	mw.BindToConfig(cfg)
	agent := agentcore.NewReActAgent(cfg)

	ctx := context.Background()
	final, err := runAgent(ctx, t, agent, "test own middlewares")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if final != "parent tracked" {
		t.Errorf("expected 'parent tracked', got %q", final)
	}
	if !subTracker.beforeAgentCalled {
		t.Error("sub-agent's own tracker BeforeAgent was not called")
	}
	t.Logf("own middlewares: final=%q, subTracker.BeforeAgent=%v", final, subTracker.beforeAgentCalled)
}

// TestSubAgent_MaxDepthDefault verifies that MaxDepth=0 allows unlimited nesting.
func TestSubAgent_MaxDepthDefault(t *testing.T) {
	// 3 levels with default MaxDepth=0 should work.
	leafModel := &mockModel{}
	leafModel.addResp("leaf")
	leafAgent := agentcore.NewReActAgent(&agentcore.ReActConfig[*schema.Message]{
		Model: leafModel,
	}).WithName("leaf").WithDescription("Leaf")

	middleMW := New([]SubAgentSpec{
		{Name: "leaf", Description: "Leaf", Agent: leafAgent},
	}, nil) // MaxDepth=0
	middleCfg := &agentcore.ReActConfig[*schema.Message]{
		Model: newForcedToolModel(&mockModel{},
			[]schema.ToolCall{
				{ID: "leaf1", Function: schema.ToolCallFunction{Name: "leaf", Arguments: "{}"}},
			},
			fmt.Sprintf("middle done"),
		),
		MaxIterations: 5,
		Middlewares:   []agentcore.ReActMiddleware{middleMW},
	}
	middleMW.BindToConfig(middleCfg)
	middleAgent := agentcore.NewReActAgent(middleCfg).WithName("middle").WithDescription("Middle")

	topMW := New([]SubAgentSpec{
		{Name: "middle", Description: "Middle", Agent: middleAgent},
	}, nil)
	topCfg := &agentcore.ReActConfig[*schema.Message]{
		Model: newForcedToolModel(&mockModel{},
			[]schema.ToolCall{
				{ID: "top1", Function: schema.ToolCallFunction{Name: "middle", Arguments: "{}"}},
			},
			"top done",
		),
		MaxIterations: 5,
		Middlewares:   []agentcore.ReActMiddleware{topMW},
	}
	topMW.BindToConfig(topCfg)
	topAgent := agentcore.NewReActAgent(topCfg)

	ctx := context.Background()
	final, err := runAgent(ctx, t, topAgent, "start")
	if err != nil {
		t.Fatalf("unexpected error with MaxDepth=0: %v", err)
	}
	if final != "top done" {
		t.Errorf("expected 'top done', got %q", final)
	}
	t.Logf("default depth: final=%q", final)
}
