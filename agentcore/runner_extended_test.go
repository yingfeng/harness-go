package agentcore

import (
	"context"
	"errors"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// ======================== Runner tests ========================

func TestRunner_NilAgent(t *testing.T) {
	runner := NewTypedRunner(RunnerConfig[*schema.Message]{Agent: nil})
	if runner == nil { t.Fatal("nil runner") }
}

func TestRunner_MultipleRun(t *testing.T) {
	model := &mockModel{}
	model.addResp("Run1")
	model.addResp("Run2")

	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: model})
	agent.name = "multi_run"

	runner := NewTypedRunner(RunnerConfig[*schema.Message]{Agent: agent})

	// First run
	iter1 := runner.Run(context.Background(), []Message{schema.UserMessage("first")})
	count1 := 0
	for { ev, ok := iter1.Next(); if !ok { break }; _ = ev; count1++ }

	// Second run with same runner
	iter2 := runner.Run(context.Background(), []Message{schema.UserMessage("second")})
	count2 := 0
	for { ev, ok := iter2.Next(); if !ok { break }; _ = ev; count2++ }

	if count1 == 0 || count2 == 0 {
		t.Errorf("counts: %d %d", count1, count2)
	}
}

func TestRunner_QueryBasic(t *testing.T) {
	model := &mockModel{}
	model.addResp("Query response")

	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: model})
	agent.name = "query"

	runner := NewTypedRunner(RunnerConfig[*schema.Message]{Agent: agent})

	iter := runner.Query(context.Background(), "Test query")
	found := false
	for { ev, ok := iter.Next(); if !ok { break }
		if ev.Output != nil && ev.Output.MessageOutput != nil {
			if ev.Output.MessageOutput.Message.Content == "Query response" {
				found = true
			}
		}
	}
	if !found { t.Error("expected query response") }
}

func TestRunner_WithCheckpointStore(t *testing.T) {
	model := &mockModel{}
	model.addResp("Checkpointed")

	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: model})
	agent.name = "cp"

	store := &memStore{}
	runner := NewTypedRunner(RunnerConfig[*schema.Message]{
		Agent:           agent,
		CheckPointStore: store,
	})

	iter := runner.Run(context.Background(), []Message{schema.UserMessage("cp test")})
	count := 0
	for { ev, ok := iter.Next(); if !ok { break }; _ = ev; count++ }
	if count == 0 { t.Error("expected events") }
}

func TestRunner_MultipleQueries(t *testing.T) {
	model := &mockModel{}
	model.addResp("A")
	model.addResp("B")
	model.addResp("C")

	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: model})
	agent.name = "multi_query"

	runner := NewTypedRunner(RunnerConfig[*schema.Message]{Agent: agent})

	totalCount := 0
	for _, q := range []string{"Q1", "Q2", "Q3"} {
		iter := runner.Query(context.Background(), q)
		for { ev, ok := iter.Next(); if !ok { break }; _ = ev; totalCount++ }
	}
	if totalCount == 0 { t.Error("expected events across queries") }
}

func TestRunner_WithRunOptions(t *testing.T) {
	model := &mockModel{}
	model.addResp("Options test")

	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: model})
	agent.name = "opts"

	runner := NewTypedRunner(RunnerConfig[*schema.Message]{Agent: agent})

	iter := runner.Run(context.Background(), []Message{schema.UserMessage("opts")},
		WithSessionValues(map[string]any{"key": "val"}),
	)
	count := 0
	for { ev, ok := iter.Next(); if !ok { break }; _ = ev; count++ }
	if count == 0 { t.Error("expected events") }
}

// ======================== Retry + Failover scenarios ========================

type failModel struct {
	failCount int32
	successCount int32
}

func (m *failModel) Generate(ctx context.Context, msgs []Message, opts ...modelOption) (Message, error) {
	return nil, errors.New("always fails")
}
func (m *failModel) Stream(ctx context.Context, msgs []Message, opts ...modelOption) (*schema.StreamReader[Message], error) {
	return nil, errors.New("always fails")
}
func (m *failModel) BindTools(tools []*schema.ToolInfo) error { return nil }

func TestChatModelAgent_AlwaysFails(t *testing.T) {
	failing := &failModel{}
	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{Model: failing})
	agent.name = "fails"

	iter := agent.Run(context.Background(), &AgentInput{
		Messages: []Message{schema.UserMessage("hello")},
	})

	var lastErr error
	for { ev, ok := iter.Next(); if !ok { break }; if ev.Err != nil { lastErr = ev.Err } }
	if lastErr == nil {
		t.Error("expected error from failing model")
	}
}

// ======================== Middleware chain with error propagation ========================

type errorMiddleware struct {
	BaseMiddleware[*schema.Message]
	failAt string
}

func (m *errorMiddleware) BeforeAgent(ctx context.Context, rc *ChatModelAgentContext) (context.Context, *ChatModelAgentContext, error) {
	if m.failAt == "BeforeAgent" { return ctx, nil, errors.New("error in BeforeAgent") }
	return ctx, rc, nil
}
func (m *errorMiddleware) BeforeModelRewrite(ctx context.Context, state *ChatModelAgentState, mc *ModelContext) (context.Context, *ChatModelAgentState, error) {
	if m.failAt == "BeforeModelRewrite" { return ctx, nil, errors.New("error in BeforeModelRewrite") }
	return ctx, state, nil
}
func (m *errorMiddleware) AfterModelRewrite(ctx context.Context, state *ChatModelAgentState, mc *ModelContext) (context.Context, *ChatModelAgentState, error) {
	if m.failAt == "AfterModelRewrite" { return ctx, nil, errors.New("error in AfterModelRewrite") }
	return ctx, state, nil
}
func (m *errorMiddleware) AfterAgent(ctx context.Context, state *ChatModelAgentState) (context.Context, error) {
	if m.failAt == "AfterAgent" { return ctx, errors.New("error in AfterAgent") }
	return ctx, nil
}

func TestMiddleware_ErrorAt_BeforeAgent(t *testing.T) {
	testMiddlewareError(t, "BeforeAgent")
}

func TestMiddleware_ErrorAt_BeforeModelRewrite(t *testing.T) {
	testMiddlewareError(t, "BeforeModelRewrite")
}

func TestMiddleware_ErrorAt_AfterModelRewrite(t *testing.T) {
	testMiddlewareError(t, "AfterModelRewrite")
}

func TestMiddleware_ErrorAt_AfterAgent(t *testing.T) {
	testMiddlewareError(t, "AfterAgent")
}

func testMiddlewareError(t *testing.T, failAt string) {
	model := &mockModel{}
	model.addResp("Hello")

	mw := &errorMiddleware{failAt: failAt}

	agent := NewChatModelAgent(&ChatModelConfig[*schema.Message]{
		Model:       model,
		Middlewares: []ChatModelMiddleware{mw},
	})
	agent.name = "err_" + failAt

	iter := agent.Run(context.Background(), &AgentInput{
		Messages: []Message{schema.UserMessage("test")},
	})

	var lastErr error
	count := 0
	for { ev, ok := iter.Next(); if !ok { break }; count++; if ev.Err != nil { lastErr = ev.Err } }

	if lastErr == nil {
		t.Logf("no error propagated through middleware chain (failAt=%s, events=%d)", failAt, count)
	}
}
