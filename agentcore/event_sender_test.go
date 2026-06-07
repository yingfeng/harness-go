package agentcore

import (
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

func TestEventSenderModelWrapper_Creation(t *testing.T) {
	wrapper := NewEventSenderModelWrapper[*schema.Message]()
	if wrapper == nil {
		t.Fatal("nil wrapper")
	}
}

func TestEventSenderToolWrapper_Creation(t *testing.T) {
	wrapper := NewEventSenderToolWrapper[*schema.Message]()
	if wrapper == nil {
		t.Fatal("nil wrapper")
	}
}

func TestHasUserEventSenderModelWrapper_Empty(t *testing.T) {
	handlers := []TypedReActMiddleware[*schema.Message]{}
	if HasUserEventSenderModelWrapper(handlers) {
		t.Error("should be false for empty handlers")
	}
}

func TestHasUserEventSenderModelWrapper_Present(t *testing.T) {
	wrapper := NewEventSenderModelWrapper[*schema.Message]()
	handlers := []TypedReActMiddleware[*schema.Message]{wrapper}
	if !HasUserEventSenderModelWrapper(handlers) {
		t.Error("should detect user's EventSenderModelWrapper")
	}
}

func TestHasUserEventSenderToolWrapper_Present(t *testing.T) {
	wrapper := NewEventSenderToolWrapper[*schema.Message]()
	handlers := []TypedReActMiddleware[*schema.Message]{wrapper}
	if !HasUserEventSenderToolWrapper(handlers) {
		t.Error("should detect user's EventSenderToolWrapper")
	}
}

func TestEventSenderModelWrapper_AllNoOp(t *testing.T) {
	wrapper := NewEventSenderModelWrapper[*schema.Message]()
	// Verify it satisfies the interface — call each method safely.
	// Note: WrapModel requires a valid run context (calls getChatModelExecCtx),
	// so it is tested separately in agentcore_test.go integration tests.
	_, _, _ = wrapper.BeforeAgent(nil, nil)
	_, _ = wrapper.AfterAgent(nil, nil)
	_, _, _ = wrapper.BeforeModelRewrite(nil, nil, nil)
	_, _, _ = wrapper.AfterModelRewrite(nil, nil, nil)
	_, _ = wrapper.WrapToolInvoke(nil, nil, nil)
	_, _ = wrapper.WrapToolStream(nil, nil, nil)
	_, _ = wrapper.WrapEnhancedInvokableToolCall(nil, nil, nil)
	_, _ = wrapper.WrapEnhancedStreamableToolCall(nil, nil, nil)
}

func TestResumeWithData(t *testing.T) {
	info := ResumeWithData(&ReActAgentResumeData{})
	if info.ResumeData == nil {
		t.Error("ResumeData should be set")
	}
	// WasInterrupted defaults to false (Go zero value) — correct for explicit resume
	if info.WasInterrupted {
		t.Error("WasInterrupted should default to false")
	}
}

func TestExactRunPathMatch(t *testing.T) {
	a := []RunStep{{agentName: "a"}, {agentName: "b"}}
	b := []RunStep{{agentName: "a"}, {agentName: "b"}}
	if !exactRunPathMatch(a, b) {
		t.Error("equal paths should match")
	}
	if exactRunPathMatch(a, []RunStep{{agentName: "a"}}) {
		t.Error("different length paths should not match")
	}
}
