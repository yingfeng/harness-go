package agentcore

import "github.com/infiniflow/ragflow/harness/agentcore/schema"

// TypedChatModelAgentState is the exported state type for ChatModelAgent middlewares.
type TypedChatModelAgentState[M MessageType] struct {
	Messages            []M
	ToolInfos           []*schema.ToolInfo
	DeferredToolInfos   []*schema.ToolInfo
	Extra               map[string]any
	RemainingIterations int
}

type ChatModelAgentState = TypedChatModelAgentState[*schema.Message]

func NewChatModelAgentState[M MessageType](msgs []M, tools []*schema.ToolInfo, maxIter int) *TypedChatModelAgentState[M] {
	return &TypedChatModelAgentState[M]{
		Messages: msgs, ToolInfos: tools,
		RemainingIterations: maxIter, Extra: make(map[string]any),
	}
}

// ChatModelAgentContext is passed to BeforeAgent middlewares.
type ChatModelAgentContext struct {
	Instruction   string
	Tools         []Tool
	ReturnDirectly map[string]bool
	ToolSearchTool *schema.ToolInfo
}

// ToolContext provides metadata about a tool being wrapped.
type ToolContext struct {
	Name   string
	CallID string
}

// ToolCallsContext contains metadata about completed tool calls.
type ToolCallsContext struct {
	ToolCalls []ToolContext
}
