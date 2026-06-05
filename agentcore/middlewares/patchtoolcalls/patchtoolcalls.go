// Package patchtoolcalls patches incomplete tool calls in conversation history.
// When the model's tool call was interrupted or cut off, this middleware
// inserts placeholder tool messages so the conversation remains consistent.
package patchtoolcalls

import (
	"context"
	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

type middleware[M agentcore.MessageType] struct {
	agentcore.BaseMiddleware[M]
}

func New[M agentcore.MessageType]() agentcore.TypedChatModelMiddleware[M] { return &middleware[M]{} }

func (m *middleware[M]) BeforeModelRewrite(ctx context.Context, state *agentcore.TypedChatModelAgentState[M], mc *agentcore.TypedModelContext[M]) (context.Context, *agentcore.TypedChatModelAgentState[M], error) {
	// Find assistant messages with tool calls that have no corresponding tool result
	for i := 0; i < len(state.Messages)-1; i++ {
		msg := any(state.Messages[i]).(*schema.Message)
		if msg == nil || msg.Role != schema.RoleAssistant || len(msg.ToolCalls) == 0 { continue }
		// Check if next message is a tool result
		next := any(state.Messages[i+1]).(*schema.Message)
		if next != nil && next.Role == schema.RoleTool { continue }
		// Insert placeholder tool results
		for _, tc := range msg.ToolCalls {
			placeholder := schema.ToolMessage("[Tool call was not completed]", tc.ID)
			state.Messages = append(state.Messages[:i+1], append([]M{any(placeholder).(M)}, state.Messages[i+1:]...)...)
			i++
		}
	}
	return ctx, state, nil
}
