// Package summarization provides a middleware that automatically summarizes
// conversation history when token count exceeds the configured threshold.
package summarization

import (
	"context"
	"fmt"
	"strings"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

type Config struct {
	Model              agentcore.ChatModel[*schema.Message]
	MaxTokens          int
	SummaryLang        string
	SummaryInstruction string
}

type middleware[M agentcore.MessageType] struct {
	agentcore.BaseMiddleware[M]
	cfg   *Config
	model agentcore.ChatModel[M]
}

func New[M agentcore.MessageType](model agentcore.ChatModel[M], cfg *Config) agentcore.TypedChatModelMiddleware[M] {
	if cfg == nil { cfg = &Config{MaxTokens: 4096, SummaryLang: "en"} }
	if cfg.MaxTokens <= 0 { cfg.MaxTokens = 4096 }
	if cfg.SummaryInstruction == "" {
		cfg.SummaryInstruction = `Summarize the following conversation, preserving key context, decisions, and action items.`
	}
	return &middleware[M]{cfg: cfg, model: model}
}

func (m *middleware[M]) BeforeModelRewrite(ctx context.Context, state *agentcore.TypedChatModelAgentState[M], mc *agentcore.TypedModelContext[M]) (context.Context, *agentcore.TypedChatModelAgentState[M], error) {
	if len(state.Messages) <= m.cfg.MaxTokens {
		return ctx, state, nil // no trigger
	}

	// Calculate messages to summarize (all except last N)
	keepLast := 10
	summaryEnd := len(state.Messages) - keepLast
	if summaryEnd < 0 { return ctx, state, nil }
	if summaryEnd > len(state.Messages) { summaryEnd = len(state.Messages) / 2 }

	toSummarize := state.Messages[:summaryEnd]
	keep := state.Messages[summaryEnd:]

	// Generate summary using model
	summaryContent := m.summarizeMsgs(ctx, toSummarize)

	// Create summary message
	var summaryMsg M
	switch any(summaryMsg).(type) {
	case *schema.Message:
		content := fmt.Sprintf("[Previous conversation summarized]\n---\n%s\n---", summaryContent)
		summaryMsg = any(schema.SystemMessage(content)).(M)
	case *schema.AgenticMessage:
		content := fmt.Sprintf("[Previous conversation summarized]\n---\n%s\n---", summaryContent)
		summaryMsg = any(&schema.AgenticMessage{Role: schema.AgenticRoleSystem, Content: content}).(M)
	}

	// Replace summarized segment with summary
	newMsgs := make([]M, 0, 1+len(keep))
	newMsgs = append(newMsgs, summaryMsg)
	newMsgs = append(newMsgs, keep...)
	state.Messages = newMsgs

	return ctx, state, nil
}

func (m *middleware[M]) summarizeMsgs(ctx context.Context, msgs []M) string {
	if len(msgs) == 0 { return "" }
	if m.model == nil { return "(" + fmt.Sprintf("%d previous messages summarized", len(msgs)) + ")" }

	// Build summarize prompt
	var prompt strings.Builder
	prompt.WriteString(m.cfg.SummaryInstruction)
	prompt.WriteString("\n\nConversation:\n")
	for i, msg := range msgs {
		switch v := any(msg).(type) {
		case *schema.Message:
			prompt.WriteString(fmt.Sprintf("[%s]: %s\n", string(v.Role), v.Content))
			for _, tc := range v.ToolCalls {
				prompt.WriteString(fmt.Sprintf("  tool_call: %s(%s)\n", tc.Function.Name, tc.Function.Arguments))
			}
			if v.ToolName != "" {
				prompt.WriteString(fmt.Sprintf("  tool_result: %s\n", v.Content))
			}
		case *schema.AgenticMessage:
			prompt.WriteString(fmt.Sprintf("[%s]: %s\n", string(v.Role), v.Content))
		}
		if i >= 100 { prompt.WriteString("...[truncated]"); break }
	}

	// Call model for summary
	resp, err := m.model.Generate(ctx, []M{any(schema.UserMessage(prompt.String())).(M)})
	if err != nil { return fmt.Sprintf("(%d messages)", len(msgs)) }
	return extractContent(resp)
}

func extractContent[M agentcore.MessageType](msg M) string {
	switch v := any(msg).(type) {
	case *schema.Message: return v.Content
	case *schema.AgenticMessage: return v.Content
	}
	return ""
}
