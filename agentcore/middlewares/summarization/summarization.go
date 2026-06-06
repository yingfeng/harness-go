// Package summarization provides a middleware that automatically summarizes
// conversation history when token/message thresholds are exceeded.
package summarization

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// TriggerCondition defines when summarization activates.
type TriggerCondition struct {
	MaxTokens    int // Trigger when estimated tokens exceed this
	MaxMessages  int // Trigger when message count exceeds this (0 = no limit)
}

// TypedConfig configures the summarization middleware.
type TypedConfig[M agentcore.MessageType] struct {
	Model              agentcore.ChatModel[M]
	Trigger            *TriggerCondition
	TokenCounter       func(ctx context.Context, msgs []M) (int, error)
	GenModelInput      func(ctx context.Context, instruction string, msgs []M) ([]M, error)
	Finalize           func(ctx context.Context, original, summary []M) ([]M, error)
	Callback           func(ctx context.Context, before, after agentcore.TypedChatModelAgentState[M]) error
	RetryConfig        *agentcore.TypedModelRetryConfig[M]
	EmitInternalEvents bool
	MaxRetries         int
	MaxTokens          int
	SummaryLang        string
}

type Config = TypedConfig[*schema.Message]

type middleware[M agentcore.MessageType] struct {
	agentcore.BaseMiddleware[M]
	cfg   *TypedConfig[M]
}

func NewTyped[M agentcore.MessageType](cfg *TypedConfig[M]) agentcore.TypedChatModelMiddleware[M] {
	if cfg == nil { cfg = &TypedConfig[M]{MaxTokens: 160000} }
	if cfg.Trigger == nil { cfg.Trigger = &TriggerCondition{MaxTokens: 160000} }
	if cfg.MaxTokens <= 0 { cfg.MaxTokens = 160000 }
	if cfg.Trigger.MaxTokens <= 0 { cfg.Trigger.MaxTokens = cfg.MaxTokens }
	if cfg.TokenCounter == nil { cfg.TokenCounter = defaultTokenCounter[M] }
	return &middleware[M]{cfg: cfg}
}

func New(cfg *Config) agentcore.TypedChatModelMiddleware[*schema.Message] {
	return NewTyped[*schema.Message](cfg)
}

func (m *middleware[M]) BeforeModelRewrite(ctx context.Context, state *agentcore.TypedChatModelAgentState[M], mc *agentcore.TypedModelContext[M]) (context.Context, *agentcore.TypedChatModelAgentState[M], error) {
	if !m.shouldSummarize(ctx, state) { return ctx, state, nil }

	// Fire before event if enabled
	if m.cfg.EmitInternalEvents {
		ev := &agentcore.TypedAgentEvent[M]{
			Output: &agentcore.TypedAgentOutput[M]{},
		}
		_ = agentcore.TypedSendEvent(ctx, ev)
	}

	// Perform summarization
	summaryMsgs, err := m.summarize(ctx, state.Messages)
	if err != nil {
		return ctx, state, nil // Fall through on error, don't fail the agent
	}

	// Apply finalizer
	if m.cfg.Finalize != nil {
		summaryMsgs, err = m.cfg.Finalize(ctx, state.Messages, summaryMsgs)
		if err != nil { return ctx, state, nil }
	}

	// Callback
	if m.cfg.Callback != nil {
		before := *state
		state.Messages = summaryMsgs
		if err := m.cfg.Callback(ctx, before, *state); err != nil { return ctx, state, nil }
		return ctx, state, nil
	}

	state.Messages = summaryMsgs
	return ctx, state, nil
}

func (m *middleware[M]) shouldSummarize(ctx context.Context, state *agentcore.TypedChatModelAgentState[M]) bool {
	if m.cfg.Trigger.MaxMessages > 0 && len(state.Messages) > m.cfg.Trigger.MaxMessages {
		return true
	}
	if m.cfg.TokenCounter != nil && len(state.Messages) > 0 {
		tokens, err := m.cfg.TokenCounter(ctx, state.Messages)
		if err == nil && tokens > m.cfg.Trigger.MaxTokens {
			return true
		}
	}
	return false
}

func (m *middleware[M]) summarize(ctx context.Context, msgs []M) ([]M, error) {
	keepLast := 10
	if len(msgs) <= keepLast+2 { return msgs, nil } // Not enough to summarize

	split := len(msgs) - keepLast
	summarizeMsgs := msgs[:split]
	keepMsgs := msgs[split:]

	// Talk to the model
	summaryText, err := m.generateSummary(ctx, summarizeMsgs)
	if err != nil { return msgs, err }

	// Build summary message
	var sysMsg M
	switch any(sysMsg).(type) {
	case *schema.Message:
		content := fmt.Sprintf("[Previous conversation summarized]\n---\n%s\n---", summaryText)
		sysMsg = any(schema.SystemMessage(content)).(M)
	case *schema.AgenticMessage:
		sysMsg = any(&schema.AgenticMessage{Role: schema.AgenticRoleSystem,
			Content: fmt.Sprintf("[Previous conversation summarized]\n---\n%s\n---", summaryText)}).(M)
	}

	// Replace summarized portion
	result := make([]M, 0, 1+len(keepMsgs))
	result = append(result, sysMsg)
	result = append(result, keepMsgs...)
	return result, nil
}

func (m *middleware[M]) generateSummary(ctx context.Context, msgs []M) (string, error) {
	if m.cfg.Model == nil { return fmt.Sprintf("(%d messages)", len(msgs)), nil }

	// Build summary prompt
	instruction := getSummaryInstruction(m.cfg.SummaryLang)
	var promptMsgs []M
	if m.cfg.GenModelInput != nil {
		var err error
		promptMsgs, err = m.cfg.GenModelInput(ctx, instruction, msgs)
		if err != nil { return "", err }
	} else {
		var builder strings.Builder
		builder.WriteString(instruction)
		builder.WriteString("\n\nConversation:\n")
		for i, msg := range msgs {
			text := extractText(msg)
			if text != "" {
				builder.WriteString(fmt.Sprintf("[%d]: %s\n", i+1, truncateText(text, 500)))
			}
			if i > 200 { builder.WriteString("...[truncated]"); break }
		}
		promptMsgs = []M{any(schema.UserMessage(builder.String())).(M)}
	}

	// Call with retry
	var lastErr error
	maxAttempts := m.cfg.MaxRetries
	if maxAttempts <= 0 {
		maxAttempts = 1
		if m.cfg.RetryConfig != nil && m.cfg.RetryConfig.MaxRetries > 0 {
			maxAttempts = 1 + m.cfg.RetryConfig.MaxRetries
		}
	}
	for attempt := 0; attempt <= maxAttempts; attempt++ {
		resp, err := m.cfg.Model.Generate(ctx, promptMsgs)
		if err == nil {
			if m.cfg.EmitInternalEvents {
				ev := &agentcore.TypedAgentEvent[M]{
					Output: &agentcore.TypedAgentOutput[M]{},
				}
				_ = agentcore.TypedSendEvent(ctx, ev)
			}
			return extractText(resp), nil
		}
		lastErr = err
		if attempt < maxAttempts {
			time.Sleep(time.Duration(100*(1<<uint(attempt))) * time.Millisecond)
		}
	}
	return fmt.Sprintf("(%d messages)", len(msgs)), lastErr
}

// ---- Helper functions ----

func getSummaryInstruction(lang string) string {
	if lang == "zh" {
		return `你是一个对话摘要助手。请总结以下对话,保留关键上下文、决定和待办事项。
要求:
1. 保持客观,不要添加对话中没有的信息
2. 保留重要的决策、结论和行动项
3. 使用与原对话相同的语言
4. 摘要应当简明扼要`
	}
	return `You are a conversation summarizer. Summarize the following conversation, preserving key context, decisions, and action items.
Requirements:
1. Stay objective, do not add information not in the conversation
2. Preserve important decisions, conclusions, and action items
3. Use the same language as the original conversation
4. Keep the summary concise`
}

func extractText[M agentcore.MessageType](msg M) string {
	switch v := any(msg).(type) {
	case *schema.Message: return v.Content
	case *schema.AgenticMessage: return v.Content
	}
	return ""
}

func truncateText(s string, maxLen int) string {
	if len(s) <= maxLen { return s }
	return s[:maxLen] + "..."
}

func defaultTokenCounter[M agentcore.MessageType](ctx context.Context, msgs []M) (int, error) {
	total := 0
	for _, msg := range msgs {
		text := extractText(msg)
		total += len([]rune(text)) * 4 / 3 // ~1.33 chars per token
	}
	return total, nil
}

// isSummaryMessage checks if the message content contains the summary tag.
func isSummaryMessage[M agentcore.MessageType](msg M) bool {
	switch v := any(msg).(type) {
	case *schema.Message:
		return strings.Contains(v.Content, SummaryTag)
	case *schema.AgenticMessage:
		return strings.Contains(v.Content, SummaryTag)
	}
	return false
}
