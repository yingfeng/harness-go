// Package workflow provides shared model and utility for workflow examples.
package workflow

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// MockModel returns a placeholder Model that simulates LLM responses.
// When LLM_API_KEY env is set, it returns a simple OpenAI-compatible model
// stub. Without the env, it returns a mock that echoes the agent's instruction.
func MockModel(name string) agentcore.Model[*schema.Message] {
	apiKey := os.Getenv("LLM_API_KEY")
	baseURL := os.Getenv("LLM_BASE_URL")
	if apiKey != "" {
		return newOpenAIModel(apiKey, baseURL, os.Getenv("LLM_MODEL"))
	}
	return &mockChatModel{name: name}
}

// mockChatModel returns canned responses for demonstration.
type mockChatModel struct {
	name    string
	called  bool
}

func (m *mockChatModel) Generate(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.Message, error) {
	if !m.called {
		m.called = true
		content := fmt.Sprintf("[%s]: I am the %s. I received %d message(s).", m.name, m.name, len(msgs))
		var lastContent string
		if len(msgs) > 0 {
			lastContent = msgs[len(msgs)-1].Content
		}
		if lastContent != "" && len(lastContent) > 80 {
			lastContent = lastContent[:80] + "..."
		}
		if lastContent != "" {
			content += fmt.Sprintf(" Last input: %q", lastContent)
		}
		content += " [END]"
		return &schema.Message{Role: schema.RoleAssistant, Content: content}, nil
	}
	return &schema.Message{Role: schema.RoleAssistant, Content: fmt.Sprintf("[%s]: Final response from %s. [END]", m.name, m.name)}, nil
}

func (m *mockChatModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	return schema.StreamReaderFromArray([]*schema.Message{msg}), nil
}

func (m *mockChatModel) BindTools(tools []*schema.ToolInfo) error { return nil }

// openAIModel is a minimal OpenAI-compatible Model adapter.
type openAIModel struct {
	apiKey  string
	baseURL string
	model   string
}

func newOpenAIModel(apiKey, baseURL, model string) *openAIModel {
	if model == "" {
		model = "gpt-4o-mini"
	}
	if baseURL == "" {
		baseURL = "https://api.openai.com/v1"
	}
	return &openAIModel{apiKey: apiKey, baseURL: strings.TrimRight(baseURL, "/"), model: model}
}

func (m *openAIModel) Generate(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.Message, error) {
	return m.generate(ctx, msgs, false)
}

func (m *openAIModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.generate(ctx, msgs, true)
	if err != nil {
		return nil, err
	}
	return schema.StreamReaderFromArray([]*schema.Message{msg}), nil
}

func (m *openAIModel) BindTools(tools []*schema.ToolInfo) error { return nil }

func (m *openAIModel) generate(ctx context.Context, msgs []*schema.Message, _ bool) (*schema.Message, error) {
	req := map[string]any{
		"model": m.model,
	}
	var apiMsgs []map[string]any
	for _, msg := range msgs {
		m := map[string]any{"role": string(msg.Role), "content": msg.Content}
		if len(msg.ToolCalls) > 0 {
			var tcs []map[string]any
			for _, tc := range msg.ToolCalls {
				tcs = append(tcs, map[string]any{
					"id":   tc.ID,
					"type": tc.Type,
					"function": map[string]string{
						"name":      tc.Function.Name,
						"arguments": tc.Function.Arguments,
					},
				})
			}
			m["tool_calls"] = tcs
		}
		if msg.ToolName != "" {
			m["tool_call_id"] = msg.Name
		}
		apiMsgs = append(apiMsgs, m)
	}
	req["messages"] = apiMsgs

	resp, err := postJSON(ctx, m.baseURL+"/chat/completions", map[string]string{
		"Authorization": "Bearer " + m.apiKey,
		"Content-Type":  "application/json",
	}, req)
	if err != nil {
		return nil, fmt.Errorf("openai: %w", err)
	}

	choices, _ := resp["choices"].([]any)
	if len(choices) == 0 {
		return nil, fmt.Errorf("openai: no choices in response")
	}
	choice, _ := choices[0].(map[string]any)
	delta, _ := choice["message"].(map[string]any)
	if delta == nil {
		return nil, fmt.Errorf("openai: no message in choice")
	}

	role, _ := delta["role"].(string)
	content, _ := delta["content"].(string)

	var toolCalls []schema.ToolCall
	if tcs, ok := delta["tool_calls"].([]any); ok {
		for _, tc := range tcs {
			tcm, _ := tc.(map[string]any)
			tcID, _ := tcm["id"].(string)
			tcType, _ := tcm["type"].(string)
			fn, _ := tcm["function"].(map[string]any)
			fnName, _ := fn["name"].(string)
			fnArgs, _ := fn["arguments"].(string)
			toolCalls = append(toolCalls, schema.ToolCall{
				ID:   tcID,
				Type: tcType,
				Function: schema.ToolCallFunction{
					Name:      fnName,
					Arguments: fnArgs,
				},
			})
		}
	}

	return &schema.Message{
		Role:      schema.RoleType(role),
		Content:   content,
		ToolCalls: toolCalls,
	}, nil
}
