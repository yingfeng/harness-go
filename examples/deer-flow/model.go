package main

import (
	"context"
	"fmt"
	"os"
	"strings"
)

// ChatModel is the interface for LLM chat models used in deer-flow.
// It is compatible with harness-go's ChatModel interface pattern.
type ChatModel interface {
	Generate(ctx context.Context, messages []map[string]string, tools []MockTool) (string, error)
	GenerateWithTool(ctx context.Context, messages []map[string]string, tools []MockTool) (string, string, error)
}

// ---- OpenAI-compatible implementation ----

// openAIModel implements ChatModel using the OpenAI API.
type openAIModel struct {
	apiKey  string
	baseURL string
	model   string
}

func newChatModel(ctx context.Context) (ChatModel, error) {
	apiKey := os.Getenv("OPENAI_API_KEY")
	baseURL := os.Getenv("OPENAI_BASE_URL")
	model := os.Getenv("OPENAI_MODEL")

	if apiKey != "" && model != "" {
		if baseURL == "" {
			baseURL = "https://api.openai.com/v1"
		}
		return &openAIModel{
			apiKey:  apiKey,
			baseURL: strings.TrimRight(baseURL, "/"),
			model:   model,
		}, nil
	}

	// Fall back to mock model
	fmt.Println("OPENAI_API_KEY not set — using mock model for demonstration")
	fmt.Println("Set OPENAI_API_KEY, OPENAI_BASE_URL, OPENAI_MODEL for real LLM calls")
	return &mockModel{}, nil
}

func (m *openAIModel) Generate(ctx context.Context, messages []map[string]string, tools []MockTool) (string, error) {
	// Build a prompt from messages
	var sb strings.Builder
	for _, msg := range messages {
		role := msg["role"]
		content := msg["content"]
		sb.WriteString(fmt.Sprintf("<%s>\n%s\n</%s>\n\n", role, content, role))
	}
	sb.WriteString("Generate a response:\n")
	return m.callLLM(ctx, sb.String())
}

func (m *openAIModel) GenerateWithTool(ctx context.Context, messages []map[string]string, _ []MockTool) (string, string, error) {
	var sb strings.Builder
	for _, msg := range messages {
		role := msg["role"]
		content := msg["content"]
		sb.WriteString(fmt.Sprintf("<%s>\n%s\n</%s>\n\n", role, content, role))
	}
	resp, err := m.callLLM(ctx, sb.String()+"Select and use the appropriate tool:\n")
	return resp, "", err
}

func (m *openAIModel) callLLM(ctx context.Context, prompt string) (string, error) {
	// Placeholder: in production, make an HTTP call to the OpenAI API
	return fmt.Sprintf("[%s response to: %s...]", m.model, truncate(prompt, 50)), nil
}

// ---- Mock model for demo without API key ----

type mockModel struct{}

func (m *mockModel) Generate(ctx context.Context, messages []map[string]string, tools []MockTool) (string, error) {
	lastMsg := ""
	if len(messages) > 0 {
		lastMsg = messages[len(messages)-1]["content"]
	}
	return fmt.Sprintf("Research analysis complete. Based on the request: %s", truncate(lastMsg, 60)), nil
}

func (m *mockModel) GenerateWithTool(ctx context.Context, messages []map[string]string, tools []MockTool) (string, string, error) {
	for _, tool := range tools {
		if tool.Name == "hand_to_planner" {
			lastMsg := ""
			if len(messages) > 0 {
				lastMsg = messages[len(messages)-1]["content"]
			}
			if len(lastMsg) > 20 {
				return tool.Name, "User wants research: " + truncate(lastMsg, 80), nil
			}
		}
	}
	resp, _ := m.Generate(ctx, messages, tools)
	return "", resp, nil
}

func truncate(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n]) + "..."
}
