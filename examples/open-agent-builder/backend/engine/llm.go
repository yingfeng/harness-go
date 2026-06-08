package engine

import (
	"context"
	"fmt"
	"net/http"
	"time"

	openai "github.com/sashabaranov/go-openai"
)

// ChatCompletionResponse is the response from CreateChatCompletion.
type ChatCompletionResponse struct {
	Choices []struct {
		Message struct {
			Content string
		}
	}
}

// ChatMessage represents a single message in the conversation.
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// LLMClient wraps the OpenAI SDK for LLM calls.
type LLMClient struct {
	client  *openai.Client
	config  ModelConfig
}

// NewLLMClient creates an LLM client from a ModelConfig.
func NewLLMClient(cfg ModelConfig) *LLMClient {
	ocfg := openai.DefaultConfig(cfg.APIKey)
	if cfg.APIBase != "" {
		ocfg.BaseURL = cfg.APIBase
	}
	timeout := cfg.TimeoutSeconds
	if timeout <= 0 {
		timeout = 120
	}
	ocfg.HTTPClient = &http.Client{Timeout: time.Duration(timeout) * time.Second}
	return &LLMClient{client: openai.NewClientWithConfig(ocfg), config: cfg}
}

// CreateChatCompletion sends messages to the LLM and returns the response.
func (lc *LLMClient) CreateChatCompletion(ctx context.Context, messages []ChatMessage, modelOverride string) (ChatCompletionResponse, error) {
	model := lc.config.Model
	if modelOverride != "" {
		model = modelOverride
	}

	oaMsgs := make([]openai.ChatCompletionMessage, len(messages))
	for i, m := range messages {
		oaMsgs[i] = openai.ChatCompletionMessage{Role: m.Role, Content: m.Content}
	}

	req := openai.ChatCompletionRequest{
		Model:    model,
		Messages: oaMsgs,
	}
	if lc.config.Temperature > 0 {
		req.Temperature = float32(lc.config.Temperature)
	}
	if lc.config.MaxTokens > 0 {
		req.MaxTokens = lc.config.MaxTokens
	}

	resp, err := lc.client.CreateChatCompletion(ctx, req)
	if err != nil {
		return ChatCompletionResponse{}, fmt.Errorf("llm api error: %w", err)
	}

	var result ChatCompletionResponse
	for _, choice := range resp.Choices {
		result.Choices = append(result.Choices, struct {
			Message struct{ Content string }
		}{Message: struct{ Content string }{Content: choice.Message.Content}})
	}
	return result, nil
}
