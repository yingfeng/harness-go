package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ChatModel is the interface for LLM chat models used in deer-flow.
type ChatModel interface {
	Generate(ctx context.Context, messages []map[string]string, tools []MockTool) (string, error)
	GenerateWithTool(ctx context.Context, messages []map[string]string, tools []MockTool) (string, string, error)
}

// ---- OpenAI function calling implementation ----

type openAIModel struct {
	apiKey  string
	baseURL string
	model   string
}

func newChatModel(ctx context.Context, cfg *Config) (ChatModel, error) {
	if len(cfg.Models) > 0 {
		m := cfg.Models[0]
		if m.APIKey != "" && m.Model != "" {
			baseURL := m.APIBase
			if baseURL == "" {
				baseURL = "https://api.openai.com/v1"
			}
			fmt.Printf("Using LLM: model=%s, base=%s\n", m.Model, baseURL)
			return &openAIModel{
				apiKey:  m.APIKey,
				baseURL: strings.TrimRight(baseURL, "/"),
				model:   m.Model,
			}, nil
		}
	}
	fmt.Println("No API key configured — using mock model for demonstration")
	return &mockModel{}, nil
}

func modelName(cfg *Config) string {
	if cfg != nil && len(cfg.Models) > 0 && cfg.Models[0].Model != "" && cfg.Models[0].APIKey != "" {
		return cfg.Models[0].Model
	}
	return "mock"
}

// ---- OpenAI API types ----

type oaiMessage struct {
	Role      string        `json:"role"`
	Content   string        `json:"content"`
	ToolCalls []oaiToolCall `json:"tool_calls,omitempty"`
	ToolCallID string       `json:"tool_call_id,omitempty"`
}

type oaiToolCall struct {
	Index    int         `json:"index,omitempty"`
	ID       string      `json:"id"`
	Type     string      `json:"type"`
	Function oaiFunction `json:"function"`
}

type oaiFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type oaiTool struct {
	Type     string      `json:"type"`
	Function oaiFuncDef  `json:"function"`
}

type oaiFuncDef struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Parameters  oaiParams   `json:"parameters"`
}

type oaiParams struct {
	Type       string                `json:"type"`
	Properties map[string]oaiPropDef `json:"properties"`
	Required   []string              `json:"required"`
}

type oaiPropDef struct {
	Type        string `json:"type"`
	Description string `json:"description"`
}

type oaiRequest struct {
	Model       string       `json:"model"`
	Messages    []oaiMessage `json:"messages"`
	Tools       []oaiTool    `json:"tools,omitempty"`
	ToolChoice  interface{}  `json:"tool_choice,omitempty"`
}

type oaiChoice struct {
	Index   int        `json:"index"`
	Message oaiMessage `json:"message"`
	FinishReason string `json:"finish_reason"`
}

type oaiResponse struct {
	Choices []oaiChoice `json:"choices"`
}

// toolToOAI converts a MockTool to OpenAI tool definition.
func toolToOAI(t MockTool) oaiTool {
	// Simple default: all tools accept a single "input" string parameter
	return oaiTool{
		Type: "function",
		Function: oaiFuncDef{
			Name:        t.Name,
			Description: t.Description,
			Parameters: oaiParams{
				Type: "object",
				Properties: map[string]oaiPropDef{
					"input": {
						Type:        "string",
						Description: "The input for the tool",
					},
				},
				Required: []string{"input"},
			},
		},
	}
}

func (m *openAIModel) Generate(ctx context.Context, messages []map[string]string, tools []MockTool) (string, error) {
	deadline, _ := ctx.Deadline()
	fmt.Printf("[LLM] Generate called, msgs=%d, tools=%d, deadline=%v\n", len(messages), len(tools), deadline)

	oaiMsgs := make([]oaiMessage, 0, len(messages))
	for _, msg := range messages {
		oaiMsgs = append(oaiMsgs, oaiMessage{Role: msg["role"], Content: msg["content"]})
	}

	oaiTools := make([]oaiTool, 0, len(tools))
	for _, t := range tools {
		oaiTools = append(oaiTools, toolToOAI(t))
	}

	body := oaiRequest{
		Model:    m.model,
		Messages: oaiMsgs,
	}
	if len(oaiTools) > 0 {
		body.Tools = oaiTools
	}

	result, err := m.callLLM(ctx, body)
	if err != nil {
		fmt.Printf("[LLM] Generate ERROR: %v\n", err)
		return "", err
	}

	// 如果 LLM 选择了调用工具，执行工具并返回结果
	if toolName, toolArgs, isTool := ParseToolResult(result); isTool {
		fmt.Printf("[LLM] Generate executing tool: name=%s, args=%s\n", toolName, toolArgs)
		for _, t := range tools {
			if t.Name == toolName {
				// 从 args JSON 中提取 input 字段，或直接使用整个 args
				input := toolArgs
				var argsObj map[string]interface{}
				if err := json.Unmarshal([]byte(toolArgs), &argsObj); err == nil {
					if in, ok := argsObj["input"].(string); ok {
						input = in
					}
				}
				toolResult, err := t.Execute(ctx, input)
				if err != nil {
					return "", fmt.Errorf("tool %s: %w", toolName, err)
				}
				fmt.Printf("[LLM] Generate tool %s executed, result_len=%d\n", toolName, len(toolResult))
				return toolResult, nil
			}
		}
		fmt.Printf("[LLM] Generate tool %s not found, returning args\n", toolName)
		return toolArgs, nil
	}

	fmt.Printf("[LLM] Generate OK, len=%d\n", len(result))
	return result, nil
}

func (m *openAIModel) GenerateWithTool(ctx context.Context, messages []map[string]string, tools []MockTool) (string, string, error) {
	deadline, _ := ctx.Deadline()
	fmt.Printf("[LLM] GenerateWithTool called, msgs=%d, tools=%d, deadline=%v\n", len(messages), len(tools), deadline)

	oaiMsgs := make([]oaiMessage, 0, len(messages))
	for _, msg := range messages {
		oaiMsgs = append(oaiMsgs, oaiMessage{Role: msg["role"], Content: msg["content"]})
	}

	oaiTools := make([]oaiTool, 0, len(tools))
	for _, t := range tools {
		oaiTools = append(oaiTools, toolToOAI(t))
	}

	body := oaiRequest{
		Model:      m.model,
		Messages:   oaiMsgs,
		ToolChoice: "auto",
	}
	if len(oaiTools) > 0 {
		body.Tools = oaiTools
	}

	result, err := m.callLLM(ctx, body)
	if err != nil {
		fmt.Printf("[LLM] GenerateWithTool ERROR: %v\n", err)
		return "", "", err
	}

	// For GenerateWithTool, we process the response to extract tool calls.
	// The result is a JSON string that includes tool_calls info.
	// We parse it here and return tool_name, tool_args.
	return result, "", nil
}

// callLLM sends the request and returns the assistant content.
// For tool calls, returns a JSON with tool name + args.
func (m *openAIModel) callLLM(ctx context.Context, body oaiRequest) (string, error) {
	data, _ := json.Marshal(body)
	url := m.baseURL + "/chat/completions"

	fmt.Printf("[LLM] callLLM: POST %s, body_size=%d, model=%s\n", url, len(data), m.model)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+m.apiKey)

	client := &http.Client{Timeout: 120 * time.Second}
	t0 := time.Now()
	resp, err := client.Do(req)
	elapsed := time.Since(t0)
	if err != nil {
		fmt.Printf("[LLM] callLLM HTTP ERROR after %v: %v\n", elapsed, err)
		return "", fmt.Errorf("API call: %w", err)
	}
	defer resp.Body.Close()

	fmt.Printf("[LLM] callLLM response: status=%d, elapsed=%v\n", resp.StatusCode, elapsed)

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		errMsg := fmt.Sprintf("API error %d: %s", resp.StatusCode, string(raw))
		fmt.Printf("[LLM] callLLM ERROR: %s\n", errMsg)
		return "", fmt.Errorf("%s", errMsg)
	}

	var oaiResp oaiResponse
	if err := json.Unmarshal(raw, &oaiResp); err != nil {
		fmt.Printf("[LLM] callLLM parse ERROR: %v, raw=%s\n", err, string(raw[:min(len(raw), 200)]))
		return "", fmt.Errorf("parse response: %w", err)
	}
	if len(oaiResp.Choices) == 0 {
		return "", fmt.Errorf("no choices in response")
	}

	msg := oaiResp.Choices[0].Message

	// If there are tool calls, return a JSON with tool info
	if len(msg.ToolCalls) > 0 {
		tc := msg.ToolCalls[0]
		fmt.Printf("[LLM] callLLM tool_call: name=%s, args_len=%d\n", tc.Function.Name, len(tc.Function.Arguments))
		// Return JSON: {"tool": name, "args": args}
		resultJSON, _ := json.Marshal(map[string]string{
			"tool": tc.Function.Name,
			"args": tc.Function.Arguments,
		})
		return string(resultJSON), nil
	}

	fmt.Printf("[LLM] callLLM OK: content_len=%d\n", len(msg.Content))
	return msg.Content, nil
}

// ---- Parse tool results from callLLM response ----
// GenerateWithTool returns the raw JSON from callLLM.
// The coordinator/router should parse this JSON to extract the tool name.

// ParseToolResult extracts tool name and arguments from GenerateWithTool output.
func ParseToolResult(output string) (toolName string, toolArgs string, isTool bool) {
	if output == "" {
		return "", "", false
	}
	if output[0] != '{' {
		return "", output, false
	}
	var parsed struct {
		Tool string `json:"tool"`
		Args string `json:"args"`
	}
	if err := json.Unmarshal([]byte(output), &parsed); err != nil || parsed.Tool == "" {
		return "", output, false
	}
	return parsed.Tool, parsed.Args, true
}

// ---- Mock model ----

type mockModel struct{}

func (m *mockModel) Generate(ctx context.Context, messages []map[string]string, tools []MockTool) (string, error) {
	lastMsg := ""
	if len(messages) > 0 {
		lastMsg = messages[len(messages)-1]["content"]
	}
	return fmt.Sprintf("Research analysis complete. Based on the request: %s", truncateModel(lastMsg, 60)), nil
}

func (m *mockModel) GenerateWithTool(ctx context.Context, messages []map[string]string, tools []MockTool) (string, string, error) {
	for _, tool := range tools {
		if tool.Name == "hand_to_planner" {
			lastMsg := ""
			if len(messages) > 0 {
				lastMsg = messages[len(messages)-1]["content"]
			}
			if len(lastMsg) > 20 {
				resultJSON, _ := json.Marshal(map[string]string{
					"tool": tool.Name,
					"args": "User wants research: " + truncateModel(lastMsg, 80),
				})
				return string(resultJSON), "", nil
			}
		}
	}
	resp, _ := m.Generate(ctx, messages, tools)
	return resp, "", nil
}

func truncateModel(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n]) + "..."
}
