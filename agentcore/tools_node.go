package agentcore

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// ToolsNodeConfig configures the tools node for a ReActAgent.
type ToolsNodeConfig struct {
	// Tools is the list of tools available for execution.
	Tools []Tool

	// ReturnDirectly specifies tool names that cause the agent to return immediately.
	ReturnDirectly map[string]bool

	// ToolCallMiddlewares are middleware wrappers applied during tool invocation.
	ToolCallMiddlewares []ToolCallMiddleware

	// EmitInternalEvents enables forwarding internal events from AgentTool children.
	EmitInternalEvents bool
}

// ToolCallMiddleware wraps tool call endpoints with custom behavior.
type ToolCallMiddleware struct {
	// Invokable wraps synchronous tool calls. Nil means no wrapping.
	Invokable func(ctx context.Context, next InvokableToolEndpoint, tc *ToolContext) (InvokableToolEndpoint, error)
	// Streamable wraps streaming tool calls. Nil means no wrapping.
	Streamable func(ctx context.Context, next StreamableToolEndpoint, tc *ToolContext) (StreamableToolEndpoint, error)
	// EnhancedInvokable wraps enhanced synchronous tool calls. Nil means no wrapping.
	EnhancedInvokable func(ctx context.Context, next EnhancedInvokableToolEndpoint, tc *ToolContext) (EnhancedInvokableToolEndpoint, error)
	// EnhancedStreamable wraps enhanced streaming tool calls. Nil means no wrapping.
	EnhancedStreamable func(ctx context.Context, next EnhancedStreamableToolEndpoint, tc *ToolContext) (EnhancedStreamableToolEndpoint, error)
}

// ToolsNode handles tool extraction from model output, dispatching to tools,
// collecting results, and applying middleware chains.
type ToolsNode[M MessageType] struct {
	config  *ToolsNodeConfig
	toolMap  map[string]Tool
}

// NewToolsNode creates a new ToolsNode with the given configuration.
func NewToolsNode[M MessageType](cfg *ToolsNodeConfig) *ToolsNode[M] {
	tn := &ToolsNode[M]{config: cfg}
	tn.toolMap = make(map[string]Tool, len(cfg.Tools))
	for _, t := range cfg.Tools {
		tn.toolMap[t.Name()] = t
	}
	return tn
}

// Execute processes all tool calls found in the model response.
// It returns the list of tool result messages to append to state,
// and any agent action (e.g., return-directly) that should be handled.
//
// When multiple independent tool calls are present, Execute runs them concurrently
// using a bounded goroutine pool (default max concurrency = 10), reducing total
// latency from O(sum) to O(max). For a single tool call, no goroutine is spawned.
func (tn *ToolsNode[M]) Execute(ctx context.Context, resp M, state *TypedReActAgentState[M], _ interface{}) ([]M, *AgentAction, error) {
	toolCalls := extractToolCalls(resp)
	if len(toolCalls) == 0 {
		return nil, nil, nil
	}

	if len(toolCalls) == 1 {
		// Fast path: single tool call, no goroutine overhead.
		tc := toolCalls[0]
		var action *AgentAction
		if tn.config.ReturnDirectly != nil && tn.config.ReturnDirectly[tc.Function.Name] {
			action = NewExitAction()
		}
		toolMsg, err := tn.executeOne(ctx, tc)
		if err != nil {
			return nil, action, fmt.Errorf("tool '%s': %w", tc.Function.Name, err)
		}
		return []M{toolMsg}, action, nil
	}

	// Concurrent path: execute multiple tool calls in parallel.
	const maxConcurrency = 10
	sem := make(chan struct{}, maxConcurrency)
	var mu sync.Mutex
	results := make([]M, len(toolCalls))
	var action *AgentAction
	var firstErr error
	var wg sync.WaitGroup

	for i, tc := range toolCalls {
		if tn.config.ReturnDirectly != nil && tn.config.ReturnDirectly[tc.Function.Name] {
			mu.Lock()
			action = NewExitAction()
			mu.Unlock()
		}

		wg.Add(1)
		go func(idx int, call schema.ToolCall) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			toolMsg, err := tn.executeOne(ctx, call)
			mu.Lock()
			defer mu.Unlock()
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("tool '%s': %w", call.Function.Name, err)
				return
			}
			results[idx] = toolMsg
		}(i, tc)
	}
	wg.Wait()

	if firstErr != nil {
		return nil, action, firstErr
	}

	// Filter out nil results (from failed tools).
	filtered := results[:0]
	for _, r := range results {
		if !isNilMessage(r) {
			filtered = append(filtered, r)
		}
	}
	return filtered, action, nil
}

func (tn *ToolsNode[M]) executeOne(ctx context.Context, tc schema.ToolCall) (M, error) {

	tool, ok := tn.toolMap[tc.Function.Name]
	if !ok {
		errMsg := fmt.Sprintf("tool '%s' not found", tc.Function.Name)
		return tn.makeToolMsg(errMsg, tc.ID), nil
	}

	// Determine if this is an enhanced tool
	if et, ok := tool.(EnhancedTool); ok {
		return tn.executeEnhanced(ctx, tc, et)
	}

	// Standard tool path
	args := &schema.ToolArgument{
		Name:      tc.Function.Name,
		Arguments: tc.Function.Arguments,
		CallID:    tc.ID,
	}

	ep := func(ctx context.Context, args *schema.ToolArgument, opts ...ToolOption) (*schema.ToolResult, error) {
		result, err := tool.Invoke(ctx, args.Arguments, opts...)
		if err != nil {
			return &schema.ToolResult{Name: args.Name, Error: err.Error(), ToolCallID: args.CallID}, nil
		}
		return &schema.ToolResult{Name: args.Name, Content: result, ToolCallID: args.CallID}, nil
	}

	tCtx := &ToolContext{Name: tc.Function.Name, CallID: tc.ID}

	// Apply user middlewares (standard path - wrap as InvokableToolEndpoint for compat)
	wrappedEp := ep
	for _, mw := range tn.config.ToolCallMiddlewares {
		if mw.Invokable == nil { continue }
		// Adapt enhanced endpoint to standard for middleware chain
		compatEp := wrappedEp
		var adaptErr error
		wrappedEp = func(ctx context.Context, args *schema.ToolArgument, opts ...ToolOption) (*schema.ToolResult, error) {
			stdEp := func(ctx context.Context, jsonArgs string, o ...ToolOption) (string, error) {
				r, e := compatEp(ctx, &schema.ToolArgument{Arguments: jsonArgs}, o...)
				if e != nil { return "", e }
				if r.Error != "" { return "", fmt.Errorf("tool error: %s", r.Error) }
				return r.Content, nil
			}
			var wrappedStd InvokableToolEndpoint = stdEp
			wrappedStd, adaptErr = mw.Invokable(ctx, wrappedStd, tCtx)
			if adaptErr != nil { return nil, adaptErr }
			result, err := wrappedStd(ctx, args.Arguments, opts...)
			if err != nil { return &schema.ToolResult{Name: args.Name, Error: err.Error()}, nil }
			return &schema.ToolResult{Name: args.Name, Content: result}, nil
		}
		if adaptErr != nil { return tn.makeToolMsg(fmt.Sprintf("middleware error: %v", adaptErr), tc.ID), adaptErr }
	}

	result, err := wrappedEp(ctx, args)
	if err != nil {
		return tn.makeToolMsg(fmt.Sprintf("Error: %v", err), tc.ID), nil
	}

	content := result.Content
	if result.Error != "" && content == "" {
		content = fmt.Sprintf("Error: %s", result.Error)
	}
	return tn.makeToolMsg(content, tc.ID), nil
}

func (tn *ToolsNode[M]) executeEnhanced(ctx context.Context, tc schema.ToolCall, et EnhancedTool) (M, error) {
	args := &schema.ToolArgument{
		Name:      tc.Function.Name,
		Arguments: tc.Function.Arguments,
		CallID:    tc.ID,
	}

	ep := func(ctx context.Context, args *schema.ToolArgument, opts ...ToolOption) (*schema.ToolResult, error) {
		return et.EnhancedInvoke(ctx, args, opts...)
	}

	tCtx := &ToolContext{Name: tc.Function.Name, CallID: tc.ID}

	// Apply enhanced middleware chain
	for _, mw := range tn.config.ToolCallMiddlewares {
		if mw.EnhancedInvokable == nil { continue }
		var err error
		ep, err = mw.EnhancedInvokable(ctx, ep, tCtx)
		if err != nil { return tn.makeToolMsg(fmt.Sprintf("middleware error: %v", err), tc.ID), err }
	}

	result, err := ep(ctx, args)
	if err != nil {
		return tn.makeToolMsg(fmt.Sprintf("Error: %v", err), tc.ID), nil
	}

	content := result.Content
	if result.Error != "" && content == "" {
		content = fmt.Sprintf("Error: %s", result.Error)
	}
	return tn.makeToolMsg(content, tc.ID), nil
}

func (tn *ToolsNode[M]) makeToolMsg(content, callID string) M {
	msg := schema.ToolMessage(content, callID)
	return any(msg).(M)
}

// ---- Helper: convert tool results for event emission ----

func toolResultToEvent[M MessageType](msg M, roleName string) *TypedAgentEvent[M] {
	if m, ok := any(msg).(*schema.Message); ok {
		return any(typedEventFromMessage(m, nil, schema.RoleTool, roleName)).(*TypedAgentEvent[M])
	}
	return nil
}

// ---- JSON helpers ----

func parseToolArgs(argsJSON string, target any) error {
	if err := json.Unmarshal([]byte(argsJSON), target); err != nil {
		return fmt.Errorf("invalid tool arguments JSON: %w", err)
	}
	return nil
}
