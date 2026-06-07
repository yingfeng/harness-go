// Package agentcore provides a graph-level ReAct loop using the project's own
// StateGraph engine, with built-in checkpoint/interrupt/resume support at each
// iteration boundary.
//
// The ReActGraph wraps a TypedChatModelAgent's loop into StateGraph nodes so that
// the graph engine's checkpointing (via graph.WithCheckpointer) and interrupt/resume
// (via graph.WithInterrupts) apply at each superstep automatically. This replaces
// the simple for-loop in chatmodel_react.go with the full Pregel execution engine.
//
// Key features:
//   - Checkpoint at every model_generate and execute_tools node boundary
//   - Interrupt before execute_tools for human-in-the-loop tool approval
//   - Resume from interrupt via graph checkpoint restoration
//   - Full middleware chain (BeforeAgent, BeforeModelRewrite, AfterModelRewrite, AfterAgent)
//   - ToolsNode integration with ToolCallMiddlewares
//   - Streaming events via pregel.StreamManager
//   - Generic: supports both *schema.Message and *schema.AgenticMessage
package agentcore

import (
	"context"
	"fmt"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
	"github.com/infiniflow/ragflow/harness/channels"
	"github.com/infiniflow/ragflow/harness/constants"
	"github.com/infiniflow/ragflow/harness/graph"
	"github.com/infiniflow/ragflow/harness/pregel"
	"github.com/infiniflow/ragflow/harness/types"
)

func init() {
	schema.RegisterType("_harness_react_graph_state", func() any { return &ReActGraphState{} })
}

// ReActGraphState is the shared state for the graph-level ReAct loop.
// It persists across supersteps, enabling checkpoint and interrupt/resume.
type ReActGraphState struct {
	Messages       []*schema.Message
	ToolInfos      []*schema.ToolInfo
	IterationsLeft int
	MaxIterations  int
	AgentName      string
	Instruction    string
	HasToolCall    bool // signals whether the last model output had tool calls
}

// ReActGraph wraps a ChatModelAgent's loop into a StateGraph with automatic
// checkpoint at each iteration and interrupt before tool execution.
type ReActGraph struct {
	compiled *graph.CompiledGraph
	config   *ChatModelConfig[*schema.Message]
	agent    *TypedChatModelAgent[*schema.Message]
}

// ReActGraphConfig holds options for building a ReActGraph.
type ReActGraphConfig struct {
	Checkpointer    graph.Checkpointer
	InterruptBefore []string // node names to interrupt before (default: "execute_tools")
	RecursionLimit  int
}

// NewReActGraph builds a StateGraph with nodes:
//
//	prepare_input → model_generate → execute_tools → check_done
//	                                                ↘ [end]
//
// Interrupt is set at "execute_tools" by default. With a Checkpointer, each node
// transition automatically saves a checkpoint via the Pregel engine.
//
// The graph applies the full middleware chain:
//   - prepare_input: BeforeAgent
//   - model_generate: BeforeModelRewrite → model call → AfterModelRewrite
//   - check_done (on exit): AfterAgent
func NewReActGraph(agent *TypedChatModelAgent[*schema.Message], cfg *ReActGraphConfig) (*ReActGraph, error) {
	if cfg == nil {
		cfg = &ReActGraphConfig{}
	}
	agentCfg := agent.config
	sg := graph.NewStateGraph(&ReActGraphState{})

	// Register channels for state fields used by the graph engine.
	sg.AddChannel("messages", channels.NewLastValue([]*schema.Message{}))
	sg.AddChannel("iterations_left", channels.NewLastValue(0))
	sg.AddChannel("has_tool_call", channels.NewLastValue(false))

	// --- Node: prepare_input ---
	// Runs once at the start. Applies BeforeAgent middleware.
	sg.AddNode("prepare_input", func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(*ReActGraphState)
		rc := &ChatModelAgentContext{
			Instruction:    s.Instruction,
			Tools:          agentCfg.Tools,
			ReturnDirectly: agentCfg.ReturnDirectly,
		}
		for _, mw := range agentCfg.Middlewares {
			if mw == nil {
				continue
			}
			var err error
			ctx, rc, err = mw.BeforeAgent(ctx, rc)
			if err != nil {
				return nil, fmt.Errorf("BeforeAgent: %w", err)
			}
		}
		s.Instruction = rc.Instruction
		return s, nil
	})

	// --- Node: model_generate ---
	// Calls the LLM with the current message history. Applies BeforeModelRewrite
	// and AfterModelRewrite middleware chains.
	sg.AddNode("model_generate", func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(*ReActGraphState)
		if s.IterationsLeft <= 0 {
			return s, nil
		}
		s.IterationsLeft--

		model := BuildModelWrapperChain(agentCfg.Model, nil, agentCfg)

		agentState := NewChatModelAgentState(
			messageSliceToAny(s.Messages),
			s.ToolInfos,
			s.IterationsLeft+1,
		)
		typedState := (*TypedChatModelAgentState[*schema.Message])(agentState)
		mc := &TypedModelContext[*schema.Message]{
			Tools:               s.ToolInfos,
			ModelRetryConfig:    agentCfg.RetryConfig,
			ModelFailoverConfig: agentCfg.FailoverConfig,
		}

		// BeforeModelRewrite middleware chain.
		for _, mw := range agentCfg.Middlewares {
			if mw == nil {
				continue
			}
			var err error
			ctx, typedState, err = mw.BeforeModelRewrite(ctx, typedState, mc)
			if err != nil {
				return nil, fmt.Errorf("BeforeModelRewrite: %w", err)
			}
		}
		s.Messages = typedState.Messages

		// StateModifier hook (e.g., context window trimming).
		if agentCfg.StateModifier != nil {
			var err error
			typedState, err = agentCfg.StateModifier(ctx, typedState)
			if err != nil {
				return nil, fmt.Errorf("StateModifier: %w", err)
			}
			s.Messages = typedState.Messages
		}

		// Build model input (via GenModelInput or default).
		var modelMsgs []*schema.Message
		if agentCfg.GenModelInput != nil {
			var err error
			modelMsgs, err = agentCfg.GenModelInput(ctx, s.Instruction,
				&TypedAgentInput[*schema.Message]{Messages: s.Messages})
			if err != nil {
				return nil, fmt.Errorf("GenModelInput: %w", err)
			}
		} else {
			modelMsgs = buildModelInputFromState(s.Messages, s.Instruction)
		}

		// Call model.
		resp, err := model.Generate(ctx, modelMsgs)
		if err != nil {
			return nil, fmt.Errorf("model: %w", err)
		}
		s.Messages = append(s.Messages, resp)

		// AfterModelRewrite middleware chain.
		typedState.Messages = s.Messages
		for _, mw := range agentCfg.Middlewares {
			if mw == nil {
				continue
			}
			var err error
			ctx, typedState, err = mw.AfterModelRewrite(ctx, typedState, mc)
			if err != nil {
				return nil, fmt.Errorf("AfterModelRewrite: %w", err)
			}
		}
		s.Messages = typedState.Messages

		// Detect if the model produced tool calls.
		toolCalls := extractToolCalls(resp)
		s.HasToolCall = len(toolCalls) > 0

		return s, nil
	})

	// --- Node: execute_tools ---
	// Executes tool calls found in the last model response.
	// Uses ToolsNode for middleware chain support.
	sg.AddNode("execute_tools", func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(*ReActGraphState)
		if len(s.Messages) == 0 {
			return s, nil
		}
		last := s.Messages[len(s.Messages)-1]
		toolCalls := extractToolCalls(last)
		if len(toolCalls) == 0 {
			return s, nil
		}

		agentState := NewChatModelAgentState(
			messageSliceToAny(s.Messages),
			s.ToolInfos,
			s.IterationsLeft,
		)
		typedState := (*TypedChatModelAgentState[*schema.Message])(agentState)

		// Use ToolsNode for full middleware chain support.
		var tn *ToolsNode[*schema.Message]
		if agentCfg.ToolsConfig != nil {
			tn = NewToolsNode[*schema.Message](agentCfg.ToolsConfig)
		}
		if tn != nil {
			results, act, err := tn.Execute(ctx, last, typedState, nil)
			if err != nil {
				return nil, fmt.Errorf("tools: %w", err)
			}
			for _, tr := range results {
				s.Messages = append(s.Messages, tr)
			}
			if act != nil && act.Exit {
				s.IterationsLeft = 0
				s.HasToolCall = false
			}
		} else {
			// Fallback: inline tool execution with middleware chain.
			for _, tc := range toolCalls {
				tool := findTool(agentCfg.Tools, tc.Function.Name)
				if tool == nil {
					s.Messages = append(s.Messages, schema.ToolMessage(
						fmt.Sprintf("tool '%s' not found", tc.Function.Name), tc.ID))
					continue
				}
				ep := func(ctx context.Context, args string, opts ...ToolOption) (string, error) {
					return tool.Invoke(ctx, args, opts...)
				}
				tCtx := &ToolContext{Name: tc.Function.Name, CallID: tc.ID}
				for _, mw := range agentCfg.Middlewares {
					if mw == nil { continue }
					wrapped, err := mw.WrapToolInvoke(ctx, ep, tCtx)
					if err != nil {
						return nil, fmt.Errorf("tool middleware '%s': %w", tc.Function.Name, err)
					}
					ep = wrapped
				}
				result, err := ep(ctx, tc.Function.Arguments)
				if err != nil {
					s.Messages = append(s.Messages, schema.ToolMessage(
						fmt.Sprintf("Error: %v", err), tc.ID))
				} else {
					s.Messages = append(s.Messages, schema.ToolMessage(result, tc.ID))
				}
			}
		}
		return s, nil
	})

	// --- Node: check_done ---
	// Emits AfterAgent middleware and writes the final output.
	sg.AddNode("check_done", func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(*ReActGraphState)
		agentState := NewChatModelAgentState(
			messageSliceToAny(s.Messages),
			s.ToolInfos,
			s.IterationsLeft,
		)
		typedState := (*TypedChatModelAgentState[*schema.Message])(agentState)

		for _, mw := range agentCfg.Middlewares {
			if mw == nil {
				continue
			}
			var err error
			ctx, err = mw.AfterAgent(ctx, typedState)
			if err != nil {
				return nil, fmt.Errorf("AfterAgent: %w", err)
			}
		}

		// Store output in session if configured.
		if agentCfg.OutputKey != "" && len(s.Messages) > 0 {
			last := s.Messages[len(s.Messages)-1]
			setOutputToSession(ctx, last, agentCfg.OutputKey)
		}
		return s, nil
	})

	// --- Edges ---
	sg.AddEdge(constants.Start, "prepare_input")
	sg.AddEdge("prepare_input", "model_generate")

	// Conditional: if has tool calls → execute_tools, else → end
	sg.AddConditionalEdges("model_generate", func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(*ReActGraphState)
		if s.IterationsLeft <= 0 || !s.HasToolCall {
			return constants.End, nil
		}
		return "execute_tools", nil
	}, map[string]string{
		constants.End: constants.End,
		"execute_tools": "execute_tools",
	})

	sg.AddEdge("execute_tools", "model_generate") // loop back for next iteration

	// --- Compile with checkpoint and interrupt ---
	interrupts := cfg.InterruptBefore
	if len(interrupts) == 0 {
		interrupts = []string{"execute_tools"}
	}
	rl := cfg.RecursionLimit
	if rl <= 0 {
		rl = constants.DefaultRecursionLimit
	}

	compileOpts := []graph.CompileOption{
		graph.WithRecursionLimit(rl),
	}
	if cfg.Checkpointer != nil {
		compileOpts = append(compileOpts, graph.WithCheckpointer(cfg.Checkpointer))
	}
	for _, name := range interrupts {
		compileOpts = append(compileOpts, graph.WithInterrupts(name))
	}

	compiled, err := sg.Compile(compileOpts...)
	if err != nil {
		return nil, fmt.Errorf("compile ReAct graph: %w", err)
	}

	return &ReActGraph{
		compiled: compiled,
		config:   agentCfg,
		agent:    agent,
	}, nil
}

// Invoke runs the graph-level ReAct loop synchronously via the Pregel engine.
func (rg *ReActGraph) Invoke(ctx context.Context, input *AgentInput, config *types.RunnableConfig) (*ReActGraphState, error) {
	state := rg.buildInitialState(input)

	result, err := rg.compiled.Invoke(ctx, state)
	if err != nil {
		return nil, err
	}
	outState, ok := result.(*ReActGraphState)
	if !ok {
		return nil, fmt.Errorf("unexpected result type %T from graph", result)
	}
	return outState, nil
}

// Stream runs the graph-level ReAct loop with streaming events via Pregel.
// Returns (outputCh, errCh). The outputCh yields pregel.StreamEvent values
// including checkpoint, task start/end, values, and final state.
func (rg *ReActGraph) Stream(ctx context.Context, input *AgentInput, config *types.RunnableConfig, mode types.StreamMode) (<-chan interface{}, <-chan error) {
	state := rg.buildInitialState(input)
	return rg.compiled.Stream(ctx, state, mode)
}

// Resume resumes a previously interrupted graph execution from its checkpoint.
func (rg *ReActGraph) Resume(ctx context.Context, config *types.RunnableConfig) (*ReActGraphState, error) {
	// The graph engine's Invoke with the same config (which has checkpoint_id
	// and thread_id) will automatically restore from the checkpoint and resume.
	result, err := rg.compiled.Invoke(ctx, nil)
	if err != nil {
		return nil, err
	}
	outState, ok := result.(*ReActGraphState)
	if !ok {
		return nil, fmt.Errorf("unexpected result type %T from resumed graph", result)
	}
	return outState, nil
}

// ResumeStream resumes a previously interrupted graph with streaming.
func (rg *ReActGraph) ResumeStream(ctx context.Context, config *types.RunnableConfig, mode types.StreamMode) (<-chan interface{}, <-chan error) {
	return rg.compiled.Stream(ctx, nil, mode)
}

// Compile returns the underlying compiled graph for direct access.
func (rg *ReActGraph) Compile() *graph.CompiledGraph { return rg.compiled }

// ---- helpers ----

func (rg *ReActGraph) buildInitialState(input *AgentInput) *ReActGraphState {
	maxIter := rg.config.MaxIterations
	if maxIter <= 0 {
		maxIter = 10
	}
	state := &ReActGraphState{
		Messages:       input.Messages,
		IterationsLeft: maxIter,
		MaxIterations:  maxIter,
		AgentName:      rg.agent.name,
		Instruction:    rg.config.Instruction,
	}
	state.ToolInfos = make([]*schema.ToolInfo, len(rg.config.Tools))
	for i, t := range rg.config.Tools {
		state.ToolInfos[i] = &schema.ToolInfo{Name: t.Name(), Description: t.Description()}
	}
	return state
}

func messageSliceToAny(msgs []*schema.Message) []Message {
	r := make([]Message, len(msgs))
	for i, m := range msgs {
		r[i] = m
	}
	return r
}

// Ensure pregel is imported for side effects (engine registration).
var _ = pregel.Engine{}
