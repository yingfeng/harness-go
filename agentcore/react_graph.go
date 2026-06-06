// Package agentcore provides a graph-level ReAct loop using the project's own
// StateGraph engine, with built-in checkpoint/interrupt/resume support.
//
// The ReActGraph wraps a TypedChatModelAgent's loop into StateGraph nodes so that
// the graph engine's checkpointing (via WithCheckpointer) and interrupt/resume
// (via WithInterrupts) apply at each iteration boundary automatically.
package agentcore

import (
	"context"
	"fmt"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
	"github.com/infiniflow/ragflow/harness/constants"
	"github.com/infiniflow/ragflow/harness/graph"
	"github.com/infiniflow/ragflow/harness/types"
)

func init() {
	schema.RegisterType("_harness_react_graph_state", func() any { return &ReActGraphState{} })
}

// ReActGraphState is the shared state for the graph-level ReAct loop.
type ReActGraphState struct {
	Messages        []*schema.Message
	ToolInfos       []*schema.ToolInfo
	IterationsLeft  int
	MaxIterations   int
	AgentName       string
	Instruction     string
}

// ReActGraph wraps a ChatModelAgent in a StateGraph with automatic checkpoint
// at each iteration and interrupt at the "execute_tools" node for human-in-the-loop.
type ReActGraph struct {
	compiled *graph.CompiledGraph
	config   *ChatModelConfig[*schema.Message]
	agent    *TypedChatModelAgent[*schema.Message]
}

// NewReActGraph builds a StateGraph with nodes:
//
//	prepare_input → model_generate → execute_tools → check_done
//	                                                ↘ [end]
//
// Interrupt is set at "execute_tools". With a Checkpointer on config, each node
// transition saves a checkpoint automatically.
func NewReActGraph(agent *TypedChatModelAgent[*schema.Message], cptr graph.Checkpointer) (*ReActGraph, error) {
	cfg := agent.config
	sg := graph.NewStateGraph(&ReActGraphState{})

	// --- Nodes ---

	sg.AddNode("prepare_input", func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(*ReActGraphState)
		rc := &ChatModelAgentContext{
			Instruction:    s.Instruction,
			Tools:          cfg.Tools,
			ReturnDirectly: cfg.ReturnDirectly,
		}
		for _, mw := range cfg.Middlewares {
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

	sg.AddNode("model_generate", func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(*ReActGraphState)
		if s.IterationsLeft <= 0 {
			return s, nil
		}
		s.IterationsLeft--

		model := BuildModelWrapperChain(cfg.Model, nil, cfg)

		agentState := NewChatModelAgentState(
			messageSliceToAny(s.Messages),
			s.ToolInfos,
			s.IterationsLeft+1,
		)
		typedState := (*TypedChatModelAgentState[*schema.Message])(agentState)
		mc := &TypedModelContext[*schema.Message]{
			Tools:               s.ToolInfos,
			ModelRetryConfig:    cfg.RetryConfig,
			ModelFailoverConfig: cfg.FailoverConfig,
		}
		for _, mw := range cfg.Middlewares {
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

		modelMsgs := buildModelInputFromState(s.Messages, s.Instruction)
		resp, err := model.Generate(ctx, modelMsgs)
		if err != nil {
			return nil, fmt.Errorf("model: %w", err)
		}
		s.Messages = append(s.Messages, resp)

		for _, mw := range cfg.Middlewares {
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
		return s, nil
	})

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

		var tn *ToolsNode[*schema.Message]
		if cfg.ToolsConfig != nil {
			tn = NewToolsNode[*schema.Message](cfg.ToolsConfig)
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
			}
		} else {
			for _, tc := range toolCalls {
				tool := findTool(cfg.Tools, tc.Function.Name)
				if tool == nil {
					s.Messages = append(s.Messages, schema.ToolMessage(
						fmt.Sprintf("tool '%s' not found", tc.Function.Name), tc.ID))
					continue
				}
				result, err := tool.Invoke(ctx, tc.Function.Arguments)
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

	// --- Edges ---

	sg.AddEdge(constants.Start, "prepare_input")
	sg.AddEdge("prepare_input", "model_generate")
	sg.AddEdge("model_generate", "execute_tools")
	sg.AddConditionalEdges("execute_tools", func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(*ReActGraphState)
		if s.IterationsLeft <= 0 {
			return constants.End, nil
		}
		if len(s.Messages) > 0 {
			last := s.Messages[len(s.Messages)-1]
			tcs := extractToolCalls(last)
			if len(tcs) == 0 {
				return constants.End, nil
			}
		}
		return "model_generate", nil
	}, map[string]string{
		constants.End:   constants.End,
		"model_generate": "model_generate",
	})

	// --- Compile with checkpoint and interrupt ---

	compiled, err := sg.Compile(
		graph.WithCheckpointer(cptr),
		graph.WithInterrupts("execute_tools"),
	)
	if err != nil {
		return nil, fmt.Errorf("compile ReAct graph: %w", err)
	}

	return &ReActGraph{
		compiled: compiled,
		config:   cfg,
		agent:    agent,
	}, nil
}

// Invoke runs the graph-level ReAct loop synchronously.
func (rg *ReActGraph) Invoke(ctx context.Context, input *AgentInput) (*ReActGraphState, error) {
	state := &ReActGraphState{
		Messages:       input.Messages,
		IterationsLeft: rg.config.MaxIterations,
		MaxIterations:  rg.config.MaxIterations,
		AgentName:      rg.agent.name,
		Instruction:    rg.config.Instruction,
	}
	if state.IterationsLeft <= 0 {
		state.IterationsLeft = 10
	}
	state.ToolInfos = make([]*schema.ToolInfo, len(rg.config.Tools))
	for i, t := range rg.config.Tools {
		state.ToolInfos[i] = &schema.ToolInfo{Name: t.Name(), Description: t.Description()}
	}

	result, err := rg.compiled.Invoke(ctx, state)
	if err != nil {
		return nil, err
	}
	outState, ok := result.(*ReActGraphState)
	if !ok {
		return nil, fmt.Errorf("unexpected result type %T", result)
	}
	return outState, nil
}

// Stream runs the graph-level ReAct loop with streaming events.
// Returns (outputCh, errCh). The outputCh yields pregel.StreamEvent values.
func (rg *ReActGraph) Stream(ctx context.Context, input *AgentInput, mode string) (<-chan interface{}, <-chan error) {
	state := &ReActGraphState{
		Messages:       input.Messages,
		IterationsLeft: rg.config.MaxIterations,
		MaxIterations:  rg.config.MaxIterations,
		AgentName:      rg.agent.name,
		Instruction:    rg.config.Instruction,
	}
	if state.IterationsLeft <= 0 {
		state.IterationsLeft = 10
	}
	state.ToolInfos = make([]*schema.ToolInfo, len(rg.config.Tools))
	for i, t := range rg.config.Tools {
		state.ToolInfos[i] = &schema.ToolInfo{Name: t.Name(), Description: t.Description()}
	}

	modeVal := types.StreamMode(mode)
	if modeVal == "" {
		modeVal = types.StreamModeValues
	}
	return rg.compiled.Stream(ctx, state, modeVal)
}

// Compile returns the underlying compiled graph.
func (rg *ReActGraph) Compile() *graph.CompiledGraph { return rg.compiled }

// ---- helpers ----

func messageSliceToAny(msgs []*schema.Message) []Message {
	r := make([]Message, len(msgs))
	for i, m := range msgs {
		r[i] = m
	}
	return r
}
