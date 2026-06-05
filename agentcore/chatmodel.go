package agentcore

import (
	"context"
	"fmt"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
	"github.com/infiniflow/ragflow/harness/agentcore/internal"
)

// ChatModelConfig holds configuration for TypedChatModelAgent.
type ChatModelConfig[M MessageType] struct {
	Model              ChatModel[M]
	Tools              []Tool
	Instruction        string
	MaxIterations      int
	Middlewares        []TypedChatModelMiddleware[M]
	RetryConfig        *RetryConfig[M]
	FailoverConfig     *FailoverConfig[M]
	ReturnDirectly     map[string]bool
}

func DefaultChatModelConfig[M MessageType]() *ChatModelConfig[M] {
	return &ChatModelConfig[M]{MaxIterations: 10, Instruction: internal.DefaultSystemPrompt}
}

// TypedChatModelAgent implements the ReAct (Reasoning + Acting) pattern.
// It repeatedly calls the model, executes tool calls, and continues
// until a final answer or max iterations.
type TypedChatModelAgent[M MessageType] struct {
	name    string
	desc    string
	config  *ChatModelConfig[M]
}

var _ ResumableAgent = &TypedChatModelAgent[*schema.Message]{}
var _ TypedResumableAgent[*schema.AgenticMessage] = &TypedChatModelAgent[*schema.AgenticMessage]{}

func NewChatModelAgent[M MessageType](cfg *ChatModelConfig[M]) *TypedChatModelAgent[M] {
	return &TypedChatModelAgent[M]{
		name: "chat_model_agent", desc: "ReAct agent using a chat model",
		config: cfg,
	}
}
func (a *TypedChatModelAgent[M]) WithName(n string) *TypedChatModelAgent[M]                   { a.name = n; return a }
func (a *TypedChatModelAgent[M]) WithDescription(d string) *TypedChatModelAgent[M]            { a.desc = d; return a }
func (a *TypedChatModelAgent[M]) Name(_ context.Context) string                              { return a.name }
func (a *TypedChatModelAgent[M]) Description(_ context.Context) string                       { return a.desc }
func (a *TypedChatModelAgent[M]) GetType() string                                            { return "ChatModelAgent" }

func (a *TypedChatModelAgent[M]) Run(ctx context.Context, input *TypedAgentInput[M], opts ...RunOption) *AsyncIterator[*TypedAgentEvent[M]] {
	it, gen := NewAsyncIteratorPair[*TypedAgentEvent[M]]()
	go a.run(ctx, input, gen, opts...)
	return it
}

func (a *TypedChatModelAgent[M]) Resume(ctx context.Context, info *ResumeInfo, opts ...RunOption) *AsyncIterator[*TypedAgentEvent[M]] {
	it, gen := NewAsyncIteratorPair[*TypedAgentEvent[M]]()
	go a.resume(ctx, info, gen, opts...)
	return it
}

type chatModelExecCtx[M MessageType] struct {
	generator *AsyncGenerator[*TypedAgentEvent[M]]
	cancelCtx *cancelContext
}

func (ec *chatModelExecCtx[M]) send(event *TypedAgentEvent[M]) {
	if ec == nil || ec.generator == nil { return }
	if ec.cancelCtx != nil && ec.cancelCtx.isImmediate() { return }
	ec.generator.trySend(event)
}

type chatModelExecCtxForMessage = chatModelExecCtx[*schema.Message]
type chatModelExecCtxKey[M MessageType] struct{}

func withChatModelExecCtx[M MessageType](ctx context.Context, ec *chatModelExecCtx[M]) context.Context {
	return context.WithValue(ctx, chatModelExecCtxKey[M]{}, ec)
}
func getTypedChatModelExecCtx[M MessageType](ctx context.Context) *chatModelExecCtx[M] {
	if v := ctx.Value(chatModelExecCtxKey[M]{}); v != nil { return v.(*chatModelExecCtx[M]) }
	return nil
}
func getChatModelExecCtx(ctx context.Context) *chatModelExecCtxForMessage {
	return getTypedChatModelExecCtx[*schema.Message](ctx)
}

func (a *TypedChatModelAgent[M]) run(ctx context.Context, input *TypedAgentInput[M], gen *AsyncGenerator[*TypedAgentEvent[M]], opts ...RunOption) {
	defer func() {
		if r := recover(); r != nil { gen.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("panic: %v", r)}) }
		gen.Close()
	}()

	ec := &chatModelExecCtx[M]{generator: gen}
	ctx = withChatModelExecCtx(ctx, ec)

	maxIter := a.config.MaxIterations
	if maxIter <= 0 { maxIter = 10 }

	state := NewChatModelAgentState(input.Messages, toolsToInfosTyped[M](a.config.Tools), maxIter)

	// BeforeAgent middlewares
	rc := &ChatModelAgentContext{
		Instruction: a.config.Instruction, Tools: a.config.Tools,
		ReturnDirectly: a.config.ReturnDirectly,
	}
	for _, mw := range a.config.Middlewares {
		if mw == nil { continue }
		var err error
		ctx, rc, err = mw.BeforeAgent(ctx, rc)
		if err != nil { gen.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("BeforeAgent: %w", err)}); return }
	}

	// ReAct loop
	for state.RemainingIterations > 0 {
		state.RemainingIterations--

		// BeforeModelRewrite
		mc := &TypedModelContext[M]{
			Tools: state.ToolInfos, ModelRetryConfig: a.config.RetryConfig,
			ModelFailoverConfig: a.config.FailoverConfig,
		}
		for _, mw := range a.config.Middlewares {
			if mw == nil { continue }
			var err error
			ctx, state, err = mw.BeforeModelRewrite(ctx, state, mc)
			if err != nil { gen.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("BeforeModelRewrite: %w", err)}); return }
		}

		// Build model input
		modelMsgs := buildModelInput(state, rc)

		// Call model (wrapped by middlewares)
		model := a.config.Model
		for _, mw := range a.config.Middlewares {
			if mw == nil { continue }
			wrapped, err := mw.WrapModel(ctx, model, mc)
			if err != nil { gen.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("WrapModel: %w", err)}); return }
			model = wrapped
		}

		resp, err := model.Generate(ctx, modelMsgs)
		if err != nil { gen.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("model: %w", err)}); return }

		gen.Send(typedModelOutputEvent(resp, nil))
		state.Messages = append(state.Messages, resp)

		for _, mw := range a.config.Middlewares {
			if mw == nil { continue }
			var err error
			ctx, state, err = mw.AfterModelRewrite(ctx, state, mc)
			if err != nil { gen.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("AfterModelRewrite: %w", err)}); return }
		}

		toolCalls := extractToolCalls(resp)
		if len(toolCalls) == 0 { break } // final answer

		for _, tc := range toolCalls {
			tool := findTool(rc.Tools, tc.Function.Name)
			if tool == nil {
				errMsg := fmt.Sprintf("tool '%s' not found", tc.Function.Name)
				tr := schema.ToolMessage(errMsg, tc.ID)
				state.Messages = append(state.Messages, any(tr).(M))
				if m, ok := any(tr).(*schema.Message); ok {
					gen.Send(any(typedEventFromMessage(m, nil, schema.RoleTool, tc.Function.Name)).(*TypedAgentEvent[M]))
				}
				continue
			}

			ep := func(ctx context.Context, args string, opts ...toolOption) (string, error) {
				return tool.Invoke(ctx, args)
			}
			tCtx := &ToolContext{Name: tc.Function.Name, CallID: tc.ID}
			for _, mw := range a.config.Middlewares {
				if mw == nil { continue }
				wrapped, err := mw.WrapToolInvoke(ctx, ep, tCtx)
				if err != nil { return }
				ep = wrapped
			}

			result, err := ep(ctx, tc.Function.Arguments)
			var toolMsg M
			if err != nil {
				toolMsg = any(schema.ToolMessage(fmt.Sprintf("Error: %v", err), tc.ID)).(M)
			} else {
				toolMsg = any(schema.ToolMessage(result, tc.ID)).(M)
			}
			state.Messages = append(state.Messages, toolMsg)
			if m, ok := any(toolMsg).(*schema.Message); ok {
			gen.Send(any(typedEventFromMessage(m, nil, schema.RoleTool, tc.Function.Name)).(*TypedAgentEvent[M]))
		}
		}
	}

	if state.RemainingIterations <= 0 {
		gen.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("exceeded max iterations (%d)", a.config.MaxIterations)})
		return
	}

	for _, mw := range a.config.Middlewares {
		if mw == nil { continue }
		var err error
		ctx, err = mw.AfterAgent(ctx, state)
		if err != nil { gen.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("AfterAgent: %w", err)}); return }
	}
}

func (a *TypedChatModelAgent[M]) resume(ctx context.Context, info *ResumeInfo, gen *AsyncGenerator[*TypedAgentEvent[M]], opts ...RunOption) {
	if info.WasInterrupted {
		if s, ok := info.InterruptState.(*TypedChatModelAgentState[M]); ok {
			a.run(ctx, &TypedAgentInput[M]{Messages: s.Messages, EnableStreaming: info.EnableStreaming}, gen, opts...)
			return
		}
	}
	gen.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("resume called but agent was not interrupted or state is invalid")})
	gen.Close()
}

// ---- Helpers ----

func toolsToInfosTyped[M MessageType](tools []Tool) []*schema.ToolInfo {
	infos := make([]*schema.ToolInfo, 0, len(tools))
	for _, t := range tools {
		infos = append(infos, &schema.ToolInfo{Name: t.Name(), Description: t.Description()})
	}
	return infos
}

func buildModelInput[M MessageType](state *TypedChatModelAgentState[M], rc *ChatModelAgentContext) []M {
	var msgs []M
	if rc.Instruction != "" {
		var sys M
		switch any(sys).(type) {
		case *schema.Message:
			sys = any(schema.SystemMessage(rc.Instruction)).(M)
		case *schema.AgenticMessage:
			sys = any(&schema.AgenticMessage{Role: schema.AgenticRoleSystem, Content: rc.Instruction}).(M)
		}
		msgs = append(msgs, sys)
	}
	msgs = append(msgs, state.Messages...)
	return msgs
}

func extractToolCalls[M MessageType](resp M) []schema.ToolCall {
	switch msg := any(resp).(type) {
	case *schema.Message:
		return msg.ToolCalls
	case *schema.AgenticMessage:
		var calls []schema.ToolCall
		for _, b := range msg.ContentBlocks {
			if b.Type == "tool_call" && b.ToolCall != nil {
				calls = append(calls, schema.ToolCall{
					ID: b.ToolCall.ID,
					Function: schema.ToolCallFunction{Name: b.ToolCall.Name, Arguments: b.ToolCall.Arguments},
				})
			}
		}
		return calls
	default:
		return nil
	}
}

func findTool(tools []Tool, name string) Tool {
	for _, t := range tools {
		if t.Name() == name { return t }
	}
	return nil
}
