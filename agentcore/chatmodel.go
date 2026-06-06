package agentcore

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

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
	RetryConfig        *TypedModelRetryConfig[M]
	FailoverConfig     *FailoverConfig[M]
	ReturnDirectly     map[string]bool
	OutputKey          string
	GenModelInput      TypedGenModelInput[M]
	ToolsConfig        *ToolsNodeConfig
	EmitInternalEvents bool
}

func DefaultChatModelConfig[M MessageType]() *ChatModelConfig[M] {
	return &ChatModelConfig[M]{MaxIterations: 10, Instruction: internal.DefaultSystemPrompt}
}

// ChatModelAgentResumeData holds data provided during resume to modify agent behavior.
type ChatModelAgentResumeData struct {
	HistoryModifier func(ctx context.Context, messages []Message) []Message
}

// TypedChatModelAgent implements the ReAct (Reasoning + Acting) pattern.
//
// Production features:
//   - freeze-once: after first Run/Resume, configuration is frozen (atomic)
//   - ToolsNode abstraction with middleware chain support
//   - Enhanced Tool (4 endpoint types) support via handler interface
//   - DeferredToolInfos for server-side tool search
//   - EmitInternalEvents for AgentTool event forwarding
//   - AfterToolCallsHook for TurnLoop integration
//   - ResumeWithData / HistoryModifier for resume customization
//   - gob encodability check on SetRunLocalValue
type TypedChatModelAgent[M MessageType] struct {
	name        string
	desc        string
	config      *ChatModelConfig[M]

	once   sync.Once
	frozen uint32
	run    typedRunFunc[M]
	exeCtx *execContext
}

var _ ResumableAgent = &TypedChatModelAgent[*schema.Message]{}
var _ TypedResumableAgent[*schema.AgenticMessage] = &TypedChatModelAgent[*schema.AgenticMessage]{}

type TypedGenModelInput[M MessageType] func(ctx context.Context, instruction string, input *TypedAgentInput[M]) ([]M, error)

func defaultGenModelInput(ctx context.Context, instruction string, input *AgentInput) ([]Message, error) {
	msgs := make([]Message, 0, len(input.Messages)+1)
	if instruction != "" {
		processed := resolveTemplate(instruction, ctx)
		msgs = append(msgs, schema.SystemMessage(processed))
	}
	msgs = append(msgs, input.Messages...)
	return msgs, nil
}

func resolveTemplate(tmpl string, ctx context.Context) string {
	s := getSession(ctx)
	if s == nil { return tmpl }
	result := tmpl
	for k, v := range s.Values {
		repl := fmt.Sprintf("{%s}", k)
		if sv, ok := v.(string); ok { result = strings.ReplaceAll(result, repl, sv) }
	}
	return result
}

func NewChatModelAgent[M MessageType](cfg *ChatModelConfig[M]) *TypedChatModelAgent[M] {
	if cfg == nil { cfg = DefaultChatModelConfig[M]() }
	a := &TypedChatModelAgent[M]{name: "chat_model_agent", desc: "ReAct agent using a chat model", config: cfg}
	if cfg.ToolsConfig == nil && len(cfg.Tools) > 0 {
		cfg.ToolsConfig = &ToolsNodeConfig{Tools: cfg.Tools, ReturnDirectly: cfg.ReturnDirectly}
	}
	return a
}
func (a *TypedChatModelAgent[M]) WithName(n string) *TypedChatModelAgent[M] { a.name = n; return a }
func (a *TypedChatModelAgent[M]) WithDescription(d string) *TypedChatModelAgent[M] { a.desc = d; return a }
func (a *TypedChatModelAgent[M]) Name(_ context.Context) string              { return a.name }
func (a *TypedChatModelAgent[M]) Description(_ context.Context) string       { return a.desc }
func (a *TypedChatModelAgent[M]) GetType() string                           { return "ChatModelAgent" }

// ---- Freeze mechanism ----

func (a *TypedChatModelAgent[M]) IsFrozen() bool { return atomic.LoadUint32(&a.frozen) == 1 }

func (a *TypedChatModelAgent[M]) freeze() { atomic.StoreUint32(&a.frozen, 1) }

// ---- Run / Resume ----

func (a *TypedChatModelAgent[M]) Run(ctx context.Context, input *TypedAgentInput[M], opts ...RunOption) *AsyncIterator[*TypedAgentEvent[M]] {
	it, gen := NewAsyncIteratorPair[*TypedAgentEvent[M]]()
	go func() {
		defer func() {
			if r := recover(); r != nil { gen.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("panic: %v", r)}) }
			gen.Close()
		}()
		runFunc := a.buildRunFunc(ctx)
		runFunc(ctx, &typedRunParams[M]{input: input, generator: gen})
		a.freeze()
	}()
	return it
}

func (a *TypedChatModelAgent[M]) Resume(ctx context.Context, info *ResumeInfo, opts ...RunOption) *AsyncIterator[*TypedAgentEvent[M]] {
	it, gen := NewAsyncIteratorPair[*TypedAgentEvent[M]]()
	go func() {
		defer func() {
			if r := recover(); r != nil { gen.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("panic: %v", r)}) }
			gen.Close()
		}()
		if info.WasInterrupted {
			if s, ok := info.InterruptState.(*TypedChatModelAgentState[M]); ok {
				runFunc := a.buildRunFunc(ctx)
				params := &typedRunParams[M]{input: &TypedAgentInput[M]{Messages: s.Messages, EnableStreaming: info.EnableStreaming}, generator: gen, interruptState: s, resumeInfo: info}
				if info.ResumeData != nil { if rd, ok := info.ResumeData.(*ChatModelAgentResumeData); ok && rd.HistoryModifier != nil { params.historyModifier = rd.HistoryModifier } }
				runFunc(ctx, params)
				a.freeze()
				return
			}
		}
		gen.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("resume called but agent was not interrupted or state is invalid")})
	}()
	return it
}

// ---- Internal types ----

type typedRunFunc[M MessageType] func(ctx context.Context, p *typedRunParams[M])

type typedRunParams[M MessageType] struct {
	input            *TypedAgentInput[M]
	generator        *AsyncGenerator[*TypedAgentEvent[M]]
	interruptState    *TypedChatModelAgentState[M]
	resumeInfo       *ResumeInfo
	historyModifier  func(context.Context, []Message) []Message
	afterToolCallsHook func(ctx context.Context) error
}

// chatModelExecCtx carries per-execution state for event sending, cancellation,
// retry signal propagation, and after-tool-calls hooks.
type chatModelExecCtx struct {
	generator         *AsyncGenerator[*TypedAgentEvent[*schema.Message]]
	cancelCtx         *cancelContext
	suppressEventSend bool
	retrySignal       *retrySignal
	failoverLastModel ChatModel[*schema.Message]
	afterToolCallsHook func(ctx context.Context) error
}

func (ec *chatModelExecCtx) send(ev any) {
	if ec != nil && ec.generator != nil {
		if te, ok := ev.(*TypedAgentEvent[*schema.Message]); ok { ec.generator.Send(te) }
	}
}

type execContext struct {
	instruction       string
	returnDirectly    map[string]bool
	toolInfos         []*schema.ToolInfo
	deferredToolInfos []*schema.ToolInfo
	toolSearchTool    *schema.ToolInfo
	emitInternalEvents bool
}

// ---- Run function builder ----

func (a *TypedChatModelAgent[M]) buildRunFunc(ctx context.Context) typedRunFunc[M] {
	var onceRun typedRunFunc[M]
	a.once.Do(func() {
		ec, err := a.prepareExecContext(ctx)
		if err != nil { onceRun = func(_ context.Context, _ *typedRunParams[M]) {}; a.run = onceRun; return }
		a.exeCtx = ec
		if len(a.config.Tools) > 0 || (a.config.ToolsConfig != nil && len(a.config.ToolsConfig.Tools) > 0) {
			onceRun = a.buildReActRunFunc()
		} else { onceRun = a.buildNoToolsRunFunc() }
		a.run = onceRun
	})
	if a.run != nil { return a.run }
	return a.buildReActRunFunc()
}

func (a *TypedChatModelAgent[M]) prepareExecContext(_ context.Context) (*execContext, error) {
	instruction := a.config.Instruction
	if instruction == "" { instruction = internal.DefaultSystemPrompt }
	rd := a.config.ReturnDirectly
	if rd == nil { rd = make(map[string]bool) }
	return &execContext{instruction: instruction, returnDirectly: rd, toolInfos: toolsToInfosTyped[M](a.config.Tools), emitInternalEvents: a.config.EmitInternalEvents}, nil
}

// ---- No-tools run function ----

func (a *TypedChatModelAgent[M]) buildNoToolsRunFunc() typedRunFunc[M] {
	return func(ctx context.Context, p *typedRunParams[M]) {
		// BeforeAgent middleware
		rc := &ChatModelAgentContext{Instruction: a.exeCtx.instruction, Tools: a.config.Tools, ReturnDirectly: a.exeCtx.returnDirectly}
		for _, mw := range a.config.Middlewares {
			if mw == nil { continue }
			var err error
			ctx, rc, err = mw.BeforeAgent(ctx, rc)
			if err != nil {
				p.generator.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("BeforeAgent: %w", err)})
				return
			}
		}

		model := BuildModelWrapperChain(a.config.Model, nil, a.config)
		state := NewChatModelAgentState(p.input.Messages, a.exeCtx.toolInfos, a.config.MaxIterations)

		// BeforeModelRewrite middleware
		mc := &TypedModelContext[M]{Tools: state.ToolInfos, ModelRetryConfig: a.config.RetryConfig, ModelFailoverConfig: a.config.FailoverConfig}
		for _, mw := range a.config.Middlewares {
			if mw == nil { continue }
			var err error
			ctx, state, err = mw.BeforeModelRewrite(ctx, state, mc)
			if err != nil {
				p.generator.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("BeforeModelRewrite: %w", err)})
				return
			}
		}

		modelMsgs := buildModelInputFromState[M](p.input.Messages, rc.Instruction)
		resp, err := model.Generate(ctx, modelMsgs)
		if err != nil { p.generator.Send(&TypedAgentEvent[M]{Err: err}); return }
		p.generator.Send(typedModelOutputEvent(resp, nil))
		state.Messages = append(state.Messages, resp)

		// AfterModelRewrite middleware
		for _, mw := range a.config.Middlewares {
			if mw == nil { continue }
			var err error
			ctx, state, err = mw.AfterModelRewrite(ctx, state, mc)
			if err != nil {
				p.generator.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("AfterModelRewrite: %w", err)})
				return
			}
		}

		if a.config.OutputKey != "" && !isNilMessage(resp) { setOutputToSession(ctx, resp, a.config.OutputKey) }

		// AfterAgent middleware
		for _, mw := range a.config.Middlewares {
			if mw == nil { continue }
			var err2 error
			ctx, err2 = mw.AfterAgent(ctx, state)
			if err2 != nil {
				p.generator.Send(&TypedAgentEvent[M]{Err: err2})
				return
			}
		}
	}
}

// ---- ReAct run function ----

func (a *TypedChatModelAgent[M]) buildReActRunFunc() typedRunFunc[M] {
	return func(ctx context.Context, p *typedRunParams[M]) {
		maxIter := a.config.MaxIterations
		if maxIter <= 0 { maxIter = 10 }

		var state *TypedChatModelAgentState[M]
		if p.interruptState != nil { state = p.interruptState
		} else { state = NewChatModelAgentState(p.input.Messages, a.exeCtx.toolInfos, maxIter) }

		// Apply history modifier for resume
		if p.historyModifier != nil && len(state.Messages) > 0 {
			switch any(state.Messages[0]).(type) {
			case *schema.Message:
				msgs := make([]Message, len(state.Messages))
				for i, m := range state.Messages { msgs[i] = any(m).(Message) }
				modified := p.historyModifier(ctx, msgs)
				state.Messages = make([]M, len(modified))
				for i, m := range modified { state.Messages[i] = any(m).(M) }
			}
		}

		// BeforeAgent middlewares
		rc := &ChatModelAgentContext{Instruction: a.exeCtx.instruction, Tools: a.config.Tools, ReturnDirectly: a.exeCtx.returnDirectly, ToolSearchTool: a.exeCtx.toolSearchTool}
		for _, mw := range a.config.Middlewares {
			if mw == nil { continue }
			var err error
			ctx, rc, err = mw.BeforeAgent(ctx, rc)
			if err != nil {
				p.generator.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("BeforeAgent: %w", err)})
				return
			}
		}

		model := BuildModelWrapperChain(a.config.Model, nil, a.config)

		var tn *ToolsNode[M]
		if a.config.ToolsConfig != nil { tn = NewToolsNode[M](a.config.ToolsConfig) }

		for state.RemainingIterations > 0 {
			state.RemainingIterations--

			mc := &TypedModelContext[M]{Tools: state.ToolInfos, DeferredToolInfos: state.DeferredToolInfos, ModelRetryConfig: a.config.RetryConfig, ModelFailoverConfig: a.config.FailoverConfig}
			for _, mw := range a.config.Middlewares {
				if mw == nil { continue }
				var err error
				ctx, state, err = mw.BeforeModelRewrite(ctx, state, mc)
				if err != nil {
					p.generator.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("BeforeModelRewrite: %w", err)})
					return
				}
			}

			var modelMsgs []M
		if a.config.GenModelInput != nil {
			var err error
			modelMsgs, err = a.config.GenModelInput(ctx, rc.Instruction, &TypedAgentInput[M]{Messages: state.Messages})
			if err != nil {
				p.generator.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("GenModelInput: %w", err)})
				return
			}
		} else { modelMsgs = buildModelInputFromState(state.Messages, rc.Instruction) }

			resp, err := model.Generate(ctx, modelMsgs)
			if err != nil { p.generator.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("model: %w", err)}); return }
			p.generator.Send(typedModelOutputEvent(resp, nil))
			state.Messages = append(state.Messages, resp)

			for _, mw := range a.config.Middlewares {
				if mw == nil { continue }
				var err error
				ctx, state, err = mw.AfterModelRewrite(ctx, state, mc)
				if err != nil {
					p.generator.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("AfterModelRewrite: %w", err)})
					return
				}
			}

			toolCalls := extractToolCalls(resp)
			if len(toolCalls) == 0 { break }

			var action *AgentAction
			if tn != nil {
				results, act, err := tn.Execute(ctx, resp, state, nil)
				if err != nil { p.generator.Send(&TypedAgentEvent[M]{Err: err}); return }
				for _, tr := range results { state.Messages = append(state.Messages, tr) }
				action = act
			} else {
				action, _ = a.executeInlineTools(ctx, toolCalls, rc, state, p.generator)
			}
			if action != nil && action.Exit { break }
		}

		if state.RemainingIterations <= 0 {
			p.generator.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("exceeded max iterations (%d)", a.config.MaxIterations)})
			return
		}
		if a.config.OutputKey != "" && len(state.Messages) > 0 {
			if last := state.Messages[len(state.Messages)-1]; !isNilMessage(last) {
				setOutputToSession(ctx, last, a.config.OutputKey)
			}
		}
		if p.afterToolCallsHook != nil {
			if err := p.afterToolCallsHook(ctx); err != nil {
				p.generator.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("after_tool_calls_hook: %w", err)})
			}
		}
		for _, mw := range a.config.Middlewares {
			if mw == nil { continue }
			var err error
			ctx, err = mw.AfterAgent(ctx, state)
			if err != nil {
				p.generator.Send(&TypedAgentEvent[M]{Err: fmt.Errorf("AfterAgent: %w", err)})
				return
			}
		}
	}
}

// executeInlineTools is the fallback when no ToolsNode is configured.
func (a *TypedChatModelAgent[M]) executeInlineTools(
	ctx context.Context,
	toolCalls []schema.ToolCall,
	rc *ChatModelAgentContext,
	state *TypedChatModelAgentState[M],
	gen *AsyncGenerator[*TypedAgentEvent[M]],
) (*AgentAction, error) {

	var action *AgentAction
	for _, tc := range toolCalls {
		if rc.ReturnDirectly != nil && rc.ReturnDirectly[tc.Function.Name] { action = NewExitAction() }

		tool := findTool(rc.Tools, tc.Function.Name)
		if tool == nil {
			tr := schema.ToolMessage(fmt.Sprintf("tool '%s' not found", tc.Function.Name), tc.ID)
			state.Messages = append(state.Messages, any(tr).(M))
			if m, ok := any(tr).(*schema.Message); ok { gen.Send(any(typedEventFromMessage(m, nil, schema.RoleTool, tc.Function.Name)).(*TypedAgentEvent[M])) }
			continue
		}

		ep := func(ctx context.Context, args string, opts ...toolOption) (string, error) { return tool.Invoke(ctx, args) }
		tCtx := &ToolContext{Name: tc.Function.Name, CallID: tc.ID}
		for _, mw := range a.config.Middlewares {
			if mw == nil { continue }
			wrapped, err := mw.WrapToolInvoke(ctx, ep, tCtx)
			if err != nil { return action, err }
			ep = wrapped
		}
		result, err := ep(ctx, tc.Function.Arguments)
		var toolMsg M
		if err != nil { toolMsg = any(schema.ToolMessage(fmt.Sprintf("Error: %v", err), tc.ID)).(M)
		} else { toolMsg = any(schema.ToolMessage(result, tc.ID)).(M) }
		state.Messages = append(state.Messages, toolMsg)
		if m, ok := any(toolMsg).(*schema.Message); ok { gen.Send(any(typedEventFromMessage(m, nil, schema.RoleTool, tc.Function.Name)).(*TypedAgentEvent[M])) }
	}
	return action, nil
}

// ---- Helpers ----

func buildModelInputFromState[M MessageType](messages []M, instruction string) []M {
	var msgs []M
	if instruction != "" { msgs = append(msgs, any(schema.SystemMessage(instruction)).(M)) }
	for _, m := range messages { msgs = append(msgs, m) }
	return msgs
}

func setOutputToSession[M MessageType](ctx context.Context, msg M, key string) {
	if !isNilMessage(msg) {
		s := getSession(ctx)
		if s != nil { s.Values[key] = extractTextContent(msg) }
	}
}

func toolsToInfosTyped[M MessageType](tools []Tool) []*schema.ToolInfo {
	infos := make([]*schema.ToolInfo, 0, len(tools))
	for _, t := range tools { infos = append(infos, &schema.ToolInfo{Name: t.Name(), Description: t.Description()}) }
	return infos
}

func extractTextContent[M MessageType](msg M) string {
	switch v := any(msg).(type) {
	case *schema.Message: return v.Content
	case *schema.AgenticMessage:
		var texts []string
		for _, b := range v.ContentBlocks { if b.Type == "text" { texts = append(texts, b.Text) } }
		return strings.Join(texts, "\n")
	default: return ""
	}
}

// findTool finds a tool by name from a list of tools.
func findTool(tools []Tool, name string) Tool {
	for _, t := range tools {
		if t.Name() == name { return t }
	}
	return nil
}

// extractToolCalls extracts tool calls from a model response message.
// It handles both *schema.Message (with ToolCalls field) and generic types.
func extractToolCalls[M MessageType](resp M) []schema.ToolCall {
	switch v := any(resp).(type) {
	case *schema.Message:
		if len(v.ToolCalls) > 0 { return v.ToolCalls }
	case *schema.AgenticMessage:
		var tc []schema.ToolCall
		for _, b := range v.ContentBlocks {
			if b.Type == "tool_use" && b.ToolCall != nil && b.ToolCall.ID != "" && b.ToolCall.Name != "" {
				tc = append(tc, schema.ToolCall{
					ID: b.ToolCall.ID,
					Function: schema.ToolCallFunction{Name: b.ToolCall.Name, Arguments: b.ToolCall.Arguments},
				})
			}
		}
		return tc
	}
	return nil
}

// streamWithCancel wraps a streaming model call with cancel detection.
func streamWithCancel[M MessageType](s *schema.StreamReader[M], cc *cancelContext) *schema.StreamReader[M] {
	if cc == nil { return s }
	select {
	case <-cc.immediateChan:
		s.Close()
		r := schema.NewStreamReader[M]()
		var zero M
		r.Send(zero, ErrStreamCanceled)
		r.Close()
		return r
	default:
	}
	r := schema.NewStreamReader[M]()
	go func() {
		defer r.Close()
		defer s.Close()
		ch := make(chan struct{ Data M; Err error }, 64)
		go func() {
			defer close(ch)
			for {
				d, e := s.Recv()
				ch <- struct{ Data M; Err error }{d, e}
				if e != nil { return }
			}
		}()
		for {
			select {
			case <-cc.immediateChan:
				var z M
				r.Send(z, ErrStreamCanceled)
				return
			case v := <-ch:
				if v.Err != nil { return }
				r.Send(v.Data, nil)
			}
		}
	}()
	return r
}

// getChatModelExecCtx retrieves the chat model execution context from context.
func getChatModelExecCtx(ctx context.Context) *chatModelExecCtx {
	rc := getRunCtx(ctx)
	if rc == nil { return nil }
	// The exec ctx is stored on the run session or passed via context value
	if ec, ok := rc.Session.Values["__exec_ctx"].(*chatModelExecCtx); ok { return ec }
	return nil
}

// getTypedChatModelExecCtx retrieves the typed execution context from context.
func getTypedChatModelExecCtx[M MessageType](ctx context.Context) *chatModelExecCtx {
	return getChatModelExecCtx(ctx)
}
