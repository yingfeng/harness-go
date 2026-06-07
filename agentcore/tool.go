package agentcore

import (
	"context"
	"fmt"

	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

// AgentToolOptions configures an AgentTool.
type AgentToolOptions struct {
	FullChatHistoryAsInput bool
	EmitInternalEvents     bool // Forward inner agent's events to parent stream
}

// AgentToolOption configures the AgentTool.
type AgentToolOption func(*AgentToolOptions)

// WithFullChatHistoryAsInput uses the full chat history as input to the inner agent.
func WithFullChatHistoryAsInput() AgentToolOption {
	return func(o *AgentToolOptions) { o.FullChatHistoryAsInput = true }
}

// WithEmitInternalEvents enables forwarding internal events from the wrapped agent
// to the parent agent's event stream. This allows real-time streaming of nested
// agent output to the end user via Runner.
//
// Action Scoping:
//   - Interrupted actions are propagated via CompositeInterrupt for proper interrupt/resume
//   - Exit, TransferToAgent, BreakLoop actions are scoped to the agent tool boundary (ignored outside)
//
// Note: These forwarded events are NOT recorded in the parent agent's runSession.
// They are only emitted to the end-user and have no effect on the parent agent's state or checkpoint.
func WithEmitInternalEvents() AgentToolOption {
	return func(o *AgentToolOptions) { o.EmitInternalEvents = true }
}

// NewAgentTool wraps an Agent as a Tool for use by other agents.
// The agent must have non-empty Name and Description, used as the tool name/description.
//
// Action Scoping:
//   - Exit, TransferToAgent, BreakLoop actions from the inner agent are ignored outside the tool
//   - Interrupted actions are propagated via CompositeInterrupt for proper interrupt/resume
func NewAgentTool(ctx context.Context, agent Agent, options ...AgentToolOption) Tool {
	opts := &AgentToolOptions{}
	for _, o := range options { o(opts) }
	name := agent.Name(ctx)
	if name == "" { name = "agent_tool" }
	desc := agent.Description(ctx)
	return &agentTool{
		name: name, desc: desc, agent: agent,
		opts: opts, baseCtx: ctx,
	}
}

type agentTool struct {
	name    string
	desc    string
	agent   Agent
	opts    *AgentToolOptions
	baseCtx context.Context
}

func (t *agentTool) Name() string        { return t.name }
func (t *agentTool) Description() string  { return t.desc }

func (t *agentTool) Invoke(ctx context.Context, args string, opts ...ToolOption) (string, error) {
	// Merge parent context with base context for agent execution
	runCtx := context.Background()
	if t.baseCtx != nil { runCtx = t.baseCtx }
	_ = ctx

	runner := NewTypedRunner(RunnerConfig[*schema.Message]{Agent: t.agent})
	messages := []Message{schema.UserMessage(args)}

	iter := runner.Run(runCtx, messages)

	// If EmitInternalEvents is set, get the parent's exec ctx to forward events
	var parentEC *reActExecCtx
	if t.opts.EmitInternalEvents { parentEC = getChatModelExecCtx(runCtx) }

	var result string
	var interrupted bool
	for {
		ev, ok := iter.Next()
		if !ok { break }
		if ev.Err != nil { return "", fmt.Errorf("agent tool '%s': %w", t.name, ev.Err) }

		// EmitInternalEvents: forward events to parent stream
		if parentEC != nil && t.opts.EmitInternalEvents {
			parentEC.send(ev)
		}

		if ev.Action != nil && ev.Action.Interrupted != nil {
			interrupted = true
			result += fmt.Sprintf("[interrupted: %v]", ev.Action.Interrupted.Data)
			break
		}
		if ev.Action != nil && (ev.Action.Exit || ev.Action.TransferToAgent != nil || ev.Action.BreakLoop != nil) {
			// Scoped: these actions are for the inner agent only, not propagated
			continue
		}
		if ev.Output != nil && ev.Output.MessageOutput != nil {
			if !ev.Output.MessageOutput.IsStreaming && ev.Output.MessageOutput.Message != nil {
				msg := ev.Output.MessageOutput.Message
				if msg.Role == schema.RoleAssistant {
					result += msg.Content
				}
			}
		}
	}
	if interrupted {
		return result, fmt.Errorf("agent tool '%s' was interrupted", t.name)
	}
	return result, nil
}

func (t *agentTool) Stream(ctx context.Context, args string, opts ...ToolOption) (*schema.StreamReader[string], error) {
	r, err := t.Invoke(ctx, args, opts...)
	if err != nil { return nil, err }
	return schema.StreamReaderFromArray([]string{r}), nil
}
