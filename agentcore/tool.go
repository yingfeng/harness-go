package agentcore

import (
	"context"
	"github.com/infiniflow/ragflow/agent/agentcore/schema"
)

// NewAgentTool wraps an Agent as a Tool for use by other agents.
func NewAgentTool(ctx context.Context, agent Agent, name, desc string) Tool {
	return &agentTool{name: name, desc: desc, agent: agent, ctx: ctx}
}

type agentTool struct {
	name  string
	desc  string
	agent Agent
	ctx   context.Context
}

func (t *agentTool) Name() string                                            { return t.name }
func (t *agentTool) Description() string                                      { return t.desc }
func (t *agentTool) Invoke(ctx context.Context, args string, opts ...toolOption) (string, error) {
	r := NewRunner(t.ctx, RunnerConfig[*schema.Message]{Agent: t.agent})
	input := schema.UserMessage(args)
	var result string
	iter := r.Run(ctx, []Message{input}, nil...)
	for { ev, ok := iter.Next(); if !ok { break }
		if ev.Err != nil { return "", ev.Err }
		if ev.Output != nil && ev.Output.MessageOutput != nil && !ev.Output.MessageOutput.IsStreaming {
			if m := ev.Output.MessageOutput.Message; m != nil { result += m.Content }
		}
	}
	return result, nil
}
func (t *agentTool) Stream(ctx context.Context, args string, opts ...toolOption) (*schema.StreamReader[string], error) {
	r, err := t.Invoke(ctx, args, opts...)
	if err != nil { return nil, err }
	return schema.StreamReaderFromArray([]string{r}), nil
}
