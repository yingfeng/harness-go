package agentcore

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"

	"github.com/infiniflow/ragflow/agent/agentcore/schema"
)

// ---- Resume types ----

type ResumeInfo struct {
	EnableStreaming bool
	*InterruptInfo
	WasInterrupted bool
	InterruptState any
	IsResumeTarget bool
	ResumeData     any
}

type InterruptInfo struct {
	Data              any
	InterruptContexts []*InterruptCtx
}

// ---- Address types ----

type Address = []AddressSegment

type AddressSegment struct {
	Type AddressSegmentType
	ID   string
}

type AddressSegmentType string

const (
	AddressSegmentAgent AddressSegmentType = "agent"
	AddressSegmentTool  AddressSegmentType = "tool"
)

var allowedAddrSegTypes = []AddressSegmentType{AddressSegmentAgent, AddressSegmentTool}

type InterruptCtx struct {
	ID      string
	Address Address
	Info    any
	State   any
}

type InterruptSignal struct {
	ID       string
	Address  Address
	Info     any
	State    any
	Children []*InterruptSignal
}

// ---- Interrupt constructors ----

func Interrupt(ctx context.Context, info any) *AgentEvent {
	return TypedInterrupt[*schema.Message](ctx, info)
}

func TypedInterrupt[M MessageType](ctx context.Context, info any) *TypedAgentEvent[M] {
	return &TypedAgentEvent[M]{Action: &AgentAction{Interrupted: &InterruptInfo{Data: info}}}
}

func StatefulInterrupt(ctx context.Context, info, state any) *AgentEvent {
	return TypedStatefulInterrupt[*schema.Message](ctx, info, state)
}

func TypedStatefulInterrupt[M MessageType](ctx context.Context, info, state any) *TypedAgentEvent[M] {
	return &TypedAgentEvent[M]{Action: &AgentAction{
		Interrupted: &InterruptInfo{Data: info},
		internalInterrupted: &InterruptSignal{Info: info, State: state},
	}}
}

func CompositeInterrupt(ctx context.Context, info, state any, subs ...*InterruptSignal) *AgentEvent {
	return TypedCompositeInterrupt[*schema.Message](ctx, info, state, subs...)
}

func TypedCompositeInterrupt[M MessageType](ctx context.Context, info, state any, subs ...*InterruptSignal) *TypedAgentEvent[M] {
	children := make([]*InterruptSignal, len(subs))
	copy(children, subs)
	return &TypedAgentEvent[M]{Action: &AgentAction{
		Interrupted: &InterruptInfo{Data: info},
		internalInterrupted: &InterruptSignal{Info: info, State: state, Children: children},
	}}
}

func AppendAddressSegment(ctx context.Context, t AddressSegmentType, id string) context.Context { return ctx }
func FromInterruptContexts(ctxs []*InterruptCtx) *InterruptSignal { return &InterruptSignal{} }

// ---- Checkpoint store ----

type CheckPointStore interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, data []byte) error
}

type InterruptState struct{ State any }

type checkpointPayload struct {
	RunCtx              *runContext
	Info                *InterruptInfo
	EnableStreaming     bool
	InterruptID2Address map[string]Address
	InterruptID2State   map[string]InterruptState
}

func init() {
	schema.RegisterType("agentcore_checkpoint", func() any { return &checkpointPayload{} })
	schema.RegisterType("agentcore_interrupt_state", func() any { return &InterruptState{} })
}

func loadCheckpoint(store CheckPointStore, ctx context.Context, cid string) (context.Context, *runContext, *ResumeInfo, error) {
	data, exist, err := store.Get(ctx, cid)
	if err != nil { return nil, nil, nil, fmt.Errorf("checkpoint get: %w", err) }
	if !exist { return nil, nil, nil, fmt.Errorf("checkpoint %s not found", cid) }
	var p checkpointPayload
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&p); err != nil {
		return nil, nil, nil, fmt.Errorf("decode checkpoint: %w", err)
	}
	return ctx, p.RunCtx, &ResumeInfo{EnableStreaming: p.EnableStreaming, InterruptInfo: p.Info}, nil
}

func saveCheckpoint(store CheckPointStore, ctx context.Context, key string, enableStreaming bool, info *InterruptInfo, is *InterruptSignal) error {
	if store == nil { return nil }
	rc := getRunCtx(ctx)
	id2addr, id2state := signalToMaps(is)
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(checkpointPayload{
		RunCtx: rc, Info: info, EnableStreaming: enableStreaming,
		InterruptID2Address: id2addr, InterruptID2State: id2state,
	}); err != nil {
		return fmt.Errorf("encode checkpoint: %w", err)
	}
	return store.Set(ctx, key, buf.Bytes())
}

func signalToMaps(is *InterruptSignal) (map[string]Address, map[string]InterruptState) {
	a, s := make(map[string]Address), make(map[string]InterruptState)
	if is == nil { return a, s }
	if is.ID != "" {
		a[is.ID] = is.Address
		if is.State != nil { s[is.ID] = InterruptState{State: is.State} }
	}
	for _, c := range is.Children {
		ca, cs := signalToMaps(c)
		for k, v := range ca { a[k] = v }
		for k, v := range cs { s[k] = v }
	}
	return a, s
}

func getNextResumeAgent(ctx context.Context, info *ResumeInfo) (string, error) {
	return "", errors.New("no child agents leading to interrupted agent found")
}

func getNextResumeAgents(ctx context.Context, info *ResumeInfo) (map[string]bool, error) {
	return nil, errors.New("no child agents leading to interrupted agent found")
}

func buildResumeInfo(ctx context.Context, nextID string, info *ResumeInfo) (context.Context, *ResumeInfo) {
	ctx = AppendAddressSegment(ctx, AddressSegmentAgent, nextID)
	ri := &ResumeInfo{EnableStreaming: info.EnableStreaming, InterruptInfo: info.InterruptInfo}
	ri.WasInterrupted = info.WasInterrupted
	if info.WasInterrupted { ri.IsResumeTarget = info.IsResumeTarget; ri.ResumeData = info.ResumeData }
	ctx = updateRunPathOnly(ctx, nextID)
	return ctx, ri
}
