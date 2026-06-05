package agentcore

import "sync"

// AsyncIterator provides blocking iteration over a typed stream.
type AsyncIterator[T any] struct {
	gen   *AsyncGenerator[T]
	index int
	mu    sync.Mutex
}

func (it *AsyncIterator[T]) Next() (T, bool) {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.gen == nil || it.index >= len(it.gen.items) {
		var zero T
		return zero, false
	}
	item := it.gen.items[it.index]
	it.index++
	return item, true
}

func (it *AsyncIterator[T]) Close() {
	if it.gen != nil {
		it.gen.Close()
	}
}

// AsyncGenerator produces items for an AsyncIterator.
type AsyncGenerator[T any] struct {
	items  []T
	mu     sync.Mutex
	closed bool
}

func NewAsyncIteratorPair[T any]() (*AsyncIterator[T], *AsyncGenerator[T]) {
	gen := &AsyncGenerator[T]{items: make([]T, 0)}
	return &AsyncIterator[T]{gen: gen}, gen
}

func (g *AsyncGenerator[T]) Send(item T) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.closed {
		g.items = append(g.items, item)
	}
}

func (g *AsyncGenerator[T]) trySend(item T) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return false
	}
	g.items = append(g.items, item)
	return true
}

func (g *AsyncGenerator[T]) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
}

func (g *AsyncGenerator[T]) IsClosed() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closed
}

// ===== Copy helpers =====

func copyTypedAgentEvent[M MessageType](event *TypedAgentEvent[M]) *TypedAgentEvent[M] {
	if event == nil {
		return nil
	}
	cp := &TypedAgentEvent[M]{
		AgentName: event.AgentName,
		Err:       event.Err,
	}
	if event.RunPath != nil {
		cp.RunPath = make([]RunStep, len(event.RunPath))
		for i, s := range event.RunPath {
			cp.RunPath[i] = RunStep{agentName: s.agentName}
		}
	}
	if event.Output != nil {
		cp.Output = &TypedAgentOutput[M]{CustomizedOutput: event.Output.CustomizedOutput}
		if event.Output.MessageOutput != nil {
			cp.Output.MessageOutput = &TypedMessageVariant[M]{
				IsStreaming: event.Output.MessageOutput.IsStreaming,
				Message:     event.Output.MessageOutput.Message,
				Role:        event.Output.MessageOutput.Role,
				AgenticRole: event.Output.MessageOutput.AgenticRole,
				ToolName:    event.Output.MessageOutput.ToolName,
			}
		}
	}
	if event.Action != nil {
		cp.Action = &AgentAction{
			Exit: event.Action.Exit, Interrupted: event.Action.Interrupted,
			TransferToAgent: event.Action.TransferToAgent, BreakLoop: event.Action.BreakLoop,
			CustomizedAction: event.Action.CustomizedAction,
			internalInterrupted: event.Action.internalInterrupted,
		}
	}
	return cp
}

func setAutomaticClose[M MessageType](event *TypedAgentEvent[M]) {}
func typedSetAutomaticClose[M MessageType](event *TypedAgentEvent[M]) { setAutomaticClose(event) }
func addTypedEvent[M MessageType](s *runSession, event *TypedAgentEvent[M]) {
	if s != nil {
		s.addEvent(event)
	}
}

func copyMap[K comparable, V any](src map[K]V) map[K]V {
	if src == nil {
		return nil
	}
	dst := make(map[K]V, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func cloneSlice[T any](src []T) []T {
	if src == nil {
		return nil
	}
	dst := make([]T, len(src))
	copy(dst, src)
	return dst
}
