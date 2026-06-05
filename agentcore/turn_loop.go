package agentcore

import (
	"context"
	"sort"
	"sync"
)

// TurnLoop implements a priority-based task scheduler for multi-turn execution.
type TurnLoop struct {
	mu    sync.Mutex
	tasks []turnTask
}

type turnTask struct {
	name     string
	priority int
	action   func(context.Context) error
}

func NewTurnLoop() *TurnLoop { return &TurnLoop{} }

func (tl *TurnLoop) Add(name string, priority int, action func(context.Context) error) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	tl.tasks = append(tl.tasks, turnTask{name: name, priority: priority, action: action})
}

func (tl *TurnLoop) Run(ctx context.Context) error {
	tl.mu.Lock()
	tasks := make([]turnTask, len(tl.tasks))
	copy(tasks, tl.tasks)
	tl.tasks = nil
	tl.mu.Unlock()

	sort.Slice(tasks, func(i, j int) bool { return tasks[i].priority > tasks[j].priority })
	for _, t := range tasks {
		select {
		case <-ctx.Done(): return ctx.Err()
		default:
			if err := t.action(ctx); err != nil { return err }
		}
	}
	return nil
}
