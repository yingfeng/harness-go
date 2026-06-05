package deep

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/infiniflow/ragflow/harness/agentcore"
)

type TaskManager struct{ tasks []*Task }

func NewTaskManager() *TaskManager { return &TaskManager{} }
func (m *TaskManager) Create(desc string, deps ...string) *Task {
	t := &Task{ID: fmt.Sprintf("task_%d", len(m.tasks)+1), Description: desc, State: TaskPending, Dependencies: deps}
	m.tasks = append(m.tasks, t); return t
}
func (m *TaskManager) List() []*Task { return m.tasks }

func TaskCreateTool(m *TaskManager) agentcore.Tool {
	return agentcore.NewBaseTool("create_task", "Create a new sub-task", func(ctx context.Context, args string) (string, error) {
		var in struct{ Description string; Dependencies []string }
		if err := json.Unmarshal([]byte(args), &in); err != nil { return "", err }
		b, _ := json.Marshal(m.Create(in.Description, in.Dependencies...))
		return string(b), nil
	})
}
func TaskListTool(m *TaskManager) agentcore.Tool {
	return agentcore.NewBaseTool("list_tasks", "List all sub-tasks", func(ctx context.Context, args string) (string, error) {
		b, _ := json.Marshal(m.List()); return string(b), nil
	})
}
