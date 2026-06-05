package deep

type TaskState string
const ( TaskPending TaskState = "pending"; TaskRunning TaskState = "running"; TaskCompleted TaskState = "completed"; TaskFailed TaskState = "failed" )

type Task struct {
	ID string `json:"id"`; Description string `json:"description"`; State TaskState `json:"state"`
	Result string `json:"result,omitempty"`; Error string `json:"error,omitempty"`
	Dependencies []string `json:"dependencies,omitempty"`
}
