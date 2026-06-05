// Package filesystem provides file system tool middleware.
// It adds read/write/edit/search/execute capabilities as agent tools.
package filesystem

import (
	"context"
	"github.com/infiniflow/ragflow/agent/agentcore"
)

type Backend interface {
	Read(path string) (string, error)
	Write(path, content string) error
	Glob(pattern string) ([]string, error)
	Grep(pattern, path string) (string, error)
	Execute(command string) (string, error)
}

type Config struct{ Backend Backend }

type middleware[M agentcore.MessageType] struct {
	agentcore.BaseMiddleware[M]
	backend Backend
}

func New[M agentcore.MessageType](backend Backend) *middleware[M] {
	return &middleware[M]{backend: backend}
}

func (m *middleware[M]) BeforeAgent(ctx context.Context, rc *agentcore.ChatModelAgentContext) (context.Context, *agentcore.ChatModelAgentContext, error) {
	if m.backend != nil {
		rc.Tools = append(rc.Tools,
			newReadTool(m.backend), newWriteTool(m.backend),
			newGlobTool(m.backend), newGrepTool(m.backend), newExecTool(m.backend))
	}
	return ctx, rc, nil
}

func newReadTool(b Backend) agentcore.Tool {
	return agentcore.NewBaseTool("read_file", "Read file contents", func(ctx context.Context, args string) (string, error) { return b.Read(args) })
}
func newWriteTool(b Backend) agentcore.Tool {
	return agentcore.NewBaseTool("write_file", "Write content to a file", func(ctx context.Context, args string) (string, error) { return "", nil })
}
func newGlobTool(b Backend) agentcore.Tool {
	return agentcore.NewBaseTool("glob", "Find files matching a pattern", func(ctx context.Context, args string) (string, error) {
		results, err := b.Glob(args)
		if err != nil { return "", err }
		s := ""; for _, r := range results { s += r + "\n" }; return s, nil
	})
}
func newGrepTool(b Backend) agentcore.Tool {
	return agentcore.NewBaseTool("grep", "Search for text in files", func(ctx context.Context, args string) (string, error) { return b.Grep(args, ".") })
}
func newExecTool(b Backend) agentcore.Tool {
	return agentcore.NewBaseTool("execute", "Execute a shell command", func(ctx context.Context, args string) (string, error) { return b.Execute(args) })
}
