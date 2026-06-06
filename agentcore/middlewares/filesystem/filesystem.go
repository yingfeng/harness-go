// Package filesystem provides a middleware that registers file system tools
// (read, write, edit, ls, glob, grep, execute) for agent use.
package filesystem

import (
	"context"
	"fmt"
	"strings"

	"github.com/infiniflow/ragflow/harness/agentcore"
)

// toolFunc adapts a backend method to the Tool invoke signature.
type toolFunc func(ctx context.Context, args string) (string, error)

func wrapBackend(fn func(string) (string, error)) toolFunc {
	return func(ctx context.Context, args string) (string, error) { return fn(args) }
}
func wrapBackendVoid(fn func(string) error) toolFunc {
	return func(ctx context.Context, args string) (string, error) { return "", fn(args) }
}

type Backend interface {
	Read(path string) (string, error)
	Write(path, content string) error
	Edit(path, old, new string) error
	Ls(path string) ([]string, error)
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
	if m.backend == nil { return ctx, rc, nil }
	rc.Tools = append(rc.Tools, m.newReadTool(), m.newWriteTool(), m.newEditTool(),
		m.newLsTool(), m.newGlobTool(), m.newGrepTool(), m.newExecTool())
	return ctx, rc, nil
}

func (m *middleware[M]) newReadTool() agentcore.Tool {
	return agentcore.NewBaseTool("read_file", "Read file contents. Args: path.",
		wrapBackend(m.backend.Read))
}
func (m *middleware[M]) newWriteTool() agentcore.Tool {
	return agentcore.NewBaseTool("write_file", "Write content to a file. Args: path|content.",
		func(ctx context.Context, args string) (string, error) {
			parts := strings.SplitN(args, "|", 2)
			if len(parts) < 2 { return "", fmt.Errorf("expected path|content") }
			return "", m.backend.Write(parts[0], parts[1])
		})
}
func (m *middleware[M]) newEditTool() agentcore.Tool {
	return agentcore.NewBaseTool("edit_file", "Edit file by replacing text. Args: path|old|new.",
		func(ctx context.Context, args string) (string, error) {
			parts := strings.SplitN(args, "|", 3)
			if len(parts) < 3 { return "", fmt.Errorf("expected path|old|new") }
			return "", m.backend.Edit(parts[0], parts[1], parts[2])
		})
}
func (m *middleware[M]) newLsTool() agentcore.Tool {
	return agentcore.NewBaseTool("ls", "List directory contents. Args: path.",
		func(ctx context.Context, args string) (string, error) {
			results, err := m.backend.Ls(args)
			if err != nil { return "", err }
			return strings.Join(results, "\n"), nil
		})
}
func (m *middleware[M]) newGlobTool() agentcore.Tool {
	return agentcore.NewBaseTool("glob", "Find files matching a glob pattern.",
		wrapBackend(func(path string) (string, error) {
			results, err := m.backend.Glob(path)
			if err != nil { return "", err }
			return strings.Join(results, "\n"), nil
		}))
}
func (m *middleware[M]) newGrepTool() agentcore.Tool {
	return agentcore.NewBaseTool("grep", "Search text in files. Args: pattern|path.",
		func(ctx context.Context, args string) (string, error) {
			parts := strings.SplitN(args, "|", 2)
			pattern, path := parts[0], "."
			if len(parts) > 1 { path = parts[1] }
			return m.backend.Grep(pattern, path)
		})
}
func (m *middleware[M]) newExecTool() agentcore.Tool {
	return agentcore.NewBaseTool("execute", "Execute a shell command.",
		wrapBackend(m.backend.Execute))
}
