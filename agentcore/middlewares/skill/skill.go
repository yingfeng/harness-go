// Package skill provides skill loading and execution middleware.
// Skills are pre-defined capabilities that can be loaded and used by agents.
package skill

import (
	"context"
	"fmt"

	"github.com/infiniflow/ragflow/harness/agentcore"
)

type ExecMode int

const (
	ModeInline ExecMode = iota
	ModeFork
	ModeForkWithContext
)

type Config struct {
	Name          string
	Content       string
	ExecutionMode ExecMode
}

type middleware[M agentcore.MessageType] struct {
	agentcore.BaseMiddleware[M]
	skills     []Config
	filesystem FileSystemBackend
}

type FileSystemBackend interface {
	Read(path string) (string, error)
}

func New[M agentcore.MessageType](skills ...Config) agentcore.TypedChatModelMiddleware[M] {
	return &middleware[M]{skills: skills}
}

func NewWithBackend[M agentcore.MessageType](fs FileSystemBackend, skills ...Config) agentcore.TypedChatModelMiddleware[M] {
	return &middleware[M]{skills: skills, filesystem: fs}
}

func (m *middleware[M]) BeforeAgent(ctx context.Context, rc *agentcore.ChatModelAgentContext) (context.Context, *agentcore.ChatModelAgentContext, error) {
	for _, s := range m.skills {
		content := s.Content
		if content == "" && m.filesystem != nil {
			if c, err := m.filesystem.Read("SKILL.md"); err == nil {
				content = c
			}
		}
		if content == "" { continue }

		switch s.ExecutionMode {
		case ModeInline:
			rc.Instruction = rc.Instruction + "\n\n## Skill: " + s.Name + "\n" + truncate(content, 4000)
		case ModeFork, ModeForkWithContext:
			// For fork modes, add a tool that loads the skill on demand
			rc.Tools = append(rc.Tools, m.newSkillTool(s.Name, content))
		}
	}
	return ctx, rc, nil
}

func (m *middleware[M]) newSkillTool(name, content string) agentcore.Tool {
	return agentcore.NewBaseTool("load_skill_"+name,
		fmt.Sprintf("Load and execute the '%s' skill. Args: parameters for the skill.", name),
		func(ctx context.Context, args string) (string, error) {
			return fmt.Sprintf("### Skill: %s\n\n%s\n\n---\nResults: %s", name, truncate(content, 2000), args), nil
		})
}

func truncate(s string, n int) string {
	if len(s) <= n { return s }
	return s[:n] + "\n...(truncated)"
}
