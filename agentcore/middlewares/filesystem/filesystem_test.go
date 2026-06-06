package filesystem

import (
	"context"
	"strings"
	"testing"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

type testBackend struct {
	readResult string
	readErr    error
}

func (b *testBackend) Read(path string) (string, error)                     { return b.readResult, b.readErr }
func (b *testBackend) Write(path, content string) error                     { return nil }
func (b *testBackend) Edit(path, old, new string) error                     { return nil }
func (b *testBackend) Ls(path string) ([]string, error)                     { return nil, nil }
func (b *testBackend) Glob(pattern string) ([]string, error)                { return []string{"a.txt", "b.go"}, nil }
func (b *testBackend) Grep(pattern, path string) (string, error)            { return "match1\nmatch2", nil }
func (b *testBackend) Execute(command string) (string, error)               { return "done", nil }

func TestNew_NilBackend(t *testing.T) {
	mw := New[*schema.Message](nil)
	rc := &agentcore.ChatModelAgentContext{Instruction: "base", Tools: make([]agentcore.Tool, 0)}
	_, newRc, err := mw.BeforeAgent(context.Background(), rc)
	if err != nil { t.Fatalf("BeforeAgent: %v", err) }
	if len(newRc.Tools) != 0 {
		t.Error("nil backend should not add tools")
	}
}

func TestNew_AddsAllTools(t *testing.T) {
	mw := New[*schema.Message](&testBackend{readResult: "hello"})
	rc := &agentcore.ChatModelAgentContext{Instruction: "base", Tools: make([]agentcore.Tool, 0)}
	_, newRc, err := mw.BeforeAgent(context.Background(), rc)
	if err != nil { t.Fatalf("BeforeAgent: %v", err) }
	if len(newRc.Tools) != 7 {
		t.Errorf("expected 7 tools, got %d", len(newRc.Tools))
	}
}

func TestTool_Read_Function(t *testing.T) {
	mw := New[*schema.Message](&testBackend{readResult: "file content"})
	rc := &agentcore.ChatModelAgentContext{}
	_, newRc, _ := mw.BeforeAgent(context.Background(), rc)
	// Find read_file tool
	for _, tool := range newRc.Tools {
		if tool.Name() == "read_file" {
			result, err := tool.Invoke(context.Background(), "test.txt")
			if err != nil { t.Fatalf("read_file: %v", err) }
			if result != "file content" { t.Errorf("got %q", result) }
			return
		}
	}
	t.Error("read_file tool not found")
}

func TestTool_Write_Function(t *testing.T) {
	mw := New[*schema.Message](&testBackend{})
	rc := &agentcore.ChatModelAgentContext{}
	_, newRc, _ := mw.BeforeAgent(context.Background(), rc)
	for _, tool := range newRc.Tools {
		if tool.Name() == "write_file" {
			_, err := tool.Invoke(context.Background(), "test.txt|content")
			if err != nil { t.Fatalf("write_file: %v", err) }
			return
		}
	}
	t.Error("write_file tool not found")
}

func TestTool_Glob_Function(t *testing.T) {
	mw := New[*schema.Message](&testBackend{})
	rc := &agentcore.ChatModelAgentContext{}
	_, newRc, _ := mw.BeforeAgent(context.Background(), rc)
	for _, tool := range newRc.Tools {
		if tool.Name() == "glob" {
			result, err := tool.Invoke(context.Background(), "*.txt")
			if err != nil { t.Fatalf("glob: %v", err) }
			if !strings.Contains(result, "a.txt") { t.Errorf("expected a.txt in %q", result) }
			return
		}
	}
	t.Error("glob tool not found")
}
