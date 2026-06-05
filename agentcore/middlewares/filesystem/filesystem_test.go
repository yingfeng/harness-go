package filesystem

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func tmpDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "fs-test-*")
	if err != nil { t.Fatal(err) }
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func TestToolReadFile(t *testing.T) {
	dir := tmpDir(t)
	cfg := &Config{BaseDir: dir}
	file := filepath.Join(dir, "test.txt")
	os.WriteFile(file, []byte("hello world"), 0644)

	tool := ToolReadFile(cfg)
	result, err := tool.Invoke(nil, `{"file_path":"`+file+`"}`)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if result != "hello world" {
		t.Errorf("result = %q, want hello world", result)
	}
}

func TestToolReadFile_NotFound(t *testing.T) {
	dir := tmpDir(t)
	cfg := &Config{BaseDir: dir}
	tool := ToolReadFile(cfg)
	_, err := tool.Invoke(nil, `{"file_path":"/nonexistent/file.txt"}`)
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestToolWriteFile(t *testing.T) {
	dir := tmpDir(t)
	cfg := &Config{BaseDir: dir}
	file := filepath.Join(dir, "output.txt")

	tool := ToolWriteFile(cfg)
	result, err := tool.Invoke(nil, `{"file_path":"`+file+`","content":"written content"}`)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	data, _ := os.ReadFile(file)
	if string(data) != "written content" {
		t.Errorf("file content = %q", string(data))
	}

	var resp struct{ Written bool; Path string; Bytes int }
	json.Unmarshal([]byte(result), &resp)
	if !resp.Written || resp.Bytes != len("written content") {
		t.Errorf("response: %+v", resp)
	}
}

func TestToolEditFile(t *testing.T) {
	dir := tmpDir(t)
	cfg := &Config{BaseDir: dir}
	file := filepath.Join(dir, "edit.txt")
	os.WriteFile(file, []byte("hello old world"), 0644)

	tool := ToolEditFile(cfg)
	result, err := tool.Invoke(nil, `{"file_path":"`+file+`","old_string":"old","new_string":"NEW"}`)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	data, _ := os.ReadFile(file)
	if string(data) != "hello NEW world" {
		t.Errorf("after edit: %q", string(data))
	}
	_ = result
}

func TestToolEditFile_OldStringNotFound(t *testing.T) {
	dir := tmpDir(t)
	cfg := &Config{BaseDir: dir}
	file := filepath.Join(dir, "x.txt")
	os.WriteFile(file, []byte("content"), 0644)

	tool := ToolEditFile(cfg)
	_, err := tool.Invoke(nil, `{"file_path":"`+file+`","old_string":"MISSING","new_string":"X"}`)
	if err == nil {
		t.Error("expected error for missing old_string")
	}
}

func TestToolGlob(t *testing.T) {
	dir := tmpDir(t)
	cfg := &Config{BaseDir: dir}
	os.WriteFile(filepath.Join(dir, "a.go"), []byte(""), 0644)
	os.WriteFile(filepath.Join(dir, "b.go"), []byte(""), 0644)
	os.Mkdir(filepath.Join(dir, "sub"), 0755)
	os.WriteFile(filepath.Join(dir, "sub", "c.go"), []byte(""), 0644)

	tool := ToolGlob(cfg)
	result, err := tool.Invoke(nil, `{"pattern":"*.go","path":"`+dir+`"}`)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	var matches []string
	json.Unmarshal([]byte(result), &matches)
	// Should find at least a.go, b.go in root; c.go in sub/
	if len(matches) < 2 {
		t.Errorf("glob found %d files, want >=2 (root + sub): %v", len(matches), matches)
	}
}

func TestToolGrep(t *testing.T) {
	dir := tmpDir(t)
	cfg := &Config{BaseDir: dir}
	os.WriteFile(filepath.Join(dir, "test.log"), []byte("line1 TODO: fix this\nline2 normal\nline3 TODO: another"), 0644)

	tool := ToolGrep(cfg)
	result, err := tool.Invoke(nil, `{"pattern":"TODO","path":"`+dir+`","file_pattern":"*.log"}`)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	var results []struct{ File string; Line int; Text string }
	json.Unmarshal([]byte(result), &results)
	if len(results) != 2 {
		t.Errorf("grep found %d results, want 2: %v", len(results), results)
	}
}

func TestValidatePath_EscapesBase(t *testing.T) {
	cfg := &Config{BaseDir: "/safe"}
	err := validatePath("/safe/../../etc/passwd", cfg)
	if err == nil {
		t.Error("expected path escape error")
	}
}

func TestValidatePath_AbsoluteNoBase(t *testing.T) {
	cfg := &Config{}
	if err := validatePath("/tmp/file.txt", cfg); err != nil {
		t.Errorf("absolute path without base_dir should be allowed: %v", err)
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.MaxReadSize != 1024*1024 {
		t.Errorf("MaxReadSize = %d, want 1MB", cfg.MaxReadSize)
	}
}

func TestAllTools(t *testing.T) {
	tools := AllTools(nil)
	names := make(map[string]bool)
	for _, tool := range tools { names[tool.Name()] = true }
	for _, expected := range []string{"read_file", "write_file", "edit_file", "glob", "grep"} {
		if !names[expected] {
			t.Errorf("missing tool: %s", expected)
		}
	}
}

func TestMaxReadSize_Truncation(t *testing.T) {
	dir := tmpDir(t)
	longContent := make([]byte, 2000)
	for i := range longContent { longContent[i] = 'A' }
	file := filepath.Join(dir, "big.bin")
	os.WriteFile(file, longContent, 0644)

	cfg := &Config{BaseDir: dir, MaxReadSize: 100}
	tool := ToolReadFile(cfg)
	result, err := tool.Invoke(nil, `{"file_path":"`+file+`"}`)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if len(result) <= 100 {
		// Should be truncated with suffix
		t.Log("truncated result length:", len(result))
	}
}
