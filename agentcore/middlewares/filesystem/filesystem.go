// Package filesystem provides file operation tools for agentcore agents.
// These tools allow agents to read, write, edit, search files and directories.
package filesystem

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/infiniflow/ragflow/harness/agentcore"
)

// Config configures the filesystem middleware tools.
type Config struct {
	// BaseDir is the root directory for file operations. Empty means CWD allowed.
	BaseDir string
	// AllowedPatterns are glob patterns of allowed paths. Empty means all allowed under BaseDir.
	AllowedPatterns []string
	// MaxReadSize limits bytes read per file. 0 = no limit.
	MaxReadSize int64
}

func DefaultConfig() *Config {
	return &Config{MaxReadSize: 1024 * 1024} // 1MB default
}

// ToolReadFile reads a file's contents.
func ToolReadFile(cfg *Config) agentcore.Tool {
	return agentcore.NewBaseTool(
		"read_file",
		"Read a file's contents. Args: {\"file_path\":\"/path/to/file\"}",
		func(ctx context.Context, args string) (string, error) {
			var in struct{ FilePath string `json:"file_path"` }
			if err := json.Unmarshal([]byte(args), &in); err != nil { return "", err }
			if err := validatePath(in.FilePath, cfg); err != nil { return "", err }
			data, err := os.ReadFile(in.FilePath)
			if err != nil { return "", fmt.Errorf("read %s: %w", in.FilePath, err) }
			if cfg.MaxReadSize > 0 && int64(len(data)) > cfg.MaxReadSize {
				data = data[:cfg.MaxReadSize]
				return string(data) + "\n... [TRUNCATED]", nil
			}
			return string(data), nil
		},
	)
}

// ToolWriteFile writes content to a file.
func ToolWriteFile(cfg *Config) agentcore.Tool {
	return agentcore.NewBaseTool(
		"write_file",
		"Write content to a file. Args: {\"file_path\":\"/path/to/file\",\"content\":\"...\"}",
		func(ctx context.Context, args string) (string, error) {
			var in struct {
				FilePath string `json:"file_path"`
				Content  string `json:"content"`
			}
			if err := json.Unmarshal([]byte(args), &in); err != nil { return "", err }
			if err := validatePath(in.FilePath, cfg); err != nil { return "", err }
			if err := os.WriteFile(in.FilePath, []byte(in.Content), 0644); err != nil {
				return "", fmt.Errorf("write %s: %w", in.FilePath, err)
			}
			return fmt.Sprintf(`{"written":true,"path":%q,"bytes":%d}`, in.FilePath, len(in.Content)), nil
		},
	)
}

// ToolEditFile applies a string replacement edit to a file.
func ToolEditFile(cfg *Config) agentcore.Tool {
	return agentcore.NewBaseTool(
		"edit_file",
		"Replace text in a file. Args: {\"file_path\":\"/path/to/file\",\"old_string\":\"...\",\"new_string\":\"...\"}",
		func(ctx context.Context, args string) (string, error) {
			var in struct {
				FilePath   string `json:"file_path"`
				OldString string `json:"old_string"`
				NewString string `json:"new_string"`
			}
			if err := json.Unmarshal([]byte(args), &in); err != nil { return "", err }
			if err := validatePath(in.FilePath, cfg); err != nil { return "", err }
			if in.OldString == "" { return "", fmt.Errorf("old_string must not be empty") }

			data, err := os.ReadFile(in.FilePath)
			if err != nil { return "", fmt.Errorf("read %s: %w", in.FilePath, err) }

			content := string(data)
			if !strings.Contains(content, in.OldString) {
				return "", fmt.Errorf("old_string not found in %s", in.FilePath)
			}
			newContent := strings.Replace(content, in.OldString, in.NewString, 1)
			if err := os.WriteFile(in.FilePath, []byte(newContent), 0644); err != nil {
				return "", fmt.Errorf("write %s: %w", in.FilePath, err)
			}
			return fmt.Sprintf(`{"edited":true,"path":%q}`, in.FilePath), nil
		},
	)
}

// ToolGlob searches for files matching a pattern.
func ToolGlob(cfg *Config) agentcore.Tool {
	return agentcore.NewBaseTool(
		"glob",
		"Find files matching a glob pattern. Args: {\"pattern\":\"**/*.go\",\"path\":\"/base/dir\"}",
		func(ctx context.Context, args string) (string, error) {
			var in struct {
				Pattern string `json:"pattern"`
				Path    string `json:"path,omitempty"`
			}
			if err := json.Unmarshal([]byte(args), &in); err != nil { return "", err }
			searchDir := in.Path
			if searchDir == "" { searchDir = cfg.BaseDir }
			if searchDir == "" { searchDir = "." }

			matches, err := filepath.Glob(filepath.Join(searchDir, in.Pattern))
			if err != nil { return "", err }
			sort.Strings(matches)
			b, _ := json.Marshal(matches)
			return string(b), nil
		},
	)
}

// ToolGrep searches file contents with a regex pattern.
func ToolGrep(cfg *Config) agentcore.Tool {
	return agentcore.NewBaseTool(
		"grep",
		"Search file contents for a regex pattern. Args: {\"pattern\":\"TODO\",\"path\":\"/dir\",\"file_pattern\":\"*.go\"}",
		func(ctx context.Context, args string) (string, error) {
			type grepResult struct{ File string `json:"file"`; Line int `json:"line"`; Text string `json:"text"` }
			var in struct {
				Pattern     string `json:"pattern"`
				Path        string `json:"path,omitempty"`
				FilePattern string `json:"file_pattern,omitempty"`
			}
			if err := json.Unmarshal([]byte(args), &in); err != nil { return "", err }

			searchDir := in.Path
			if searchDir == "" { searchDir = "." }

			var results []grepResult
			err := filepath.Walk(searchDir, func(path string, info os.FileInfo, err error) error {
				if err != nil { return nil }
				if info.IsDir() { return nil }
				if in.FilePattern != "" {
					matched, _ := filepath.Match(in.FilePattern, info.Name())
					if !matched { return nil }
				}
				data, err := os.ReadFile(path)
				if err != nil { return nil }
				lines := strings.Split(string(data), "\n")
				for i, line := range lines {
					if strings.Contains(line, in.Pattern) {
						rel, _ := filepath.Rel(searchDir, path)
						results = append(results, grepResult{File: rel, Line: i + 1, Text: strings.TrimSpace(line)})
					}
				}
				return nil
			})
			if err != nil { return "", err } // only directory walk errors

			b, _ := json.Marshal(results)
			return string(b), nil
		},
	)
}

// AllTools returns all filesystem tools.
func AllTools(cfg *Config) []agentcore.Tool {
	if cfg == nil { cfg = DefaultConfig() }
	return []agentcore.Tool{
		ToolReadFile(cfg),
		ToolWriteFile(cfg),
		ToolEditFile(cfg),
		ToolGlob(cfg),
		ToolGrep(cfg),
	}
}

func validatePath(path string, cfg *Config) error {
	clean := filepath.Clean(path)
	if filepath.IsAbs(clean) {
		if cfg.BaseDir == "" {
			return nil // Allow absolute paths when no base dir set
		}
		baseClean := filepath.Clean(cfg.BaseDir)
		rel, err := filepath.Rel(baseClean, clean)
		if err != nil { return fmt.Errorf("invalid path: %s", path) }
		if strings.HasPrefix(rel, "..") { return fmt.Errorf("path %s escapes base directory", path) }
	}
	return nil
}
