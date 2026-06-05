// Package filesystem provides file system abstractions for agent file operations.
package filesystem

// FileInfo provides information about a file.
type FileInfo struct {
	Name    string `json:"name"`
	Size    int64  `json:"size"`
	IsDir   bool   `json:"is_dir"`
	ModTime string `json:"mod_time"`
}

// Backend defines the interface for file system operations.
type Backend interface {
	Read(path string) (string, error)
	Write(path, content string) error
	Edit(path, old, new string) error
	Glob(pattern string) ([]string, error)
	Grep(pattern, path string) (string, error)
	Stat(path string) (*FileInfo, error)
	Mkdir(path string) error
	Remove(path string) error
	List(dir string) ([]FileInfo, error)
}

// Shell defines an interface for shell execution.
type Shell interface {
	Execute(command string) (string, error)
	ExecuteStreaming(command string) (<-chan string, error)
}
