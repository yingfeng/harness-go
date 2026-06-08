package engine

import "fmt"

// MySQLConfig holds the database connection parameters.
type MySQLConfig struct {
	Host     string `yaml:"host" json:"host"`
	Port     int    `yaml:"port" json:"port"`
	User     string `yaml:"user" json:"user"`
	Password string `yaml:"password" json:"-"`
	Database string `yaml:"database" json:"database"`
}

// DSN builds a MySQL DSN string from the config.
func (m *MySQLConfig) DSN() string {
	port := m.Port
	if port == 0 {
		port = 3306
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", m.User, m.Password, m.Host, port, m.Database)
}

// ModelConfig defines a single LLM model entry from config.yaml.
type ModelConfig struct {
	Name           string  `yaml:"name" json:"name"`
	DisplayName    string  `yaml:"display_name" json:"displayName"`
	Model          string  `yaml:"model" json:"model"`
	APIBase        string  `yaml:"api_base" json:"apiBase"`
	APIKey         string  `yaml:"api_key" json:"apiKey"`
	MaxTokens      int     `yaml:"max_tokens" json:"maxTokens"`
	Temperature    float64 `yaml:"temperature" json:"temperature"`
	TimeoutSeconds int     `yaml:"timeout_seconds" json:"timeoutSeconds"`
}

// AppConfig is the top-level config file structure.
type AppConfig struct {
	MySQL  *MySQLConfig  `yaml:"mysql,omitempty" json:"mysql,omitempty"`
	Models []ModelConfig `yaml:"models" json:"models"`
}

// WorkflowNode is the visual node definition from the frontend.
type WorkflowNode struct {
	ID       string            `json:"id"`
	Type     string            `json:"type"`
	Position map[string]float64 `json:"position"`
	Data     NodeData          `json:"data"`
}

// NodeData holds the per-node configuration.
type NodeData struct {
	Label         string  `json:"label"`
	NodeName      string  `json:"nodeName"`
	ModelName     string  `json:"modelName,omitempty"`
	Instructions  string  `json:"instructions,omitempty"`
	SystemPrompt  string  `json:"systemPrompt,omitempty"`
	Temperature   float64 `json:"temperature,omitempty"`
	Condition     string  `json:"condition,omitempty"`
	TransformCode string  `json:"transformCode,omitempty"`
}

// WorkflowEdge is the visual edge definition.
type WorkflowEdge struct {
	ID           string `json:"id"`
	Source       string `json:"source"`
	Target       string `json:"target"`
	Label        string `json:"label,omitempty"`
	SourceHandle string `json:"sourceHandle,omitempty"`
}

// WorkflowDef is the complete workflow definition from the frontend.
type WorkflowDef struct {
	ID    string         `json:"id"`
	Name  string         `json:"name"`
	Nodes []WorkflowNode `json:"nodes"`
	Edges []WorkflowEdge `json:"edges"`
}

// SSEEvent types matching the frontend expectations.
type SSEEvent struct {
	Event string      `json:"event"`
	Data  interface{} `json:"data"`
}

// NodeStatus represents the execution status of a single node.
type NodeStatus struct {
	NodeID string `json:"nodeId"`
	Status string `json:"status"`
	Output string `json:"output,omitempty"`
	Error  string `json:"error,omitempty"`
}

// ExecutionResult holds the full result of a workflow execution.
type ExecutionResult struct {
	ExecutionID string                `json:"executionId"`
	Status      string                `json:"status"`
	CurrentNode string                `json:"currentNode"`
	NodeResults map[string]*NodeStatus `json:"nodeResults"`
	Variables   map[string]interface{} `json:"variables"`
	Error       string                `json:"error,omitempty"`
}
