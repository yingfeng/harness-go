// Package server provides a Agent Harness API server compatible with agent-harness-sdk.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/infiniflow/ragflow/harness/checkpoint"
	"github.com/infiniflow/ragflow/harness/graph"
	"github.com/infiniflow/ragflow/harness/types"
)

// Server implements the Agent Harness API server compatible with agent-harness-sdk.
type Server struct {
	mu          sync.RWMutex
	router      *http.ServeMux
	graphs      map[string]*graph.CompiledGraph
	assistants  map[string]*Assistant
	threads     map[string]*Thread
	runs        map[string]*Run
	checkpoints map[string]*checkpoint.Checkpoint
	store       Store
	config      *ServerConfig
}

// ServerConfig configures the Agent Harness server.
type ServerConfig struct {
	Host string
	Port int
	// AuthToken is optional API key for authentication
	AuthToken string
	// Store for persistent storage
	Store Store
}

// Store defines the interface for persistent storage.
type Store interface {
	Get(ctx context.Context, namespace []string, key string) (Item, error)
	Put(ctx context.Context, namespace []string, key string, value Item) error
	Delete(ctx context.Context, namespace []string, key string) error
	Search(ctx context.Context, namespace []string, query string, limit int) ([]Item, error)
	ListNamespaces(ctx context.Context, prefix []string) ([][]string, error)
}

// Item represents a stored item.
type Item struct {
	Key       string                 `json:"key"`
	Value     map[string]interface{} `json:"value"`
	Namespace []string               `json:"namespace"`
	CreatedAt time.Time              `json:"created_at"`
	UpdatedAt time.Time              `json:"updated_at"`
}

// Assistant represents a configured graph instance.
type Assistant struct {
	AssistantID  string                 `json:"assistant_id"`
	GraphID      string                 `json:"graph_id"`
	Name         string                 `json:"name,omitempty"`
	Config       map[string]interface{} `json:"config,omitempty"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt    time.Time              `json:"created_at"`
	UpdatedAt    time.Time              `json:"updated_at"`
	Version      int                    `json:"version"`
}

// Thread represents a conversation thread.
type Thread struct {
	ThreadID  string                 `json:"thread_id"`
	Status    string                 `json:"status"`
	Config    map[string]interface{} `json:"config,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt time.Time              `json:"created_at"`
	UpdatedAt time.Time              `json:"updated_at"`
}

// Run represents a graph execution run.
type Run struct {
	RunID        string                 `json:"run_id"`
	ThreadID     string                 `json:"thread_id,omitempty"`
	AssistantID  string                 `json:"assistant_id"`
	Status       string                 `json:"status"`
	Input        interface{}            `json:"input,omitempty"`
	Output       interface{}            `json:"output,omitempty"`
	Config       map[string]interface{} `json:"config,omitempty"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt    time.Time              `json:"created_at"`
	UpdatedAt    time.Time              `json:"updated_at"`
	StreamMode   []string               `json:"stream_mode,omitempty"`
}

// NewServer creates a new Agent Harness API server.
func NewServer(config *ServerConfig) *Server {
	if config == nil {
		config = &ServerConfig{
			Host: "0.0.0.0",
			Port: 8123,
		}
	}

	s := &Server{
		router:      http.NewServeMux(),
		graphs:      make(map[string]*graph.CompiledGraph),
		assistants:  make(map[string]*Assistant),
		threads:     make(map[string]*Thread),
		runs:        make(map[string]*Run),
		checkpoints: make(map[string]*checkpoint.Checkpoint),
		store:       config.Store,
		config:      config,
	}

	s.setupRoutes()
	return s
}

// RegisterGraph registers a compiled graph with the server.
func (s *Server) RegisterGraph(graphID string, g *graph.CompiledGraph) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.graphs[graphID] = g
}

// setupRoutes is no longer needed as we use direct routing in ServeHTTP
func (s *Server) setupRoutes() {
	// Routes are handled directly in ServeHTTP
}

// handleRequest is the main request handler that routes to specific handlers.
func (s *Server) handleRequest(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	
	// Remove trailing slash for consistent routing
	path = strings.TrimSuffix(path, "/")
	
	// Route based on path prefix
	switch {
	case path == "/health":
		s.handleHealth(w, r)
	case strings.HasPrefix(path, "/assistants"):
		s.handleAssistantsAPI(w, r, path)
	case strings.HasPrefix(path, "/threads"):
		s.handleThreadsAPI(w, r, path)
	case strings.HasPrefix(path, "/runs"):
		s.handleRunsAPI(w, r, path)
	case strings.HasPrefix(path, "/store"):
		s.handleStoreAPI(w, r, path)
	default:
		s.writeError(w, http.StatusNotFound, "Not found")
	}
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// CORS headers
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Authentication
	if s.config.AuthToken != "" {
		apiKey := r.Header.Get("X-API-Key")
		if apiKey == "" {
			apiKey = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		}
		if apiKey != s.config.AuthToken {
			s.writeError(w, http.StatusUnauthorized, "Unauthorized")
			return
		}
	}

	// Direct routing without http.ServeMux to avoid automatic redirect behavior
	path := r.URL.Path
	
	// Route based on path prefix
	switch {
	case path == "/health":
		s.handleHealth(w, r)
	case strings.HasPrefix(path, "/assistants"):
		s.handleAssistantsAPI(w, r, path)
	case strings.HasPrefix(path, "/threads"):
		s.handleThreadsAPI(w, r, path)
	case strings.HasPrefix(path, "/runs"):
		s.handleRunsAPI(w, r, path)
	case strings.HasPrefix(path, "/store"):
		s.handleStoreAPI(w, r, path)
	default:
		s.writeError(w, http.StatusNotFound, "Not found")
	}
}

// Start starts the server.
func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
	log.Printf("Agent Harness server starting on %s", addr)
	return http.ListenAndServe(addr, s)
}

// handleHealth handles health check requests.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleAssistantsAPI handles all assistant-related API calls
func (s *Server) handleAssistantsAPI(w http.ResponseWriter, r *http.Request, fullPath string) {
	path := strings.TrimPrefix(fullPath, "/assistants")
	path = strings.TrimPrefix(path, "/")
	
	// Check for specific sub-routes first
	if path == "search" {
		if r.Method == http.MethodPost {
			s.handleSearchAssistants(w, r)
		} else {
			s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		}
		return
	}
	
	if path == "count" {
		if r.Method == http.MethodPost {
			s.handleCountAssistants(w, r)
		} else {
			s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		}
		return
	}
	
	// Handle /assistants (list/create)
	if path == "" {
		switch r.Method {
		case http.MethodGet:
			s.handleListAssistants(w, r)
		case http.MethodPost:
			s.handleCreateAssistant(w, r)
		default:
			s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		}
		return
	}
	
	// Handle /assistants/{assistant_id}
	assistantID := strings.Split(path, "/")[0]
	switch r.Method {
	case http.MethodGet:
		s.handleGetAssistant(w, r, assistantID)
	case http.MethodPatch:
		s.handleUpdateAssistant(w, r, assistantID)
	case http.MethodDelete:
		s.handleDeleteAssistant(w, r, assistantID)
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleListAssistants lists all assistants.
func (s *Server) handleListAssistants(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	assistants := make([]*Assistant, 0, len(s.assistants))
	for _, a := range s.assistants {
		assistants = append(assistants, a)
	}

	s.writeJSON(w, http.StatusOK, assistants)
}

// handleCreateAssistant creates a new assistant.
func (s *Server) handleCreateAssistant(w http.ResponseWriter, r *http.Request) {
	var req struct {
		GraphID  string                 `json:"graph_id"`
		Name     string                 `json:"name,omitempty"`
		Config   map[string]interface{} `json:"config,omitempty"`
		Metadata map[string]interface{} `json:"metadata,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if graph exists
	if _, ok := s.graphs[req.GraphID]; !ok {
		s.writeError(w, http.StatusNotFound, "Graph not found")
		return
	}

	assistant := &Assistant{
		AssistantID: uuid.New().String(),
		GraphID:     req.GraphID,
		Name:        req.Name,
		Config:      req.Config,
		Metadata:    req.Metadata,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		Version:     1,
	}

	s.assistants[assistant.AssistantID] = assistant
	s.writeJSON(w, http.StatusCreated, assistant)
}

// handleGetAssistant gets a specific assistant.
func (s *Server) handleGetAssistant(w http.ResponseWriter, r *http.Request, assistantID string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	assistant, ok := s.assistants[assistantID]
	if !ok {
		s.writeError(w, http.StatusNotFound, "Assistant not found")
		return
	}

	s.writeJSON(w, http.StatusOK, assistant)
}

// handleUpdateAssistant updates an assistant.
func (s *Server) handleUpdateAssistant(w http.ResponseWriter, r *http.Request, assistantID string) {
	var req struct {
		Name     string                 `json:"name,omitempty"`
		Config   map[string]interface{} `json:"config,omitempty"`
		Metadata map[string]interface{} `json:"metadata,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	assistant, ok := s.assistants[assistantID]
	if !ok {
		s.writeError(w, http.StatusNotFound, "Assistant not found")
		return
	}

	if req.Name != "" {
		assistant.Name = req.Name
	}
	if req.Config != nil {
		assistant.Config = req.Config
	}
	if req.Metadata != nil {
		assistant.Metadata = req.Metadata
	}
	assistant.UpdatedAt = time.Now()
	assistant.Version++

	s.writeJSON(w, http.StatusOK, assistant)
}

// handleDeleteAssistant deletes an assistant.
func (s *Server) handleDeleteAssistant(w http.ResponseWriter, r *http.Request, assistantID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.assistants[assistantID]; !ok {
		s.writeError(w, http.StatusNotFound, "Assistant not found")
		return
	}

	delete(s.assistants, assistantID)
	w.WriteHeader(http.StatusNoContent)
}

// handleSearchAssistants searches assistants.
func (s *Server) handleSearchAssistants(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Metadata       map[string]interface{} `json:"metadata,omitempty"`
		Limit          int                    `json:"limit,omitempty"`
		Offset         int                    `json:"offset,omitempty"`
		GraphID        string                 `json:"graph_id,omitempty"`
		Name           string                 `json:"name,omitempty"`
		ResponseFormat string                 `json:"response_format,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// Handle empty body
		req.Limit = 10
	}

	if req.Limit <= 0 {
		req.Limit = 10
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	assistants := make([]*Assistant, 0)
	for _, a := range s.assistants {
		// Filter by graph_id
		if req.GraphID != "" && a.GraphID != req.GraphID {
			continue
		}

		// Filter by name (substring match)
		if req.Name != "" && !strings.Contains(strings.ToLower(a.Name), strings.ToLower(req.Name)) {
			continue
		}

		// Simple metadata matching
		if req.Metadata != nil {
			match := true
			for k, v := range req.Metadata {
				if a.Metadata[k] != v {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}
		assistants = append(assistants, a)
		if len(assistants) >= req.Limit {
			break
		}
	}

	// Python SDK expects direct array by default, or object with pagination when response_format="object"
	if req.ResponseFormat == "object" {
		w.Header().Set("Content-Type", "application/json")
		s.writeJSON(w, http.StatusOK, map[string]interface{}{
			"assistants": assistants,
			"next":       nil,
		})
	} else {
		// Return direct array for compatibility with Python SDK default
		s.writeJSON(w, http.StatusOK, assistants)
	}
}

// handleCountAssistants counts assistants.
func (s *Server) handleCountAssistants(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Metadata map[string]interface{} `json:"metadata,omitempty"`
		GraphID  string                 `json:"graph_id,omitempty"`
		Name     string                 `json:"name,omitempty"`
	}

	// Try to decode body (optional)
	_ = json.NewDecoder(r.Body).Decode(&req)

	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0
	for _, a := range s.assistants {
		// Filter by graph_id
		if req.GraphID != "" && a.GraphID != req.GraphID {
			continue
		}

		// Filter by name (substring match)
		if req.Name != "" && !strings.Contains(strings.ToLower(a.Name), strings.ToLower(req.Name)) {
			continue
		}

		// Simple metadata matching
		if req.Metadata != nil {
			match := true
			for k, v := range req.Metadata {
				if a.Metadata[k] != v {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}
		count++
	}

	// Python SDK expects direct integer, not a JSON object
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(count)
}

// handleThreadsAPI handles all thread-related API calls
func (s *Server) handleThreadsAPI(w http.ResponseWriter, r *http.Request, fullPath string) {
	path := strings.TrimPrefix(fullPath, "/threads")
	path = strings.TrimPrefix(path, "/")
	
	// Check for specific sub-routes first
	if path == "search" {
		if r.Method == http.MethodPost {
			s.handleSearchThreads(w, r)
		} else {
			s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		}
		return
	}
	
	if path == "count" {
		if r.Method == http.MethodPost {
			s.handleCountThreads(w, r)
		} else {
			s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		}
		return
	}
	
	// Handle /threads (list/create)
	if path == "" {
		switch r.Method {
		case http.MethodGet:
			s.handleListThreads(w, r)
		case http.MethodPost:
			s.handleCreateThread(w, r)
		default:
			s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		}
		return
	}
	
	// Extract threadID and sub-path
	parts := strings.SplitN(path, "/", 2)
	threadID := parts[0]
	
	if len(parts) == 1 {
		// /threads/{thread_id}
		switch r.Method {
		case http.MethodGet:
			s.handleGetThread(w, r, threadID)
		case http.MethodPatch:
			s.handleUpdateThread(w, r, threadID)
		case http.MethodDelete:
			s.handleDeleteThread(w, r, threadID)
		default:
			s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		}
		return
	}
	
	// Handle sub-routes: /threads/{thread_id}/xxx
	subPath := parts[1]
	
	if strings.HasPrefix(subPath, "runs") {
		// Handle /threads/{thread_id}/runs
		subParts := strings.SplitN(subPath, "/", 2)
		if len(subParts) == 1 || subParts[1] == "" {
			// /threads/{thread_id}/runs
			switch r.Method {
			case http.MethodGet:
				s.handleListThreadRuns(w, r, threadID)
			case http.MethodPost:
				s.handleCreateRun(w, r, threadID)
			default:
				s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
			}
		} else {
			// /threads/{thread_id}/runs/{run_id} or /threads/{thread_id}/runs/stream
			runPart := subParts[1]
			if strings.HasPrefix(runPart, "stream") {
				s.handleStreamThreadRun(w, r)
			} else {
				runID := strings.Split(runPart, "/")[0]
				switch r.Method {
				case http.MethodGet:
					s.handleGetRun(w, r, runID)
				case http.MethodDelete:
					s.handleDeleteRun(w, r, runID)
				default:
					s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
				}
			}
		}
	} else if strings.HasPrefix(subPath, "state") {
		s.handleThreadState(w, r, threadID)
	} else if strings.HasPrefix(subPath, "history") {
		s.handleThreadHistory(w, r, threadID)
	} else {
		s.writeError(w, http.StatusNotFound, "Not found")
	}
}

// handleListThreads lists all threads.
func (s *Server) handleListThreads(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	threads := make([]*Thread, 0, len(s.threads))
	for _, t := range s.threads {
		threads = append(threads, t)
	}

	s.writeJSON(w, http.StatusOK, threads)
}

// handleCreateThread creates a new thread.
func (s *Server) handleCreateThread(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Config   map[string]interface{} `json:"config,omitempty"`
		Metadata map[string]interface{} `json:"metadata,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	thread := &Thread{
		ThreadID:  uuid.New().String(),
		Status:    "idle",
		Config:    req.Config,
		Metadata:  req.Metadata,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	s.threads[thread.ThreadID] = thread
	s.writeJSON(w, http.StatusCreated, thread)
}

// handleGetThread gets a specific thread.
func (s *Server) handleGetThread(w http.ResponseWriter, r *http.Request, threadID string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	thread, ok := s.threads[threadID]
	if !ok {
		s.writeError(w, http.StatusNotFound, "Thread not found")
		return
	}

	s.writeJSON(w, http.StatusOK, thread)
}

// handleUpdateThread updates a thread.
func (s *Server) handleUpdateThread(w http.ResponseWriter, r *http.Request, threadID string) {
	var req struct {
		Config   map[string]interface{} `json:"config,omitempty"`
		Metadata map[string]interface{} `json:"metadata,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	thread, ok := s.threads[threadID]
	if !ok {
		s.writeError(w, http.StatusNotFound, "Thread not found")
		return
	}

	if req.Config != nil {
		thread.Config = req.Config
	}
	if req.Metadata != nil {
		thread.Metadata = req.Metadata
	}
	thread.UpdatedAt = time.Now()

	s.writeJSON(w, http.StatusOK, thread)
}

// handleDeleteThread deletes a thread.
func (s *Server) handleDeleteThread(w http.ResponseWriter, r *http.Request, threadID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.threads[threadID]; !ok {
		s.writeError(w, http.StatusNotFound, "Thread not found")
		return
	}

	delete(s.threads, threadID)
	w.WriteHeader(http.StatusNoContent)
}

// handleSearchThreads searches threads.
func (s *Server) handleSearchThreads(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Metadata map[string]interface{} `json:"metadata,omitempty"`
		Limit    int                    `json:"limit,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	threads := make([]*Thread, 0)
	for _, t := range s.threads {
		if req.Metadata != nil {
			match := true
			for k, v := range req.Metadata {
				if t.Metadata[k] != v {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}
		threads = append(threads, t)
		if req.Limit > 0 && len(threads) >= req.Limit {
			break
		}
	}

	s.writeJSON(w, http.StatusOK, threads)
}

// handleCountThreads counts threads.
func (s *Server) handleCountThreads(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.writeJSON(w, http.StatusOK, map[string]int{
		"count": len(s.threads),
	})
}

// handleRunsAPI handles all run-related API calls
func (s *Server) handleRunsAPI(w http.ResponseWriter, r *http.Request, fullPath string) {
	path := strings.TrimPrefix(fullPath, "/runs")
	path = strings.TrimPrefix(path, "/")
	
	// Check for specific sub-routes
	if path == "stream" {
		s.handleStreamRun(w, r)
		return
	}
	
	if path == "wait" {
		s.handleWaitRun(w, r)
		return
	}
	
	if path == "batch" {
		s.handleBatchRuns(w, r)
		return
	}
	
	// Handle /runs (create)
	if path == "" {
		if r.Method == http.MethodPost {
			s.handleCreateRun(w, r, "")
		} else {
			s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		}
		return
	}
	
	// Handle /runs/{run_id}
	runID := strings.Split(path, "/")[0]
	if r.Method == http.MethodGet {
		s.handleGetRun(w, r, runID)
	} else if r.Method == http.MethodDelete {
		s.handleDeleteRun(w, r, runID)
	} else {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleCreateRun creates a new run.
func (s *Server) handleCreateRun(w http.ResponseWriter, r *http.Request, threadID string) {
	var req struct {
		AssistantID string                 `json:"assistant_id"`
		Input       interface{}            `json:"input,omitempty"`
		Config      map[string]interface{} `json:"config,omitempty"`
		Metadata    map[string]interface{} `json:"metadata,omitempty"`
		StreamMode  []string               `json:"stream_mode,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Validate assistant
	assistant, ok := s.assistants[req.AssistantID]
	if !ok {
		s.writeError(w, http.StatusNotFound, "Assistant not found")
		return
	}

	// Validate thread if provided
	if threadID != "" {
		if _, ok := s.threads[threadID]; !ok {
			s.writeError(w, http.StatusNotFound, "Thread not found")
			return
		}
	}

	// Get the graph
	g, ok := s.graphs[assistant.GraphID]
	if !ok {
		s.writeError(w, http.StatusNotFound, "Graph not found")
		return
	}

	run := &Run{
		RunID:       uuid.New().String(),
		ThreadID:    threadID,
		AssistantID: req.AssistantID,
		Status:      "pending",
		Input:       req.Input,
		Config:      req.Config,
		Metadata:    req.Metadata,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		StreamMode:  req.StreamMode,
	}

	s.runs[run.RunID] = run

	// Execute the graph asynchronously
	go s.executeRun(run, g)

	s.writeJSON(w, http.StatusCreated, run)
}

// executeRun executes a run.
func (s *Server) executeRun(run *Run, g *graph.CompiledGraph) {
	ctx := context.Background()

	s.mu.Lock()
	run.Status = "running"
	s.mu.Unlock()

	// Prepare config
	config := &types.RunnableConfig{}
	if run.Config != nil {
		if recursionLimit, ok := run.Config["recursion_limit"].(float64); ok {
			config.RecursionLimit = int(recursionLimit)
		}
		if configurable, ok := run.Config["configurable"].(map[string]interface{}); ok {
			config.Configurable = configurable
		}
	}

	// Invoke the graph
	output, err := g.Invoke(ctx, run.Input, config)

	s.mu.Lock()
	defer s.mu.Unlock()

	if err != nil {
		run.Status = "error"
		run.Metadata = map[string]interface{}{
			"error": err.Error(),
		}
	} else {
		run.Status = "success"
		run.Output = output
	}
	run.UpdatedAt = time.Now()
}

// handleListThreadRuns lists all runs for a thread.
func (s *Server) handleListThreadRuns(w http.ResponseWriter, r *http.Request, threadID string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	runs := make([]*Run, 0)
	for _, run := range s.runs {
		if run.ThreadID == threadID {
			runs = append(runs, run)
		}
	}

	s.writeJSON(w, http.StatusOK, runs)
}

// handleGetRun gets a specific run.
func (s *Server) handleGetRun(w http.ResponseWriter, r *http.Request, runID string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	run, ok := s.runs[runID]
	if !ok {
		s.writeError(w, http.StatusNotFound, "Run not found")
		return
	}

	s.writeJSON(w, http.StatusOK, run)
}

// handleDeleteRun deletes a run.
func (s *Server) handleDeleteRun(w http.ResponseWriter, r *http.Request, runID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.runs[runID]; !ok {
		s.writeError(w, http.StatusNotFound, "Run not found")
		return
	}

	delete(s.runs, runID)
	w.WriteHeader(http.StatusNoContent)
}

// handleStreamRun handles streaming run creation.
func (s *Server) handleStreamRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// TODO: Implement SSE streaming
	s.writeError(w, http.StatusNotImplemented, "Streaming not yet implemented")
}

// handleStreamThreadRun handles streaming run within a thread.
func (s *Server) handleStreamThreadRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// TODO: Implement SSE streaming
	s.writeError(w, http.StatusNotImplemented, "Streaming not yet implemented")
}

// handleWaitRun handles wait for run completion.
func (s *Server) handleWaitRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// TODO: Implement wait for completion
	s.writeError(w, http.StatusNotImplemented, "Wait not yet implemented")
}

// handleBatchRuns handles batch run creation.
func (s *Server) handleBatchRuns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// TODO: Implement batch runs
	s.writeError(w, http.StatusNotImplemented, "Batch runs not yet implemented")
}

// handleThreadState handles thread state operations.
func (s *Server) handleThreadState(w http.ResponseWriter, r *http.Request, threadID string) {
	switch r.Method {
	case http.MethodGet:
		s.handleGetThreadState(w, r, threadID)
	case http.MethodPost:
		s.handleUpdateThreadState(w, r, threadID)
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleGetThreadState gets thread state.
func (s *Server) handleGetThreadState(w http.ResponseWriter, r *http.Request, threadID string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.threads[threadID]; !ok {
		s.writeError(w, http.StatusNotFound, "Thread not found")
		return
	}

	// TODO: Implement state retrieval
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"thread_id": threadID,
		"state":     nil,
	})
}

// handleUpdateThreadState updates thread state.
func (s *Server) handleUpdateThreadState(w http.ResponseWriter, r *http.Request, threadID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	thread, ok := s.threads[threadID]
	if !ok {
		s.writeError(w, http.StatusNotFound, "Thread not found")
		return
	}

	var req struct {
		Values   interface{}            `json:"values"`
		Config   map[string]interface{} `json:"config,omitempty"`
		Metadata map[string]interface{} `json:"metadata,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	thread.UpdatedAt = time.Now()

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"thread_id": threadID,
		"values":    req.Values,
	})
}

// handleThreadHistory handles thread history.
func (s *Server) handleThreadHistory(w http.ResponseWriter, r *http.Request, threadID string) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.threads[threadID]; !ok {
		s.writeError(w, http.StatusNotFound, "Thread not found")
		return
	}

	// TODO: Implement history retrieval
	s.writeJSON(w, http.StatusOK, []interface{}{})
}

// handleStoreAPI handles all store-related API calls
func (s *Server) handleStoreAPI(w http.ResponseWriter, r *http.Request, fullPath string) {
	path := strings.TrimPrefix(fullPath, "/store")
	path = strings.TrimPrefix(path, "/")
	
	if path == "namespaces" {
		s.handleStoreNamespaces(w, r)
		return
	}
	
	if path == "" {
		if r.Method == http.MethodPost {
			s.handleStore(w, r)
		} else {
			s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		}
		return
	}
	
	s.handleStoreDetail(w, r)
}

// handleStore handles store operations.
func (s *Server) handleStore(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// TODO: Implement store search
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"items": []interface{}{},
	})
}

// handleStoreDetail handles individual store operations.
func (s *Server) handleStoreDetail(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleGetStoreItem(w, r)
	case http.MethodPut:
		s.handlePutStoreItem(w, r)
	case http.MethodDelete:
		s.handleDeleteStoreItem(w, r)
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleGetStoreItem gets a store item.
func (s *Server) handleGetStoreItem(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		s.writeError(w, http.StatusNotImplemented, "Store not configured")
		return
	}

	// TODO: Implement get item
	s.writeError(w, http.StatusNotImplemented, "Not implemented")
}

// handlePutStoreItem puts a store item.
func (s *Server) handlePutStoreItem(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		s.writeError(w, http.StatusNotImplemented, "Store not configured")
		return
	}

	// TODO: Implement put item
	s.writeError(w, http.StatusNotImplemented, "Not implemented")
}

// handleDeleteStoreItem deletes a store item.
func (s *Server) handleDeleteStoreItem(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		s.writeError(w, http.StatusNotImplemented, "Store not configured")
		return
	}

	// TODO: Implement delete item
	s.writeError(w, http.StatusNotImplemented, "Not implemented")
}

// handleStoreNamespaces handles store namespace listing.
func (s *Server) handleStoreNamespaces(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	if s.store == nil {
		s.writeJSON(w, http.StatusOK, [][]string{})
		return
	}

	// TODO: Implement namespace listing
	s.writeJSON(w, http.StatusOK, [][]string{})
}

// writeJSON writes a JSON response.
func (s *Server) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("Error encoding JSON: %v", err)
	}
}

// writeError writes an error response.
func (s *Server) writeError(w http.ResponseWriter, status int, message string) {
	s.writeJSON(w, status, map[string]string{
		"error":   message,
		"message": message,
	})
}
