package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ---- Data structures (LangGraph SDK compatible) ----

type AgentThread struct {
	ThreadID  string                 `json:"thread_id"`
	CreatedAt string                 `json:"created_at"`
	UpdatedAt string                 `json:"updated_at"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	Values    *ThreadValues          `json:"values,omitempty"`
}

type ThreadValues struct {
	Title    string        `json:"title"`
	Messages []interface{} `json:"messages"`
}

type Run struct {
	RunID     string `json:"run_id"`
	ThreadID  string `json:"thread_id"`
	Status    string `json:"status"`
	Input     string `json:"input,omitempty"`
	Output    string `json:"output,omitempty"`
	Error     string `json:"error,omitempty"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
	cancel    context.CancelFunc
}

type RunMessage struct {
	RunID     string            `json:"run_id"`
	Seq       int               `json:"seq"`
	Content   interface{}       `json:"content"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	CreatedAt string            `json:"created_at"`
}

type Agent struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Model       string   `json:"model"`
	ToolGroups  []string `json:"tool_groups"`
	Skills      []string `json:"skills"`
	Soul        string   `json:"soul,omitempty"`
}

type MemoryData struct {
	Version     int       `json:"version"`
	LastUpdated time.Time `json:"last_updated"`
	Facts       []*Fact   `json:"facts"`
}

type Fact struct {
	ID         string    `json:"id"`
	Content    string    `json:"content"`
	Category   string    `json:"category"`
	Confidence float64   `json:"confidence"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type Skill struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Content     string `json:"content"`
	Path        string `json:"path"`
	Category    string `json:"category"`    // "public" or "custom"
	License     string `json:"license,omitempty"`
	Enabled     bool   `json:"enabled"`     // 前端可切换
}

// Store holds all in-memory state.
type Store struct {
	mu       sync.RWMutex
	threads  map[string]*AgentThread
	runs     map[string]*Run
	messages map[string][]*RunMessage
	agents   map[string]*Agent
	memory   *MemoryData
	skills   []*Skill
	msgSeq   map[string]int
	compiled *ResearchWorkflow
	llm      ChatModel
}

func NewStoreWithConfig(cfg *Config, llm ChatModel) *Store {
	s := &Store{
		threads:  make(map[string]*AgentThread),
		runs:     make(map[string]*Run),
		messages: make(map[string][]*RunMessage),
		agents:   make(map[string]*Agent),
		skills:   loadSkillsFrom(cfg.Skills.Dir),
		msgSeq:   make(map[string]int),
		memory: &MemoryData{
			Version:     1,
			LastUpdated: time.Now(),
			Facts:       make([]*Fact, 0),
		},
		llm: llm,
	}
	s.agents["lead_agent"] = &Agent{
		Name:        "lead_agent",
		Description: "Default research agent that coordinates sub-agents",
		Model:       modelName(cfg),
		ToolGroups:  []string{},
		Skills:      []string{},
	}
	return s
}

// ====================================================================
// API Routes (LangGraph SDK compatible)
// ====================================================================

func (s *Store) registerAPI(mux *http.ServeMux) {
	// Threads
	mux.HandleFunc("POST /api/threads/search", s.handleThreadsSearch)
	mux.HandleFunc("POST /api/threads", s.handleCreateThread)
	mux.HandleFunc("GET /api/threads/{thread_id}", s.handleGetThread)
	mux.HandleFunc("PATCH /api/threads/{thread_id}", s.handlePatchThread)
	mux.HandleFunc("DELETE /api/threads/{thread_id}", s.handleDeleteThread)
	mux.HandleFunc("POST /api/threads/{thread_id}/delete", s.handleDeleteThread)
	mux.HandleFunc("POST /api/threads/{thread_id}/copy", s.handleCopyThread)
	mux.HandleFunc("GET /api/threads/{thread_id}/state", s.handleGetThreadState)
	mux.HandleFunc("POST /api/threads/{thread_id}/state", s.handleUpdateThreadState)
	mux.HandleFunc("POST /api/threads/{thread_id}/history", s.handleGetThreadHistory)
	mux.HandleFunc("POST /api/threads/{thread_id}/suggestions", s.handleSuggestions)

	// Runs
	mux.HandleFunc("GET /api/threads/{thread_id}/runs", s.handleListRuns)
	mux.HandleFunc("POST /api/threads/{thread_id}/runs", s.handleCreateRun)
	mux.HandleFunc("POST /api/threads/{thread_id}/runs/stream", s.handleStreamRun)
	mux.HandleFunc("POST /api/threads/{thread_id}/runs/wait", s.handleWaitRun)
	mux.HandleFunc("GET /api/threads/{thread_id}/runs/{run_id}", s.handleGetRun)
	mux.HandleFunc("GET /api/threads/{thread_id}/runs/{run_id}/stream", s.handleJoinRunStream)
	mux.HandleFunc("GET /api/threads/{thread_id}/runs/{run_id}/join", s.handleJoinRun)
	mux.HandleFunc("POST /api/threads/{thread_id}/runs/{run_id}/cancel", s.handleCancelRun)
	mux.HandleFunc("GET /api/threads/{thread_id}/runs/{run_id}/messages", s.handleRunMessages)
	mux.HandleFunc("GET /api/threads/{thread_id}/token-usage", s.handleTokenUsage)

	// Assistants (LangGraph SDK compat)
	mux.HandleFunc("POST /api/assistants/search", s.handleAssistantsSearch)
	mux.HandleFunc("GET /api/assistants/{assistant_id}", s.handleGetAssistant)
	mux.HandleFunc("GET /api/assistants/{assistant_id}/graph", s.handleAssistantGraph)
	mux.HandleFunc("GET /api/assistants/{assistant_id}/schemas", s.handleAssistantSchemas)

	// Agents
	mux.HandleFunc("GET /api/agents", s.handleListAgents)
	mux.HandleFunc("GET /api/agents/{name}", s.handleGetAgent)
	mux.HandleFunc("POST /api/agents", s.handleCreateAgent)
	mux.HandleFunc("PUT /api/agents/{name}", s.handleUpdateAgent)
	mux.HandleFunc("DELETE /api/agents/{name}", s.handleDeleteAgent)
	mux.HandleFunc("GET /api/agents/check", s.handleCheckAgentName)

	// Skills
	mux.HandleFunc("GET /api/skills", s.handleListSkills)
	mux.HandleFunc("PUT /api/skills/{skill_name}", s.handleUpdateSkill)
	mux.HandleFunc("GET /api/skills/{name}", s.handleGetSkill)
	mux.HandleFunc("POST /api/skills/install", s.handleInstallSkill)

	// Memory
	mux.HandleFunc("GET /api/memory", s.handleGetMemory)
	mux.HandleFunc("DELETE /api/memory", s.handleDeleteMemory)
	mux.HandleFunc("GET /api/memory/export", s.handleMemoryExport)
	mux.HandleFunc("POST /api/memory/import", s.handleMemoryImport)
	mux.HandleFunc("POST /api/memory/facts", s.handleCreateFact)
	mux.HandleFunc("DELETE /api/memory/facts/{fact_id}", s.handleDeleteFact)
	mux.HandleFunc("PATCH /api/memory/facts/{fact_id}", s.handleUpdateFact)
	mux.HandleFunc("GET /api/memory/status", s.handleMemoryStatus)

	// MCP
	mux.HandleFunc("GET /api/mcp/config", s.handleGetMCPConfig)
	mux.HandleFunc("PUT /api/mcp/config", s.handleUpdateMCPConfig)

	// Models
	mux.HandleFunc("GET /api/models", s.handleListModels)

	// Auth stubs
	s.registerAuthRoutes(mux)
}

// ====================================================================
// Thread operations
// ====================================================================

func (s *Store) CreateThread(title string) *AgentThread {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	t := &AgentThread{
		ThreadID:  uuid.NewString(),
		CreatedAt: now,
		UpdatedAt: now,
		Metadata:  map[string]interface{}{"agent_name": "lead_agent"},
		Values:    &ThreadValues{Title: title, Messages: []interface{}{}},
	}
	s.threads[t.ThreadID] = t
	return t
}

func (s *Store) ListThreads(limit, offset int) []*AgentThread {
	s.mu.RLock()
	defer s.mu.RUnlock()
	all := make([]*AgentThread, 0, len(s.threads))
	for _, t := range s.threads {
		all = append(all, t)
	}
	// Sort by updated_at desc
	for i := 0; i < len(all); i++ {
		for j := i + 1; j < len(all); j++ {
			if all[i].UpdatedAt < all[j].UpdatedAt {
				all[i], all[j] = all[j], all[i]
			}
		}
	}
	if offset >= len(all) {
		return nil
	}
	end := offset + limit
	if end > len(all) {
		end = len(all)
	}
	return all[offset:end]
}

// ====================================================================
// HTTP Handlers
// ====================================================================

func (s *Store) handleThreadsSearch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Limit  int `json:"limit"`
		Offset int `json:"offset"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		req.Limit = 50
	}
	r.Body.Close()
	if req.Limit <= 0 {
		req.Limit = 50
	}
	writeJSON(w, 200, s.ListThreads(req.Limit, req.Offset))
}

func (s *Store) handleCreateThread(w http.ResponseWriter, r *http.Request) {
	t := s.CreateThread(fmt.Sprintf("Chat %s", time.Now().Format("15:04")))
	writeJSON(w, 201, t)
}

func (s *Store) handleSuggestions(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, []interface{}{})
}

func (s *Store) handleGetThread(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("thread_id")
	s.mu.RLock()
	t := s.threads[id]
	s.mu.RUnlock()
	if t == nil {
		writeError(w, 404, "thread not found")
		return
	}
	writeJSON(w, 200, t)
}

func (s *Store) handlePatchThread(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("thread_id")
	var req struct {
		Metadata       map[string]interface{} `json:"metadata,omitempty"`
		Values         map[string]interface{} `json:"values,omitempty"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	s.mu.Lock()
	t := s.threads[id]
	if t == nil {
		s.mu.Unlock()
		writeError(w, 404, "not found")
		return
	}
	if req.Metadata != nil {
		if t.Metadata == nil {
			t.Metadata = req.Metadata
		} else {
			for k, v := range req.Metadata {
				t.Metadata[k] = v
			}
		}
	}
	if req.Values != nil {
		if title, ok := req.Values["title"].(string); ok && t.Values != nil {
			t.Values.Title = title
		}
	}
	t.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	s.mu.Unlock()
	writeJSON(w, 200, t)
}

func (s *Store) handleCopyThread(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("thread_id")
	s.mu.RLock()
	orig := s.threads[id]
	s.mu.RUnlock()
	if orig == nil {
		writeError(w, 404, "not found")
		return
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	copy := &AgentThread{
		ThreadID:  uuid.NewString(),
		CreatedAt: now,
		UpdatedAt: now,
		Metadata:  orig.Metadata,
		Values:    orig.Values,
	}
	s.mu.Lock()
	s.threads[copy.ThreadID] = copy
	s.mu.Unlock()
	writeJSON(w, 201, copy)
}

func (s *Store) handleGetThreadState(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("thread_id")
	s.mu.RLock()
	t := s.threads[id]
	s.mu.RUnlock()
	if t == nil {
		writeJSON(w, 200, map[string]interface{}{"checkpoint": nil, "values": nil})
		return
	}
	writeJSON(w, 200, map[string]interface{}{
		"checkpoint": map[string]interface{}{
			"thread_id": t.ThreadID,
			"checkpoint_id": "1",
		},
		"values": t.Values,
	})
}

func (s *Store) handleDeleteThread(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("thread_id")
	s.mu.Lock()
	delete(s.threads, id)
	s.mu.Unlock()
	writeJSON(w, 200, map[string]string{"status": "deleted"})
}

func (s *Store) handleUpdateThreadState(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("thread_id")
	var req struct {
		Values map[string]interface{} `json:"values"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	s.mu.Lock()
	t := s.threads[id]
	if t == nil {
		s.mu.Unlock()
		writeError(w, 404, "not found")
		return
	}
	t.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if req.Values != nil {
		if title, ok := req.Values["title"].(string); ok && t.Values != nil {
			t.Values.Title = title
		}
	}
	s.mu.Unlock()
	writeJSON(w, 200, t)
}

func (s *Store) handleGetThreadHistory(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("thread_id")
	s.mu.RLock()
	t := s.threads[id]
	s.mu.RUnlock()
	if t == nil {
		writeJSON(w, 200, []interface{}{})
		return
	}
	// LangGraph SDK expects array of state snapshots.
	writeJSON(w, 200, []interface{}{})
}

// ====================================================================
// Run operations (using CompiledGraph.Invoke via StateGraph)
// ====================================================================

func (s *Store) handleCreateRun(w http.ResponseWriter, r *http.Request) {
	threadID := r.PathValue("thread_id")
	s.mu.RLock()
	_, exists := s.threads[threadID]
	s.mu.RUnlock()
	if !exists {
		writeError(w, 404, "thread not found")
		return
	}

	var req struct {
		Input map[string]interface{} `json:"input"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	inputText := extractInputText(req.Input)

	// Create run
	now := time.Now().UTC().Format(time.RFC3339Nano)
	run := &Run{
		RunID:     uuid.NewString(),
		ThreadID:  threadID,
		Status:    "pending",
		Input:     inputText,
		CreatedAt: now,
		UpdatedAt: now,
	}
	s.mu.Lock()
	s.runs[run.RunID] = run
	s.messages[run.RunID] = make([]*RunMessage, 0)
	s.msgSeq[run.RunID] = 0
	s.mu.Unlock()

	writeJSON(w, 201, run)

	// Execute asynchronously
	go s.executeResearchGraph(run.RunID, inputText)
}

func (s *Store) handleStreamRun(w http.ResponseWriter, r *http.Request) {
	threadID := r.PathValue("thread_id")
	var req struct {
		Input map[string]interface{} `json:"input"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	inputText := extractInputText(req.Input)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	run := &Run{
		RunID:     uuid.NewString(),
		ThreadID:  threadID,
		Status:    "pending",
		Input:     inputText,
		CreatedAt: now,
		UpdatedAt: now,
	}
	s.mu.Lock()
	s.runs[run.RunID] = run
	s.messages[run.RunID] = make([]*RunMessage, 0)
	s.msgSeq[run.RunID] = 0
	s.mu.Unlock()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, 500, "streaming not supported")
		return
	}

	fmt.Fprintf(w, "event: metadata\ndata: {\"run_id\":%q,\"thread_id\":%q}\n\n", run.RunID, threadID)
	flusher.Flush()

	events := make(chan string, 256)
	go s.executeResearchGraphStreaming(run.RunID, inputText, events)

	for eventJSON := range events {
		select {
		case <-r.Context().Done():
			return
		default:
		}
		// 从 JSON 中提取 event type，写入正确的 SSE 格式
		// LangGraph SDK StreamManager 要求 event: 字段匹配 "messages/complete"、"metadata"、"end" 等
		var evtMeta struct {
			Type string `json:"type"`
		}
		sseType := ""
		if err := json.Unmarshal([]byte(eventJSON), &evtMeta); err == nil {
			sseType = evtMeta.Type
		}
		switch sseType {
		case "done":
			fmt.Fprintf(w, "event: end\ndata: {}\n\n")
			flusher.Flush()
			return
		case "error":
			fmt.Fprintf(w, "event: error\ndata: %s\n\n", eventJSON)
		case "metadata":
			fmt.Fprintf(w, "event: metadata\ndata: %s\n\n", eventJSON)
		case "messages", "values":
			// 从 JSON 中提取 data 字段作为 SSE data 发送
			fmt.Fprintf(w, "event: %s\ndata: ", sseType)
			var evtData struct {
				Data json.RawMessage `json:"data"`
			}
			if err := json.Unmarshal([]byte(eventJSON), &evtData); err == nil && len(evtData.Data) > 0 {
				w.Write(evtData.Data)
			}
			fmt.Fprintf(w, "\n\n")
		default:
			fmt.Fprintf(w, "data: %s\n\n", eventJSON)
		}
		flusher.Flush()
	}
}

func (s *Store) handleWaitRun(w http.ResponseWriter, r *http.Request) {
	threadID := r.PathValue("thread_id")
	var req struct {
		Input map[string]interface{} `json:"input"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	inputText := extractInputText(req.Input)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	run := &Run{
		RunID:     uuid.NewString(),
		ThreadID:  threadID,
		Status:    "pending",
		Input:     inputText,
		CreatedAt: now,
		UpdatedAt: now,
	}
	s.mu.Lock()
	s.runs[run.RunID] = run
	s.messages[run.RunID] = make([]*RunMessage, 0)
	s.msgSeq[run.RunID] = 0
	s.mu.Unlock()

	events := make(chan string, 256)
	go s.executeResearchGraphStreaming(run.RunID, inputText, events)
	for range events {
	}

	s.mu.RLock()
	final := s.runs[run.RunID]
	s.mu.RUnlock()
	writeJSON(w, 200, final)
}

func (s *Store) handleListRuns(w http.ResponseWriter, r *http.Request) {
	threadID := r.PathValue("thread_id")
	s.mu.RLock()
	var result []*Run
	for _, run := range s.runs {
		if run.ThreadID == threadID {
			result = append(result, run)
		}
	}
	s.mu.RUnlock()
	if result == nil {
		result = []*Run{}
	}
	writeJSON(w, 200, result)
}

func (s *Store) handleGetRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("run_id")
	s.mu.RLock()
	run := s.runs[runID]
	s.mu.RUnlock()
	if run == nil {
		writeError(w, 404, "run not found")
		return
	}
	writeJSON(w, 200, run)
}

func (s *Store) handleCancelRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("run_id")
	s.mu.Lock()
	run := s.runs[runID]
	if run == nil {
		s.mu.Unlock()
		writeError(w, 404, "not found")
		return
	}
	if run.cancel != nil {
		run.cancel()
	}
	run.Status = "cancelled"
	run.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	s.mu.Unlock()
	writeJSON(w, 200, map[string]string{"status": "cancelled"})
}

func (s *Store) handleJoinRunStream(w http.ResponseWriter, r *http.Request) {
	// LangGraph SDK joinStream - SSE stream for existing run
	runID := r.PathValue("run_id")
	s.mu.RLock()
	run := s.runs[runID]
	s.mu.RUnlock()
	if run == nil {
		writeError(w, 404, "run not found")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher, _ := w.(http.Flusher)
	fmt.Fprintf(w, "event: metadata\ndata: {\"run_id\":%q}\n\n", runID)
	flusher.Flush()
	fmt.Fprintf(w, "event: end\ndata: {}\n\n")
	flusher.Flush()
}

func (s *Store) handleJoinRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("run_id")
	s.mu.RLock()
	run := s.runs[runID]
	s.mu.RUnlock()
	if run == nil {
		writeError(w, 404, "run not found")
		return
	}
	writeJSON(w, 200, run)
}

func (s *Store) handleAssistantsSearch(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	agents := make([]map[string]interface{}, 0, len(s.agents))
	for _, a := range s.agents {
		agents = append(agents, map[string]interface{}{
			"assistant_id": a.Name,
			"name":         a.Name,
			"description":  a.Description,
			"model":        a.Model,
			"config":       map[string]interface{}{},
			"created_at":   time.Now().UTC().Format(time.RFC3339Nano),
		})
	}
	s.mu.RUnlock()
	if agents == nil {
		agents = []map[string]interface{}{}
	}
	writeJSON(w, 200, agents)
}

func (s *Store) handleGetAssistant(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("assistant_id")
	s.mu.RLock()
	a := s.agents[id]
	s.mu.RUnlock()
	if a == nil {
		writeError(w, 404, "not found")
		return
	}
	writeJSON(w, 200, map[string]interface{}{
		"assistant_id": a.Name,
		"name":         a.Name,
		"description":  a.Description,
		"model":        a.Model,
		"config":       map[string]interface{}{},
		"created_at":   time.Now().UTC().Format(time.RFC3339Nano),
	})
}

func (s *Store) handleAssistantGraph(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]interface{}{
		"nodes": []map[string]interface{}{
			{"id": "__start__", "type": "schema", "data": map[string]interface{}{}},
			{"id": "coordinator", "type": "node", "data": map[string]interface{}{}},
			{"id": "planner", "type": "node", "data": map[string]interface{}{}},
			{"id": "researcher", "type": "node", "data": map[string]interface{}{}},
			{"id": "coder", "type": "node", "data": map[string]interface{}{}},
			{"id": "reporter", "type": "node", "data": map[string]interface{}{}},
			{"id": "__end__", "type": "schema", "data": map[string]interface{}{}},
		},
		"edges": []map[string]interface{}{
			{"source": "__start__", "target": "coordinator"},
			{"source": "coordinator", "target": "planner"},
			{"source": "planner", "target": "researcher"},
			{"source": "researcher", "target": "coder"},
			{"source": "coder", "target": "reporter"},
			{"source": "reporter", "target": "__end__"},
		},
	})
}

func (s *Store) handleAssistantSchemas(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]interface{}{
		"input_schema":  map[string]interface{}{"type": "object", "properties": map[string]interface{}{}},
		"output_schema": map[string]interface{}{"type": "object", "properties": map[string]interface{}{}},
		"state_schema":  map[string]interface{}{"type": "object", "properties": map[string]interface{}{}},
	})
}

func (s *Store) handleRunMessages(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("run_id")
	s.mu.RLock()
	msgs := s.messages[runID]
	s.mu.RUnlock()
	if msgs == nil {
		msgs = []*RunMessage{}
	}
	writeJSON(w, 200, map[string]interface{}{"data": msgs})
}

func (s *Store) handleTokenUsage(w http.ResponseWriter, r *http.Request) {
	threadID := r.PathValue("thread_id")
	writeJSON(w, 200, map[string]interface{}{
		"thread_id": threadID, "total_tokens": 0,
		"total_input_tokens": 0, "total_output_tokens": 0,
		"total_runs": 0, "by_model": map[string]interface{}{},
		"by_caller": map[string]interface{}{"lead_agent": 0, "subagent": 0, "middleware": 0},
	})
}

// ====================================================================
// Research graph execution (powered by graphengine CompiledGraph)
// ====================================================================

func (s *Store) executeResearchGraph(runID, input string) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	state := &DeerState{
		UserInput:        input,
		Messages:         []string{},
		Goto:             NodeCoordinator,
		MaxIterations:    10,
		PlanAutoApproved: true,
		ResearchResults:  make(map[string]string),
	}

	final, err := s.compiled.Invoke(ctx, state)
	s.mu.Lock()
	run := s.runs[runID]
	if run == nil {
		s.mu.Unlock()
		return
	}
	if err != nil {
		run.Status = "failed"
		run.Error = err.Error()
	} else if final != nil {
		run.Status = "completed"
		run.Output = final.Report
	}
	run.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	s.mu.Unlock()
}

func (s *Store) executeResearchGraphStreaming(runID, input string, events chan<- string) {
	llm := s.llm
	defer func() {
		fmt.Printf("[execute] goroutine ending for run=%s\n", runID)
		close(events)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	state := &DeerState{
		UserInput:        input,
		Messages:         []string{},
		Goto:             NodeCoordinator,
		MaxIterations:    10,
		PlanAutoApproved: true,
		ResearchResults:  make(map[string]string),
	}

	// Helper: emit a complete messages event (for non-streaming case like human msg)
	emitMsg := func(role, content string) string {
		seq := s.addMsg(runID, role, content, role)
		msgID := uuid.NewString()
		events <- fmt.Sprintf(`{"type":"messages","data":[{"type":"%s","content":%q,"id":%q},null],"seq":%d}`, role, content, msgID, seq)
		return msgID
	}

	// Helper: stream an AI message token by token via GenerateStream.
	// Emits values events after each chunk to trigger UI re-renders.
	streamMsg := func(label string, tools []MockTool, msgs []map[string]string) (string, error) {
		msgID := uuid.NewString()
		accumulated := label
		onChunk := func(chunk string) {
			accumulated += chunk
			// 每次 chunk 后发送 values 事件（含当前累积文本）以触发 UI 重渲染
			events <- fmt.Sprintf(`{"type":"values","data":{"messages":[{"type":"ai","content":%q,"id":%q}]}}`, accumulated, msgID)
		}
		full, err := llm.GenerateStream(ctx, msgs, tools, onChunk)
		if err != nil {
			return "", err
		}
		content := label + full
		seq2 := s.addMsg(runID, "ai", content, "ai")
		// 发送 complete 事件标记消息完成
		events <- fmt.Sprintf(`{"type":"messages","data":[{"type":"ai","content":%q,"id":%q},null],"seq":%d}`, content, msgID, seq2)
		state.Messages = append(state.Messages, content)
		return full, nil
	}

	// 构建 skills 注入段
	skillsSection := s.buildSkillsPromptSection()

	// Send user message immediately
	humanID := emitMsg("human", input)

	fmt.Printf("[execute] starting streaming research for run=%s\n", runID)

	// ====== Step 1: Coordinator ======
	coordMsgs := buildLLMMessages(state, CoordinatorPrompt+skillsSection)
	coordOutput, _, err := llm.GenerateWithTool(ctx, coordMsgs, []MockTool{handToPlannerTool()})
	if err != nil {
		s.failRun(runID, fmt.Sprintf("coordinator: %v", err))
		events <- fmt.Sprintf(`{"type":"error","data":%q}`, err.Error())
		events <- `{"type":"done","data":"failed"}`
		return
	}
	if toolName, toolArgs, isTool := ParseToolResult(coordOutput); isTool && toolName == "hand_to_planner" {
		emitMsg("ai", fmt.Sprintf("[Coordinator] Routing to planner. Input: %s", toolArgs))
	} else {
		// 非研究请求：流式返回 coordinator 回复
		_, err := streamMsg("[Coordinator] ", nil, coordMsgs)
		if err != nil {
			s.failRun(runID, fmt.Sprintf("coordinator: %v", err))
			events <- fmt.Sprintf(`{"type":"error","data":%q}`, err.Error())
			events <- `{"type":"done","data":"failed"}`
			return
		}
		s.completeRun(runID, state)
		s.emitValues(runID, events, humanID, input, state)
		events <- fmt.Sprintf(`{"type":"metadata","data":{"run_id":%q}}`, runID)
		events <- `{"type":"done","data":"completed"}`
		return
	}

	// ====== Step 2: Planner (streaming) ======
	planMsgs := buildLLMMessages(state, PlannerPrompt+skillsSection)
	_, err = streamMsg("[Planner] Research Plan:\n", []MockTool{planTool()}, planMsgs)
	if err != nil {
		s.failRun(runID, fmt.Sprintf("planner: %v", err))
		events <- fmt.Sprintf(`{"type":"error","data":%q}`, err.Error())
		events <- `{"type":"done","data":"failed"}`
		return
	}
	state.PlanTitle = fmt.Sprintf("Research Plan: %s", truncate(state.UserInput, 40))
	state.PlanThought = "Systematic research approach"
	state.PlanSteps = generateDefaultPlan(state.UserInput)
	state.MaxIterations = len(state.PlanSteps) * 2
	state.CurrentStepIdx = 0

	// ====== Step 3: ResearchTeam - execute all steps (streaming) ======
	for state.CurrentStepIdx < len(state.PlanSteps) && state.IterationCount < state.MaxIterations {
		step := &state.PlanSteps[state.CurrentStepIdx]
		emitMsg("ai", fmt.Sprintf("[ResearchTeam] Executing step %d: %s (%s)", state.CurrentStepIdx+1, step.Description, step.Type))

		var result string
		switch step.Type {
		case "research":
				resMsgs := buildStepMessages(state, *step, ResearcherPrompt+skillsSection)
				agentName := "[Researcher] Findings for '" + step.Description + "':\n"
				result, err = streamMsg(agentName, []MockTool{searchTool()}, resMsgs)
			case "processing":
				codMsgs := buildStepMessages(state, *step, CoderPrompt+skillsSection)
				agentName := "[Coder] Analysis for '" + step.Description + "':\n"
				result, err = streamMsg(agentName, []MockTool{pythonREPLTool()}, codMsgs)
		default:
			state.CurrentStepIdx++
			continue
		}
		if err != nil {
			s.failRun(runID, fmt.Sprintf("step %d: %v", state.CurrentStepIdx, err))
			events <- fmt.Sprintf(`{"type":"error","data":%q}`, err.Error())
			events <- `{"type":"done","data":"failed"}`
			return
		}
		step.ExecutionRes = result
		if state.ResearchResults == nil {
			state.ResearchResults = make(map[string]string)
		}
		state.ResearchResults[step.Description] = result
		state.CurrentStepIdx++
		state.IterationCount++
	}

	// ====== Step 4: Reporter (streaming) ======
	repMsgs := buildReportMessages(state, ReporterPrompt+skillsSection)
	report, err := streamMsg("[Reporter] Final Report:\n", nil, repMsgs)
	if err != nil {
		s.failRun(runID, fmt.Sprintf("reporter: %v", err))
		events <- fmt.Sprintf(`{"type":"error","data":%q}`, err.Error())
		events <- `{"type":"done","data":"failed"}`
		return
	}
	state.Report = report

	// Complete
	s.completeRun(runID, state)
	s.emitValues(runID, events, humanID, input, state)
	events <- fmt.Sprintf(`{"type":"metadata","data":{"run_id":%q}}`, runID)
	events <- `{"type":"done","data":"completed"}`
}

// Helper: update run status to failed
func (s *Store) failRun(runID, errMsg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if run := s.runs[runID]; run != nil {
		run.Status = "failed"
		run.Error = errMsg
		run.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
}

// Helper: update run status to completed
func (s *Store) completeRun(runID string, state *DeerState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if run := s.runs[runID]; run != nil {
		run.Status = "completed"
		run.Output = state.Report
		run.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
}

// Helper: emit values event for UI refresh
func (s *Store) emitValues(runID string, events chan<- string, humanID, input string, state *DeerState) {
	var allMessages []interface{}
	allMessages = append(allMessages, map[string]interface{}{"type": "human", "content": input, "id": humanID})
	for _, msg := range state.Messages {
		allMessages = append(allMessages, map[string]interface{}{"type": "ai", "content": msg, "id": uuid.NewString()})
	}
	if state.Report != "" {
		allMessages = append(allMessages, map[string]interface{}{"type": "ai", "content": state.Report, "id": uuid.NewString()})
	}
	valuesJSON, _ := json.Marshal(map[string]interface{}{
		"messages": allMessages,
		"title":    state.PlanTitle,
	})
	events <- fmt.Sprintf(`{"type":"values","data":%s}`, string(valuesJSON))
}

func (s *Store) addMsg(runID, role, content, caller string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	seq := s.msgSeq[runID]
	s.msgSeq[runID] = seq + 1
	msg := &RunMessage{
		RunID: runID, Seq: seq,
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Metadata:  map[string]string{"caller": caller},
		Content:   map[string]interface{}{"type": role, "content": content},
	}
	s.messages[runID] = append(s.messages[runID], msg)
	return seq
}

// ====================================================================
// Agent handlers
// ====================================================================

func (s *Store) handleListAgents(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	agents := make([]*Agent, 0, len(s.agents))
	for _, a := range s.agents {
		agents = append(agents, a)
	}
	s.mu.RUnlock()
	writeJSON(w, 200, map[string]interface{}{"agents": agents})
}

func (s *Store) handleGetAgent(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	s.mu.RLock()
	a := s.agents[name]
	s.mu.RUnlock()
	if a == nil {
		writeError(w, 404, "not found")
		return
	}
	writeJSON(w, 200, a)
}

func (s *Store) handleCreateAgent(w http.ResponseWriter, r *http.Request) {
	var a Agent
	json.NewDecoder(r.Body).Decode(&a)
	if a.Name == "" {
		writeError(w, 400, "name required")
		return
	}
	s.mu.Lock()
	if _, ok := s.agents[a.Name]; ok {
		s.mu.Unlock()
		writeError(w, 409, "already exists")
		return
	}
	s.agents[a.Name] = &a
	s.mu.Unlock()
	writeJSON(w, 201, &a)
}

func (s *Store) handleUpdateAgent(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var a Agent
	json.NewDecoder(r.Body).Decode(&a)
	s.mu.Lock()
	existing := s.agents[name]
	if existing == nil {
		s.mu.Unlock()
		writeError(w, 404, "not found")
		return
	}
	if a.Description != "" {
		existing.Description = a.Description
	}
	if a.Model != "" {
		existing.Model = a.Model
	}
	s.mu.Unlock()
	writeJSON(w, 200, existing)
}

func (s *Store) handleDeleteAgent(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	s.mu.Lock()
	delete(s.agents, name)
	s.mu.Unlock()
	writeJSON(w, 200, map[string]string{"status": "deleted"})
}

func (s *Store) handleCheckAgentName(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	s.mu.RLock()
	_, exists := s.agents[name]
	s.mu.RUnlock()
	writeJSON(w, 200, map[string]interface{}{"available": !exists, "name": name})
}

// ====================================================================
// Skills, Memory, Models, Auth
// ====================================================================

func (s *Store) handleListSkills(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]interface{}{"skills": s.skills})
}

func (s *Store) handleGetSkill(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	for _, sk := range s.skills {
		if sk.Name == name {
			writeJSON(w, 200, sk)
			return
		}
	}
	writeError(w, 404, "not found")
}

func (s *Store) handleInstallSkill(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name    string `json:"name"`
		Content string `json:"content"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	skill := &Skill{Name: req.Name, Description: req.Content, Content: req.Content}
	s.mu.Lock()
	s.skills = append(s.skills, skill)
	s.mu.Unlock()
	writeJSON(w, 201, skill)
}

func (s *Store) handleDeleteMemory(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.memory = &MemoryData{
		Version:     1,
		LastUpdated: time.Now(),
		Facts:       make([]*Fact, 0),
	}
	s.mu.Unlock()
	writeJSON(w, 200, map[string]string{"status": "deleted"})
}

func (s *Store) handleMemoryExport(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	mem := s.memory
	s.mu.RUnlock()
	writeJSON(w, 200, mem)
}

func (s *Store) handleMemoryImport(w http.ResponseWriter, r *http.Request) {
	var mem MemoryData
	if err := json.NewDecoder(r.Body).Decode(&mem); err != nil {
		writeError(w, 400, "invalid memory data")
		return
	}
	s.mu.Lock()
	s.memory = &mem
	s.memory.LastUpdated = time.Now()
	s.mu.Unlock()
	writeJSON(w, 200, map[string]string{"status": "imported"})
}

func (s *Store) handleCreateFact(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Content  string  `json:"content"`
		Category string  `json:"category"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Content == "" {
		writeError(w, 400, "content required")
		return
	}
	fact := &Fact{
		ID:         uuid.NewString(),
		Content:    req.Content,
		Category:   req.Category,
		Confidence: 0.5,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	s.mu.Lock()
	s.memory.Facts = append(s.memory.Facts, fact)
	s.memory.LastUpdated = time.Now()
	s.mu.Unlock()
	writeJSON(w, 201, fact)
}

func (s *Store) handleDeleteFact(w http.ResponseWriter, r *http.Request) {
	factID := r.PathValue("fact_id")
	s.mu.Lock()
	facts := s.memory.Facts
	for i, f := range facts {
		if f.ID == factID {
			s.memory.Facts = append(facts[:i], facts[i+1:]...)
			s.memory.LastUpdated = time.Now()
			break
		}
	}
	s.mu.Unlock()
	writeJSON(w, 200, map[string]string{"status": "deleted"})
}

func (s *Store) handleUpdateFact(w http.ResponseWriter, r *http.Request) {
	factID := r.PathValue("fact_id")
	var req struct {
		Content  string  `json:"content"`
		Category string  `json:"category"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	s.mu.Lock()
	for _, f := range s.memory.Facts {
		if f.ID == factID {
			if req.Content != "" {
				f.Content = req.Content
			}
			if req.Category != "" {
				f.Category = req.Category
			}
			f.UpdatedAt = time.Now()
			break
		}
	}
	s.memory.LastUpdated = time.Now()
	s.mu.Unlock()
	writeJSON(w, 200, map[string]string{"status": "updated"})
}

func (s *Store) handleGetMCPConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]interface{}{
		"config": map[string]interface{}{},
		"enabled": false,
	})
}

func (s *Store) handleUpdateMCPConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]string{"status": "ok"})
}

func (s *Store) handleUpdateSkill(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("skill_name")
	var req struct {
		Enabled *bool `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, "invalid request body")
		return
	}
	s.mu.Lock()
	for _, sk := range s.skills {
		if sk.Name == name {
			if req.Enabled != nil {
				sk.Enabled = *req.Enabled
			}
			s.mu.Unlock()
			writeJSON(w, 200, sk)
			return
		}
	}
	s.mu.Unlock()
	writeError(w, 404, "skill not found")
}

func (s *Store) handleGetMemory(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	mem := s.memory
	s.mu.RUnlock()
	writeJSON(w, 200, mem)
}

func (s *Store) handleMemoryStatus(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	mem := s.memory
	s.mu.RUnlock()
	writeJSON(w, 200, map[string]interface{}{
		"config": map[string]interface{}{
			"enabled": true, "storage_path": "memory.json",
			"debounce_seconds": 5, "max_facts": 1000,
			"fact_confidence_threshold": 0.5, "injection_enabled": true,
			"max_injection_tokens": 500,
		},
		"data": mem,
	})
}

func (s *Store) handleListModels(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]interface{}{
		"models": []map[string]interface{}{
			{"name": "default", "model": "mock-model", "display_name": "DeerFlow Default",
				"supports_thinking": false, "supports_reasoning_effort": false},
		},
		"token_usage": map[string]interface{}{"enabled": false},
	})
}

func (s *Store) registerAuthRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/auth/me", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]interface{}{
			"user": map[string]interface{}{
				"id": "demo-user", "email": "demo@local",
				"display_name": "Demo User", "is_admin": true, "needs_setup": false,
			},
		})
	})
	mux.HandleFunc("POST /api/v1/auth/login/local", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "access_token", Value: "demo", Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode, Expires: time.Now().Add(24 * time.Hour)})
		writeJSON(w, 200, map[string]interface{}{"user": map[string]string{"id": "demo-user"}, "token": "demo"})
	})
	mux.HandleFunc("POST /api/v1/auth/register", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "access_token", Value: "demo", Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode, Expires: time.Now().Add(24 * time.Hour)})
		writeJSON(w, 201, map[string]interface{}{"user": map[string]string{"id": "demo-user"}, "token": "demo"})
	})
	mux.HandleFunc("POST /api/v1/auth/logout", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "access_token", Value: "", Path: "/", HttpOnly: true, MaxAge: -1})
		writeJSON(w, 200, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /api/v1/auth/setup-status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]interface{}{"setup_required": false, "configured": true})
	})
	mux.HandleFunc("POST /api/v1/auth/initialize", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]string{"status": "already_initialized"})
	})
	mux.HandleFunc("POST /api/v1/auth/change-password", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]string{"status": "ok"})
	})
}

// buildSkillsPromptSection generates an XML block with enabled skills for the LLM system prompt.
func (s *Store) buildSkillsPromptSection() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var enabled []*Skill
	for _, sk := range s.skills {
		if sk.Enabled && sk.Category == "public" {
			enabled = append(enabled, sk)
		}
	}
	if len(enabled) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("\n\n<skill_system>\nYou have access to skills that provide optimized workflows for specific tasks.\n")
	sb.WriteString("When a user query matches a skill's use case, load and follow the skill's instructions.\n\n")
	sb.WriteString("<available_skills>\n")
	for _, sk := range enabled {
		escapedDesc := strings.ReplaceAll(sk.Description, "\"", "'")
		sb.WriteString(fmt.Sprintf("  <skill>\n    <name>%s</name>\n    <description>%s</description>\n    <location>%s</location>\n  </skill>\n", sk.Name, escapedDesc, sk.Path))
	}
	sb.WriteString("</available_skills>\n</skill_system>")
	return sb.String()
}

// ====================================================================
// Skills loading
// ====================================================================

func loadSkillsFrom(dir string) []*Skill {
	if dir == "" {
		return nil
	}
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return nil
	}
	var skills []*Skill

	// Scan both public/ and custom/ subdirectories
	categories := []string{"public", "custom"}
	for _, cat := range categories {
		catDir := filepath.Join(dir, cat)
		catInfo, err := os.Stat(catDir)
		if err != nil || !catInfo.IsDir() {
			continue
		}
		entries, err := os.ReadDir(catDir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			skillPath := filepath.Join(catDir, entry.Name(), "SKILL.md")
			data, err := os.ReadFile(skillPath)
			if err != nil {
				continue
			}
			content := string(data)

			// Parse YAML front-matter: ---\nkey: val\n...\n---\n
			name, desc, license := parseFrontMatter(content)
			if name == "" {
				name = entry.Name()
			}
			if desc == "" {
				desc = fmt.Sprintf("Skill: %s", name)
			}

			skills = append(skills, &Skill{
				Name:        name,
				Description: desc,
				Content:     content,
				Path:        skillPath,
				Category:    cat,
				License:     license,
				Enabled:     true, // 默认启用
			})
		}
	}
	return skills
}

// parseFrontMatter extracts name, description, license from YAML front-matter.
func parseFrontMatter(content string) (name, description, license string) {
	if len(content) < 4 || content[:4] != "---\n" && content[:4] != "---\r" {
		return "", "", ""
	}
	// Find closing ---
	endIdx := strings.Index(content[4:], "\n---")
	if endIdx < 0 {
		return "", "", ""
	}
	front := content[4 : 4+endIdx]
	lines := strings.Split(front, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "name:") {
			name = strings.TrimSpace(line[5:])
			name = strings.Trim(name, "\"'")
		} else if strings.HasPrefix(line, "description:") {
			desc := strings.TrimSpace(line[12:])
			desc = strings.Trim(desc, "\"'")
			if len(desc) > 200 {
				desc = desc[:200] + "..."
			}
			description = desc
		} else if strings.HasPrefix(line, "license:") {
			license = strings.TrimSpace(line[8:])
			license = strings.Trim(license, "\"'")
		}
	}
	return
}

// ====================================================================
// Utils
// ====================================================================

func extractInputText(input map[string]interface{}) string {
	if input == nil {
		return ""
	}
	if msgs, ok := input["messages"]; ok {
		if arr, ok := msgs.([]interface{}); ok && len(arr) > 0 {
			if m, ok := arr[len(arr)-1].(map[string]interface{}); ok {
				if c, ok := m["content"]; ok {
					switch v := c.(type) {
					case string:
						return v
					case []interface{}:
						if len(v) > 0 {
							if part, ok := v[0].(map[string]interface{}); ok {
								if text, ok := part["text"].(string); ok {
									return text
								}
							}
						}
					}
				}
			}
		}
	}
	return fmt.Sprintf("%v", input)
}
