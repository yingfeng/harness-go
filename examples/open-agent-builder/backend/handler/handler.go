// Package handler provides HTTP handlers for the open-agent-builder backend.
package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/google/uuid"
	"github.com/infiniflow/ragflow/harness/examples/open-agent-builder/engine"
	"github.com/infiniflow/ragflow/harness/examples/open-agent-builder/store"
)

// Handler holds dependencies for HTTP handlers.
type Handler struct {
	eng       *engine.WorkflowEngine
	store     *store.Store
	registry  *engine.ModelRegistry
	execStore map[string]context.CancelFunc
}

// New creates a Handler.
func New(eng *engine.WorkflowEngine, st *store.Store, reg *engine.ModelRegistry) *Handler {
	return &Handler{
		eng:       eng,
		store:     st,
		registry:  reg,
		execStore: make(map[string]context.CancelFunc),
	}
}

// RegisterAPI registers routes on the given mux.
func (h *Handler) RegisterAPI(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/workflow/execute", h.handleExecute)
	mux.HandleFunc("GET /api/workflow/stream", h.handleStream)
	mux.HandleFunc("GET /api/models", h.handleGetModels)
	mux.HandleFunc("PUT /api/models/api-key", h.handleUpdateAPIKey)
	mux.HandleFunc("GET /api/templates", h.handleGetTemplates)
	mux.HandleFunc("GET /api/health", h.handleHealth)
	mux.HandleFunc("OPTIONS /", func(w http.ResponseWriter, r *http.Request) {
		corsHeaders(w)
		w.WriteHeader(http.StatusNoContent)
	})
}

func corsHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
}

// handleExecute accepts a workflow definition and starts execution.
func (h *Handler) handleExecute(w http.ResponseWriter, r *http.Request) {
	corsHeaders(w)
	w.Header().Set("Content-Type", "application/json")

	var body struct {
		Workflow json.RawMessage `json:"workflow"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}
	if body.Workflow == nil {
		http.Error(w, `{"error":"missing workflow field"}`, http.StatusBadRequest)
		return
	}

	wf, err := engine.UnmarshalWorkflowDef(body.Workflow)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusBadRequest)
		return
	}

	execID := uuid.New().String()
	ctx, cancel := context.WithCancel(context.Background())
	h.execStore[execID] = cancel

	eventsCh := make(chan engine.SSEEvent, 100)
	go func() {
		defer func() { delete(h.execStore, execID) }()
		h.eng.Execute(ctx, wf, eventsCh)
	}()

	eventStreamsMu.Lock()
	eventStreams[execID] = &streamEntry{ch: eventsCh, cancel: cancel}
	eventStreamsMu.Unlock()

	json.NewEncoder(w).Encode(map[string]string{"executionId": execID})
}

type streamEntry struct {
	ch     <-chan engine.SSEEvent
	cancel context.CancelFunc
}

var (
	eventStreams   = make(map[string]*streamEntry)
	eventStreamsMu sync.Mutex
)

// handleStream sends SSE events for a given executionId.
func (h *Handler) handleStream(w http.ResponseWriter, r *http.Request) {
	execID := r.URL.Query().Get("id")
	if execID == "" {
		http.Error(w, `{"error":"missing id parameter"}`, http.StatusBadRequest)
		return
	}

	eventStreamsMu.Lock()
	entry, ok := eventStreams[execID]
	eventStreamsMu.Unlock()

	if !ok {
		http.Error(w, `{"error":"execution not found"}`, http.StatusNotFound)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, `{"error":"streaming not supported"}`, http.StatusInternalServerError)
		return
	}

	corsHeaders(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-entry.ch:
			if !ok {
				return
			}
			data, _ := json.Marshal(evt.Data)
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evt.Event, string(data))
			flusher.Flush()
		}
	}
}

// handleGetModels returns the full model list (with empty API keys for security).
func (h *Handler) handleGetModels(w http.ResponseWriter, r *http.Request) {
	corsHeaders(w)
	w.Header().Set("Content-Type", "application/json")

	type safeModel struct {
		Name        string  `json:"name"`
		DisplayName string  `json:"displayName"`
		Model       string  `json:"model"`
		APIBase     string  `json:"apiBase"`
		HasAPIKey   bool    `json:"hasApiKey"`
		MaxTokens   int     `json:"maxTokens"`
		Temperature float64 `json:"temperature"`
	}
	safe := make([]safeModel, 0)
	for _, m := range h.registry.Models {
		safe = append(safe, safeModel{
			Name:        m.Name,
			DisplayName: m.DisplayName,
			Model:       m.Model,
			APIBase:     m.APIBase,
			HasAPIKey:   m.APIKey != "",
			MaxTokens:   m.MaxTokens,
			Temperature: m.Temperature,
		})
	}
	json.NewEncoder(w).Encode(safe)
}

// handleUpdateAPIKey updates the API key for a specific model.
func (h *Handler) handleUpdateAPIKey(w http.ResponseWriter, r *http.Request) {
	corsHeaders(w)
	w.Header().Set("Content-Type", "application/json")

	var body struct {
		Name   string `json:"name"`
		APIKey string `json:"apiKey"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}
	if body.Name == "" || body.APIKey == "" {
		http.Error(w, `{"error":"name and apiKey required"}`, http.StatusBadRequest)
		return
	}

	updated := false
	for i := range h.registry.Models {
		if h.registry.Models[i].Name == body.Name {
			h.registry.Models[i].APIKey = body.APIKey
			updated = true
			break
		}
	}
	if !updated {
		http.Error(w, `{"error":"model not found"}`, http.StatusNotFound)
		return
	}

	// Persist to MySQL if available.
	if h.store != nil {
		if err := h.store.SaveModels(h.registry.Models); err != nil {
			log.Printf("persist models error: %v", err)
		}
	}

	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleGetTemplates returns all built-in workflow templates.
func (h *Handler) handleGetTemplates(w http.ResponseWriter, r *http.Request) {
	corsHeaders(w)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(engine.Templates)
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	corsHeaders(w)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
