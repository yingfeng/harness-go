// Package server provides tests for the Agent Harness API server.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/infiniflow/ragflow/harness/graphengine/constants"
	"github.com/infiniflow/ragflow/harness/graphengine/graph"
)

// SimpleState is a simple state for testing
type SimpleState struct {
	Value string `json:"value"`
}

func createTestGraph(t *testing.T) *graph.CompiledGraph {
	builder := graph.NewStateGraph(&SimpleState{})
	builder.AddNode("start", func(ctx context.Context, state interface{}) (interface{}, error) {
		return state, nil
	})
	builder.SetEntryPoint("start")
	builder.AddEdge("start", constants.End)
	g, err := builder.Compile()
	if err != nil {
		t.Fatalf("Failed to compile graph: %v", err)
	}
	if g == nil {
		t.Fatal("Compiled graph is nil")
	}
	return g
}

func TestServerHealth(t *testing.T) {
	srv := NewServer(nil)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var resp map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if resp["status"] != "ok" {
		t.Errorf("Expected status 'ok', got %s", resp["status"])
	}
}

func TestServerAssistants(t *testing.T) {
	srv := NewServer(nil)
	srv.RegisterGraph("test-graph", createTestGraph(t))

	// Test create assistant
	createReq := map[string]interface{}{
		"graph_id": "test-graph",
		"name":     "Test Assistant",
		"config": map[string]interface{}{
			"recursion_limit": 25,
		},
	}
	body, _ := json.Marshal(createReq)

	req := httptest.NewRequest(http.MethodPost, "/assistants", bytes.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d: %s, Location: %s", w.Code, w.Body.String(), w.Header().Get("Location"))
	}

	var assistant Assistant
	if err := json.Unmarshal(w.Body.Bytes(), &assistant); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if assistant.GraphID != "test-graph" {
		t.Errorf("Expected graph_id 'test-graph', got %s", assistant.GraphID)
	}

	if assistant.Name != "Test Assistant" {
		t.Errorf("Expected name 'Test Assistant', got %s", assistant.Name)
	}

	// Test list assistants
	req = httptest.NewRequest(http.MethodGet, "/assistants", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var assistants []*Assistant
	if err := json.Unmarshal(w.Body.Bytes(), &assistants); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if len(assistants) != 1 {
		t.Errorf("Expected 1 assistant, got %d", len(assistants))
	}

	// Test get assistant
	req = httptest.NewRequest(http.MethodGet, "/assistants/"+assistant.AssistantID, nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	// Test search assistants
	searchReq := map[string]interface{}{
		"limit": 10,
	}
	body, _ = json.Marshal(searchReq)
	req = httptest.NewRequest(http.MethodPost, "/assistants/search", bytes.NewReader(body))
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	// Test count assistants
	req = httptest.NewRequest(http.MethodPost, "/assistants/count", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var countResp map[string]int
	if err := json.Unmarshal(w.Body.Bytes(), &countResp); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if countResp["count"] != 1 {
		t.Errorf("Expected count 1, got %d", countResp["count"])
	}
}

func TestServerThreads(t *testing.T) {
	srv := NewServer(nil)

	// Test create thread
	createReq := map[string]interface{}{
		"metadata": map[string]interface{}{
			"user_id": "test-user",
		},
	}
	body, _ := json.Marshal(createReq)

	req := httptest.NewRequest(http.MethodPost, "/threads", bytes.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d: %s, Location: %s", w.Code, w.Body.String(), w.Header().Get("Location"))
	}

	var thread Thread
	if err := json.Unmarshal(w.Body.Bytes(), &thread); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if thread.Status != "idle" {
		t.Errorf("Expected status 'idle', got %s", thread.Status)
	}

	// Test list threads
	req = httptest.NewRequest(http.MethodGet, "/threads", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var threads []*Thread
	if err := json.Unmarshal(w.Body.Bytes(), &threads); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if len(threads) != 1 {
		t.Errorf("Expected 1 thread, got %d", len(threads))
	}

	// Test get thread
	req = httptest.NewRequest(http.MethodGet, "/threads/"+thread.ThreadID, nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

func TestServerRuns(t *testing.T) {
	srv := NewServer(nil)
	srv.RegisterGraph("test-graph", createTestGraph(t))

	// Create assistant first
	assistantReq := map[string]interface{}{
		"graph_id": "test-graph",
		"name":     "Test Assistant",
	}
	body, _ := json.Marshal(assistantReq)
	req := httptest.NewRequest(http.MethodPost, "/assistants", bytes.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	var assistant Assistant
	json.Unmarshal(w.Body.Bytes(), &assistant)

	// Create thread
	req = httptest.NewRequest(http.MethodPost, "/threads", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	var thread Thread
	json.Unmarshal(w.Body.Bytes(), &thread)

	// Test create run
	runReq := map[string]interface{}{
		"assistant_id": assistant.AssistantID,
		"input": map[string]interface{}{
			"message": "Hello",
		},
		"stream_mode": []string{"values"},
	}
	body, _ = json.Marshal(runReq)
	req = httptest.NewRequest(http.MethodPost, "/runs", bytes.NewReader(body))
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d: %s, Location: %s", w.Code, w.Body.String(), w.Header().Get("Location"))
	}

	var run Run
	if err := json.Unmarshal(w.Body.Bytes(), &run); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if run.AssistantID != assistant.AssistantID {
		t.Errorf("Expected assistant_id %s, got %s", assistant.AssistantID, run.AssistantID)
	}

	if run.Status != "pending" && run.Status != "running" {
		t.Errorf("Expected status 'pending' or 'running', got %s", run.Status)
	}

	// Test create run in thread
	runReq = map[string]interface{}{
		"assistant_id": assistant.AssistantID,
		"input": map[string]interface{}{
			"message": "Hello in thread",
		},
	}
	body, _ = json.Marshal(runReq)
	req = httptest.NewRequest(http.MethodPost, "/threads/"+thread.ThreadID+"/runs", bytes.NewReader(body))
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d: %s, Location: %s", w.Code, w.Body.String(), w.Header().Get("Location"))
	}

	// Wait for run to complete
	time.Sleep(100 * time.Millisecond)

	// Test get run
	req = httptest.NewRequest(http.MethodGet, "/runs/"+run.RunID, nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

func TestServerAuthentication(t *testing.T) {
	config := &ServerConfig{
		AuthToken: "test-api-key",
	}
	srv := NewServer(config)

	// Test without auth
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", w.Code)
	}

	// Test with wrong auth
	req = httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("X-API-Key", "wrong-key")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", w.Code)
	}

	// Test with correct auth
	req = httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("X-API-Key", "test-api-key")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	// Test with Bearer token
	req = httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Authorization", "Bearer test-api-key")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

func TestServerCORS(t *testing.T) {
	srv := NewServer(nil)

	// Test preflight request
	req := httptest.NewRequest(http.MethodOptions, "/health", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	// Check CORS headers
	if w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("Expected CORS header Access-Control-Allow-Origin: *")
	}
}
