package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	isServer := len(os.Args) > 1 && os.Args[1] == "server"
	if isServer {
		runServer()
	} else {
		runCLI()
	}
}

func runCLI() {
	ctx := context.Background()
	cfg := LoadConfig()
	llm, err := newChatModel(ctx, cfg)
	if err != nil {
		log.Fatalf("init LLM: %v", err)
	}
	compiled, err := BuildResearchGraph(ctx, llm)
	if err != nil {
		log.Fatalf("build graph: %v", err)
	}
	fmt.Println("=== DeerFlow: Multi-Agent Research System (CLI) ===")
	fmt.Println("Enter a research topic, or type 'quit' to exit.")
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		input := strings.TrimSpace(scanner.Text())
		if input == "" || input == "quit" {
			break
		}
		state := &DeerState{
			UserInput:        input,
			Messages:         []string{},
			Goto:             NodeCoordinator,
			MaxIterations:    10,
			PlanAutoApproved: true,
			ResearchResults:  make(map[string]string),
		}
		s, err := compiled.Invoke(ctx, state)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nError: %v\n", err)
			continue
		}
		if s.Report != "" {
			fmt.Printf("\n=== Final Report ===\n%s\n", s.Report)
		}
		if len(s.Messages) > 0 {
			last := s.Messages[len(s.Messages)-1]
			fmt.Printf("\nLast message: %s\n", last)
		}
		fmt.Println()
	}
}

func runServer() {
	ctx := context.Background()
	cfg := LoadConfig()
	llm, err := newChatModel(ctx, cfg)
	if err != nil {
		log.Fatalf("init LLM: %v", err)
	}
	compiled, err := BuildResearchGraph(ctx, llm)
	if err != nil {
		log.Fatalf("build graph: %v", err)
	}

	store := NewStoreWithConfig(cfg, llm)
	store.compiled = compiled

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handleHealth)
	store.registerAPI(mux)
	handler := corsMiddleware(mux)

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	fmt.Printf("DeerFlow server starting on http://%s\n", addr)
	fmt.Printf("Config: host=%s port=%d model=%s skills=%s\n",
		cfg.Server.Host, cfg.Server.Port, modelName(cfg), cfg.Skills.Dir)
	server := &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0, // 0 = disabled, 必需因为 SSE 流式响应可能持续多分钟
		IdleTimeout:  120 * time.Second,
	}
	fmt.Printf("DeerFlow HTTP server starting on %s\n", addr)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("server: %v", err)
	}
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == "" {
			origin = "*"
		}
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-CSRF-Token")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		if r.Method == "OPTIONS" {
			w.WriteHeader(204)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]interface{}{
		"status": "ok", "version": "2.0.0",
		"time": time.Now().UTC().Format(time.RFC3339),
	})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func readJSON(r *http.Request, v interface{}) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v)
}
