// Example server implementation for Agent Harness Go.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/infiniflow/ragflow/harness/constants"
	"github.com/infiniflow/ragflow/harness/graph"
	"github.com/infiniflow/ragflow/harness/server"
)

// SimpleState is a simple state for the example
type SimpleState struct {
	Message string `json:"message"`
}

func main() {
	// Create a simple graph
	builder := graph.NewStateGraph(&SimpleState{})

	// Add nodes
	builder.AddNode("start", func(ctx context.Context, state interface{}) (interface{}, error) {
		input, ok := state.(map[string]interface{})
		if !ok {
			return state, nil
		}
		input["step"] = "started"
		return input, nil
	})

	builder.AddNode("process", func(ctx context.Context, state interface{}) (interface{}, error) {
		input, ok := state.(map[string]interface{})
		if !ok {
			return state, nil
		}
		input["step"] = "processed"
		if msg, ok := input["message"].(string); ok {
			input["response"] = fmt.Sprintf("Processed: %s", msg)
		}
		return input, nil
	})

	// Add edges
	builder.AddEdge("start", "process")
	builder.AddEdge("process", constants.End)

	// Set entry point
	builder.SetEntryPoint("start")

	// Compile the graph
	compiledGraph, err := builder.Compile()
	if err != nil {
		log.Fatalf("Failed to compile graph: %v", err)
	}

	// Create server
	config := &server.ServerConfig{
		Host: "0.0.0.0",
		Port: 8123,
	}

	srv := server.NewServer(config)

	// Register the graph
	srv.RegisterGraph("simple-graph", compiledGraph)

	// Create an assistant for this graph
	assistantReq := map[string]interface{}{
		"graph_id": "simple-graph",
		"name":     "Simple Assistant",
		"config": map[string]interface{}{
			"recursion_limit": 25,
		},
	}

	log.Printf("Assistant request: %+v", assistantReq)

	// Start the server
	log.Println("Starting Agent Harness server on http://localhost:8123")
	log.Println("API endpoints:")
	log.Println("  POST /assistants - Create assistant")
	log.Println("  GET  /assistants - List assistants")
	log.Println("  POST /threads - Create thread")
	log.Println("  GET  /threads - List threads")
	log.Println("  POST /runs - Create run")
	log.Println("  POST /threads/{thread_id}/runs - Create run in thread")
	log.Println("  GET  /health - Health check")

	if err := srv.Start(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
