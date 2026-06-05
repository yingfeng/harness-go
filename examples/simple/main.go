// This example demonstrates a simple state graph with two nodes.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/infiniflow/ragflow/harness"
)

// State defines the graph state.
type State struct {
	Messages []string
	Counter  int
}

func main() {
	ctx := context.Background()

	// Create graph builder
	builder := harness.NewStateGraph(State{})

	// Add nodes
	builder.AddNode("agent", func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(State)
		s.Messages = append(s.Messages, "Hello from agent")
		s.Counter++
		fmt.Printf("Agent node executed. Counter: %d\n", s.Counter)
		return s, nil
	})

	builder.AddNode("tool", func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(State)
		s.Messages = append(s.Messages, "Hello from tool")
		s.Counter += 10
		fmt.Printf("Tool node executed. Counter: %d\n", s.Counter)
		return s, nil
	})

	// Add edges
	if err := builder.AddEdge(harness.Start, "agent"); err != nil {
		log.Fatal(err)
	}
	if err := builder.AddEdge("agent", "tool"); err != nil {
		log.Fatal(err)
	}
	if err := builder.AddEdge("tool", harness.End); err != nil {
		log.Fatal(err)
	}

	// Compile graph
	graph, err := builder.Compile()
	if err != nil {
		log.Fatalf("Failed to compile graph: %v", err)
	}

	// Run graph
	result, err := graph.Invoke(ctx, State{
		Messages: []string{"Starting..."},
		Counter:  0,
	})
	if err != nil {
		log.Fatalf("Failed to run graph: %v", err)
	}

	fmt.Printf("Final state: %+v\n", result)
}
