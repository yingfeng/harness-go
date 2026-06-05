// This example demonstrates using a checkpointer for persistence.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/infiniflow/ragflow/harness"
)

// State defines the graph state.
type State struct {
	Step    int
	Message string
}

func main() {
	ctx := context.Background()

	// Create graph builder
	builder := harness.NewStateGraph(State{})

	// Add step node
	builder.AddNode("step", func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(State)
		s.Step++
		s.Message = fmt.Sprintf("Completed step %d", s.Step)
		fmt.Printf("Step %d executed\n", s.Step)
		return s, nil
	})

	// Add edges
	if err := builder.AddEdge(harness.Start, "step"); err != nil {
		log.Fatal(err)
	}
	if err := builder.AddEdge("step", harness.End); err != nil {
		log.Fatal(err)
	}

	// Create in-memory checkpointer
	checkpointer := harness.NewMemorySaver()

	// Compile graph with checkpointer
	graph, err := builder.Compile(harness.WithCheckpointer(checkpointer))
	if err != nil {
		log.Fatalf("Failed to compile graph: %v", err)
	}

	// Create a thread config
	config := harness.NewRunnableConfig()
	config.Configurable = map[string]interface{}{
		"thread_id": "example-thread-1",
	}

	// Run graph multiple times with the same thread ID
	// The state will be persisted between runs
	for i := 0; i < 3; i++ {
		result, err := graph.Invoke(ctx, State{
			Step:    0, // This will be merged with checkpointed state
			Message: "",
		}, config)
		if err != nil {
			log.Fatalf("Failed to run graph (iteration %d): %v", i, err)
		}

		state := result.(State)
		fmt.Printf("Iteration %d: Step=%d, Message=%s\n\n", i+1, state.Step, state.Message)
	}

	fmt.Println("Checkpointer example completed!")
}
