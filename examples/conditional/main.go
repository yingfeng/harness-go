// This example demonstrates conditional edges in a state graph.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/infiniflow/ragflow/harness"
)

// State defines the graph state.
type State struct {
	Value   int
	History []string
}

func main() {
	ctx := context.Background()

	// Create graph builder
	builder := harness.NewStateGraph(State{})

	// Add start node
	builder.AddNode("start", func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(State)
		s.Value = 50
		s.History = append(s.History, fmt.Sprintf("Start: value=%d", s.Value))
		return s, nil
	})

	// Add decision node
	builder.AddNode("decision", func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(State)
		s.History = append(s.History, fmt.Sprintf("Decision: value=%d", s.Value))
		return s, nil
	})

	// Add high value node
	builder.AddNode("high_value", func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(State)
		s.Value *= 2
		s.History = append(s.History, fmt.Sprintf("High value: value=%d", s.Value))
		return s, nil
	})

	// Add low value node
	builder.AddNode("low_value", func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(State)
		s.Value /= 2
		s.History = append(s.History, fmt.Sprintf("Low value: value=%d", s.Value))
		return s, nil
	})

	// Add end node
	builder.AddNode("end", func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(State)
		s.History = append(s.History, fmt.Sprintf("End: value=%d", s.Value))
		return s, nil
	})

	// Add edges
	if err := builder.AddEdge(harness.Start, "start"); err != nil {
		log.Fatal(err)
	}
	if err := builder.AddEdge("start", "decision"); err != nil {
		log.Fatal(err)
	}

	// Add conditional edges from decision
	if err := builder.AddConditionalEdges("decision", func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(State)
		if s.Value > 30 {
			return "high", nil
		}
		return "low", nil
	}, map[string]string{
		"high": "high_value",
		"low":  "low_value",
	}); err != nil {
		log.Fatal(err)
	}

	if err := builder.AddEdge("high_value", "end"); err != nil {
		log.Fatal(err)
	}
	if err := builder.AddEdge("low_value", "end"); err != nil {
		log.Fatal(err)
	}
	if err := builder.AddEdge("end", harness.End); err != nil {
		log.Fatal(err)
	}

	// Compile graph
	graph, err := builder.Compile()
	if err != nil {
		log.Fatalf("Failed to compile graph: %v", err)
	}

	// Run graph
	result, err := graph.Invoke(ctx, State{
		Value:   0,
		History: []string{},
	})
	if err != nil {
		log.Fatalf("Failed to run graph: %v", err)
	}

	finalState := result.(State)
	fmt.Println("\nExecution history:")
	for _, h := range finalState.History {
		fmt.Printf("  %s\n", h)
	}
	fmt.Printf("\nFinal value: %d\n", finalState.Value)
}
