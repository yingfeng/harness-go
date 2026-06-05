// Package main demonstrates OpenTelemetry integration with Agent Harness.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/infiniflow/ragflow/harness/pregel"
	"github.com/infiniflow/ragflow/harness/telemetry"
	"github.com/infiniflow/ragflow/harness/runnable"
	"github.com/infiniflow/ragflow/harness/types"
)

func main() {
	// Initialize OpenTelemetry for development with console exporter
	shutdown, err := telemetry.InitForDevelopment("harness-example")
	if err != nil {
		log.Fatalf("Failed to initialize OpenTelemetry: %v", err)
	}
	defer shutdown(context.Background())

	// Create a telemetry provider and tracer
	provider, err := telemetry.NewDefaultTelemetryProvider()
	if err != nil {
		log.Fatalf("Failed to create telemetry provider: %v", err)
	}
	tracer := telemetry.NewRunnableTracer(provider)

	// Create a simple Agent Harness workflow
	ctx := context.Background()

	// Create instrumented nodes
	processNode := tracer.TraceNode("process", func(ctx context.Context, input interface{}) (interface{}, error) {
		time.Sleep(50 * time.Millisecond)
		return fmt.Sprintf("Processed: %v", input), nil
	})

	validateNode := tracer.TraceNode("validate", func(ctx context.Context, input interface{}) (interface{}, error) {
		time.Sleep(30 * time.Millisecond)
		return fmt.Sprintf("Validated: %v", input), nil
	})

	// Create runnable nodes
	processRunnable := runnable.NewInjectableRunnable("process", processNode)
	validateRunnable := runnable.NewInjectableRunnable("validate", validateNode)

	// Add additional tracing
	tracedProcess := tracer.TraceRunnable(processRunnable)
	tracedValidate := tracer.TraceRunnable(validateRunnable)

	// Execute the workflow with tracing
	fmt.Println("Executing workflow with OpenTelemetry instrumentation...")

	// Step 1: Process
	fmt.Println("Step 1: Processing")
	result1, err := tracedProcess.Invoke(ctx, "input data")
	if err != nil {
		log.Fatalf("Process failed: %v", err)
	}
	fmt.Printf("Result: %v\n", result1)

	// Step 2: Validate
	fmt.Println("Step 2: Validating")
	result2, err := tracedValidate.Invoke(ctx, result1)
	if err != nil {
		log.Fatalf("Validate failed: %v", err)
	}
	fmt.Printf("Result: %v\n", result2)

	// Manual span example
	fmt.Println("Step 3: Custom operation")
	err = telemetry.RunInSpan(ctx, "custom-operation", func(ctx context.Context) error {
		time.Sleep(40 * time.Millisecond)
		fmt.Println("Custom operation completed")
		return nil
	})
	if err != nil {
		log.Fatalf("Custom operation failed: %v", err)
	}

	// Record metrics
	fmt.Println("Recording custom metrics...")
	provider.Metrics.RecordMessagesProcessed(ctx, 5)
	provider.Metrics.RecordTokensProcessed(ctx, 128)
	provider.Metrics.RecordCacheHit(ctx, "test-key")
	provider.Metrics.RecordStreamEventEmitted(ctx, "node_update")

	// Demonstrate checkpoint tracing
	fmt.Println("Demonstrating checkpoint tracing...")
	saveCheckpoint := func(ctx context.Context) error {
		time.Sleep(20 * time.Millisecond)
		fmt.Println("Checkpoint saved")
		return nil
	}
	tracedCheckpoint := tracer.TraceCheckpoint("save", saveCheckpoint)
	err = tracedCheckpoint(ctx)
	if err != nil {
		log.Fatalf("Checkpoint failed: %v", err)
	}

	fmt.Println("\nWorkflow completed!")
	fmt.Println("Check the console output for detailed trace and metric information.")
}

// Example with Pregel Engine
func exampleWithPregel() {
	// Initialize OpenTelemetry
	shutdown, err := telemetry.InitForDevelopment("pregel-example")
	if err != nil {
		log.Fatal(err)
	}
	defer shutdown(context.Background())

	// Create Pregel engine with telemetry
	_ = pregel.NewEngine(
		pregel.WithDebug(true),
		pregel.WithRecursionLimit(10),
	)

	// Create instrumented nodes
	_ = func(ctx context.Context, input interface{}) (interface{}, error) {
		time.Sleep(50 * time.Millisecond)
		return "node1 result", nil
	}

	_ = func(ctx context.Context, input interface{}) (interface{}, error) {
		time.Sleep(30 * time.Millisecond)
		return "node2 result", nil
	}

	// Add nodes to engine (pseudo-code)
	// engine.AddNode("node1", node1)
	// engine.AddNode("node2", node2)

	// Execute with tracing
	_ = context.Background()
	_ = &types.RunnableConfig{
		ThreadID: "example-thread",
	}

	// result, err := engine.Invoke(ctx, config, "initial input")
	// if err != nil {
	// 	log.Fatal(err)
	// }

	fmt.Println("Pregel workflow completed!")
}
