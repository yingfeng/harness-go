// Package telemetry provides examples for using OpenTelemetry with LangGraph.
package telemetry

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/infiniflow/ragflow/harness/graphengine/runnable"
	"github.com/infiniflow/ragflow/harness/graphengine/types"
)

// Example_init demonstrates basic initialization of OpenTelemetry.
func Example_init() {
	// Initialize OpenTelemetry with default configuration
	shutdown, err := InitWithDefaults()
	if err != nil {
		log.Fatalf("Failed to initialize OpenTelemetry: %v", err)
	}
	defer shutdown(context.Background())

	// Your application code here
	fmt.Println("OpenTelemetry initialized successfully")
}

// Example_initForProduction demonstrates production configuration.
func Example_initForProduction() {
	// Initialize for production with OTLP exporter
	shutdown, err := InitForProduction(
		"my-agent-harness-app",
		"1.0.0",
		"production",
		"otel-collector:4317",
	)
	if err != nil {
		log.Fatalf("Failed to initialize OpenTelemetry: %v", err)
	}
	defer shutdown(context.Background())

	fmt.Println("Production OpenTelemetry initialized")
}

// Example_initForDevelopment demonstrates development configuration.
func Example_initForDevelopment() {
	// Initialize for development with console exporter
	shutdown, err := InitForDevelopment("my-agent-harness-dev")
	if err != nil {
		log.Fatalf("Failed to initialize OpenTelemetry: %v", err)
	}
	defer shutdown(context.Background())

	fmt.Println("Development OpenTelemetry initialized with console output")
}

// Example_traceRunnable demonstrates tracing a runnable.
func Example_traceRunnable() {
	// Initialize OpenTelemetry
	shutdown, err := InitForDevelopment("example")
	if err != nil {
		log.Fatal(err)
	}
	defer shutdown(context.Background())

	// Create a telemetry provider
	provider, err := NewDefaultTelemetryProvider()
	if err != nil {
		log.Fatal(err)
	}

	// Create a runnable tracer
	tracer := NewRunnableTracer(provider)

	// Create a simple runnable
	myRunnable := runnable.NewInjectableRunnable("my-node", func(ctx context.Context, input interface{}) (interface{}, error) {
		// Simulate some work
		time.Sleep(100 * time.Millisecond)
		return "result: " + fmt.Sprintf("%v", input), nil
	})

	// Wrap with instrumentation
	tracedRunnable := tracer.TraceRunnable(myRunnable)

	// Execute
	ctx := context.Background()
	result, err := tracedRunnable.Invoke(ctx, "hello")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Result: %v\n", result)
	// Output:
	// Result: result: hello
}

// Example_traceRunnableFunc demonstrates tracing a runnable function.
func Example_traceRunnableFunc() {
	// Initialize OpenTelemetry
	shutdown, err := InitForDevelopment("example")
	if err != nil {
		log.Fatal(err)
	}
	defer shutdown(context.Background())

	provider, err := NewDefaultTelemetryProvider()
	if err != nil {
		log.Fatal(err)
	}

	tracer := NewRunnableTracer(provider)

	// Define a function
	myFunc := func(ctx context.Context, input interface{}) (interface{}, error) {
		time.Sleep(50 * time.Millisecond)
		return input, nil
	}

	// Wrap with instrumentation
	tracedFunc := tracer.TraceRunnableFunc("my-function", myFunc)

	// Execute
	ctx := context.Background()
	result, err := tracedFunc.Invoke(ctx, "test input")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Result: %v\n", result)
	// Output:
	// Result: test input
}

// Example_traceNode demonstrates tracing a node function.
func Example_traceNode() {
	// Initialize OpenTelemetry
	shutdown, err := InitForDevelopment("example")
	if err != nil {
		log.Fatal(err)
	}
	defer shutdown(context.Background())

	provider, err := NewDefaultTelemetryProvider()
	if err != nil {
		log.Fatal(err)
	}

	tracer := NewRunnableTracer(provider)

	// Define a node function
	nodeFunc := func(ctx context.Context, input interface{}) (interface{}, error) {
		// Simulate node execution
		time.Sleep(75 * time.Millisecond)
		return fmt.Sprintf("processed: %v", input), nil
	}

	// Wrap with instrumentation
	tracedNode := tracer.TraceNode("my-node", nodeFunc)

	// Execute
	ctx := context.Background()
	result, err := tracedNode(ctx, "input data")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Node result: %v\n", result)
	// Output:
	// Node result: processed: input data
}

// Example_instrumentRunnable demonstrates the convenience function.
func Example_instrumentRunnable() {
	// Initialize OpenTelemetry
	shutdown, err := InitForDevelopment("example")
	if err != nil {
		log.Fatal(err)
	}
	defer shutdown(context.Background())

	// Create a runnable
	myRunnable := runnable.NewInjectableRunnable("auto-instrumented", func(ctx context.Context, input interface{}) (interface{}, error) {
		time.Sleep(100 * time.Millisecond)
		return input, nil
	})

	// Instrument automatically
	tracedRunnable, err := InstrumentRunnable(myRunnable)
	if err != nil {
		log.Fatal(err)
	}

	// Execute
	ctx := context.Background()
	result, err := tracedRunnable.Invoke(ctx, "data")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Auto-instrumented result: %v\n", result)
	// Output:
	// Auto-instrumented result: data
}

// Example_withConfig demonstrates adding configuration to context.
func Example_withConfig() {
	// Initialize OpenTelemetry
	shutdown, err := InitForDevelopment("example")
	if err != nil {
		log.Fatal(err)
	}
	defer shutdown(context.Background())

	provider, err := NewDefaultTelemetryProvider()
	if err != nil {
		log.Fatal(err)
	}

	tracer := NewRunnableTracer(provider)

	// Create configuration
	config := &types.RunnableConfig{
		ThreadID: "thread-123",
	}

	// Create runnable with config context
	myRunnable := runnable.NewInjectableRunnable("configured-node", func(ctx context.Context, input interface{}) (interface{}, error) {
		time.Sleep(50 * time.Millisecond)
		return input, nil
	})

	tracedRunnable := tracer.TraceRunnable(myRunnable)

	// Execute with config context
	ctx := context.Background()
	ctx = WithConfig(config)(ctx)

	result, err := tracedRunnable.Invoke(ctx, "input")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Result with config: %v\n", result)
	// Output:
	// Result with config: input
}

// Example_traceCheckpoint demonstrates tracing checkpoint operations.
func Example_traceCheckpoint() {
	// Initialize OpenTelemetry
	shutdown, err := InitForDevelopment("example")
	if err != nil {
		log.Fatal(err)
	}
	defer shutdown(context.Background())

	provider, err := NewDefaultTelemetryProvider()
	if err != nil {
		log.Fatal(err)
	}

	tracer := NewRunnableTracer(provider)

	// Define a checkpoint save operation
	saveCheckpoint := func(ctx context.Context) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	// Wrap with instrumentation
	tracedSave := tracer.TraceCheckpoint("save", saveCheckpoint)

	// Execute
	ctx := context.Background()
	err = tracedSave(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Checkpoint saved successfully")
	// Output:
	// Checkpoint saved successfully
}

// Example_traceCache demonstrates tracing cache operations.
func Example_traceCache() {
	// Initialize OpenTelemetry
	shutdown, err := InitForDevelopment("example")
	if err != nil {
		log.Fatal(err)
	}
	defer shutdown(context.Background())

	provider, err := NewDefaultTelemetryProvider()
	if err != nil {
		log.Fatal(err)
	}

	tracer := NewRunnableTracer(provider)

	// Define a cache lookup operation
	cacheLookup := func(ctx context.Context) (interface{}, error) {
		time.Sleep(10 * time.Millisecond)
		return "cached-value", nil
	}

	// Wrap with instrumentation
	tracedLookup := tracer.TraceCache("lookup", cacheLookup)

	// Execute
	ctx := context.Background()
	value, err := tracedLookup(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Cache value: %v\n", value)
	// Output:
	// Cache value: cached-value
}

// Example_customConfig demonstrates custom configuration.
func Example_customConfig() {
	cfg := &Config{
		ServiceName:        "custom-service",
		ServiceVersion:     "2.0.0",
		Environment:        "staging",
		EnableTracing:      true,
		EnableMetrics:      true,
		SampleRate:         0.5,
		OTLPEndpoint:       "otel:4317",
		UseConsoleExporter: false,
		ResourceAttributes: map[string]string{
			"team":         "agent-harness",
			"owner":        "platform",
			"custom.field": "value",
		},
	}

	shutdown, err := Init(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer shutdown(context.Background())

	fmt.Println("Custom OpenTelemetry configuration initialized")
}

// Example_runInSpan demonstrates using span helpers.
func Example_runInSpan() {
	ctx := context.Background()

	err := RunInSpan(ctx, "my-operation", func(ctx context.Context) error {
		// Do some work
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Operation completed")
	// Output:
	// Operation completed
}

// Example_runInSpanWithResult demonstrates using span helpers with return values.
func Example_runInSpanWithResult() {
	ctx := context.Background()

	result, err := RunInSpanWithResult(ctx, "my-operation", func(ctx context.Context) (string, error) {
		time.Sleep(50 * time.Millisecond)
		return "success", nil
	})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Result: %v\n", result)
	// Output:
	// Result: success
}

// TestInstrumentation demonstrates comprehensive instrumentation.
func TestInstrumentation(t *testing.T) {
	// Initialize for testing (no side effects)
	shutdown, err := InitForTesting()
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())

	provider, err := NewDefaultTelemetryProvider()
	if err != nil {
		t.Fatal(err)
	}

	tracer := NewRunnableTracer(provider)

	// Test node execution
	node := func(ctx context.Context, input interface{}) (interface{}, error) {
		time.Sleep(10 * time.Millisecond)
		return fmt.Sprintf("processed: %v", input), nil
	}

	tracedNode := tracer.TraceNode("test-node", node)
	ctx := context.Background()
	result, err := tracedNode(ctx, "test")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := "processed: test"
	if result != expected {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}
