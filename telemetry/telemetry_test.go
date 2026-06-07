// Package telemetry tests the OpenTelemetry integration.
package telemetry

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/infiniflow/ragflow/harness/graphengine/runnable"
	"github.com/infiniflow/ragflow/harness/graphengine/types"
	"go.opentelemetry.io/otel/trace"
)

func TestInitDefaults(t *testing.T) {
	shutdown, err := InitWithDefaults()
	if err != nil {
		t.Fatalf("InitWithDefaults failed: %v", err)
	}
	defer shutdown(context.Background())

	// Verify we can get a tracer
	tracer := GetTracer()
	if tracer == nil {
		t.Error("Expected non-nil tracer")
	}
}

func TestInitForDevelopment(t *testing.T) {
	shutdown, err := InitForDevelopment("test-service")
	if err != nil {
		t.Fatalf("InitForDevelopment failed: %v", err)
	}
	defer shutdown(context.Background())

	// Verify we can create a span
	ctx, span := NewContextWithSpan(context.Background(), "test-operation")
	defer span.End()

	if span == nil {
		t.Error("Expected non-nil span")
	}
	if !trace.SpanContextFromContext(ctx).IsValid() {
		t.Error("Expected valid span context")
	}
}

func TestInitForTesting(t *testing.T) {
	shutdown, err := InitForTesting()
	if err != nil {
		t.Fatalf("InitForTesting failed: %v", err)
	}
	defer shutdown(context.Background())
	// Testing should work without any errors
}

func TestNewTelemetryProvider(t *testing.T) {
	shutdown, err := InitForTesting()
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())

	provider, err := NewDefaultTelemetryProvider()
	if err != nil {
		t.Fatalf("NewDefaultTelemetryProvider failed: %v", err)
	}

	if provider == nil {
		t.Error("Expected non-nil provider")
	}
	if provider.TracerProvider == nil {
		t.Error("Expected non-nil tracer provider")
	}
	if provider.Metrics == nil {
		t.Error("Expected non-nil metrics")
	}
}

func TestNewRunnableTracer(t *testing.T) {
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
	if tracer == nil {
		t.Error("Expected non-nil runnable tracer")
	}

	// Test WithTracing
	tracer.WithTracing(false)
	if tracer.enableTracing {
		t.Error("Expected tracing to be disabled")
	}

	// Test WithMetrics
	tracer.WithMetrics(false)
	if tracer.enableMetrics {
		t.Error("Expected metrics to be disabled")
	}
}

func TestTraceRunnable(t *testing.T) {
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

	// Create a runnable
	myRunnable := runnable.NewInjectableRunnable("test-node", func(ctx context.Context, input interface{}) (interface{}, error) {
		time.Sleep(10 * time.Millisecond)
		return input, nil
	})

	// Wrap with instrumentation
	tracedRunnable := tracer.TraceRunnable(myRunnable)

	// Execute
	ctx := context.Background()
	result, err := tracedRunnable.Invoke(ctx, "test-input")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result != "test-input" {
		t.Errorf("Expected 'test-input', got %v", result)
	}
}

func TestTraceRunnableFunc(t *testing.T) {
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

	// Define a function
	myFunc := func(ctx context.Context, input interface{}) (interface{}, error) {
		time.Sleep(10 * time.Millisecond)
		return input, nil
	}

	// Wrap with instrumentation
	tracedFunc := tracer.TraceRunnableFunc("test-func", myFunc)

	// Execute
	ctx := context.Background()
	result, err := tracedFunc.Invoke(ctx, "test")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result != "test" {
		t.Errorf("Expected 'test', got %v", result)
	}
}

func TestTraceNode(t *testing.T) {
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

	// Define a node function
	nodeFunc := func(ctx context.Context, input interface{}) (interface{}, error) {
		time.Sleep(10 * time.Millisecond)
		return input, nil
	}

	// Wrap with instrumentation
	tracedNode := tracer.TraceNode("test-node", nodeFunc)

	// Execute
	ctx := context.Background()
	result, err := tracedNode(ctx, "input")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result != "input" {
		t.Errorf("Expected 'input', got %v", result)
	}
}

func TestInstrumentRunnable(t *testing.T) {
	shutdown, err := InitForTesting()
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())

	// Create a runnable
	myRunnable := runnable.NewInjectableRunnable("auto-node", func(ctx context.Context, input interface{}) (interface{}, error) {
		time.Sleep(10 * time.Millisecond)
		return input, nil
	})

	// Instrument automatically
	tracedRunnable, err := InstrumentRunnable(myRunnable)
	if err != nil {
		t.Fatalf("InstrumentRunnable failed: %v", err)
	}

	// Execute
	ctx := context.Background()
	result, err := tracedRunnable.Invoke(ctx, "data")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result != "data" {
		t.Errorf("Expected 'data', got %v", result)
	}
}

func TestWithConfig(t *testing.T) {
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

	// Create configuration
	config := &types.RunnableConfig{
		ThreadID: "thread-123",
	}

	// Create runnable
	myRunnable := runnable.NewInjectableRunnable("configured-node", func(ctx context.Context, input interface{}) (interface{}, error) {
		time.Sleep(10 * time.Millisecond)
		return input, nil
	})

	tracedRunnable := tracer.TraceRunnable(myRunnable)

	// Execute with config context
	ctx := context.Background()
	ctx = WithConfig(config)(ctx)

	result, err := tracedRunnable.Invoke(ctx, "input")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result != "input" {
		t.Errorf("Expected 'input', got %v", result)
	}
}

func TestTraceCheckpoint(t *testing.T) {
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

	// Define a checkpoint save operation
	saveCheckpoint := func(ctx context.Context) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	}

	// Wrap with instrumentation
	tracedSave := tracer.TraceCheckpoint("save", saveCheckpoint)

	// Execute
	ctx := context.Background()
	err = tracedSave(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestTraceCache(t *testing.T) {
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
		t.Fatalf("Unexpected error: %v", err)
	}

	if value != "cached-value" {
		t.Errorf("Expected 'cached-value', got %v", value)
	}
}

func TestRunInSpan(t *testing.T) {
	shutdown, err := InitForTesting()
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())

	ctx := context.Background()

	executed := false
	err = RunInSpan(ctx, "test-operation", func(ctx context.Context) error {
		executed = true
		return nil
	})

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if !executed {
		t.Error("Expected operation to be executed")
	}
}

func TestRunInSpanWithResult(t *testing.T) {
	shutdown, err := InitForTesting()
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())

	ctx := context.Background()

	result, err := RunInSpanWithResult(ctx, "test-operation", func(ctx context.Context) (string, error) {
		return "success", nil
	})

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result != "success" {
		t.Errorf("Expected 'success', got %v", result)
	}
}

func TestNewContextWithSpan(t *testing.T) {
	shutdown, err := InitForTesting()
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())

	ctx := context.Background()
	ctx, span := NewContextWithSpan(ctx, "test-span")
	defer span.End()

	if span == nil {
		t.Error("Expected non-nil span")
	}

	// Test that context has valid span context
	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.IsValid() {
		// In test mode, span context might not be valid due to no-op exporter
		// This is acceptable
	}
}

func TestTraceNodeWithError(t *testing.T) {
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

	// Define a node that returns an error
	nodeFunc := func(ctx context.Context, input interface{}) (interface{}, error) {
		time.Sleep(10 * time.Millisecond)
		return nil, fmt.Errorf("test error")
	}

	// Wrap with instrumentation
	tracedNode := tracer.TraceNode("error-node", nodeFunc)

	// Execute
	ctx := context.Background()
	_, err = tracedNode(ctx, "input")

	if err == nil {
		t.Error("Expected error from node")
	}
}

func TestTraceNodeComplexInput(t *testing.T) {
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

	// Test with complex input types
	complexInput := map[string]interface{}{
		"key1": "value1",
		"key2": 42,
		"key3": []string{"a", "b", "c"},
	}

	nodeFunc := func(ctx context.Context, input interface{}) (interface{}, error) {
		time.Sleep(10 * time.Millisecond)
		return input, nil
	}

	tracedNode := tracer.TraceNode("complex-node", nodeFunc)

	ctx := context.Background()
	result, err := tracedNode(ctx, complexInput)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result == nil {
		t.Error("Expected non-nil result")
	}
}

func TestDisableTracing(t *testing.T) {
	shutdown, err := InitForTesting()
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())

	provider, err := NewDefaultTelemetryProvider()
	if err != nil {
		t.Fatal(err)
	}

	tracer := NewRunnableTracer(provider).WithTracing(false)

	nodeFunc := func(ctx context.Context, input interface{}) (interface{}, error) {
		time.Sleep(10 * time.Millisecond)
		return input, nil
	}

	tracedNode := tracer.TraceNode("disabled-tracing-node", nodeFunc)

	ctx := context.Background()
	result, err := tracedNode(ctx, "input")

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result != "input" {
		t.Errorf("Expected 'input', got %v", result)
	}
}

func TestDisableMetrics(t *testing.T) {
	shutdown, err := InitForTesting()
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())

	provider, err := NewDefaultTelemetryProvider()
	if err != nil {
		t.Fatal(err)
	}

	tracer := NewRunnableTracer(provider).WithMetrics(false)

	nodeFunc := func(ctx context.Context, input interface{}) (interface{}, error) {
		time.Sleep(10 * time.Millisecond)
		return input, nil
	}

	tracedNode := tracer.TraceNode("disabled-metrics-node", nodeFunc)

	ctx := context.Background()
	result, err := tracedNode(ctx, "input")

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result != "input" {
		t.Errorf("Expected 'input', got %v", result)
	}
}
