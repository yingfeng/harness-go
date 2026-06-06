# OpenTelemetry Observability for LangGraph Go

This package provides comprehensive OpenTelemetry observability support for LangGraph Go, including distributed tracing, metrics, and logging capabilities.

## Features

- ✅ **Distributed Tracing**: Trace node executions, checkpoint operations, and cache operations
- ✅ **Metrics**: Collect performance metrics for execution duration, error rates, cache hit/miss ratios
- ✅ **Context Propagation**: Automatic trace context propagation across components
- ✅ **Multiple Exporters**: Support for OTLP (Jaeger, Tempo, etc.) and console exporters
- ✅ **Configurable Sampling**: Control trace sampling rate for production environments
- ✅ **Seamless Integration**: Easy integration with existing runnable components

## Installation

```bash
go get go.opentelemetry.io/otel
go get go.opentelemetry.io/otel/sdk
go get go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc
go get go.opentelemetry.io/otel/exporters/stdout/stdouttrace
```

## Quick Start

### Basic Initialization

```go
package main

import (
    "context"
    "log"
    
    "github.com/infiniflow/ragflow/harness/telemetry"
)

func main() {
    // Initialize OpenTelemetry with default settings
    shutdown, err := telemetry.InitWithDefaults()
    if err != nil {
        log.Fatal(err)
    }
    defer shutdown(context.Background())
    
    // Your application code here
}
```

### Production Setup

```go
// Initialize for production with OTLP exporter
shutdown, err := telemetry.InitForProduction(
    "my-langgraph-app",
    "1.0.0",
    "production",
    "otel-collector:4317",
)
if err != nil {
    log.Fatal(err)
}
defer shutdown(context.Background())
```

### Development Setup

```go
// Initialize for development with console exporter
shutdown, err := telemetry.InitForDevelopment("my-langgraph-dev")
if err != nil {
    log.Fatal(err)
}
defer shutdown(context.Background())
```

## Usage

### Instrumenting Runnable Components

```go
import (
    "github.com/infiniflow/ragflow/harness/runnable"
    "github.com/infiniflow/ragflow/harness/telemetry"
)

// Create a telemetry provider
provider, err := telemetry.NewDefaultTelemetryProvider()
if err != nil {
    log.Fatal(err)
}

// Create a runnable tracer
tracer := telemetry.NewRunnableTracer(provider)

// Create your runnable
myRunnable := runnable.NewInjectableRunnable("my-node", func(ctx context.Context, input interface{}) (interface{}, error) {
    // Your node logic here
    return "result", nil
})

// Wrap with instrumentation
tracedRunnable := tracer.TraceRunnable(myRunnable)

// Execute
result, err := tracedRunnable.Invoke(ctx, input)
```

### Automatic Instrumentation

```go
// Use the convenience function for automatic instrumentation
tracedRunnable, err := telemetry.InstrumentRunnable(myRunnable)
if err != nil {
    log.Fatal(err)
}
```

### Instrumenting Node Functions

```go
// Define a node function
nodeFunc := func(ctx context.Context, input interface{}) (interface{}, error) {
    // Your node logic
    return input, nil
}

// Wrap with instrumentation
tracedNode := tracer.TraceNode("my-node", nodeFunc)

// Execute
result, err := tracedNode(ctx, input)
```

### Adding Configuration Context

```go
import "github.com/infiniflow/ragflow/harness/types"

// Create configuration
config := &types.RunnableConfig{
    ThreadID:     "thread-123",
    CheckpointID: "checkpoint-456",
}

// Add to context
ctx := telemetry.WithConfig(config)(context.Background())

// Execute with enhanced context
result, err := tracedRunnable.Invoke(ctx, input)
```

### Manual Span Management

```go
// Create a span manually
ctx, span := telemetry.NewContextWithSpan(context.Background(), "my-operation")
defer span.End()

// Do work in this span
err := doSomeWork(ctx)
if err != nil {
    telemetry.SetSpanError(span, err)
}
```

### Using Span Helpers

```go
// Run code within a span
err := telemetry.RunInSpan(ctx, "my-operation", func(ctx context.Context) error {
    // Your code here
    return nil
})

// Run code with return value
result, err := telemetry.RunInSpanWithResult(ctx, "my-operation", 
    func(ctx context.Context) (string, error) {
        return "success", nil
    }
)
```

## Configuration

### Custom Configuration

```go
cfg := &telemetry.Config{
    ServiceName:        "my-service",
    ServiceVersion:     "1.0.0",
    Environment:        "production",
    EnableTracing:      true,
    EnableMetrics:      true,
    SampleRate:         0.1, // Sample 10% of traces
    OTLPEndpoint:       "otel-collector:4317",
    UseConsoleExporter: false,
    ResourceAttributes: map[string]string{
        "team":  "langgraph",
        "owner": "platform",
    },
}

shutdown, err := telemetry.Init(cfg)
defer shutdown(context.Background())
```

### Environment Variables

- `OTEL_SERVICE_NAME`: Service name (default: "harness-go")
- `OTEL_EXPORTER_CONSOLE`: Set to "true" to use console exporter
- `OTEL_EXPORTER_OTLP_ENDPOINT`: OTLP collector endpoint (default: "localhost:4317")

## Metrics

The following metrics are automatically collected:

- `langgraph.node.execution.duration`: Duration of node execution (ms)
- `langgraph.node.execution.count`: Count of node executions
- `langgraph.node.error.count`: Count of node errors
- `langgraph.cache.hit.count`: Count of cache hits
- `langgraph.cache.miss.count`: Count of cache misses
- `langgraph.checkpoint.save.duration`: Duration of checkpoint saves (ms)
- `langgraph.checkpoint.load.duration`: Duration of checkpoint loads (ms)
- `langgraph.messages.processed.count`: Count of messages processed
- `langgraph.tokens.processed.count`: Count of tokens processed
- `langgraph.stream.events.emitted.count`: Count of stream events emitted

### Recording Custom Metrics

```go
provider, _ := telemetry.NewDefaultTelemetryProvider()
metrics := provider.Metrics

// Record a custom node execution
ctx := context.Background()
metrics.RecordNodeExecution(ctx, "my-node", "string", "string", 100*time.Millisecond, nil)

// Record cache hit/miss
metrics.RecordCacheHit(ctx, "cache-key")
metrics.RecordCacheMiss(ctx, "cache-key")

// Record messages processed
metrics.RecordMessagesProcessed(ctx, 5)

// Record tokens processed
metrics.RecordTokensProcessed(ctx, 128)
```

## Spans

The following spans are automatically created:

- `node.<node_name>`: Node execution spans
- `checkpoint.<operation>`: Checkpoint operation spans
- `cache.<operation>`: Cache operation spans

### Span Attributes

Common span attributes include:

- `langgraph.node_name`: Name of the node
- `langgraph.input_type`: Type of input
- `langgraph.output_type`: Type of output
- `langgraph.thread_id`: Thread identifier
- `langgraph.checkpoint_id`: Checkpoint identifier
- `langgraph.step`: Step number
- `langgraph.error_type`: Type of error (if any)
- `langgraph.input_size`: Estimated input size in bytes
- `langgraph.output_size`: Estimated output size in bytes
- `langgraph.cache_hit`: Whether cache was hit
- `langgraph.cache_key`: Cache key
- `langgraph.retry_attempt`: Retry attempt number
- `langgraph.retry_max_attempts`: Maximum retry attempts
- `langgraph.graph_type`: Type of graph

## Integration with Popular Observability Platforms

### Jaeger

```go
// Initialize with OTLP exporter pointing to Jaeger
shutdown, err := telemetry.InitForProduction(
    "my-app",
    "1.0.0",
    "production",
    "jaeger:4317",
)
```

### Grafana Tempo

```go
// Initialize with OTLP exporter pointing to Tempo
shutdown, err := telemetry.InitForProduction(
    "my-app",
    "1.0.0",
    "production",
    "tempo:4317",
)
```

### Honeycomb

```go
// Initialize with OTLP exporter pointing to Honeycomb
shutdown, err := telemetry.InitForProduction(
    "my-app",
    "1.0.0",
    "production",
    "api.honeycomb.io:443",
)
```

## Testing

```go
// Use InitForTesting to avoid side effects in tests
shutdown, err := telemetry.InitForTesting()
if err != nil {
    t.Fatal(err)
}
defer shutdown(context.Background())
```

## Best Practices

1. **Initialize Early**: Call `Init()` at the start of your application
2. **Shutdown Gracefully**: Always call the shutdown function when exiting
3. **Use Appropriate Sampling**: Set sample rate lower in production (e.g., 0.1)
4. **Add Context**: Include relevant context (thread ID, checkpoint ID) in spans
5. **Handle Errors**: Record errors in spans using `SetSpanError()`
6. **Test Locally**: Use console exporter during development
7. **Production Exporter**: Use OTLP exporter with a collector in production

## Advanced Usage

### Custom Span Names

```go
// Create span with custom name
ctx, span := telemetry.NewContextWithSpan(ctx, "custom.operation.name")
defer span.End()
```

### Adding Custom Attributes

```go
span.SetAttributes(
    attribute.String("custom.key", "custom.value"),
    attribute.Int("custom.number", 42),
)
```

### Nested Spans

```go
ctx, parentSpan := telemetry.NewContextWithSpan(ctx, "parent")
defer parentSpan.End()

// Child span automatically gets parent context
ctx, childSpan := telemetry.NewContextWithSpan(ctx, "child")
defer childSpan.End()
```

## Examples

See `example_test.go` for comprehensive usage examples.

## License

See LICENSE file for details.
