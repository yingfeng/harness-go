// Package telemetry provides OpenTelemetry observability support for LangGraph.
// It includes distributed tracing, metrics, and logging capabilities.
package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	// InstrumentationName is the name of the instrumentation.
	InstrumentationName = "github.com/infiniflow/ragflow/harness"
	// InstrumentationVersion is the version of the instrumentation.
	InstrumentationVersion = "1.0.0"
)

// SpanAttributes holds common attributes for spans.
var SpanAttributes = struct {
	NodeName         attribute.Key
	InputType        attribute.Key
	OutputType       attribute.Key
	ThreadID         attribute.Key
	CheckpointID     attribute.Key
	Step             attribute.Key
	ErrorType        attribute.Key
	InputSize        attribute.Key
	OutputSize       attribute.Key
	CacheHit         attribute.Key
	CacheKey         attribute.Key
	RetryAttempt     attribute.Key
	RetryMaxAttempts attribute.Key
	GraphType        attribute.Key
}{
	NodeName:         "langgraph.node_name",
	InputType:        "langgraph.input_type",
	OutputType:       "langgraph.output_type",
	ThreadID:         "langgraph.thread_id",
	CheckpointID:     "langgraph.checkpoint_id",
	Step:             "langgraph.step",
	ErrorType:        "langgraph.error_type",
	InputSize:        "langgraph.input_size",
	OutputSize:       "langgraph.output_size",
	CacheHit:         "langgraph.cache_hit",
	CacheKey:         "langgraph.cache_key",
	RetryAttempt:     "langgraph.retry_attempt",
	RetryMaxAttempts: "langgraph.retry_max_attempts",
	GraphType:        "langgraph.graph_type",
}

// MetricNames holds the names of common metrics.
var MetricNames = struct {
	NodeExecutionDuration     string
	NodeExecutionCount        string
	NodeErrorCount            string
	CacheHitCount             string
	CacheMissCount            string
	CheckpointSaveDuration    string
	CheckpointLoadDuration    string
	MessagesProcessedCount   string
	TokensProcessedCount      string
	StreamEventsEmittedCount  string
	BackgroundTaskCount       string
	BackgroundTaskDuration    string
}{
	NodeExecutionDuration:     "langgraph.node.execution.duration",
	NodeExecutionCount:        "langgraph.node.execution.count",
	NodeErrorCount:            "langgraph.node.error.count",
	CacheHitCount:             "langgraph.cache.hit.count",
	CacheMissCount:            "langgraph.cache.miss.count",
	CheckpointSaveDuration:    "langgraph.checkpoint.save.duration",
	CheckpointLoadDuration:    "langgraph.checkpoint.load.duration",
	MessagesProcessedCount:    "langgraph.messages.processed.count",
	TokensProcessedCount:      "langgraph.tokens.processed.count",
	StreamEventsEmittedCount:  "langgraph.stream.events.emitted.count",
	BackgroundTaskCount:       "langgraph.background.task.count",
	BackgroundTaskDuration:    "langgraph.background.task.duration",
}

// Metrics holds the metrics for instrumentation.
type Metrics struct {
	meter metric.Meter

	// Node metrics
	nodeExecutionDuration     metric.Float64Histogram
	nodeExecutionCount        metric.Int64Counter
	nodeErrorCount            metric.Int64Counter

	// Cache metrics
	cacheHitCount  metric.Int64Counter
	cacheMissCount metric.Int64Counter

	// Checkpoint metrics
	checkpointSaveDuration metric.Float64Histogram
	checkpointLoadDuration metric.Float64Histogram

	// Message metrics
	messagesProcessedCount metric.Int64Counter
	tokensProcessedCount   metric.Int64Counter

	// Stream metrics
	streamEventsEmittedCount metric.Int64Counter

	// Background task metrics
	backgroundTaskCount    metric.Int64Counter
	backgroundTaskDuration metric.Float64Histogram
}

// TracerProvider wraps the OpenTelemetry tracer provider.
type TracerProvider struct {
	tracer trace.Tracer
}

// TelemetryProvider provides the complete telemetry functionality.
type TelemetryProvider struct {
	TracerProvider *TracerProvider
	Metrics         *Metrics
}

// NewMetrics initializes the metrics.
func NewMetrics(meter metric.Meter) (*Metrics, error) {
	nodeExecutionDuration, err := meter.Float64Histogram(
		MetricNames.NodeExecutionDuration,
		metric.WithDescription("Duration of node execution"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, err
	}

	nodeExecutionCount, err := meter.Int64Counter(
		MetricNames.NodeExecutionCount,
		metric.WithDescription("Count of node executions"),
		metric.WithUnit("{execution}"),
	)
	if err != nil {
		return nil, err
	}

	nodeErrorCount, err := meter.Int64Counter(
		MetricNames.NodeErrorCount,
		metric.WithDescription("Count of node errors"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		return nil, err
	}

	cacheHitCount, err := meter.Int64Counter(
		MetricNames.CacheHitCount,
		metric.WithDescription("Count of cache hits"),
		metric.WithUnit("{hit}"),
	)
	if err != nil {
		return nil, err
	}

	cacheMissCount, err := meter.Int64Counter(
		MetricNames.CacheMissCount,
		metric.WithDescription("Count of cache misses"),
		metric.WithUnit("{miss}"),
	)
	if err != nil {
		return nil, err
	}

	checkpointSaveDuration, err := meter.Float64Histogram(
		MetricNames.CheckpointSaveDuration,
		metric.WithDescription("Duration of checkpoint saves"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, err
	}

	checkpointLoadDuration, err := meter.Float64Histogram(
		MetricNames.CheckpointLoadDuration,
		metric.WithDescription("Duration of checkpoint loads"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, err
	}

	messagesProcessedCount, err := meter.Int64Counter(
		MetricNames.MessagesProcessedCount,
		metric.WithDescription("Count of messages processed"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	tokensProcessedCount, err := meter.Int64Counter(
		MetricNames.TokensProcessedCount,
		metric.WithDescription("Count of tokens processed"),
		metric.WithUnit("{token}"),
	)
	if err != nil {
		return nil, err
	}

	streamEventsEmittedCount, err := meter.Int64Counter(
		MetricNames.StreamEventsEmittedCount,
		metric.WithDescription("Count of stream events emitted"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return nil, err
	}

	backgroundTaskCount, err := meter.Int64Counter(
		MetricNames.BackgroundTaskCount,
		metric.WithDescription("Count of background tasks executed"),
		metric.WithUnit("{task}"),
	)
	if err != nil {
		return nil, err
	}

	backgroundTaskDuration, err := meter.Float64Histogram(
		MetricNames.BackgroundTaskDuration,
		metric.WithDescription("Duration of background task execution"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, err
	}

	return &Metrics{
		meter:                    meter,
		nodeExecutionDuration:    nodeExecutionDuration,
		nodeExecutionCount:       nodeExecutionCount,
		nodeErrorCount:           nodeErrorCount,
		cacheHitCount:            cacheHitCount,
		cacheMissCount:           cacheMissCount,
		checkpointSaveDuration:   checkpointSaveDuration,
		checkpointLoadDuration:   checkpointLoadDuration,
		messagesProcessedCount:   messagesProcessedCount,
		tokensProcessedCount:     tokensProcessedCount,
		streamEventsEmittedCount: streamEventsEmittedCount,
		backgroundTaskCount:      backgroundTaskCount,
		backgroundTaskDuration:  backgroundTaskDuration,
	}, nil
}

// NewTracerProvider creates a new tracer provider.
func NewTracerProvider(tracerProvider trace.TracerProvider) *TracerProvider {
	tracer := tracerProvider.Tracer(
		InstrumentationName,
		trace.WithInstrumentationVersion(InstrumentationVersion),
	)
	return &TracerProvider{
		tracer: tracer,
	}
}

// Tracer returns the underlying tracer.
func (tp *TracerProvider) Tracer() trace.Tracer {
	return tp.tracer
}

// NewTelemetryProvider creates a new telemetry provider.
func NewTelemetryProvider(tracerProvider trace.TracerProvider, meterProvider metric.MeterProvider) (*TelemetryProvider, error) {
	meter := meterProvider.Meter(
		InstrumentationName,
		metric.WithInstrumentationVersion(InstrumentationVersion),
	)

	metrics, err := NewMetrics(meter)
	if err != nil {
		return nil, err
	}

	return &TelemetryProvider{
		TracerProvider: NewTracerProvider(tracerProvider),
		Metrics:         metrics,
	}, nil
}

// RecordNodeExecution records a node execution with metrics and tracing.
func (m *Metrics) RecordNodeExecution(ctx context.Context, nodeName, inputType, outputType string, duration time.Duration, err error) {
	attrs := []attribute.KeyValue{
		SpanAttributes.NodeName.String(nodeName),
		SpanAttributes.InputType.String(inputType),
		SpanAttributes.OutputType.String(outputType),
	}

	if err != nil {
		attrs = append(attrs, SpanAttributes.ErrorType.String(err.Error()))
	}

	// Record duration
	m.nodeExecutionDuration.Record(ctx, float64(duration.Milliseconds()), metric.WithAttributes(attrs...))

	// Increment execution count
	m.nodeExecutionCount.Add(ctx, 1, metric.WithAttributes(attrs...))

	// Increment error count if error occurred
	if err != nil {
		m.nodeErrorCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

// RecordCacheHit records a cache hit.
func (m *Metrics) RecordCacheHit(ctx context.Context, cacheKey string) {
	attrs := []attribute.KeyValue{SpanAttributes.CacheKey.String(cacheKey)}
	m.cacheHitCount.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordCacheMiss records a cache miss.
func (m *Metrics) RecordCacheMiss(ctx context.Context, cacheKey string) {
	attrs := []attribute.KeyValue{SpanAttributes.CacheKey.String(cacheKey)}
	m.cacheMissCount.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordCheckpointSave records a checkpoint save operation.
func (m *Metrics) RecordCheckpointSave(ctx context.Context, duration time.Duration) {
	m.checkpointSaveDuration.Record(ctx, float64(duration.Milliseconds()))
}

// RecordCheckpointLoad records a checkpoint load operation.
func (m *Metrics) RecordCheckpointLoad(ctx context.Context, duration time.Duration) {
	m.checkpointLoadDuration.Record(ctx, float64(duration.Milliseconds()))
}

// RecordMessagesProcessed records the number of messages processed.
func (m *Metrics) RecordMessagesProcessed(ctx context.Context, count int64) {
	m.messagesProcessedCount.Add(ctx, count)
}

// RecordTokensProcessed records the number of tokens processed.
func (m *Metrics) RecordTokensProcessed(ctx context.Context, count int64) {
	m.tokensProcessedCount.Add(ctx, count)
}

// RecordStreamEventEmitted records a stream event emitted.
func (m *Metrics) RecordStreamEventEmitted(ctx context.Context, eventType string) {
	attrs := []attribute.KeyValue{attribute.String("event_type", eventType)}
	m.streamEventsEmittedCount.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordBackgroundTask records a background task execution.
func (m *Metrics) RecordBackgroundTask(ctx context.Context, taskType string, duration time.Duration, err error) {
	attrs := []attribute.KeyValue{attribute.String("task_type", taskType)}
	if err != nil {
		attrs = append(attrs, attribute.Bool("error", true))
	}
	m.backgroundTaskCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	m.backgroundTaskDuration.Record(ctx, float64(duration.Milliseconds()), metric.WithAttributes(attrs...))
}

// StartNodeSpan starts a span for node execution.
func (tp *TracerProvider) StartNodeSpan(ctx context.Context, nodeName string) (context.Context, trace.Span) {
	return tp.tracer.Start(ctx, "node."+nodeName,
		trace.WithAttributes(SpanAttributes.NodeName.String(nodeName)),
		trace.WithSpanKind(trace.SpanKindInternal),
	)
}

// StartCheckpointSpan starts a span for checkpoint operations.
func (tp *TracerProvider) StartCheckpointSpan(ctx context.Context, operation string) (context.Context, trace.Span) {
	return tp.tracer.Start(ctx, "checkpoint."+operation,
		trace.WithAttributes(attribute.String("operation", operation)),
		trace.WithSpanKind(trace.SpanKindClient),
	)
}

// StartCacheSpan starts a span for cache operations.
func (tp *TracerProvider) StartCacheSpan(ctx context.Context, operation string) (context.Context, trace.Span) {
	return tp.tracer.Start(ctx, "cache."+operation,
		trace.WithAttributes(attribute.String("operation", operation)),
		trace.WithSpanKind(trace.SpanKindInternal),
	)
}

// SetSpanError sets an error on the span.
func SetSpanError(span trace.Span, err error) {
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
	}
}

// SetSpanAttributes sets multiple attributes on the span.
func SetSpanAttributes(span trace.Span, attrs []attribute.KeyValue) {
	span.SetAttributes(attrs...)
}

// SetSpanInputSize sets the input size attribute.
func SetSpanInputSize(span trace.Span, size int) {
	span.SetAttributes(SpanAttributes.InputSize.Int(size))
}

// SetSpanOutputSize sets the output size attribute.
func SetSpanOutputSize(span trace.Span, size int) {
	span.SetAttributes(SpanAttributes.OutputSize.Int(size))
}

// DefaultTracerProvider returns the global tracer provider.
func DefaultTracerProvider() *TracerProvider {
	return NewTracerProvider(otel.GetTracerProvider())
}

// DefaultMeterProvider returns the global meter provider.
func DefaultMeterProvider() metric.MeterProvider {
	return otel.GetMeterProvider()
}

// NewDefaultTelemetryProvider creates a telemetry provider using global providers.
func NewDefaultTelemetryProvider() (*TelemetryProvider, error) {
	return NewTelemetryProvider(otel.GetTracerProvider(), otel.GetMeterProvider())
}
