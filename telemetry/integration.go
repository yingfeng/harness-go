// Package telemetry provides OpenTelemetry integrations for runnable components.
package telemetry

import (
	"context"
	"fmt"
	"time"

	"github.com/infiniflow/ragflow/harness/graphengine/runnable"
	"github.com/infiniflow/ragflow/harness/graphengine/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// RunnableTracer is an OpenTelemetry-based tracer for runnable components.
type RunnableTracer struct {
	provider    *TelemetryProvider
	enableTracing bool
	enableMetrics bool
}

// NewRunnableTracer creates a new runnable tracer with OpenTelemetry support.
func NewRunnableTracer(provider *TelemetryProvider) *RunnableTracer {
	return &RunnableTracer{
		provider:      provider,
		enableTracing: true,
		enableMetrics: true,
	}
}

// WithTracing enables or disables tracing.
func (rt *RunnableTracer) WithTracing(enabled bool) *RunnableTracer {
	rt.enableTracing = enabled
	return rt
}

// WithMetrics enables or disables metrics.
func (rt *RunnableTracer) WithMetrics(enabled bool) *RunnableTracer {
	rt.enableMetrics = enabled
	return rt
}

// TraceRunnable wraps an InjectableRunnable with OpenTelemetry instrumentation.
func (rt *RunnableTracer) TraceRunnable(r *runnable.InjectableRunnable) *runnable.InjectableRunnable {
	return r.WithCallback(rt.createCallback())
}

// TraceRunnableFunc wraps a runnable function with OpenTelemetry instrumentation.
func (rt *RunnableTracer) TraceRunnableFunc(name string, fn func(context.Context, interface{}) (interface{}, error)) *runnable.InjectableRunnable {
	return rt.TraceRunnable(runnable.NewInjectableRunnable(name, fn))
}

// createCallback creates a callback that records telemetry data.
func (rt *RunnableTracer) createCallback() runnable.Callback {
	return &runnable.CallbackFunc{
		OnStartFunc: func(ctx context.Context, name string, input interface{}) {
			if !rt.enableTracing {
				return
			}
			
			// Get the current span from context if available
			span := trace.SpanFromContext(ctx)
			if !span.IsRecording() {
				// Start a new span if not already tracing
				_, newSpan := rt.provider.TracerProvider.StartNodeSpan(ctx, name)
				if inputCtx := getContextWithSpan(ctx, newSpan); inputCtx != ctx {
					// Replace context with span
					// Note: This is a limitation of the callback interface
				}
			}
			
			// Record input type
			inputType := fmt.Sprintf("%T", input)
			if span := trace.SpanFromContext(ctx); span.IsRecording() {
				span.SetAttributes(
					SpanAttributes.InputType.String(inputType),
					SpanAttributes.NodeName.String(name),
				)
				SetSpanInputSize(span, estimateSize(input))
			}
		},
		OnEndFunc: func(ctx context.Context, name string, output interface{}, err error) {
			if !rt.enableTracing && !rt.enableMetrics {
				return
			}
			
			span := trace.SpanFromContext(ctx)
			if !span.IsRecording() {
				return
			}
			
			// Record output type
			outputType := fmt.Sprintf("%T", output)
			span.SetAttributes(SpanAttributes.OutputType.String(outputType))
			SetSpanOutputSize(span, estimateSize(output))
			
			// Set error if any
			if err != nil {
				SetSpanError(span, err)
				span.SetStatus(codes.Error, err.Error())
			} else {
				span.SetStatus(codes.Ok, "")
			}
			
			// End the span
			span.End()
		},
		OnErrorFunc: func(ctx context.Context, name string, err error) {
			if !rt.enableTracing {
				return
			}
			
			span := trace.SpanFromContext(ctx)
			if span.IsRecording() {
				SetSpanError(span, err)
				span.SetStatus(codes.Error, err.Error())
			}
		},
	}
}

// TraceNode wraps a node function with detailed instrumentation.
func (rt *RunnableTracer) TraceNode(nodeName string, fn func(context.Context, interface{}) (interface{}, error)) func(context.Context, interface{}) (interface{}, error) {
	return func(ctx context.Context, input interface{}) (interface{}, error) {
		// Start timing
		inputType := fmt.Sprintf("%T", input)
		
		// Start span
		var span trace.Span
		if rt.enableTracing {
			ctx, span = rt.provider.TracerProvider.StartNodeSpan(ctx, nodeName)
			span.SetAttributes(
				SpanAttributes.NodeName.String(nodeName),
				SpanAttributes.InputType.String(inputType),
				SpanAttributes.InputSize.Int(estimateSize(input)),
			)
		}
		
		// Execute the function
		output, err := fn(ctx, input)
		
		// End timing
		duration := time.Since(time.Now())
		
		// Record metrics
		if rt.enableMetrics {
			outputType := fmt.Sprintf("%T", output)
			rt.provider.Metrics.RecordNodeExecution(ctx, nodeName, inputType, outputType, duration, err)
		}
		
		// End span
		if rt.enableTracing && span.IsRecording() {
			span.SetAttributes(SpanAttributes.OutputType.String(fmt.Sprintf("%T", output)))
			SetSpanOutputSize(span, estimateSize(output))
			
			if err != nil {
				SetSpanError(span, err)
				span.SetStatus(codes.Error, err.Error())
			} else {
				span.SetStatus(codes.Ok, "")
			}
			span.End()
		}
		
		return output, err
	}
}

// TraceCheckpoint wraps checkpoint operations with instrumentation.
func (rt *RunnableTracer) TraceCheckpoint(operation string, fn func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		startTime := time.Now()
		
		var span trace.Span
		if rt.enableTracing {
			ctx, span = rt.provider.TracerProvider.StartCheckpointSpan(ctx, operation)
		}
		
		err := fn(ctx)
		
		duration := time.Since(startTime)
		
		if rt.enableMetrics {
			if operation == "save" {
				rt.provider.Metrics.RecordCheckpointSave(ctx, duration)
			} else if operation == "load" {
				rt.provider.Metrics.RecordCheckpointLoad(ctx, duration)
			}
		}
		
		if rt.enableTracing && span.IsRecording() {
			if err != nil {
				SetSpanError(span, err)
			}
			span.End()
		}
		
		return err
	}
}

// TraceCache wraps cache operations with instrumentation.
func (rt *RunnableTracer) TraceCache(operation string, fn func(context.Context) (interface{}, error)) func(context.Context) (interface{}, error) {
	return func(ctx context.Context) (interface{}, error) {
		startTime := time.Now()

		var span trace.Span
		if rt.enableTracing {
			ctx, span = rt.provider.TracerProvider.StartCacheSpan(ctx, operation)
		}

		result, err := fn(ctx)

		if rt.enableMetrics {
			duration := time.Since(startTime)
			if err == nil {
				rt.provider.Metrics.RecordCacheHit(ctx, operation)
			} else {
				rt.provider.Metrics.RecordCacheMiss(ctx, operation)
			}
			_ = duration // available for future cache latency metrics
		}

		if rt.enableTracing && span.IsRecording() {
			if err != nil {
				SetSpanError(span, err)
			}
			span.End()
		}
		
		return result, err
	}
}

// WithConfig adds runnable configuration attributes to the span.
func WithConfig(config *types.RunnableConfig) func(context.Context) context.Context {
	return func(ctx context.Context) context.Context {
		span := trace.SpanFromContext(ctx)
		if !span.IsRecording() {
			return ctx
		}
		
		attrs := []attribute.KeyValue{}
		if config != nil {
			if config.ThreadID != "" {
				attrs = append(attrs, SpanAttributes.ThreadID.String(config.ThreadID))
			}
		}
		
		if len(attrs) > 0 {
			span.SetAttributes(attrs...)
		}
		
		return ctx
	}
}

// estimateSize estimates the size of a value in bytes.
// This is a rough estimation for telemetry purposes.
func estimateSize(v interface{}) int {
	if v == nil {
		return 0
	}
	
	switch val := v.(type) {
	case string:
		return len(val)
	case []string:
		size := 0
		for _, s := range val {
			size += len(s)
		}
		return size
	case []byte:
		return len(val)
	case map[string]interface{}:
		size := 0
		for key, value := range val {
			size += len(key)
			size += estimateSize(value)
		}
		return size
	default:
		// For other types, use a minimal estimate
		return 16
	}
}

// getContextWithSpan returns a new context with the span.
func getContextWithSpan(ctx context.Context, span trace.Span) context.Context {
	return trace.ContextWithSpan(ctx, span)
}

// InstrumentRunnable is a convenience function to instrument a runnable with default settings.
func InstrumentRunnable(r *runnable.InjectableRunnable) (*runnable.InjectableRunnable, error) {
	provider, err := NewDefaultTelemetryProvider()
	if err != nil {
		return nil, err
	}
	tracer := NewRunnableTracer(provider)
	return tracer.TraceRunnable(r), nil
}

// InstrumentNode is a convenience function to instrument a node function.
func InstrumentNode(name string, fn func(context.Context, interface{}) (interface{}, error)) (func(context.Context, interface{}) (interface{}, error), error) {
	provider, err := NewDefaultTelemetryProvider()
	if err != nil {
		return nil, err
	}
	tracer := NewRunnableTracer(provider)
	return tracer.TraceNode(name, fn), nil
}

// WrapRunnableWithTelemetry wraps a runnable with both tracing and metrics.
func WrapRunnableWithTelemetry(runnableFunc func(context.Context, interface{}) (interface{}, error), name string) func(context.Context, interface{}) (interface{}, error) {
	return func(ctx context.Context, input interface{}) (interface{}, error) {
		provider, err := NewDefaultTelemetryProvider()
		if err != nil {
			// Fallback: execute without telemetry
			return runnableFunc(ctx, input)
		}
		
		tracer := NewRunnableTracer(provider)
		instrumentedFn := tracer.TraceNode(name, runnableFunc)
		return instrumentedFn(ctx, input)
	}
}
