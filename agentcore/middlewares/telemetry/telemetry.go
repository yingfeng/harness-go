// Package telemetry provides an OpenTelemetry middleware for agentcore agents.
//
// Usage:
//
//	import telemetrymw "github.com/infiniflow/ragflow/harness/agentcore/middlewares/telemetry"
//
//	cfg := agentcore.DefaultChatModelConfig[*schema.Message]()
//	cfg.Middlewares = append(cfg.Middlewares, telemetrymw.New())
//
// To customize:
//
//	mw := telemetrymw.New(
//	    telemetrymw.WithTracing(true),
//	    telemetrymw.WithMetrics(true),
//	)
package telemetry

import (
	"context"
	"time"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
	"github.com/infiniflow/ragflow/harness/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Config holds configuration for the telemetry middleware.
type Config struct {
	EnableTracing bool
	EnableMetrics bool
	Provider      *telemetry.TelemetryProvider
}

// Option configures the telemetry middleware.
type Option func(*Config)

// WithTracing enables or disables distributed tracing.
func WithTracing(enabled bool) Option {
	return func(c *Config) { c.EnableTracing = enabled }
}

// WithMetrics enables or disables metrics collection.
func WithMetrics(enabled bool) Option {
	return func(c *Config) { c.EnableMetrics = enabled }
}

// WithProvider sets a custom telemetry provider. When nil, the default
// provider (from environment variables or OTLP endpoint) is used.
func WithProvider(p *telemetry.TelemetryProvider) Option {
	return func(c *Config) { c.Provider = p }
}

func defaultConfig() *Config {
	return &Config{
		EnableTracing: true,
		EnableMetrics: true,
	}
}

func newProvider(cfg *Config) *telemetry.TelemetryProvider {
	if cfg.Provider != nil {
		return cfg.Provider
	}
	p, err := telemetry.NewDefaultTelemetryProvider()
	if err != nil {
		return nil
	}
	return p
}

// Middleware is an agentcore middleware that instruments agent execution with
// OpenTelemetry tracing and metrics. It wraps model calls, tool invocations,
// and the overall agent lifecycle.
//
// TODO: Make this generic (Middleware[M]) to support AgenticMessage alongside
// *schema.Message. Currently hardcoded to *schema.Message, unlike other
// middlewares that use BaseMiddleware[M].
type Middleware struct {
	agentcore.BaseMiddleware[*schema.Message]
	cfg     *Config
	metrics *telemetry.Metrics
	tracer  trace.Tracer
}

// New creates a new telemetry middleware with default settings.
func New(opts ...Option) *Middleware {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	provider := newProvider(cfg)
	m := &Middleware{cfg: cfg}
	if provider != nil {
		m.metrics = provider.Metrics
		m.tracer = provider.TracerProvider.Tracer()
	}
	return m
}

// WrapModel wraps the model call with a tracing span and execution duration metric.
func (m *Middleware) WrapModel(ctx context.Context, model agentcore.ChatModel[*schema.Message], mc *agentcore.ModelContext) (agentcore.ChatModel[*schema.Message], error) {
	if m.tracer == nil {
		return model, nil
	}
	return &tracedModel{
		inner:   model,
		mw:      m,
		toolCnt: len(mc.Tools),
	}, nil
}

// WrapToolInvoke wraps a synchronous tool call with a span and duration metric.
func (m *Middleware) WrapToolInvoke(ctx context.Context, ep agentcore.InvokableToolEndpoint, tc *agentcore.ToolContext) (agentcore.InvokableToolEndpoint, error) {
	if m.tracer == nil {
		return ep, nil
	}
	return func(ctx context.Context, args string, opts ...agentcore.ToolOption) (string, error) {
		var span trace.Span
		if m.cfg.EnableTracing && m.tracer != nil {
			ctx, span = m.tracer.Start(ctx, "tool."+tc.Name,
				trace.WithAttributes(
					attribute.String("tool.name", tc.Name),
					attribute.Int("args.size", len(args)),
				),
				trace.WithSpanKind(trace.SpanKindInternal),
			)
		}
		start := time.Now()
		result, err := ep(ctx, args, opts...)
		duration := time.Since(start)

		if m.cfg.EnableMetrics && m.metrics != nil {
			if err != nil {
				m.metrics.RecordNodeExecution(ctx, "tool."+tc.Name, "string", "string", duration, err)
			} else {
				m.metrics.RecordNodeExecution(ctx, "tool."+tc.Name, "string", "string", duration, nil)
			}
		}

		if span != nil && span.IsRecording() {
			if err != nil {
				span.SetStatus(codes.Error, err.Error())
				telemetry.SetSpanError(span, err)
			} else {
				span.SetStatus(codes.Ok, "")
			}
			span.End()
		}
		return result, err
	}, nil
}

// WrapToolStream wraps a streaming tool call with a span.
func (m *Middleware) WrapToolStream(ctx context.Context, ep agentcore.StreamableToolEndpoint, tc *agentcore.ToolContext) (agentcore.StreamableToolEndpoint, error) {
	if m.tracer == nil {
		return ep, nil
	}
	return func(ctx context.Context, args string, opts ...agentcore.ToolOption) (*schema.StreamReader[string], error) {
		var span trace.Span
		if m.cfg.EnableTracing {
			ctx, span = m.tracer.Start(ctx, "tool.stream."+tc.Name,
				trace.WithAttributes(attribute.String("tool.name", tc.Name)),
				trace.WithSpanKind(trace.SpanKindInternal),
			)
		}
		start := time.Now()
		result, err := ep(ctx, args, opts...)
		if err != nil {
			if m.cfg.EnableMetrics && m.metrics != nil {
				m.metrics.RecordNodeExecution(ctx, "tool.stream."+tc.Name, "string", "stream", time.Since(start), err)
			}
			if span != nil {
				telemetry.SetSpanError(span, err)
				span.End()
			}
			return nil, err
		}
		if span != nil {
			span.SetStatus(codes.Ok, "")
			span.End()
		}
		return result, nil
	}, nil
}

// WrapEnhancedInvokableToolCall wraps an enhanced tool call with a span.
func (m *Middleware) WrapEnhancedInvokableToolCall(ctx context.Context, ep agentcore.EnhancedInvokableToolEndpoint, tc *agentcore.ToolContext) (agentcore.EnhancedInvokableToolEndpoint, error) {
	if m.tracer == nil {
		return ep, nil
	}
	return func(ctx context.Context, args *schema.ToolArgument, opts ...agentcore.ToolOption) (*schema.ToolResult, error) {
		var span trace.Span
		if m.cfg.EnableTracing {
			ctx, span = m.tracer.Start(ctx, "enhanced_tool."+tc.Name,
				trace.WithAttributes(attribute.String("tool.name", tc.Name)),
				trace.WithSpanKind(trace.SpanKindInternal),
			)
		}
		start := time.Now()
		result, err := ep(ctx, args, opts...)
		duration := time.Since(start)

		if m.cfg.EnableMetrics && m.metrics != nil {
			m.metrics.RecordNodeExecution(ctx, "enhanced_tool."+tc.Name, "ToolArgument", "ToolResult", duration, err)
		}
		if span != nil && span.IsRecording() {
			if err != nil {
				span.SetStatus(codes.Error, err.Error())
				telemetry.SetSpanError(span, err)
			} else {
				span.SetStatus(codes.Ok, "")
			}
			span.End()
		}
		return result, err
	}, nil
}

// WrapEnhancedStreamableToolCall wraps an enhanced streaming tool call.
func (m *Middleware) WrapEnhancedStreamableToolCall(ctx context.Context, ep agentcore.EnhancedStreamableToolEndpoint, tc *agentcore.ToolContext) (agentcore.EnhancedStreamableToolEndpoint, error) {
	if m.tracer == nil {
		return ep, nil
	}
	return func(ctx context.Context, args *schema.ToolArgument, opts ...agentcore.ToolOption) (*schema.StreamReader[*schema.ToolResult], error) {
		var span trace.Span
		if m.cfg.EnableTracing {
			ctx, span = m.tracer.Start(ctx, "enhanced_tool.stream."+tc.Name,
				trace.WithAttributes(attribute.String("tool.name", tc.Name)),
				trace.WithSpanKind(trace.SpanKindInternal),
			)
		}
		start := time.Now()
		result, err := ep(ctx, args, opts...)
		if err != nil {
			if m.cfg.EnableMetrics && m.metrics != nil {
				m.metrics.RecordNodeExecution(ctx, "enhanced_tool.stream."+tc.Name, "ToolArgument", "stream", time.Since(start), err)
			}
			if span != nil {
				telemetry.SetSpanError(span, err)
				span.End()
			}
			return nil, err
		}
		if span != nil {
			span.SetStatus(codes.Ok, "")
			span.End()
		}
		return result, nil
	}, nil
}

// tracedModel wraps a ChatModel with OpenTelemetry tracing and metrics.
type tracedModel struct {
	inner   agentcore.ChatModel[*schema.Message]
	mw      *Middleware
	toolCnt int
}

func (m *tracedModel) Generate(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.Message, error) {
	var span trace.Span
	if m.mw.cfg.EnableTracing && m.mw.tracer != nil {
		ctx, span = m.mw.tracer.Start(ctx, "model.generate",
			trace.WithAttributes(
				attribute.Int("messages.count", len(msgs)),
				attribute.Int("tools.count", m.toolCnt),
			),
			trace.WithSpanKind(trace.SpanKindClient),
		)
	}
	start := time.Now()
	resp, err := m.inner.Generate(ctx, msgs, opts...)
	duration := time.Since(start)

	if m.mw.cfg.EnableMetrics && m.mw.metrics != nil {
		if resp != nil {
			m.mw.metrics.RecordMessagesProcessed(ctx, int64(len(msgs)+1))
		}
		m.mw.metrics.RecordNodeExecution(ctx, "model.generate", "message[]", "message", duration, err)
	}
	if span != nil && span.IsRecording() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			telemetry.SetSpanError(span, err)
		} else {
			span.SetStatus(codes.Ok, "")
		}
		span.End()
	}
	return resp, err
}

func (m *tracedModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...agentcore.ModelOption) (*schema.StreamReader[*schema.Message], error) {
	var span trace.Span
	if m.mw.cfg.EnableTracing && m.mw.tracer != nil {
		ctx, span = m.mw.tracer.Start(ctx, "model.stream",
			trace.WithAttributes(
				attribute.Int("messages.count", len(msgs)),
				attribute.Int("tools.count", m.toolCnt),
			),
			trace.WithSpanKind(trace.SpanKindClient),
		)
	}
	start := time.Now()
	result, err := m.inner.Stream(ctx, msgs, opts...)
	if err != nil {
		if m.mw.cfg.EnableMetrics && m.mw.metrics != nil {
			m.mw.metrics.RecordNodeExecution(ctx, "model.stream", "message[]", "stream", time.Since(start), err)
		}
		if span != nil {
			telemetry.SetSpanError(span, err)
			span.End()
		}
		return nil, err
	}
	if span != nil {
		span.SetStatus(codes.Ok, "")
		span.End()
	}
	return result, nil
}

func (m *tracedModel) BindTools(tools []*schema.ToolInfo) error {
	return m.inner.BindTools(tools)
}

// Ensure Middleware implements the agentcore middleware interface.
var _ agentcore.ChatModelMiddleware = (*Middleware)(nil)


