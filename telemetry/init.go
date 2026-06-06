// Package telemetry provides initialization and configuration for OpenTelemetry.
package telemetry

import (
	"context"
	"fmt"
	"log"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

// Config holds the configuration for OpenTelemetry.
type Config struct {
	// ServiceName is the name of the service.
	ServiceName string
	
	// ServiceVersion is the version of the service.
	ServiceVersion string
	
	// Environment is the deployment environment (e.g., "production", "staging").
	Environment string
	
	// EnableTracing enables distributed tracing.
	EnableTracing bool
	
	// EnableMetrics enables metrics collection.
	EnableMetrics bool
	
	// EnableLogging enables structured logging.
	EnableLogging bool
	
	// SampleRate is the sampling rate for traces (0.0 to 1.0).
	SampleRate float64
	
	// OTLPEndpoint is the OTLP collector endpoint (e.g., "localhost:4317").
	OTLPEndpoint string
	
	// UseConsoleExporter uses console exporter for development.
	UseConsoleExporter bool
	
	// ResourceAttributes are additional resource attributes.
	ResourceAttributes map[string]string
}

// DefaultConfig returns the default configuration.
func DefaultConfig() *Config {
	serviceName := "harness-go"
	if name := os.Getenv("OTEL_SERVICE_NAME"); name != "" {
		serviceName = name
	}
	
	return &Config{
		ServiceName:       serviceName,
		ServiceVersion:    "1.0.0",
		Environment:       "development",
		EnableTracing:     true,
		EnableMetrics:     true,
		EnableLogging:     true,
		SampleRate:        1.0,
		OTLPEndpoint:      "localhost:4317",
		UseConsoleExporter: os.Getenv("OTEL_EXPORTER_CONSOLE") == "true",
		ResourceAttributes: make(map[string]string),
	}
}

// Init initializes OpenTelemetry with the given configuration.
// It returns a shutdown function that should be called when shutting down the application.
func Init(cfg *Config) (func(context.Context) error, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	
	// Create resource
	res, err := newResource(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}
	
	var shutdownFuncs []func(context.Context) error
	
	// Initialize tracing
	if cfg.EnableTracing {
		tracerShutdown, err := initTracing(cfg, res)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize tracing: %w", err)
		}
		shutdownFuncs = append(shutdownFuncs, tracerShutdown)
	}
	
	// Set global propagator
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	
	// Return combined shutdown function
	return func(ctx context.Context) error {
		var lastErr error
		for _, fn := range shutdownFuncs {
			if err := fn(ctx); err != nil {
				lastErr = err
			}
		}
		return lastErr
	}, nil
}

// newResource creates a new OpenTelemetry resource.
func newResource(cfg *Config) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{
		semconv.ServiceNameKey.String(cfg.ServiceName),
		semconv.ServiceVersionKey.String(cfg.ServiceVersion),
	}
	
	if cfg.Environment != "" {
		attrs = append(attrs, semconv.DeploymentEnvironmentKey.String(cfg.Environment))
	}
	
	// Add custom resource attributes
	for key, value := range cfg.ResourceAttributes {
		attrs = append(attrs, attribute.KeyValue{
			Key:   attribute.Key(key),
			Value: attribute.StringValue(value),
		})
	}
	
	// Detect resource attributes automatically
	res, err := resource.New(
		context.Background(),
		resource.WithAttributes(attrs...),
		resource.WithFromEnv(),
		resource.WithHost(),
		resource.WithOS(),
		resource.WithProcess(),
		resource.WithContainer(),
	)
	
	if err != nil {
		return nil, err
	}
	
	return res, nil
}

// initTracing initializes distributed tracing.
func initTracing(cfg *Config, res *resource.Resource) (func(context.Context) error, error) {
	var tracerExporter sdktrace.SpanExporter
	var err error
	
	if cfg.UseConsoleExporter {
		// Use console exporter for development
		tracerExporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, fmt.Errorf("failed to create console exporter: %w", err)
		}
		log.Println("Using console exporter for traces")
	} else {
		// Use OTLP exporter
		tracerExporter, err = otlptracegrpc.New(
			context.Background(),
			otlptracegrpc.WithInsecure(),
			otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
		}
		log.Printf("Using OTLP exporter at %s for traces\n", cfg.OTLPEndpoint)
	}
	
	// Create tracer provider
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(tracerExporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(cfg.SampleRate)),
	)
	
	// Set global tracer provider
	otel.SetTracerProvider(tracerProvider)
	
	return func(ctx context.Context) error {
		if err := tracerProvider.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown tracer provider: %w", err)
		}
		return nil
	}, nil
}

// InitWithDefaults initializes OpenTelemetry with default configuration.
// This is the simplest way to enable telemetry.
func InitWithDefaults() (func(context.Context) error, error) {
	return Init(nil)
}

// InitForProduction initializes OpenTelemetry for production use.
func InitForProduction(serviceName, version, environment, otlpEndpoint string) (func(context.Context) error, error) {
	cfg := &Config{
		ServiceName:        serviceName,
		ServiceVersion:     version,
		Environment:        environment,
		EnableTracing:      true,
		EnableMetrics:      true,
		EnableLogging:      true,
		SampleRate:         0.1, // Sample 10% of traces in production
		OTLPEndpoint:       otlpEndpoint,
		UseConsoleExporter: false,
		ResourceAttributes: map[string]string{
			"deployment.environment": environment,
		},
	}
	
	return Init(cfg)
}

// InitForDevelopment initializes OpenTelemetry for development use.
func InitForDevelopment(serviceName string) (func(context.Context) error, error) {
	cfg := &Config{
		ServiceName:        serviceName,
		ServiceVersion:     "dev",
		Environment:        "development",
		EnableTracing:      true,
		EnableMetrics:      true,
		EnableLogging:      true,
		SampleRate:         1.0, // Sample all traces in development
		OTLPEndpoint:       "localhost:4317",
		UseConsoleExporter: true, // Use console exporter for development
		ResourceAttributes: map[string]string{
			"deployment.environment": "development",
		},
	}
	
	return Init(cfg)
}

// InitForTesting initializes OpenTelemetry for testing use.
// It uses a no-op exporter to avoid side effects in tests.
func InitForTesting() (func(context.Context) error, error) {
	cfg := &Config{
		ServiceName:       "harness-go-test",
		ServiceVersion:    "test",
		Environment:       "test",
		EnableTracing:     false, // Disable tracing in tests
		EnableMetrics:     false, // Disable metrics in tests
		EnableLogging:     false, // Disable logging in tests
		UseConsoleExporter: false,
	}
	
	return Init(cfg)
}

// GetTracer returns the global tracer.
func GetTracer() trace.Tracer {
	return otel.Tracer(InstrumentationName, trace.WithInstrumentationVersion(InstrumentationVersion))
}

// NewContextWithSpan creates a new context with a span.
func NewContextWithSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	tracer := GetTracer()
	return tracer.Start(ctx, name)
}

// RunInSpan runs a function within a span.
func RunInSpan(ctx context.Context, name string, fn func(context.Context) error) error {
	ctx, span := NewContextWithSpan(ctx, name)
	defer span.End()
	
	return fn(ctx)
}

// RunInSpanWithResult runs a function within a span and returns its result.
func RunInSpanWithResult[T any](ctx context.Context, name string, fn func(context.Context) (T, error)) (T, error) {
	ctx, span := NewContextWithSpan(ctx, name)
	defer span.End()
	
	return fn(ctx)
}
