// Package pregel provides WebSocket support for remote graph execution.
package pregel

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/infiniflow/ragflow/harness/telemetry"
	"github.com/infiniflow/ragflow/harness/graphengine/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// WebSocketPregelProtocol implements PregelProtocol over WebSocket.
// This provides bidirectional streaming communication for remote graph execution.
type WebSocketPregelProtocol struct {
	conn            *websocket.Conn
	url             string
	headers         http.Header
	mu              sync.RWMutex
	messageChan     chan *PregelMessage
	doneChan        chan struct{}
	isClosed        bool
	readBufferSize  int
	writeBufferSize int
	pingInterval    time.Duration
	pongTimeout     time.Duration
	
	// OpenTelemetry tracing
	tracer         trace.Tracer
	enableTracing  bool
	graphName      string
}

// WebSocketConfig configures WebSocket connection.
type WebSocketConfig struct {
	URL            string
	Headers        http.Header
	ReadBufferSize int
	WriteBufferSize int
	PingInterval   time.Duration
	PongTimeout    time.Duration
	
	// OpenTelemetry settings
	EnableTracing bool
	Tracer        trace.Tracer
	GraphName     string
}

// NewWebSocketPregelProtocol creates a new WebSocket Pregel protocol.
func NewWebSocketPregelProtocol(config *WebSocketConfig) *WebSocketPregelProtocol {
	if config.ReadBufferSize == 0 {
		config.ReadBufferSize = 1024
	}
	if config.WriteBufferSize == 0 {
		config.WriteBufferSize = 1024
	}
	if config.PingInterval == 0 {
		config.PingInterval = 30 * time.Second
	}
	if config.PongTimeout == 0 {
		config.PongTimeout = 60 * time.Second
	}

	// Initialize tracer if not provided but tracing is enabled
	tracer := config.Tracer
	if tracer == nil && config.EnableTracing {
		tracer = telemetry.DefaultTracerProvider().Tracer()
	}

	return &WebSocketPregelProtocol{
		url:             config.URL,
		headers:         config.Headers,
		messageChan:     make(chan *PregelMessage, 100),
		doneChan:        make(chan struct{}),
		isClosed:        false,
		readBufferSize:  config.ReadBufferSize,
		writeBufferSize: config.WriteBufferSize,
		pingInterval:    config.PingInterval,
		pongTimeout:     config.PongTimeout,
		tracer:          tracer,
		enableTracing:   config.EnableTracing,
		graphName:       config.GraphName,
	}
}

// Connect establishes the WebSocket connection.
func (p *WebSocketPregelProtocol) Connect(ctx context.Context) error {
	var span trace.Span
	if p.enableTracing && p.tracer != nil {
		ctx, span = p.tracer.Start(ctx, "websocket.connect",
			trace.WithAttributes(
				attribute.String("websocket.url", p.url),
				attribute.String("graph.name", p.graphName),
			),
		)
		defer span.End()
	}

	dialer := websocket.Dialer{
		ReadBufferSize:  p.readBufferSize,
		WriteBufferSize: p.writeBufferSize,
	}

	conn, _, err := dialer.DialContext(ctx, p.url, p.headers)
	if err != nil {
		if span != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		}
		return fmt.Errorf("failed to connect to WebSocket: %w", err)
	}

	p.conn = conn

	if span != nil {
		span.SetStatus(codes.Ok, "connected")
	}

	// Start background goroutines
	go p.readLoop()
	go p.pingLoop()

	return nil
}

// readLoop continuously reads messages from the WebSocket.
func (p *WebSocketPregelProtocol) readLoop() {
	defer close(p.messageChan)

	for {
		select {
		case <-p.doneChan:
			return
		default:
		}

		_, data, err := p.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				// Log unexpected close
			}
			return
		}

		var message PregelMessage
		if err := json.Unmarshal(data, &message); err != nil {
			// Log unmarshal error
			continue
		}

		select {
		case p.messageChan <- &message:
		case <-p.doneChan:
			return
		}
	}
}

// pingLoop sends periodic ping messages to keep the connection alive.
func (p *WebSocketPregelProtocol) pingLoop() {
	ticker := time.NewTicker(p.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := p.conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(p.pongTimeout)); err != nil {
				// Ping failed, connection might be dead
				return
			}
		case <-p.doneChan:
			return
		}
	}
}

// Send sends a message to the remote peer.
func (p *WebSocketPregelProtocol) Send(ctx context.Context, message *PregelMessage) error {
	var span trace.Span
	if p.enableTracing && p.tracer != nil {
		ctx, span = p.tracer.Start(ctx, "websocket.send",
			trace.WithAttributes(
				attribute.String("message.type", string(message.Type)),
				attribute.String("message.id", message.ID),
				attribute.String("node.name", message.NodeName),
			),
		)
		defer span.End()
	}

	p.mu.RLock()
	if p.isClosed {
		p.mu.RUnlock()
		if span != nil {
			span.SetStatus(codes.Error, "connection closed")
		}
		return fmt.Errorf("websocket connection is closed")
	}
	conn := p.conn
	p.mu.RUnlock()

	data, err := json.Marshal(message)
	if err != nil {
		if span != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		}
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	if span != nil {
		span.SetAttributes(attribute.Int("message.size", len(data)))
	}

	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		if span != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		}
		return fmt.Errorf("failed to send message: %w", err)
	}

	if span != nil {
		span.SetStatus(codes.Ok, "sent")
	}

	return nil
}

// Receive receives a message from the remote peer.
func (p *WebSocketPregelProtocol) Receive(ctx context.Context) (*PregelMessage, error) {
	var span trace.Span
	if p.enableTracing && p.tracer != nil {
		ctx, span = p.tracer.Start(ctx, "websocket.receive",
			trace.WithAttributes(
				attribute.String("graph.name", p.graphName),
			),
		)
		defer span.End()
	}

	select {
	case message := <-p.messageChan:
		if span != nil {
			span.SetAttributes(
				attribute.String("message.type", string(message.Type)),
				attribute.String("message.id", message.ID),
				attribute.String("node.name", message.NodeName),
			)
			span.SetStatus(codes.Ok, "received")
		}
		return message, nil
	case <-ctx.Done():
		if span != nil {
			span.SetStatus(codes.Error, ctx.Err().Error())
			span.RecordError(ctx.Err())
		}
		return nil, ctx.Err()
	case <-p.doneChan:
		if span != nil {
			span.SetStatus(codes.Error, "connection closed")
		}
		return nil, fmt.Errorf("websocket connection closed")
	}
}

// Close closes the protocol connection.
func (p *WebSocketPregelProtocol) Close() error {
	p.mu.Lock()
	if p.isClosed {
		p.mu.Unlock()
		return nil
	}
	p.isClosed = true
	close(p.doneChan)
	p.mu.Unlock()

	if p.conn != nil {
		// Send close message
		p.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		// Close connection
		return p.conn.Close()
	}
	return nil
}

// IsConnected returns true if the WebSocket is connected.
func (p *WebSocketPregelProtocol) IsConnected() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return !p.isClosed && p.conn != nil
}

// RemoteGraphClient provides a high-level client for remote graph execution over WebSocket.
type RemoteGraphClient struct {
	protocol    *WebSocketPregelProtocol
	streamModes []types.StreamMode
	mu          sync.RWMutex
	handlers    map[MessageType]func(*PregelMessage)
	
	// OpenTelemetry tracing
	tracer        trace.Tracer
	enableTracing bool
	graphName     string
}

// RemoteGraphClientConfig configures the remote graph client.
type RemoteGraphClientConfig struct {
	URL            string
	Headers        http.Header
	StreamModes    []types.StreamMode
	ReadBufferSize int
	WriteBufferSize int
	PingInterval   time.Duration
	PongTimeout    time.Duration
	
	// OpenTelemetry settings
	EnableTracing bool
	Tracer        trace.Tracer
	GraphName     string
}

// NewRemoteGraphClient creates a new remote graph client.
func NewRemoteGraphClient(wsURL string, streamModes []types.StreamMode) *RemoteGraphClient {
	return NewRemoteGraphClientWithConfig(&RemoteGraphClientConfig{
		URL:         wsURL,
		StreamModes: streamModes,
	})
}

// NewRemoteGraphClientWithConfig creates a new remote graph client with configuration.
func NewRemoteGraphClientWithConfig(config *RemoteGraphClientConfig) *RemoteGraphClient {
	if config.ReadBufferSize == 0 {
		config.ReadBufferSize = 1024
	}
	if config.WriteBufferSize == 0 {
		config.WriteBufferSize = 1024
	}
	if config.PingInterval == 0 {
		config.PingInterval = 30 * time.Second
	}
	if config.PongTimeout == 0 {
		config.PongTimeout = 60 * time.Second
	}
	
	// Initialize tracer if not provided but tracing is enabled
	tracer := config.Tracer
	if tracer == nil && config.EnableTracing {
		tracer = telemetry.DefaultTracerProvider().Tracer()
	}
	
	return &RemoteGraphClient{
		protocol: &WebSocketPregelProtocol{
			url:             config.URL,
			headers:         config.Headers,
			messageChan:     make(chan *PregelMessage, 100),
			doneChan:        make(chan struct{}),
			isClosed:        false,
			readBufferSize:  config.ReadBufferSize,
			writeBufferSize: config.WriteBufferSize,
			pingInterval:    config.PingInterval,
			pongTimeout:     config.PongTimeout,
			tracer:          tracer,
			enableTracing:   config.EnableTracing,
			graphName:       config.GraphName,
		},
		streamModes:   config.StreamModes,
		handlers:      make(map[MessageType]func(*PregelMessage)),
		tracer:        tracer,
		enableTracing: config.EnableTracing,
		graphName:     config.GraphName,
	}
}

// Connect establishes connection to the remote graph server.
func (c *RemoteGraphClient) Connect(ctx context.Context) error {
	return c.protocol.Connect(ctx)
}

// Invoke executes the graph remotely and returns the final result.
func (c *RemoteGraphClient) Invoke(ctx context.Context, input interface{}, config *types.RunnableConfig) (interface{}, error) {
	var span trace.Span
	if c.enableTracing && c.tracer != nil {
		ctx, span = c.tracer.Start(ctx, "remote.graph.invoke",
			trace.WithAttributes(
				attribute.String("graph.name", c.graphName),
				attribute.String("remote.url", c.protocol.url),
			),
		)
		defer span.End()
	}

	// Send execute request
	req := &PregelMessage{
		Type:      MessageTypeExecute,
		ID:        generateMessageID(),
		Input:     input,
		Metadata: map[string]interface{}{
			"config":        config,
			"stream_modes":  c.streamModes,
			"trace_id":      "",
			"span_id":       "",
		},
		Timestamp: time.Now(),
	}

	if err := c.protocol.Send(ctx, req); err != nil {
		if span != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		}
		return nil, err
	}

	// Wait for response
	for {
		msg, err := c.protocol.Receive(ctx)
		if err != nil {
			if span != nil {
				span.SetStatus(codes.Error, err.Error())
				span.RecordError(err)
			}
			return nil, err
		}

		switch msg.Type {
		case MessageTypeExecuteResponse:
			if msg.Error != "" {
				if span != nil {
					span.SetStatus(codes.Error, msg.Error)
				}
				return nil, fmt.Errorf("remote execution error: %s", msg.Error)
			}
			if span != nil {
				span.SetStatus(codes.Ok, "executed successfully")
				if msg.Metadata != nil {
					if execTime, ok := msg.Metadata["execution_time_ms"].(float64); ok {
						span.SetAttributes(attribute.Float64("execution.time_ms", execTime))
					}
					if stepCount, ok := msg.Metadata["step_count"].(float64); ok {
						span.SetAttributes(attribute.Int64("step.count", int64(stepCount)))
					}
				}
			}
			return msg.Output, nil
		case MessageTypeCheckpoint:
			// Handle checkpoint if needed
		case MessageTypeStateUpdate:
			// Handle state update if needed
		case MessageTypeInterrupt:
			if span != nil {
				span.SetStatus(codes.Error, "interrupted")
			}
			// Handle interrupt
			return nil, fmt.Errorf("execution interrupted: %v", msg.Metadata)
		}
	}
}

// Stream executes the graph remotely and streams results.
func (c *RemoteGraphClient) Stream(ctx context.Context, input interface{}, config *types.RunnableConfig) (<-chan *PregelMessage, error) {
	var span trace.Span
	if c.enableTracing && c.tracer != nil {
		ctx, span = c.tracer.Start(ctx, "remote.graph.stream",
			trace.WithAttributes(
				attribute.String("graph.name", c.graphName),
				attribute.String("remote.url", c.protocol.url),
			),
		)
		defer span.End()
	}

	// Send execute request with streaming
	req := &PregelMessage{
		Type:      MessageTypeExecute,
		ID:        generateMessageID(),
		Input:     input,
		Metadata: map[string]interface{}{
			"config":        config,
			"stream_modes":  c.streamModes,
			"stream":        true,
			"trace_id":      "",
			"span_id":       "",
		},
		Timestamp: time.Now(),
	}

	if err := c.protocol.Send(ctx, req); err != nil {
		if span != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		}
		return nil, err
	}

	// Create output channel
	outputChan := make(chan *PregelMessage, 100)
	
// Track streaming statistics
messageCount := 0
startTime := time.Now()

// Start goroutine to forward messages
	go func() {
		defer close(outputChan)
		for {
			msg, err := c.protocol.Receive(ctx)
			if err != nil {
				if span != nil {
					span.SetStatus(codes.Error, err.Error())
					span.RecordError(err)
				}
				return
			}

			select {
			case outputChan <- msg:
				messageCount++
			case <-ctx.Done():
				if span != nil {
					span.SetStatus(codes.Error, ctx.Err().Error())
					span.RecordError(ctx.Err())
				}
				return
			}

			// Stop on final response
			if msg.Type == MessageTypeExecuteResponse {
				if span != nil {
					duration := time.Since(startTime).Milliseconds()
					span.SetStatus(codes.Ok, "streamed successfully")
					span.SetAttributes(
						attribute.Int("message.count", messageCount),
						attribute.Int64("duration.ms", duration),
					)
				}
				return
			}
		}
	}()

	return outputChan, nil
}

// OnMessage registers a handler for a specific message type.
func (c *RemoteGraphClient) OnMessage(msgType MessageType, handler func(*PregelMessage)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handlers[msgType] = handler
}

// Close closes the client connection.
func (c *RemoteGraphClient) Close() error {
	return c.protocol.Close()
}

// generateMessageID generates a unique message ID.
func generateMessageID() string {
	return fmt.Sprintf("msg_%d", time.Now().UnixNano())
}

// GetReadBufferSize returns the read buffer size.
func (p *WebSocketPregelProtocol) GetReadBufferSize() int {
	return p.readBufferSize
}

// GetWriteBufferSize returns the write buffer size.
func (p *WebSocketPregelProtocol) GetWriteBufferSize() int {
	return p.writeBufferSize
}

// GetPingInterval returns the ping interval.
func (p *WebSocketPregelProtocol) GetPingInterval() time.Duration {
	return p.pingInterval
}

// GetPongTimeout returns the pong timeout.
func (p *WebSocketPregelProtocol) GetPongTimeout() time.Duration {
	return p.pongTimeout
}
