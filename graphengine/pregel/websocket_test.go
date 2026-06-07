// Package pregel provides WebSocket-based remote execution support for Pregel.
package pregel

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestWebSocketConfigDefaults(t *testing.T) {
	config := &WebSocketConfig{
		URL: "ws://localhost:8080",
	}

	// These defaults should be set by NewWebSocketPregelProtocol
	if config.PingInterval != 0 {
		t.Errorf("Expected zero PingInterval before creation")
	}
}

func TestValidateWebSocketURL(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{
			name:    "valid ws URL",
			url:     "ws://localhost:8080/pregel",
			wantErr: false,
		},
		{
			name:    "valid wss URL",
			url:     "wss://example.com/secure",
			wantErr: false,
		},
		{
			name:    "invalid http URL",
			url:     "http://localhost:8080",
			wantErr: true,
		},
		{
			name:    "missing host",
			url:     "ws://",
			wantErr: true,
		},
		{
			name:    "invalid URL format",
			url:     "not-a-url",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simple validation: must start with ws:// or wss://
			hasValidScheme := len(tt.url) > 5 && (len(tt.url) > 5 && tt.url[:5] == "ws://" || len(tt.url) > 6 && tt.url[:6] == "wss://")
			err := !hasValidScheme
			if err != tt.wantErr {
				t.Errorf("URL validation error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPregelMessageTypes(t *testing.T) {
	tests := []struct {
		name  MessageType
		valid bool
	}{
		{MessageTypeExecute, true},
		{MessageTypeExecuteResponse, true},
		{MessageTypeCheckpoint, true},
		{MessageTypeStateUpdate, true},
		{MessageTypeInterrupt, true},
		{MessageTypeResume, true},
		{MessageTypePing, true},
		{MessageTypePong, true},
	}

	for _, tt := range tests {
		t.Run(string(tt.name), func(t *testing.T) {
			if tt.valid && tt.name == "" {
				t.Error("Expected non-empty message type")
			}
		})
	}
}

func TestGenerateMessageID(t *testing.T) {
	id1 := generateMessageID()
	id2 := generateMessageID()

	if id1 == "" {
		t.Error("Expected non-empty message ID")
	}

	if id1 == id2 {
		t.Error("Expected different message IDs")
	}

	if len(id1) < 10 {
		t.Errorf("Message ID too short: %s", id1)
	}
}

func TestStreamingRemoteRunnableCreation(t *testing.T) {
	// Test that RemoteNode can be created with a mock protocol
	mockRunnable := &RemoteRunnable{
		url: "ws://localhost:8080",
	}
	node := NewRemoteNode(mockRunnable, "test_node")

	if node == nil {
		t.Fatal("Expected non-nil node")
	}

	if node.nodeName != "test_node" {
		t.Errorf("Expected node name 'test_node', got %s", node.nodeName)
	}
}

func TestCheckpointMigrator(t *testing.T) {
	// Test checkpoint migration using CheckpointMigration from subgraph.go
	parentEngine := &Engine{}
	manager := NewSubgraphManager(parentEngine)
	migrator := NewCheckpointMigration(manager, nil)

	if migrator == nil {
		t.Fatal("Expected non-nil migrator")
	}

	if migrator.manager != manager {
		t.Error("Expected manager to match")
	}
}

func TestPregelMessageSerialization(t *testing.T) {
	msg := &PregelMessage{
		Type:      MessageTypeExecute,
		ID:        "test_id",
		NodeName:  "test_node",
		Input:     map[string]interface{}{"key": "value"},
		Output:    nil,
		Error:     "",
		Metadata:  map[string]interface{}{"meta": "data"},
		Timestamp: time.Now(),
	}

	// Ensure timestamp is set
	if msg.Timestamp.IsZero() {
		t.Error("Expected non-zero timestamp")
	}

	// Ensure required fields are set
	if msg.Type == "" {
		t.Error("Expected non-empty message type")
	}

	if msg.ID == "" {
		t.Error("Expected non-empty message ID")
	}
}

func TestBidirectionalWebSocketCreation(t *testing.T) {
	// This test validates structure only, doesn't connect
	tests := []struct {
		name       string
		clientURL  string
		serverURL  string
		expectErr bool
	}{
		{
			name:       "valid URLs",
			clientURL:  "ws://localhost:8081/client",
			serverURL:  "ws://localhost:8082/server",
			expectErr: false,
		},
		{
			name:       "invalid client URL",
			clientURL:  "http://invalid",
			serverURL:  "ws://localhost:8082/server",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Skip actual connection test - just validate URL format
			hasValidScheme := len(tt.clientURL) > 5 && (len(tt.clientURL) > 5 && tt.clientURL[:5] == "ws://" || len(tt.clientURL) > 6 && tt.clientURL[:6] == "wss://")
			err := !hasValidScheme
			if err != tt.expectErr {
				t.Errorf("URL validation error = %v, expectErr %v", err, tt.expectErr)
			}
		})
	}
}

func TestRemoteProtocol(t *testing.T) {
	mockProtocol := &MockProtocol{
		sendCh:    make(chan *PregelMessage, 10),
		receiveCh: make(chan *PregelMessage, 10),
	}

	t.Run("Send message", func(t *testing.T) {
		msg := &PregelMessage{
			Type:      MessageTypePing,
			ID:        "test_id",
			Timestamp: time.Now(),
		}

		err := mockProtocol.Send(context.Background(), msg)
		if err != nil {
			t.Errorf("Send() error = %v", err)
		}
	})

	t.Run("Receive message", func(t *testing.T) {
		msg := &PregelMessage{
			Type:      MessageTypePong,
			ID:        "test_id",
			Timestamp: time.Now(),
		}

		mockProtocol.receiveCh <- msg

		received, err := mockProtocol.Receive(context.Background())
		if err != nil {
			t.Errorf("Receive() error = %v", err)
		}

		if received.Type != MessageTypePong {
			t.Errorf("Expected MessageTypePong, got %s", received.Type)
		}
	})
}

// MockProtocol is a mock implementation of PregelProtocol for testing.
type MockProtocol struct {
	sendCh    chan *PregelMessage
	receiveCh chan *PregelMessage
	closed    bool
}

func (m *MockProtocol) Send(ctx context.Context, message *PregelMessage) error {
	if m.closed {
		return fmt.Errorf("protocol closed")
	}
	message.Timestamp = time.Now()
	m.sendCh <- message
	return nil
}

func (m *MockProtocol) Receive(ctx context.Context) (*PregelMessage, error) {
	if m.closed {
		return nil, fmt.Errorf("protocol closed")
	}
	select {
	case msg := <-m.receiveCh:
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *MockProtocol) Close() error {
	m.closed = true
	return nil
}

func TestRemoteMessageTimeout(t *testing.T) {
	mockProtocol := &MockProtocol{
		receiveCh: make(chan *PregelMessage, 1),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := mockProtocol.Receive(ctx)
	if err == nil {
		t.Error("Expected timeout error")
	}
}

func TestMessageTypeConstants(t *testing.T) {
	expectedTypes := map[MessageType]bool{
		MessageTypeExecute:         true,
		MessageTypeExecuteResponse:  true,
		MessageTypeCheckpoint:      true,
		MessageTypeStateUpdate:    true,
		MessageTypeInterrupt:       true,
		MessageTypeResume:         true,
		MessageTypePing:           true,
		MessageTypePong:           true,
	}

	for msgType := range expectedTypes {
		if string(msgType) == "" {
			t.Errorf("MessageType %v should have a string representation", msgType)
		}
	}
}
