// Package runtime tests multitenancy and runtime functionality.
package runtime

import (
	"context"
	"testing"

	"github.com/infiniflow/ragflow/harness/managed"
)

// TestRuntimeMultiTenantContext tests multi-tenant runtime contexts.
func TestRuntimeMultiTenantContext(t *testing.T) {
	ctx1 := context.Background()
	ctx2 := context.Background()

	// Create two independent runtimes using Clone()
	runtime1 := managed.DEFAULT_RUNTIME.Clone()
	runtime1.Set(ctx1, "tenant_id", "tenant-1")
	runtime1.Set(ctx1, "user_id", "user-1")

	runtime2 := managed.DEFAULT_RUNTIME.Clone()
	runtime2.Set(ctx2, "tenant_id", "tenant-2")
	runtime2.Set(ctx2, "user_id", "user-2")

	// Verify isolation: runtime1 should have tenant-1
	tenant1, ok := runtime1.Get(ctx1, "tenant_id")
	if !ok {
		t.Error("Failed to get tenant_id from runtime1")
	}
	if tenant1 != "tenant-1" {
		t.Errorf("Expected tenant-1, got %v", tenant1)
	}

	// Verify isolation: runtime2 should have tenant-2
	tenant2, ok := runtime2.Get(ctx2, "tenant_id")
	if !ok {
		t.Error("Failed to get tenant_id from runtime2")
	}
	if tenant2 != "tenant-2" {
		t.Errorf("Expected tenant-2, got %v", tenant2)
	}

	// Verify runtime1 doesn't see runtime2's data
	tenant2From1, ok := runtime1.Get(ctx1, "tenant_id")
	if !ok {
		t.Error("Failed to get tenant_id from runtime1")
	}
	if tenant2From1 == "tenant-2" {
		t.Error("Runtime1 should not see runtime2's tenant_id")
	}
}

// TestRuntimeMerge tests merging of runtime contexts.
func TestRuntimeMerge(t *testing.T) {
	ctx := context.Background()

	r1 := &managed.Runtime{}
	r1.Set(ctx, "key1", "value1")
	r1.Set(ctx, "key2", "value2")

	r2 := &managed.Runtime{}
	r2.Set(ctx, "key2", "new_value2") // Override
	r2.Set(ctx, "key3", "value3")

	// Merge r2 into r1
	merged := r1.Merge(r2)

	// key1 should remain
	val1, ok := merged.Get(ctx, "key1")
	if !ok || val1 != "value1" {
		t.Errorf("Expected key1=value1, got %v", val1)
	}

	// key2 should be overridden
	val2, ok := merged.Get(ctx, "key2")
	if !ok || val2 != "new_value2" {
		t.Errorf("Expected key2=new_value2, got %v", val2)
	}

	// key3 should be added
	val3, ok := merged.Get(ctx, "key3")
	if !ok || val3 != "value3" {
		t.Errorf("Expected key3=value3, got %v", val3)
	}
}

// TestRuntimeOverride tests overriding of runtime contexts.
func TestRuntimeOverride(t *testing.T) {
	ctx := context.Background()

	r1 := managed.NewRuntime()
	r1.Set(ctx, "key1", "value1")
	r1.Set(ctx, "key2", "value2")
	r1.Set(ctx, "key3", "value3")

	overrides := map[string]interface{}{
		"key2": "new_value2",
	}

	// Override r1 with overrides
	overridden := r1.Override(overrides)

	// key1 should remain (in Configurable)
	val1, ok := overridden.Get(ctx, "key1")
	if !ok || val1 != "value1" {
		t.Errorf("Expected key1=value1, got %v", val1)
	}

	// key3 should remain (in Configurable)
	val3, ok := overridden.Get(ctx, "key3")
	if !ok || val3 != "value3" {
		t.Errorf("Expected key3=value3, got %v", val3)
	}
}

// TestRuntimeWithManagedValues tests runtime with managed values.
func TestRuntimeWithManagedValues(t *testing.T) {
	ctx := context.Background()

	r := managed.DEFAULT_RUNTIME

	// Set managed values in scratchpad
	r.Set(ctx, "managed_value", &managed.CurrentStep{Value: 5})
	r.Set(ctx, "config", map[string]interface{}{
		"api_key": "test-key",
	})

	// Get managed value from scratchpad
	stepVal, ok := r.Get(ctx, "managed_value")
	if !ok {
		t.Error("Failed to get managed_value")
	}

	step, ok := stepVal.(*managed.CurrentStep)
	if !ok {
		t.Error("Expected CurrentStep type")
	}

	if step.Value != 5 {
		t.Errorf("Expected value 5, got %d", step.Value)
	}
}

// TestRuntimeStoreIntegration tests runtime with checkpoint store.
func TestRuntimeStoreIntegration(t *testing.T) {
	ctx := context.Background()

	store := &mockStore{
		data: make(map[string]interface{}),
	}
	_ = &managed.Runtime{
		Store: store,
	}

	// Set store data
	store.Set(ctx, "checkpoint_id", "ckpt-123")

	// Verify store is accessible
	if store.data["checkpoint_id"] != "ckpt-123" {
		t.Error("Store data not accessible")
	}
}

// TestRuntimeStreamWriter tests runtime with stream writer.
func TestRuntimeStreamWriter(t *testing.T) {
	messages := make([]string, 0)
	writer := func(msg interface{}) {
		if s, ok := msg.(string); ok {
			messages = append(messages, s)
		}
	}

	// Write to stream
	writer("message1")
	writer("message2")

	// Verify messages
	if len(messages) != 2 {
		t.Errorf("Expected 2 messages, got %d", len(messages))
	}
}

// TestGetRuntimeFromConfig tests getting runtime from config.
func TestGetRuntimeFromConfig(t *testing.T) {
	// Create a config with runtime values
	config := map[string]interface{}{
		"__runtime__": managed.DEFAULT_RUNTIME,
		"test_key":    "test_value",
	}

	// Get runtime from config
	retrieved := managed.GetRuntime(config)
	if retrieved == nil {
		t.Error("Failed to retrieve runtime from config")
	}
}

// Mock store for testing
type mockStore struct {
	data map[string]interface{}
}

func (m *mockStore) Get(ctx context.Context, key string) (interface{}, bool) {
	val, ok := m.data[key]
	return val, ok
}

func (m *mockStore) Set(ctx context.Context, key string, value interface{}) {
	m.data[key] = value
}
