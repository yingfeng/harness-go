package checkpoint

import (
	"context"
	"fmt"
	"os"
	"testing"
)

func newTestDB(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp("", "checkpoint-test-*.db")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	f.Close()
	t.Cleanup(func() { os.Remove(f.Name()) })
	return f.Name()
}

// TestSqliteSaver_PutAndGet verifies basic put/get operations.
func TestSqliteSaver_PutAndGet(t *testing.T) {
	dbPath := newTestDB(t)
	saver, err := NewSqliteSaver(dbPath)
	if err != nil {
		t.Fatalf("NewSqliteSaver: %v", err)
	}
	defer saver.Close()

	ctx := context.Background()
	config := map[string]interface{}{
		"thread_id": "thread-1",
	}
	checkpoint := map[string]interface{}{
		"state": map[string]interface{}{
			"key1": "value1",
		},
		"id": "cp-001",
	}

	err = saver.Put(ctx, config, checkpoint)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	restored, err := saver.Get(ctx, config)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if restored == nil {
		t.Fatal("expected non-nil checkpoint")
	}
}

// TestSqliteSaver_List verifies listing checkpoints for a thread.
func TestSqliteSaver_List(t *testing.T) {
	dbPath := newTestDB(t)
	saver, err := NewSqliteSaver(dbPath)
	if err != nil {
		t.Fatalf("NewSqliteSaver: %v", err)
	}
	defer saver.Close()

	ctx := context.Background()
	config := map[string]interface{}{
		"thread_id": "thread-list",
	}

	for i := 0; i < 3; i++ {
		cp := map[string]interface{}{
			"state": map[string]interface{}{"idx": i},
			"id":    fmt.Sprintf("cp-list-%d", i),
		}
		if err := saver.Put(ctx, config, cp); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	list, err := saver.List(ctx, config, 10)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) == 0 {
		t.Error("expected at least 1 checkpoint in list")
	}
}

// TestSqliteSaver_Negative verifies error handling for non-existent data.
func TestSqliteSaver_Negative(t *testing.T) {
	dbPath := newTestDB(t)
	saver, err := NewSqliteSaver(dbPath)
	if err != nil {
		t.Fatalf("NewSqliteSaver: %v", err)
	}
	defer saver.Close()

	ctx := context.Background()
	config := map[string]interface{}{
		"configurable": map[string]interface{}{
			"thread_id": "nonexistent",
		},
	}

	// Get non-existent checkpoint
	cp, err := saver.Get(ctx, config)
	if err != nil {
		t.Logf("expected error for non-existent: %v", err)
	}
	if cp != nil {
		t.Error("expected nil for non-existent checkpoint")
	}

	// List non-existent thread
	list, _ := saver.List(ctx, config, 10)
	if len(list) != 0 {
		t.Errorf("expected empty list, got %d", len(list))
	}
}
