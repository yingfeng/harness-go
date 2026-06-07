package util

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestFuture_Get(t *testing.T) {
	ctx := context.Background()
	f := NewFuture[string]()

	go func() {
		time.Sleep(100 * time.Millisecond)
		f.Set("hello")
	}()

	result, err := f.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result != "hello" {
		t.Errorf("Expected 'hello', got '%s'", result)
	}
}

func TestFuture_Error(t *testing.T) {
	ctx := context.Background()
	f := NewFuture[string]()
	expectedErr := errors.New("test error")

	go func() {
		time.Sleep(100 * time.Millisecond)
		f.SetError(expectedErr)
	}()

	result, err := f.Get(ctx)
	if err != expectedErr {
		t.Fatalf("Expected error '%v', got '%v'", expectedErr, err)
	}
	if result != "" {
		t.Errorf("Expected empty string on error, got '%s'", result)
	}
}

func TestFuture_Cancel(t *testing.T) {
	ctx := context.Background()
	f := NewFuture[string]()

	f.Cancel()
	_, err := f.Get(ctx)
	if err != nil {
		t.Fatalf("Get after cancel should not error: %v", err)
	}

	if !f.IsCancelled() {
		t.Error("Future should be cancelled")
	}
}

func TestFuture_Done(t *testing.T) {
	f := NewFuture[int]()

	select {
	case <-f.Done():
		t.Error("Done should not be closed yet")
	default:
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		f.Set(42)
	}()

	select {
	case <-f.Done():
		// Success
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for Done")
	}
}

func TestFuture_Then(t *testing.T) {
	f := NewFuture[string]()
	called := false

	f.Then(func(value string, err error) {
		called = true
		if value != "test" {
			t.Errorf("Expected 'test', got '%s'", value)
		}
	})

	f.Set("test")
	time.Sleep(100 * time.Millisecond)

	if !called {
		t.Error("Then callback was not called")
	}
}

func TestFuture_Catch(t *testing.T) {
	f := NewFuture[string]()
	called := false

	f.Catch(func(err error) {
		called = true
		if err.Error() != "test error" {
			t.Errorf("Expected error 'test error', got '%v'", err)
		}
	})

	f.SetError(errors.New("test error"))
	time.Sleep(100 * time.Millisecond)

	if !called {
		t.Error("Catch callback was not called")
	}
}

func TestFuture_Finally(t *testing.T) {
	f := NewFuture[string]()
	called := false

	f.Finally(func() {
		called = true
	})

	f.Set("test")
	time.Sleep(100 * time.Millisecond)

	if !called {
		t.Error("Finally callback was not called")
	}
}

func TestNewCompletedFuture(t *testing.T) {
	f := NewCompletedFuture(42, nil)

	ctx := context.Background()
	result, err := f.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result != 42 {
		t.Errorf("Expected 42, got %v", result)
	}

	if !f.IsComplete() {
		t.Error("Future should be complete")
	}
}

func TestWaitAll(t *testing.T) {
	ctx := context.Background()
	futures := []Future[int]{
		NewCompletedFuture(1, nil),
		NewCompletedFuture(2, nil),
		NewCompletedFuture(3, nil),
	}

	results, errs := WaitAll(ctx, futures...)
	// Check for any non-nil errors
	for i, err := range errs {
		if err != nil {
			t.Fatalf("WaitAll error at index %d: %v", i, err)
		}
	}
	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}
	if results[0] != 1 || results[1] != 2 || results[2] != 3 {
		t.Errorf("Unexpected results: %v", results)
	}
}

func TestWaitAny(t *testing.T) {
	ctx := context.Background()
	f1 := NewFuture[int]()
	f2 := NewFuture[int]()
	f3 := NewCompletedFuture[int](3, nil)

	go func() {
		time.Sleep(50 * time.Millisecond)
		f1.Set(1)
	}()

	index, value, err := WaitAny[int](ctx, f1, f2, f3)
	if err != nil {
		t.Fatalf("WaitAny failed: %v", err)
	}
	if index != 2 {
		t.Errorf("Expected index 2, got %d", index)
	}
	if value != 3 {
		t.Errorf("Expected value 3, got %v", value)
	}
}

func TestMap(t *testing.T) {
	f := NewFuture[int]()
	f.Set(10)

	mapped := Map[int, string](f, func(n int) (string, error) {
		return "value:" + string(rune('0'+n)), nil
	})

	ctx := context.Background()
	result, err := mapped.Get(ctx)
	if err != nil {
		t.Fatalf("Mapped Get failed: %v", err)
	}
	if len(result) == 0 {
		t.Errorf("Expected non-empty result, got '%s'", result)
	}
}

func TestFlatMap(t *testing.T) {
	f := NewFuture[int]()
	f.Set(5)

	mapped := FlatMap[int, string](f, func(n int) Future[string] {
		result := NewFuture[string]()
		result.Set("mapped:" + string(rune('0'+n)))
		return result
	})

	ctx := context.Background()
	result, err := mapped.Get(ctx)
	if err != nil {
		t.Fatalf("FlatMap Get failed: %v", err)
	}
	if len(result) == 0 {
		t.Errorf("Expected non-empty result, got '%s'", result)
	}
}

func TestAll(t *testing.T) {
	f1 := NewCompletedFuture(1, nil)
	f2 := NewCompletedFuture(2, nil)
	f3 := NewCompletedFuture(3, nil)

	all := All(f1, f2, f3)

	ctx := context.Background()
	results, err := all.Get(ctx)
	if err != nil {
		t.Fatalf("All Get failed: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}
}

func TestRace(t *testing.T) {
	f1 := NewFuture[int]()
	f2 := NewFuture[int]()
	f3 := NewCompletedFuture(3, nil)

	go func() {
		time.Sleep(100 * time.Millisecond)
		f1.Set(1)
	}()

	go func() {
		time.Sleep(200 * time.Millisecond)
		f2.Set(2)
	}()

	race := Race[int](f1, f2, f3)

	ctx := context.Background()
	result, err := race.Get(ctx)
	if err != nil {
		t.Fatalf("Race Get failed: %v", err)
	}
	if result != 3 {
		t.Errorf("Expected 3 (first completed), got %v", result)
	}
}

func TestFuture_GetWithTimeout(t *testing.T) {
	f := NewFuture[string]()

	// Test timeout
	_, err := f.GetWithTimeout(50 * time.Millisecond)
	if err != context.DeadlineExceeded {
		t.Errorf("Expected timeout error, got %v", err)
	}

	// Test success
	f.Set("hello")
	result, err := f.GetWithTimeout(100 * time.Millisecond)
	if err != nil {
		t.Fatalf("GetWithTimeout failed: %v", err)
	}
	if result != "hello" {
		t.Errorf("Expected 'hello', got '%s'", result)
	}
}

func TestFuture_IsComplete(t *testing.T) {
	f := NewFuture[string]()

	if f.IsComplete() {
		t.Error("Future should not be complete initially")
	}

	f.Set("test")

	if !f.IsComplete() {
		t.Error("Future should be complete after Set")
	}
}
