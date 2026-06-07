// Package util provides reusable utility types and functions.
// This file implements generic Future/Promise primitives.
package util

import (
	"context"
	"sync"
	"time"
)

// Result represents the result of a Future operation.
type Result[T any] struct {
	Value T
	Err   error
}

// Future represents a value that may not be available yet.
// It provides methods to wait for, check, and retrieve the result.
type Future[T any] interface {
	// Get blocks until the future is complete and returns the result.
	Get(ctx context.Context) (T, error)

	// GetWithTimeout blocks until the future is complete or timeout.
	GetWithTimeout(timeout time.Duration) (T, error)

	// Done returns a channel that is closed when the future is complete.
	Done() <-chan struct{}

	// IsComplete returns true if the future is complete.
	IsComplete() bool

	// IsCancelled returns true if the future was cancelled.
	IsCancelled() bool

	// Cancel attempts to cancel the future.
	Cancel()

	// Then registers a callback to be called when the future completes.
	Then(callback func(T, error))

	// Catch registers a callback to be called only on error.
	Catch(callback func(error))

	// Finally registers a callback to be called regardless of success or failure.
	Finally(callback func())
}

// CompletableFuture is a Future that can be completed by calling Set or SetError.
type CompletableFuture[T any] interface {
	Future[T]

	// Set sets the value of the future.
	Set(value T)

	// SetError sets an error on the future.
	SetError(err error)
}

// futureImpl is the default implementation of Future[T].
type futureImpl[T any] struct {
	mu        sync.RWMutex
	done      chan struct{}
	value     T
	err       error
	completed bool
	cancelled bool
}

// NewFuture creates a new incomplete Future.
func NewFuture[T any]() CompletableFuture[T] {
	return &futureImpl[T]{
		done: make(chan struct{}),
	}
}

// NewCompletedFuture creates a Future that is already complete.
func NewCompletedFuture[T any](value T, err error) Future[T] {
	f := &futureImpl[T]{
		done:      make(chan struct{}),
		value:     value,
		err:       err,
		completed: true,
	}
	close(f.done)
	return f
}

// Get blocks until the future is complete.
func (f *futureImpl[T]) Get(ctx context.Context) (T, error) {
	select {
	case <-f.done:
		f.mu.RLock()
		defer f.mu.RUnlock()
		return f.value, f.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

// GetWithTimeout blocks until the future is complete or timeout.
func (f *futureImpl[T]) GetWithTimeout(timeout time.Duration) (T, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return f.Get(ctx)
}

// Done returns a channel that is closed when the future is complete.
func (f *futureImpl[T]) Done() <-chan struct{} {
	return f.done
}

// IsComplete returns true if the future is complete.
func (f *futureImpl[T]) IsComplete() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.completed
}

// IsCancelled returns true if the future was cancelled.
func (f *futureImpl[T]) IsCancelled() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.cancelled
}

// Cancel attempts to cancel the future.
func (f *futureImpl[T]) Cancel() {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.completed {
		f.cancelled = true
		var zero T
		f.complete(zero, nil)
	}
}

// Set sets the value of the future.
func (f *futureImpl[T]) Set(value T) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.complete(value, nil)
}

// SetError sets an error on the future.
func (f *futureImpl[T]) SetError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var zero T
	f.complete(zero, err)
}

// complete marks the future as complete.
func (f *futureImpl[T]) complete(value T, err error) {
	if f.completed {
		return
	}

	f.value = value
	f.err = err
	f.completed = true
	close(f.done)
}

// Then registers a callback to be called when the future completes.
func (f *futureImpl[T]) Then(callback func(T, error)) {
	go func() {
		<-f.done
		f.mu.RLock()
		value, err := f.value, f.err
		f.mu.RUnlock()
		callback(value, err)
	}()
}

// Catch registers a callback to be called only on error.
func (f *futureImpl[T]) Catch(callback func(error)) {
	f.Then(func(value T, err error) {
		if err != nil {
			callback(err)
		}
	})
}

// Finally registers a callback to be called regardless of success or failure.
func (f *futureImpl[T]) Finally(callback func()) {
	go func() {
		<-f.done
		callback()
	}()
}

// WaitAll waits for all futures to complete and returns their results.
func WaitAll[T any](ctx context.Context, futures ...Future[T]) ([]T, []error) {
	results := make([]T, len(futures))
	errs := make([]error, len(futures))

	var wg sync.WaitGroup
	for i, f := range futures {
		wg.Add(1)
		go func(idx int, fut Future[T]) {
			defer wg.Done()
			results[idx], errs[idx] = fut.Get(ctx)
		}(i, f)
	}
	wg.Wait()

	return results, errs
}

// WaitAny waits for any future to complete and returns the first completed result.
func WaitAny[T any](ctx context.Context, futures ...Future[T]) (int, T, error) {
	if len(futures) == 0 {
		var zero T
		return 0, zero, nil
	}

	doneCh := make(chan struct {
		index int
		value T
		err   error
	}, 1)

	for i, f := range futures {
		go func(idx int, fut Future[T]) {
			value, err := fut.Get(ctx)
			select {
			case doneCh <- struct {
				index int
				value T
				err   error
			}{idx, value, err}:
			default:
			}
		}(i, f)
	}

	select {
	case result := <-doneCh:
		return result.index, result.value, result.err
	case <-ctx.Done():
		var zero T
		return 0, zero, ctx.Err()
	}
}

// Map transforms the result of a future using a function.
func Map[T, U any](f Future[T], fn func(T) (U, error)) Future[U] {
	result := NewFuture[U]()

	f.Then(func(value T, err error) {
		if err != nil {
			result.SetError(err)
			return
		}
		mapped, err := fn(value)
		if err != nil {
			result.SetError(err)
			return
		}
		result.Set(mapped)
	})

	return result
}

// FlatMap chains futures using a function that returns a future.
func FlatMap[T, U any](f Future[T], fn func(T) Future[U]) Future[U] {
	result := NewFuture[U]()

	f.Then(func(value T, err error) {
		if err != nil {
			result.SetError(err)
			return
		}
		mappedFuture := fn(value)
		mappedFuture.Then(func(mapped U, err error) {
			if err != nil {
				result.SetError(err)
				return
			}
			result.Set(mapped)
		})
	})

	return result
}

// All creates a future that completes when all input futures complete.
func All[T any](futures ...Future[T]) Future[[]T] {
	result := NewFuture[[]T]()

	if len(futures) == 0 {
		result.Set([]T{})
		return result
	}

	values := make([]T, len(futures))
	var wg sync.WaitGroup
	var once sync.Once

	for i, f := range futures {
		wg.Add(1)
		go func(idx int, fut Future[T]) {
			defer wg.Done()
			value, err := fut.Get(context.Background())
			if err != nil {
				once.Do(func() { result.SetError(err) })
				return
			}
			values[idx] = value
		}(i, f)
	}

	go func() {
		wg.Wait()
		once.Do(func() { result.Set(values) })
	}()

	return result
}

// Race creates a future that resolves with the first completed future.
func Race[T any](futures ...Future[T]) Future[T] {
	result := NewFuture[T]()

	if len(futures) == 0 {
		result.SetError(&FutureError{Message: "no futures provided"})
		return result
	}

	for _, f := range futures {
		go func(fut Future[T]) {
			value, err := fut.Get(context.Background())
			if result.IsComplete() {
				return
			}
			if err != nil {
				result.SetError(err)
				return
			}
			result.Set(value)
		}(f)
	}

	return result
}

// FutureError represents a future-related error.
type FutureError struct {
	Message string
	Code    string
}

func (e *FutureError) Error() string {
	if e.Code != "" {
		return e.Code + ": " + e.Message
	}
	return e.Message
}
