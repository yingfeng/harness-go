// Package pregel provides multi-policy retry support for Pregel execution.
package pregel

import (
	"context"
	"fmt"
	"time"

	"github.com/infiniflow/ragflow/harness/types"
)

// MultiPolicyRetryExecutor handles node execution with multiple retry policies.
// The first matching policy is used for each retry attempt.
type MultiPolicyRetryExecutor struct {
	policies []types.RetryPolicy
}

// NewMultiPolicyRetryExecutor creates a new retry executor with multiple policies.
func NewMultiPolicyRetryExecutor(policies ...types.RetryPolicy) *MultiPolicyRetryExecutor {
	return &MultiPolicyRetryExecutor{
		policies: policies,
	}
}

// Execute executes a function with multi-policy retry logic.
func (e *MultiPolicyRetryExecutor) Execute(
	ctx context.Context,
	name string,
	fn func(context.Context) (interface{}, error),
) (interface{}, error) {
	if len(e.policies) == 0 {
		// No policies, execute directly
		return fn(ctx)
	}

	var lastErr error
	var lastOutput interface{}
	attempt := 0

	for {
		// Execute the function
		output, err := fn(ctx)
		if err == nil {
			return output, nil
		}

		lastErr = err
		lastOutput = output
		attempt++

		// Find matching policy for this error
		policy := e.findMatchingPolicy(err)
		if policy == nil {
			// No matching policy, fail immediately
			return nil, fmt.Errorf("node %s failed with non-retryable error: %w", name, err)
		}

		// Check if we've exhausted attempts for this policy
		if attempt >= policy.MaxAttempts {
			break
		}

		// Calculate backoff
		backoff := e.calculateBackoff(attempt, policy)

		// Wait before retry
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("node %s cancelled during retry: %w", name, ctx.Err())
		case <-time.After(backoff):
			// Continue to next attempt
		}
	}

	return nil, &RetryExhaustedError{
		NodeName:   name,
		Attempts:   attempt,
		LastErr:    lastErr,
		LastOutput: lastOutput,
	}
}

// findMatchingPolicy finds the first policy that matches the error.
func (e *MultiPolicyRetryExecutor) findMatchingPolicy(err error) *types.RetryPolicy {
	for i := range e.policies {
		policy := &e.policies[i]
		if policy.RetryOn == nil {
			// No predicate means always retry
			return policy
		}
		if policy.RetryOn(err) {
			return policy
		}
	}
	return nil
}

// calculateBackoff calculates the backoff duration for the given attempt.
func (e *MultiPolicyRetryExecutor) calculateBackoff(attempt int, policy *types.RetryPolicy) time.Duration {
	// Calculate exponential backoff
	backoff := time.Duration(float64(policy.InitialInterval) *
		pow(policy.BackoffFactor, attempt-1))

	// Cap at max interval
	if backoff > policy.MaxInterval {
		backoff = policy.MaxInterval
	}

	// Add jitter if enabled
	if policy.Jitter {
		jitter := float64(backoff) * 0.5 * randomFloat()
		backoff = time.Duration(float64(backoff) - jitter)
	}

	return backoff
}

// randomFloat returns a random float between 0 and 1.
func randomFloat() float64 {
	// Simple pseudo-random number generator
	// In production, use crypto/rand or math/rand with proper seeding
	return float64(time.Now().UnixNano()%10000) / 10000.0
}

// MultiRetryConfig provides configuration for multi-policy retry behavior.
type MultiRetryConfig struct {
	// Policies is the list of retry policies to apply.
	Policies []types.RetryPolicy
	// OnRetry is called after each failed attempt.
	OnRetry func(attempt int, policyIndex int, err error)
	// OnSuccess is called on successful completion.
	OnSuccess func(attempt int)
	// OnPolicyMatch is called when a policy matches an error.
	OnPolicyMatch func(attempt int, policyIndex int)
}

// NewMultiRetryConfig creates a new multi-retry config with defaults.
func NewMultiRetryConfig(policies ...types.RetryPolicy) *MultiRetryConfig {
	if len(policies) == 0 {
		// Default policy
		defaultPolicy := types.DefaultRetryPolicy()
		policies = []types.RetryPolicy{defaultPolicy}
	}

	return &MultiRetryConfig{
		Policies: policies,
	}
}

// WithOnRetry sets the retry callback.
func (c *MultiRetryConfig) WithOnRetry(callback func(attempt int, policyIndex int, err error)) *MultiRetryConfig {
	c.OnRetry = callback
	return c
}

// WithOnSuccess sets the success callback.
func (c *MultiRetryConfig) WithOnSuccess(callback func(attempt int)) *MultiRetryConfig {
	c.OnSuccess = callback
	return c
}

// WithOnPolicyMatch sets the policy match callback.
func (c *MultiRetryConfig) WithOnPolicyMatch(callback func(attempt int, policyIndex int)) *MultiRetryConfig {
	c.OnPolicyMatch = callback
	return c
}

// CreateExecutor creates a MultiPolicyRetryExecutor from this config.
func (c *MultiRetryConfig) CreateExecutor() *MultiPolicyRetryExecutor {
	return NewMultiPolicyRetryExecutor(c.Policies...)
}

// Common policy presets for multi-policy retry.
var RetryPolicyPresets = struct {
	// NetworkErrors retries on network-related errors with aggressive backoff.
	NetworkErrors types.RetryPolicy
	// TemporaryErrors retries on temporary errors with moderate backoff.
	TemporaryErrors types.RetryPolicy
	// ResourceErrors retries on resource exhaustion errors with long backoff.
	ResourceErrors types.RetryPolicy
	// TimeoutErrors retries on timeout errors with moderate backoff.
	TimeoutErrors types.RetryPolicy
}{
	NetworkErrors: types.RetryPolicy{
		MaxAttempts:     5,
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     30 * time.Second,
		BackoffFactor:   2.0,
		Jitter:          true,
		RetryOn:         RetryPredicates.NetworkErrors,
	},
	TemporaryErrors: types.RetryPolicy{
		MaxAttempts:     3,
		InitialInterval: 500 * time.Millisecond,
		MaxInterval:     10 * time.Second,
		BackoffFactor:   1.5,
		Jitter:          true,
		RetryOn:         RetryPredicates.TemporaryErrors,
	},
	ResourceErrors: types.RetryPolicy{
		MaxAttempts:     10,
		InitialInterval: 1 * time.Second,
		MaxInterval:     60 * time.Second,
		BackoffFactor:   1.5,
		Jitter:          true,
		RetryOn: func(err error) bool {
			if err == nil {
				return false
			}
			errMsg := err.Error()
			resourceKeywords := []string{
				"resource exhausted",
				"too many requests",
				"rate limit",
				"quota exceeded",
				"429",
				"503",
			}
			for _, kw := range resourceKeywords {
				if contains(errMsg, kw) {
					return true
				}
			}
			return false
		},
	},
	TimeoutErrors: types.RetryPolicy{
		MaxAttempts:     3,
		InitialInterval: 1 * time.Second,
		MaxInterval:     30 * time.Second,
		BackoffFactor:   2.0,
		Jitter:          false,
		RetryOn: func(err error) bool {
			if err == nil {
				return false
			}
			errMsg := err.Error()
			timeoutKeywords := []string{
				"timeout",
				"deadline exceeded",
				"context deadline",
				"408",
				"504",
			}
			for _, kw := range timeoutKeywords {
				if contains(errMsg, kw) {
					return true
				}
			}
			return false
		},
	},
}

// CreateDefaultMultiPolicy creates a default multi-policy retry configuration.
func CreateDefaultMultiPolicy() *MultiRetryConfig {
	return NewMultiRetryConfig(
		RetryPolicyPresets.NetworkErrors,
		RetryPolicyPresets.TemporaryErrors,
		RetryPolicyPresets.TimeoutErrors,
		RetryPolicyPresets.ResourceErrors,
	)
}
