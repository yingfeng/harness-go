// Package branch provides branching and conditional edge functionality.
package branch

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/infiniflow/ragflow/harness/errors"
	"github.com/infiniflow/ragflow/harness/types"
)

// BranchSpec represents a conditional branch specification.
type BranchSpec struct {
	// Condition is the function that evaluates the branch.
	Condition types.EdgeFunc
	// Mapping from condition result to target node names.
	Mapping map[string]interface{}
	// Then is a function that takes the condition result and returns target node names.
	Then func(interface{}) []string
}

// NewBranchSpec creates a new branch specification.
func NewBranchSpec(condition types.EdgeFunc, mapping map[string]interface{}) *BranchSpec {
	return &BranchSpec{
		Condition: condition,
		Mapping:   mapping,
		Then:      nil,
	}
}

// NewBranchSpecWithFunc creates a new branch specification with a custom then function.
func NewBranchSpecWithFunc(condition types.EdgeFunc, then func(interface{}) []string) *BranchSpec {
	return &BranchSpec{
		Condition: condition,
		Mapping:   nil,
		Then:      then,
	}
}

// Evaluate evaluates the branch and returns the target node names.
func (b *BranchSpec) Evaluate(ctx context.Context, state interface{}) ([]string, error) {
	// Evaluate condition
	result, err := b.Condition(ctx, state)
	if err != nil {
		return nil, fmt.Errorf("branch condition evaluation failed: %w", err)
	}
	
	// Determine target nodes
	if b.Then != nil {
		// Use custom then function
		return b.Then(result), nil
	}
	
	// Use mapping
	resultKey := fmt.Sprintf("%v", result)
	target, ok := b.Mapping[resultKey]
	if !ok {
		// Try to find a key that matches the type of result
		for k, v := range b.Mapping {
			if k == resultKey {
				target = v
				break
			}
		}
		
		if target == nil {
			return nil, &errors.InvalidEdgeError{
				Message: fmt.Sprintf("no mapping for condition result '%s'", resultKey),
			}
		}
	}
	
	// Convert target to list of node names
	nodeNames, err := toNodeNames(target)
	if err != nil {
		return nil, fmt.Errorf("invalid branch target: %w", err)
	}
	
	return nodeNames, nil
}

// GetMapping returns the mapping from condition results to node names.
func (b *BranchSpec) GetMapping() map[string]interface{} {
	if b.Mapping == nil {
		return make(map[string]interface{})
	}
	return b.Mapping
}

// SetMapping sets the mapping for the branch.
func (b *BranchSpec) SetMapping(mapping map[string]interface{}) {
	b.Mapping = mapping
}

// GetCondition returns the condition function.
func (b *BranchSpec) GetCondition() types.EdgeFunc {
	return b.Condition
}

// SetCondition sets the condition function.
func (b *BranchSpec) SetCondition(condition types.EdgeFunc) {
	b.Condition = condition
}

// GetThen returns the then function.
func (b *BranchSpec) GetThen() func(interface{}) []string {
	return b.Then
}

// SetThen sets the then function.
func (b *BranchSpec) SetThen(then func(interface{}) []string) {
	b.Then = then
}

// PathMap represents a mapping of condition results to paths.
type PathMap map[string]interface{}

// NewPathMap creates a new path map.
func NewPathMap() PathMap {
	return make(PathMap)
}

// Set sets a mapping from key to target.
func (p PathMap) Set(key string, target interface{}) {
	p[key] = target
}

// Get gets the target for a key.
func (p PathMap) Get(key string) (interface{}, bool) {
	val, ok := p[key]
	return val, ok
}

// Contains checks if a key exists in the map.
func (p PathMap) Contains(key string) bool {
	_, ok := p[key]
	return ok
}

// Keys returns all keys in the map.
func (p PathMap) Keys() []string {
	keys := make([]string, 0, len(p))
	for k := range p {
		keys = append(keys, k)
	}
	return keys
}

// toNodeNames converts a target to a list of node names.
func toNodeNames(target interface{}) ([]string, error) {
	if target == nil {
		return nil, fmt.Errorf("target cannot be nil")
	}
	
	// If it's a string
	if str, ok := target.(string); ok {
		if str == "" {
			return nil, nil
		}
		return []string{str}, nil
	}
	
	// If it's a list/slice
	if reflect.TypeOf(target).Kind() == reflect.Slice {
		rv := reflect.ValueOf(target)
		names := make([]string, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			val := rv.Index(i).Interface()
			if str, ok := val.(string); ok {
				names[i] = str
			} else {
				names[i] = fmt.Sprintf("%v", val)
			}
		}
		return names, nil
	}
	
	// If it's a Send object
	if send, ok := target.(*types.Send); ok {
		return []string{send.Node}, nil
	}
	
	// If it's a list of Send objects
	if list, ok := target.([]*types.Send); ok {
		names := make([]string, len(list))
		for i, s := range list {
			names[i] = s.Node
		}
		return names, nil
	}
	
	// Try to convert to string
	return []string{fmt.Sprintf("%v", target)}, nil
}

// When creates a conditional branch from a function.
func When(condition types.EdgeFunc) *BranchBuilder {
	return &BranchBuilder{
		condition: condition,
		branches: make(map[string]interface{}),
	}
}

// BranchBuilder provides a fluent API for building branches.
type BranchBuilder struct {
	condition types.EdgeFunc
	branches  map[string]interface{}
	then      func(interface{}) []string
}

// Then sets the custom then function.
func (b *BranchBuilder) Then(fn func(interface{}) []string) *BranchSpec {
	b.then = fn
	return b.BranchSpec()
}

// To adds a mapping from a result value to a target.
func (b *BranchBuilder) To(result string, target interface{}) *BranchBuilder {
	b.branches[result] = target
	return b
}

// ToMany adds mappings from multiple result values to targets.
func (b *BranchBuilder) ToMany(mapping map[string]interface{}) *BranchBuilder {
	for k, v := range mapping {
		b.branches[k] = v
	}
	return b
}

// BranchSpec returns the built BranchSpec.
func (b *BranchBuilder) BranchSpec() *BranchSpec {
	return &BranchSpec{
		Condition: b.condition,
		Mapping:   b.branches,
		Then:      b.then,
	}
}

// If creates a conditional branch that checks a simple predicate.
func If(predicate func(interface{}) bool, thenTarget, elseTarget string) *BranchSpec {
	return NewBranchSpec(
		func(ctx context.Context, state interface{}) (interface{}, error) {
			if predicate(state) {
				return "true", nil
			}
			return "false", nil
		},
		map[string]interface{}{
			"true":  thenTarget,
			"false": elseTarget,
		},
	)
}

// Switch creates a switch-like branch based on a value selector.
func Switch(selector func(interface{}) (string, error), cases map[string]string, defaultCase string) *BranchSpec {
	mapping := make(map[string]interface{})
	for k, v := range cases {
		mapping[k] = v
	}
	if defaultCase != "" {
		mapping["__default__"] = defaultCase
	}
	
	return NewBranchSpec(
		func(ctx context.Context, state interface{}) (interface{}, error) {
			value, err := selector(state)
			if err != nil {
				return nil, err
			}
			
			// Check if value is in cases
			if target, ok := mapping[value]; ok {
				return target, nil
			}
			
			// Return default if exists
			if def, ok := mapping["__default__"]; ok {
				return def, nil
			}
			
			return nil, fmt.Errorf("no case for value '%s'", value)
		},
		mapping,
	)
}

// Range creates a branch based on numeric ranges.
func Range(selector func(interface{}) (float64, error), ranges map[string]NumericRange, defaultCase string) *BranchSpec {
	mapping := make(map[string]interface{})
	for name, r := range ranges {
		mapping[name] = r
	}
	if defaultCase != "" {
		mapping["__default__"] = defaultCase
	}
	
	return NewBranchSpec(
		func(ctx context.Context, state interface{}) (interface{}, error) {
			value, err := selector(state)
			if err != nil {
				return nil, err
			}
			
			// Check which range the value falls into
			for name, r := range ranges {
				if r.Contains(value) {
					return name, nil
				}
			}
			
			// Return default if exists
			if _, ok := mapping["__default__"]; ok {
				return defaultCase, nil
			}
			
			return nil, fmt.Errorf("value %.2f does not match any range", value)
		},
		mapping,
	)
}

// NumericRange represents a numeric range for branching.
type NumericRange struct {
	Min float64
	Max float64
}

// NewNumericRange creates a new numeric range.
func NewNumericRange(min, max float64) NumericRange {
	return NumericRange{
		Min: min,
		Max: max,
	}
}

// Contains checks if a value is within the range.
func (r NumericRange) Contains(value float64) bool {
	return value >= r.Min && value <= r.Max
}

// GreaterThan creates a range for values greater than min.
func GreaterThan(min float64) NumericRange {
	return NumericRange{
		Min: min,
		Max: 1e100, // effectively infinity
	}
}

// LessThan creates a range for values less than max.
func LessThan(max float64) NumericRange {
	return NumericRange{
		Min: -1e100, // effectively negative infinity
		Max: max,
	}
}

// Between creates a range for values between min and max (inclusive).
func Between(min, max float64) NumericRange {
	return NumericRange{
		Min: min,
		Max: max,
	}
}

// StringEquals creates a branch based on string equality.
func StringEquals(selector func(interface{}) (string, error), value string, thenTarget string) *BranchSpec {
	return NewBranchSpec(
		func(ctx context.Context, state interface{}) (interface{}, error) {
			selected, err := selector(state)
			if err != nil {
				return nil, err
			}
			
			if strings.TrimSpace(selected) == strings.TrimSpace(value) {
				return "true", nil
			}
			return "false", nil
		},
		map[string]interface{}{
			"true":  thenTarget,
			"false": "", // Go to END if not equal
		},
	)
}

// StringContains creates a branch based on substring check.
func StringContains(selector func(interface{}) (string, error), substring string, thenTarget string) *BranchSpec {
	return NewBranchSpec(
		func(ctx context.Context, state interface{}) (interface{}, error) {
			selected, err := selector(state)
			if err != nil {
				return nil, err
			}
			
			if strings.Contains(selected, substring) {
				return "true", nil
			}
			return "false", nil
		},
		map[string]interface{}{
			"true":  thenTarget,
			"false": "",
		},
	)
}
