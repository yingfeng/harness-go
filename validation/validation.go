// Package validation provides graph validation utilities.
package validation

import (
	"fmt"
	"strings"

	"github.com/infiniflow/ragflow/harness/constants"
	"github.com/infiniflow/ragflow/harness/errors"
)

// Validator validates graph structure.
type Validator struct {
	nodes      map[string]bool
	edges      map[string][]string
	conditions map[string]map[string]string
	entryPoint string
	finishPoints map[string]bool
}

// NewValidator creates a new validator.
func NewValidator() *Validator {
	return &Validator{
		nodes:      make(map[string]bool),
		edges:      make(map[string][]string),
		conditions: make(map[string]map[string]string),
		finishPoints: make(map[string]bool),
	}
}

// AddNode adds a node to the validator.
func (v *Validator) AddNode(name string) {
	v.nodes[name] = true
}

// AddEdge adds an edge to the validator.
func (v *Validator) AddEdge(from, to string) {
	if _, ok := v.edges[from]; !ok {
		v.edges[from] = []string{}
	}
	v.edges[from] = append(v.edges[from], to)
}

// AddCondition adds a conditional edge to the validator.
func (v *Validator) AddCondition(node string, result string, target string) {
	if _, ok := v.conditions[node]; !ok {
		v.conditions[node] = make(map[string]string)
	}
	v.conditions[node][result] = target
}

// SetEntryPoint sets the entry point.
func (v *Validator) SetEntryPoint(node string) {
	v.entryPoint = node
}

// AddFinishPoint adds a finish point.
func (v *Validator) AddFinishPoint(node string) {
	v.finishPoints[node] = true
}

// Validate validates the graph structure.
func (v *Validator) Validate() error {
	// Check entry point is set
	if v.entryPoint == "" {
		return fmt.Errorf("no entry point set")
	}
	
	// Check entry point exists
	if !v.nodes[v.entryPoint] {
		return &errors.NodeNotFoundError{NodeName: v.entryPoint}
	}
	
	// Check finish points exist
	for node := range v.finishPoints {
		if !v.nodes[node] {
			return &errors.NodeNotFoundError{NodeName: node}
		}
	}
	
	// Check all edge nodes exist
	for from, targets := range v.edges {
		if from != constants.Start && !v.nodes[from] {
			return &errors.NodeNotFoundError{NodeName: from}
		}
		for _, to := range targets {
			if to != constants.End && !v.nodes[to] {
				return &errors.NodeNotFoundError{NodeName: to}
			}
		}
	}
	
	// Check all condition targets exist
	for node, mapping := range v.conditions {
		if !v.nodes[node] {
			return &errors.NodeNotFoundError{NodeName: node}
		}
		for _, target := range mapping {
			if target != constants.End && !v.nodes[target] {
				return &errors.NodeNotFoundError{NodeName: target}
			}
		}
	}
	
	// Check that all nodes are reachable from entry point
	reachable := v.computeReachable()
	for node := range v.nodes {
		if !reachable[node] {
			return fmt.Errorf("node %s is not reachable from entry point", node)
		}
	}
	
	// Check for cycles that would cause infinite loops
	if err := v.detectCycles(); err != nil {
		return err
	}
	
	return nil
}

// computeReachable computes all reachable nodes from entry point.
func (v *Validator) computeReachable() map[string]bool {
	reachable := make(map[string]bool)
	
	queue := []string{v.entryPoint}
	reachable[v.entryPoint] = true
	
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		
		// Follow regular edges
		for _, edge := range v.edges[current] {
			if edge != constants.End && !reachable[edge] {
				reachable[edge] = true
				queue = append(queue, edge)
			}
		}
		
		// Follow conditional edges
		if mapping, ok := v.conditions[current]; ok {
			for _, target := range mapping {
				if target != constants.End && !reachable[target] {
					reachable[target] = true
					queue = append(queue, target)
				}
			}
		}
	}
	
	return reachable
}

// detectCycles detects cycles in the graph.
func (v *Validator) detectCycles() error {
	visited := make(map[string]bool)
	recStack := make(map[string]bool)
	
	for node := range v.nodes {
		if !visited[node] {
			if cycle := v.detectCycleDFS(node, visited, recStack); cycle != nil {
				return fmt.Errorf("cycle detected: %s", strings.Join(cycle, " -> "))
			}
		}
	}
	
	return nil
}

// detectCycleDFS performs DFS to detect cycles.
func (v *Validator) detectCycleDFS(node string, visited, recStack map[string]bool) []string {
	visited[node] = true
	recStack[node] = true
	
	// Regular edges
	for _, target := range v.edges[node] {
		if target == constants.End {
			continue
		}
		
		if !visited[target] {
			if subCycle := v.detectCycleDFS(target, visited, recStack); subCycle != nil {
				return append([]string{node}, subCycle...)
			}
		} else if recStack[target] {
			// Found a cycle
			return append([]string{node}, target)
		}
	}
	
	// Conditional edges
	if mapping, ok := v.conditions[node]; ok {
		for _, target := range mapping {
			if target == constants.End {
				continue
			}
			
			if !visited[target] {
				if subCycle := v.detectCycleDFS(target, visited, recStack); subCycle != nil {
					return append([]string{node}, subCycle...)
				}
			} else if recStack[target] {
				// Found a cycle
				return append([]string{node}, target)
			}
		}
	}
	
	recStack[node] = false
	return nil
}

// CheckNodeExists checks if a node exists.
func (v *Validator) CheckNodeExists(node string) error {
	if !v.nodes[node] {
		return &errors.NodeNotFoundError{NodeName: node}
	}
	return nil
}

// CheckEdgeExists checks if an edge can be added.
func (v *Validator) CheckEdgeExists(from, to string) error {
	if from != constants.Start && !v.nodes[from] {
		return &errors.NodeNotFoundError{NodeName: from}
	}
	if to != constants.End && !v.nodes[to] {
		return &errors.NodeNotFoundError{NodeName: to}
	}
	return nil
}

// GetOrphanNodes returns nodes that are not reachable.
func (v *Validator) GetOrphanNodes() []string {
	reachable := v.computeReachable()
	orphans := []string{}
	
	for node := range v.nodes {
		if !reachable[node] {
			orphans = append(orphans, node)
		}
	}
	
	return orphans
}

// GetDeadEndNodes returns nodes with no outgoing edges.
func (v *Validator) GetDeadEndNodes() []string {
	deadEnds := []string{}
	
	for node := range v.nodes {
		hasOutgoing := false
		
		// Check regular edges
		if targets, ok := v.edges[node]; ok {
			if len(targets) > 0 {
				hasOutgoing = true
			}
		}
		
		// Check conditional edges
		if mapping, ok := v.conditions[node]; ok {
			if len(mapping) > 0 {
				hasOutgoing = true
			}
		}
		
		// Finish points are OK to have no outgoing edges
		if v.finishPoints[node] {
			continue
		}
		
		if !hasOutgoing {
			deadEnds = append(deadEnds, node)
		}
	}
	
	return deadEnds
}

// GetNodeCount returns the number of nodes.
func (v *Validator) GetNodeCount() int {
	return len(v.nodes)
}

// GetEdgeCount returns the number of edges.
func (v *Validator) GetEdgeCount() int {
	count := 0
	for _, targets := range v.edges {
		count += len(targets)
	}
	return count
}

// GetConditionCount returns the number of conditional edges.
func (v *Validator) GetConditionCount() int {
	count := 0
	for _, mapping := range v.conditions {
		count += len(mapping)
	}
	return count
}

// GetLongestPath returns the longest path length from entry point.
func (v *Validator) GetLongestPath() int {
	return v.computeLongestPath(v.entryPoint, make(map[string]bool))
}

func (v *Validator) computeLongestPath(node string, visited map[string]bool) int {
	if node == constants.End || node == "" {
		return 0
	}
	
	if visited[node] {
		return 0 // Cycle detected
	}
	
	visited[node] = true
	maxPath := 0
	
	// Check regular edges
	for _, target := range v.edges[node] {
		pathLen := v.computeLongestPath(target, copyMap(visited))
		if pathLen > maxPath {
			maxPath = pathLen
		}
	}
	
	// Check conditional edges
	if mapping, ok := v.conditions[node]; ok {
		for _, target := range mapping {
			pathLen := v.computeLongestPath(target, copyMap(visited))
			if pathLen > maxPath {
				maxPath = pathLen
			}
		}
	}
	
	return maxPath + 1
}

func copyMap(m map[string]bool) map[string]bool {
	copy := make(map[string]bool, len(m))
	for k, v := range m {
		copy[k] = v
	}
	return copy
}

// ValidateNodeOptions validates node configuration options.
func ValidateNodeOptions(options map[string]interface{}) error {
	// Check for invalid keys
	validKeys := map[string]bool{
		"retry_policy":  true,
		"cache_policy":  true,
		"tags":          true,
		"metadata":      true,
		"interrupt":     true,
	}
	
	for key := range options {
		if !validKeys[key] {
			return fmt.Errorf("invalid node option: %s", key)
		}
	}
	
	// Validate retry policy if present
	if rp, ok := options["retry_policy"]; ok {
		if err := ValidateRetryPolicy(rp); err != nil {
			return err
		}
	}
	
	return nil
}

// ValidateRetryPolicy validates a retry policy.
func ValidateRetryPolicy(policy interface{}) error {
	if policy == nil {
		return nil
	}
	
	// Check if it's a map/struct with required fields
	policyMap, ok := policy.(map[string]interface{})
	if !ok {
		return fmt.Errorf("retry policy must be a map or struct")
	}
	
	requiredFields := []string{"max_attempts", "initial_interval"}
	for _, field := range requiredFields {
		if _, ok := policyMap[field]; !ok {
			return fmt.Errorf("retry policy missing required field: %s", field)
		}
	}
	
	// Validate max_attempts is positive
	if ma, ok := policyMap["max_attempts"].(int); ok && ma <= 0 {
		return fmt.Errorf("max_attempts must be positive")
	}
	
	return nil
}

// ValidateChannelName validates a channel name.
func ValidateChannelName(name string) error {
	if name == "" {
		return fmt.Errorf("channel name cannot be empty")
	}
	
	// Check for reserved names
	reserved := []string{
		constants.Start,
		constants.End,
		"__config__",
		"__send__",
		"__read__",
		"__write__",
		"__store__",
		"__runtime__",
		"__scratchpad__",
	}
	
	for _, r := range reserved {
		if name == r {
			return fmt.Errorf("'%s' is a reserved name", name)
		}
	}
	
	return nil
}
