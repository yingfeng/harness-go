package graph

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/infiniflow/ragflow/harness/channels"
	"github.com/infiniflow/ragflow/harness/constants"
	"github.com/infiniflow/ragflow/harness/errors"
	"github.com/infiniflow/ragflow/harness/types"
)

// pregelEngine implements the Pregel execution model.
type pregelEngine struct {
	graph          *StateGraph
	checkpointer   Checkpointer
	interrupts     map[string]bool
	recursionLimit int
	debug          bool
	config         *types.RunnableConfig
}

// task represents a task to be executed.
type task struct {
	id       string
	nodeName string
	input    interface{}
	config   map[string]interface{}
}

// taskResult represents the result of executing a task.
type taskResult struct {
	taskID string
	output interface{}
	err    error
}

// run executes the graph using the Pregel model.
func (e *pregelEngine) run(ctx context.Context, input interface{}) (interface{}, error) {
	// Initialize channels from graph state schema
	channelRegistry := channels.NewRegistry()
	for name, ch := range e.graph.GetChannels() {
		channelRegistry.Register(name, ch.Copy())
	}
	
	// Apply input to channels
	if err := e.applyInput(channelRegistry, input); err != nil {
		return nil, fmt.Errorf("failed to apply input: %w", err)
	}
	
	// Get thread ID for checkpointing
	threadID := e.getThreadID()
	
	// Load checkpoint if available
	if e.checkpointer != nil {
		checkpoint, err := e.checkpointer.Get(ctx, map[string]interface{}{
			"thread_id": threadID,
		})
		if err == nil && checkpoint != nil {
			if err := channelRegistry.RestoreFromCheckpoint(checkpoint); err != nil {
				return nil, fmt.Errorf("failed to restore from checkpoint: %w", err)
			}
		}
	}
	
	// Execute Pregel loop
	step := 0
	completedTasks := make(map[string]bool)
	lastCompletedNode := ""
	lastState := input
	
	for {
		// Check recursion limit
		if step >= e.recursionLimit {
			return nil, &errors.GraphRecursionError{Limit: e.recursionLimit}
		}
		
		// Determine next tasks
		tasks, err := e.getNextTasks(channelRegistry, completedTasks, lastCompletedNode, lastState)
		if err != nil {
			return nil, fmt.Errorf("failed to get next tasks: %w", err)
		}
		
		// If no tasks, we're done
		if len(tasks) == 0 {
			break
		}
		
		// Execute tasks
		results, interrupted, err := e.executeTasks(ctx, tasks, channelRegistry)
		if err != nil {
			return nil, fmt.Errorf("failed to execute tasks: %w", err)
		}
		
		// Handle interrupts
		if interrupted {
			// Save checkpoint and return interrupt info
			if e.checkpointer != nil {
				checkpoint := channelRegistry.CreateCheckpoint()
				if err := e.checkpointer.Put(ctx, map[string]interface{}{
					"thread_id": threadID,
				}, checkpoint); err != nil {
					return nil, fmt.Errorf("failed to save checkpoint: %w", err)
				}
			}
			return nil, &errors.GraphInterrupt{}
		}
		
		// Mark tasks as completed and track last state
		for _, result := range results {
			if result.err == nil {
				// Find the task that produced this result
				for _, task := range tasks {
					if task.id == result.taskID {
						completedTasks[task.nodeName] = true
						lastCompletedNode = task.nodeName
						// Merge result into lastState instead of replacing
						lastState = mergeStates(lastState, result.output)
						break
					}
				}
			}
		}
		
		// Apply writes to channels
		if err := e.applyWrites(channelRegistry, results); err != nil {
			return nil, fmt.Errorf("failed to apply writes: %w", err)
		}
		
		// Save checkpoint
		if e.checkpointer != nil {
			checkpoint := channelRegistry.CreateCheckpoint()
			if err := e.checkpointer.Put(ctx, map[string]interface{}{
				"thread_id": threadID,
				"step":      step,
			}, checkpoint); err != nil {
				return nil, fmt.Errorf("failed to save checkpoint: %w", err)
			}
		}
		
		step++
	}
	
	// Get final state
	finalState, err := e.buildOutput(channelRegistry, lastState)
	if err != nil {
		return nil, fmt.Errorf("failed to build output: %w", err)
	}
	
	return finalState, nil
}

// applyInput applies the input to the channels.
func (e *pregelEngine) applyInput(registry *channels.Registry, input interface{}) error {
	// Convert input to map
	inputMap, err := toMap(input)
	if err != nil {
		return fmt.Errorf("input must be a map or struct: %w", err)
	}
	
	// Apply each key to corresponding channel
	writes := make(map[string][]interface{})
	for key, value := range inputMap {
		// Only apply if channel exists
		if _, ok := registry.Get(key); ok {
			writes[key] = []interface{}{value}
		}
	}
	
	if len(writes) > 0 {
		return registry.UpdateChannels(writes)
	}
	return nil
}

// toMap converts a struct or map to a map[string]interface{}.
func toMap(v interface{}) (map[string]interface{}, error) {
	if v == nil {
		return nil, fmt.Errorf("nil value")
	}
	
	// If it's already a map
	if m, ok := v.(map[string]interface{}); ok {
		return m, nil
	}
	
	// Use reflection to convert struct to map
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	
	if rv.Kind() != reflect.Struct && rv.Kind() != reflect.Map {
		return map[string]interface{}{"__root__": v}, nil
	}
	
	result := make(map[string]interface{})
	
	if rv.Kind() == reflect.Map {
		for _, key := range rv.MapKeys() {
			result[fmt.Sprintf("%v", key.Interface())] = rv.MapIndex(key).Interface()
		}
		return result, nil
	}
	
	// Struct
	rt := rv.Type()
	for i := 0; i < rv.NumField(); i++ {
		field := rt.Field(i)
		// Skip unexported fields
		if field.PkgPath != "" {
			continue
		}
		value := rv.Field(i).Interface()
		result[field.Name] = value
	}
	
	return result, nil
}

// getThreadID gets or creates a thread ID.
func (e *pregelEngine) getThreadID() string {
	if e.config != nil && e.config.Configurable != nil {
		if tid, ok := e.config.Configurable["thread_id"].(string); ok {
			return tid
		}
	}
	return uuid.New().String()
}

// getNextTasks determines the next tasks to execute based on graph edges.
func (e *pregelEngine) getNextTasks(registry *channels.Registry, completedTasks map[string]bool, lastCompletedNode string, currentState interface{}) ([]*task, error) {
	tasks := make([]*task, 0)
	
	// If this is the first step and we have an entry point
	if len(completedTasks) == 0 && e.graph.entryPoint != "" {
		node, ok := e.graph.GetNode(e.graph.entryPoint)
		if !ok {
			return nil, &errors.NodeNotFoundError{NodeName: e.graph.entryPoint}
		}
		
		tasks = append(tasks, &task{
			id:       uuid.New().String(),
			nodeName: node.Name,
			input:    currentState,
			config:   e.buildTaskConfig(node),
		})
		
		return tasks, nil
	}
	
	// Find next nodes based on edges from the last completed node
	if lastCompletedNode != "" {
		nextNodes := make(map[string]bool)
		
		// Check for conditional edges first
		for _, condEdge := range e.graph.conditionalEdges {
			if condEdge.From == lastCompletedNode {
				// Execute the condition function
				conditionResult, err := condEdge.Condition(nil, currentState)
				if err != nil {
					return nil, fmt.Errorf("condition evaluation failed for node %s: %w", lastCompletedNode, err)
				}
				
				// Get target node based on condition result
				conditionKey := fmt.Sprintf("%v", conditionResult)
				targetNode, ok := condEdge.Mapping[conditionKey]
				if !ok {
					return nil, fmt.Errorf("no mapping for condition result %s from node %s", conditionKey, lastCompletedNode)
				}
				
				if targetNode == constants.End {
					// Reached end node
					return tasks, nil
				}
				
				if !completedTasks[targetNode] {
					nextNodes[targetNode] = true
				}
			}
		}
		
		// Check regular edges if no conditional edge was found
		if len(nextNodes) == 0 {
			for _, edge := range e.graph.edges {
				if edge.From == lastCompletedNode {
					if edge.To == constants.End {
						// Reached end node
						return tasks, nil
					}
					if !completedTasks[edge.To] {
						nextNodes[edge.To] = true
					}
				}
			}
		}
		
		// Create tasks for next nodes
		for nodeName := range nextNodes {
			node, ok := e.graph.GetNode(nodeName)
			if ok {
				tasks = append(tasks, &task{
					id:       uuid.New().String(),
					nodeName: node.Name,
					input:    currentState,
					config:   e.buildTaskConfig(node),
				})
			}
		}
	}
	
	return tasks, nil
}

// buildTaskConfig builds the configuration for a task.
func (e *pregelEngine) buildTaskConfig(node *Node) map[string]interface{} {
	config := make(map[string]interface{})
	
	if e.config != nil && e.config.Configurable != nil {
		for k, v := range e.config.Configurable {
			config[k] = v
		}
	}
	
	config[constants.ConfigKeyTaskID] = uuid.New().String()
	config[constants.ConfigKeyRuntime] = map[string]interface{}{
		"node_name": node.Name,
	}
	
	return config
}

// executeTasks executes the given tasks.
func (e *pregelEngine) executeTasks(ctx context.Context, tasks []*task, registry *channels.Registry) ([]*taskResult, bool, error) {
	results := make([]*taskResult, 0, len(tasks))
	
	for _, t := range tasks {
		node, ok := e.graph.GetNode(t.nodeName)
		if !ok {
			return nil, false, &errors.NodeNotFoundError{NodeName: t.nodeName}
		}
		
		// Check if this node should interrupt
		if e.interrupts[t.nodeName] || e.interrupts[types.All] {
			return nil, true, nil
		}
		
		// Execute node with retry logic
		output, err := e.executeNodeWithRetry(ctx, node, t.input)
		if err != nil {
			// Check for specific error types
			if errors.IsGraphInterrupt(err) {
				return nil, true, nil
			}
			
			results = append(results, &taskResult{
				taskID: t.id,
				output: nil,
				err:    err,
			})
			continue
		}
		
		results = append(results, &taskResult{
			taskID: t.id,
			output: output,
			err:    nil,
		})
	}
	
	return results, false, nil
}

// executeNodeWithRetry executes a node with retry logic.
func (e *pregelEngine) executeNodeWithRetry(ctx context.Context, node *Node, input interface{}) (interface{}, error) {
	retryPolicy := node.RetryPolicy
	if retryPolicy == nil {
		defaultPolicy := types.DefaultRetryPolicy()
		retryPolicy = &defaultPolicy
	}
	
	attempts := 0
	var lastErr error
	
	for attempts < retryPolicy.MaxAttempts {
		output, err := e.executeNode(ctx, node, input)
		if err == nil {
			return output, nil
		}
		
		lastErr = err
		attempts++
		
		if attempts >= retryPolicy.MaxAttempts {
			break
		}
		
		// Check if we should retry this error
		if retryPolicy.RetryOn != nil && !retryPolicy.RetryOn(err) {
			return nil, err
		}
		
		// Calculate wait time
		waitTime := retryPolicy.InitialInterval
		for i := 0; i < attempts-1; i++ {
			waitTime = time.Duration(float64(waitTime) * retryPolicy.BackoffFactor)
			if waitTime > retryPolicy.MaxInterval {
				waitTime = retryPolicy.MaxInterval
				break
			}
		}
		
		// Add jitter
		if retryPolicy.Jitter {
			waitTime = time.Duration(float64(waitTime) * (0.5 + 0.5*float64(time.Now().UnixNano()%1000)/1000))
		}
		
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(waitTime):
			// Continue to next attempt
		}
	}
	
	return nil, fmt.Errorf("max retries exceeded (%d): %w", attempts, lastErr)
}

// executeNode executes a single node.
func (e *pregelEngine) executeNode(ctx context.Context, node *Node, input interface{}) (interface{}, error) {
	// Create a context with timeout if configured
	nodeCtx := ctx
	
	// Execute the node function
	output, err := node.Function(nodeCtx, input)
	if err != nil {
		return nil, fmt.Errorf("node %s execution failed: %w", node.Name, err)
	}
	
	return output, nil
}

// applyWrites applies task outputs to channels.
func (e *pregelEngine) applyWrites(registry *channels.Registry, results []*taskResult) error {
	writes := make(map[string][]interface{})
	
	for _, result := range results {
		if result.err != nil {
			continue
		}
		
		// Convert output to writes
		outputMap, err := toMap(result.output)
		if err != nil {
			return fmt.Errorf("failed to convert output to map: %w", err)
		}
		
		for key, value := range outputMap {
			// Only apply if channel exists
			if _, ok := registry.Get(key); ok {
				writes[key] = append(writes[key], value)
			}
		}
	}
	
	if len(writes) > 0 {
		return registry.UpdateChannels(writes)
	}
	return nil
}

// buildOutput builds the output from channel values or returns last known state.
func (e *pregelEngine) buildOutput(registry *channels.Registry, lastState interface{}) (interface{}, error) {
	values, err := registry.GetValues()
	if err != nil {
		return lastState, nil
	}
	
	// If we have channel values, use them
	if len(values) > 0 {
		return values, nil
	}
	
	// Otherwise return the last known state
	return lastState, nil
}

// mergeStates merges new state into existing state.
func mergeStates(existing, new interface{}) interface{} {
	// If existing is nil, return new
	if existing == nil {
		return new
	}
	
	// If new is nil, return existing
	if new == nil {
		return existing
	}
	
	// Try to merge maps
	existingMap, ok1 := existing.(map[string]interface{})
	newMap, ok2 := new.(map[string]interface{})
	
	if ok1 && ok2 {
		result := make(map[string]interface{})
		for k, v := range existingMap {
			result[k] = v
		}
		for k, v := range newMap {
			result[k] = v
		}
		return result
	}
	
	// If types don't match for merging, return new
	return new
}
