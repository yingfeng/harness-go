// Package graph provides graph building capabilities for LangGraph Go.
package graph

import (
	"context"
	"fmt"

	"github.com/infiniflow/ragflow/harness/channels"
	"github.com/infiniflow/ragflow/harness/constants"
	"github.com/infiniflow/ragflow/harness/errors"
	"github.com/infiniflow/ragflow/harness/types"
)

// Node represents a node in the graph.
type Node struct {
	Name     string
	Function types.NodeFunc
	// Channels this node reads from
	Triggers []string
	// Channels this node writes to
	Writes   []string
	// Retry policy for this node
	RetryPolicy *types.RetryPolicy
	// Tags for the node
	Tags []string
	// Metadata for the node
	Metadata map[string]interface{}
}

// Edge represents an edge in the graph.
type Edge struct {
	From string
	To   string
}

// ConditionalEdge represents a conditional edge with a condition function.
type ConditionalEdge struct {
	From      string
	Condition types.EdgeFunc
	// Mapping from condition result to target node
	Mapping map[string]string
}

// Branch represents a branch in the graph.
type Branch struct {
	From      string
	Condition types.EdgeFunc
	// Then is called with the condition result to determine next nodes
	Then func(interface{}) []string
}

// Send represents a dynamic node invocation.
type Send struct {
	Node string
	Arg  interface{}
}

// StateGraph is a graph whose nodes communicate by reading and writing to a shared state.
type StateGraph struct {
	// Nodes in the graph
	nodes map[string]*Node
	// Edges between nodes
	edges []*Edge
	// Conditional edges
	conditionalEdges []*ConditionalEdge
	// Branches
	branches []*Branch
	// Entry point of the graph
	entryPoint string
	// Finish points of the graph
	finishPoints []string
	// Channel definitions for the state schema
	channels map[string]channels.Channel
	// Reducer functions for channels
	reducers map[string]types.ReducerFunc
	// State schema type
	stateSchema interface{}
	// Input schema type
	inputSchema interface{}
	// Output schema type
	outputSchema interface{}
}

// NewStateGraph creates a new StateGraph with the given state schema.
// The stateSchema defines the structure of the shared state.
func NewStateGraph(stateSchema interface{}) *StateGraph {
	return &StateGraph{
		nodes:            make(map[string]*Node),
		edges:            make([]*Edge, 0),
		conditionalEdges: make([]*ConditionalEdge, 0),
		branches:         make([]*Branch, 0),
		finishPoints:     make([]string, 0),
		channels:         make(map[string]channels.Channel),
		reducers:         make(map[string]types.ReducerFunc),
		stateSchema:      stateSchema,
		inputSchema:      stateSchema,
		outputSchema:     stateSchema,
	}
}

// WithInputSchema sets the input schema for the graph.
func (g *StateGraph) WithInputSchema(schema interface{}) *StateGraph {
	g.inputSchema = schema
	return g
}

// WithOutputSchema sets the output schema for the graph.
func (g *StateGraph) WithOutputSchema(schema interface{}) *StateGraph {
	g.outputSchema = schema
	return g
}

// AddNode adds a node to the graph.
func (g *StateGraph) AddNode(name string, fn types.NodeFunc) *Node {
	node := &Node{
		Name:     name,
		Function: fn,
		Triggers: make([]string, 0),
		Writes:   make([]string, 0),
		Tags:     make([]string, 0),
		Metadata: make(map[string]interface{}),
	}
	g.nodes[name] = node
	return node
}

// AddNodeWithOptions adds a node with options.
func (g *StateGraph) AddNodeWithOptions(name string, fn types.NodeFunc, opts NodeOptions) *Node {
	node := g.AddNode(name, fn)
	if opts.RetryPolicy != nil {
		node.RetryPolicy = opts.RetryPolicy
	}
	if len(opts.Tags) > 0 {
		node.Tags = append(node.Tags, opts.Tags...)
	}
	if len(opts.Metadata) > 0 {
		for k, v := range opts.Metadata {
			node.Metadata[k] = v
		}
	}
	if len(opts.Triggers) > 0 {
		node.Triggers = append(node.Triggers, opts.Triggers...)
	}
	if len(opts.Writes) > 0 {
		node.Writes = append(node.Writes, opts.Writes...)
	}
	return node
}

// NodeOptions contains options for adding a node.
type NodeOptions struct {
	RetryPolicy *types.RetryPolicy
	Tags        []string
	Metadata    map[string]interface{}
	Triggers    []string
	Writes      []string
}

// AddEdge adds an edge between two nodes.
func (g *StateGraph) AddEdge(from, to string) error {
	if _, ok := g.nodes[from]; !ok && from != constants.Start {
		return &errors.NodeNotFoundError{NodeName: from}
	}
	if _, ok := g.nodes[to]; !ok && to != constants.End {
		return &errors.NodeNotFoundError{NodeName: to}
	}
	
	g.edges = append(g.edges, &Edge{From: from, To: to})
	
	// If this is an edge from Start, set entry point to the target node
	if from == constants.Start {
		g.entryPoint = to
	}
	
	// If this is an edge to End, set the source as a finish point
	if to == constants.End {
		found := false
		for _, fp := range g.finishPoints {
			if fp == from {
				found = true
				break
			}
		}
		if !found {
			g.finishPoints = append(g.finishPoints, from)
		}
	}
	
	return nil
}

// AddConditionalEdges adds conditional edges from a node.
func (g *StateGraph) AddConditionalEdges(from string, condition types.EdgeFunc, mapping map[string]string) error {
	if _, ok := g.nodes[from]; !ok {
		return &errors.NodeNotFoundError{NodeName: from}
	}
	
	// Validate all targets exist
	for _, target := range mapping {
		if _, ok := g.nodes[target]; !ok && target != constants.End {
			return &errors.NodeNotFoundError{NodeName: target}
		}
	}
	
	g.conditionalEdges = append(g.conditionalEdges, &ConditionalEdge{
		From:      from,
		Condition: condition,
		Mapping:   mapping,
	})
	return nil
}

// AddBranch adds a branch from a node.
func (g *StateGraph) AddBranch(from string, condition types.EdgeFunc, then func(interface{}) []string) error {
	if _, ok := g.nodes[from]; !ok {
		return &errors.NodeNotFoundError{NodeName: from}
	}
	
	g.branches = append(g.branches, &Branch{
		From:      from,
		Condition: condition,
		Then:      then,
	})
	return nil
}

// SetEntryPoint sets the entry point of the graph.
func (g *StateGraph) SetEntryPoint(node string) error {
	if _, ok := g.nodes[node]; !ok {
		return &errors.NodeNotFoundError{NodeName: node}
	}
	g.entryPoint = node
	return nil
}

// SetFinishPoint sets a finish point of the graph.
func (g *StateGraph) SetFinishPoint(node string) error {
	if _, ok := g.nodes[node]; !ok {
		return &errors.NodeNotFoundError{NodeName: node}
	}
	g.finishPoints = append(g.finishPoints, node)
	return nil
}

// AddChannel adds a channel definition to the state schema.
func (g *StateGraph) AddChannel(name string, channel channels.Channel) {
	channel.SetKey(name)
	g.channels[name] = channel
}

// SetReducer sets a reducer function for a channel.
// If the channel exists, it wraps it with a ReducerChannel.
func (g *StateGraph) SetReducer(channelName string, reducer types.ReducerFunc) {
	if channel, ok := g.channels[channelName]; ok {
		// Wrap existing channel with reducer
		g.channels[channelName] = channels.NewReducerChannel(channel, reducer)
	}
	g.reducers[channelName] = reducer
}

// AddChannelWithReducer adds a channel definition with a reducer function.
func (g *StateGraph) AddChannelWithReducer(name string, channel channels.Channel, reducer types.ReducerFunc) {
	channel.SetKey(name)
	if reducer != nil {
		// Wrap channel with reducer
		g.channels[name] = channels.NewReducerChannel(channel, reducer)
		g.reducers[name] = reducer
	} else {
		g.channels[name] = channel
	}
}

// GetNode returns a node by name.
func (g *StateGraph) GetNode(name string) (*Node, bool) {
	node, ok := g.nodes[name]
	return node, ok
}

// GetNodes returns all nodes.
func (g *StateGraph) GetNodes() map[string]*Node {
	return g.nodes
}

// GetEdges returns all edges.
func (g *StateGraph) GetEdges() []*Edge {
	return g.edges
}

// GetChannels returns all channels.
func (g *StateGraph) GetChannels() map[string]channels.Channel {
	return g.channels
}

// Validate validates the graph structure.
func (g *StateGraph) Validate() error {
	if g.entryPoint == "" {
		return fmt.Errorf("no entry point set")
	}
	
	if len(g.finishPoints) == 0 {
		return fmt.Errorf("no finish points set")
	}
	
	// Check that all nodes are reachable
	reachable := g.computeReachable()
	for name := range g.nodes {
		if !reachable[name] {
			return fmt.Errorf("node %s is not reachable from entry point", name)
		}
	}
	
	// Validate state schema
	if err := g.ValidateStateSchema(); err != nil {
		return fmt.Errorf("state schema validation failed: %w", err)
	}
	
	return nil
}

// computeReachable computes all reachable nodes from the entry point.
func (g *StateGraph) computeReachable() map[string]bool {
	reachable := make(map[string]bool)
	if g.entryPoint == "" {
		return reachable
	}
	
	queue := []string{g.entryPoint}
	reachable[g.entryPoint] = true
	
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		
		// Follow regular edges
		for _, edge := range g.edges {
			if edge.From == current && !reachable[edge.To] && edge.To != constants.End {
				reachable[edge.To] = true
				queue = append(queue, edge.To)
			}
		}
		
		// Follow conditional edges - all targets are potentially reachable
		for _, condEdge := range g.conditionalEdges {
			if condEdge.From == current {
				for _, target := range condEdge.Mapping {
					if _, ok := g.nodes[target]; ok && !reachable[target] && target != constants.End {
						reachable[target] = true
						queue = append(queue, target)
					}
				}
			}
		}
		
		// Note: branches are truly dynamic and can't be statically verified
	}
	
	return reachable
}

// configureChannelsFromSchema configures channels and reducers based on state schema annotations.
func (g *StateGraph) configureChannelsFromSchema() error {
	// Get field information from schema
	fieldInfos, err := g.GetStateSchemaInfo()
	if err != nil {
		return err
	}

	// Configure channels and reducers for each field
	for fieldName, info := range fieldInfos {
		// Check if channel already exists
		if _, exists := g.channels[fieldName]; !exists {
			// Add channel
			g.channels[fieldName] = info.Channel
		}

		// Set reducer if specified in annotation
		if info.Annotation != nil && info.Annotation.Reducer != nil {
			g.reducers[fieldName] = info.Annotation.Reducer
		}
	}

	return nil
}

// Compile compiles the graph into an executable CompiledGraph.
func (g *StateGraph) Compile(opts ...CompileOption) (*CompiledGraph, error) {
	if err := g.Validate(); err != nil {
		return nil, fmt.Errorf("graph validation failed: %w", err)
	}
	
	// Configure channels and reducers from schema annotations
	if err := g.configureChannelsFromSchema(); err != nil {
		return nil, fmt.Errorf("failed to configure channels from schema: %w", err)
	}
	
	cg := &CompiledGraph{
		graph:         g,
		checkpointer:  nil,
		interrupts:    make(map[string]bool),
		recursionLimit: 25,
		debug:          false,
	}
	
	for _, opt := range opts {
		opt(cg)
	}
	
	return cg, nil
}

// CompileOption is an option for compiling a graph.
type CompileOption func(*CompiledGraph)

// WithCheckpointer sets the checkpointer for the compiled graph.
func WithCheckpointer(checkpointer Checkpointer) CompileOption {
	return func(cg *CompiledGraph) {
		cg.checkpointer = checkpointer
	}
}

// WithInterrupts sets the nodes that should trigger interrupts.
func WithInterrupts(nodes ...string) CompileOption {
	return func(cg *CompiledGraph) {
		for _, node := range nodes {
			cg.interrupts[node] = true
		}
	}
}

// WithRecursionLimit sets the recursion limit.
func WithRecursionLimit(limit int) CompileOption {
	return func(cg *CompiledGraph) {
		cg.recursionLimit = limit
	}
}

// WithDebug enables debug mode.
func WithDebug(debug bool) CompileOption {
	return func(cg *CompiledGraph) {
		cg.debug = debug
	}
}

// Checkpointer is the interface for checkpoint savers.
type Checkpointer interface {
	Get(ctx context.Context, config map[string]interface{}) (map[string]interface{}, error)
	Put(ctx context.Context, config map[string]interface{}, checkpoint map[string]interface{}) error
	List(ctx context.Context, config map[string]interface{}, limit int) ([]map[string]interface{}, error)
}

// CompiledGraph is a compiled, executable graph.
type CompiledGraph struct {
	graph          *StateGraph
	checkpointer   Checkpointer
	interrupts     map[string]bool
	recursionLimit int
	debug          bool
}

// Invoke executes the graph with the given input and returns the final state.
func (cg *CompiledGraph) Invoke(ctx context.Context, input interface{}, config ...*types.RunnableConfig) (interface{}, error) {
	rc := &types.RunnableConfig{}
	if len(config) > 0 && config[0] != nil {
		rc = config[0]
	}
	
	result, err := cg.run(ctx, input, rc, types.StreamModeValues)
	if err != nil {
		return nil, err
	}
	
	return result, nil
}

// Stream executes the graph and returns a stream of updates.
func (cg *CompiledGraph) Stream(ctx context.Context, input interface{}, mode types.StreamMode, config ...*types.RunnableConfig) (<-chan interface{}, <-chan error) {
	outputCh := make(chan interface{})
	errCh := make(chan error, 1)
	
	rc := &types.RunnableConfig{}
	if len(config) > 0 && config[0] != nil {
		rc = config[0]
	}
	
	go func() {
		defer close(outputCh)
		defer close(errCh)
		
		// Implementation would stream results
		// For now, just run and emit final result
		result, err := cg.run(ctx, input, rc, mode)
		if err != nil {
			errCh <- err
			return
		}
		
		outputCh <- result
	}()
	
	return outputCh, errCh
}

// run executes the graph.
func (cg *CompiledGraph) run(ctx context.Context, input interface{}, config *types.RunnableConfig, streamMode types.StreamMode) (interface{}, error) {
	// This is a placeholder implementation
	// The full implementation would use the pregel execution engine
	
	engine := &pregelEngine{
		graph:          cg.graph,
		checkpointer:   cg.checkpointer,
		interrupts:     cg.interrupts,
		recursionLimit: cg.recursionLimit,
		debug:          cg.debug,
		config:         config,
	}
	
	return engine.run(ctx, input)
}

// GetGraph returns the underlying StateGraph.
func (cg *CompiledGraph) GetGraph() *StateGraph {
	return cg.graph
}
