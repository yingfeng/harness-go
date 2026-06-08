package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/infiniflow/ragflow/harness/graphengine/constants"
	"github.com/infiniflow/ragflow/harness/graphengine/graph"
	"github.com/infiniflow/ragflow/harness/graphengine/types"
)

// Message represents a single message in the conversation history.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// RunState is the shared state passed through the StateGraph.
type RunState struct {
	Messages    []Message             `json:"messages"`
	Variables   map[string]interface{} `json:"variables"`
	CurrentNode string                 `json:"currentNode"`
	NodeOutputs map[string]string       `json:"nodeOutputs"`
	Route       string                 `json:"route"`
	LoopCounts  map[string]int         `json:"loopCounts"`
	Error       string                 `json:"error,omitempty"`
}

// WorkflowEngine builds and executes WorkflowDef via harness-go StateGraph.
type WorkflowEngine struct {
	registry *ModelRegistry
}

// New creates a WorkflowEngine with the given model registry.
func New(registry *ModelRegistry) *WorkflowEngine {
	return &WorkflowEngine{registry: registry}
}

// Execute runs a workflow and streams SSE events.
func (e *WorkflowEngine) Execute(ctx context.Context, wf *WorkflowDef, eventsCh chan<- SSEEvent) {
	defer close(eventsCh)

	execID := fmt.Sprintf("exec-%s", wf.ID)
	if wf.ID == "" {
		execID = "exec-default"
	}

	emit := func(event string, data interface{}) {
		select {
		case eventsCh <- SSEEvent{Event: event, Data: data}:
		case <-ctx.Done():
		}
	}

	emit("workflow_started", map[string]string{"executionId": execID})

	if err := validateGraph(wf); err != nil {
		emit("error", map[string]string{"message": err.Error()})
		emit("workflow_completed", map[string]interface{}{"executionId": execID, "status": "failed", "error": err.Error()})
		return
	}

	compiled, err := e.BuildGraph(wf, eventsCh)
	if err != nil {
		emit("error", map[string]string{"message": err.Error()})
		emit("workflow_completed", map[string]interface{}{"executionId": execID, "status": "failed", "error": err.Error()})
		return
	}

	initialState := &RunState{
		Messages:    make([]Message, 0),
		Variables:   make(map[string]interface{}),
		NodeOutputs: make(map[string]string),
		LoopCounts:  make(map[string]int),
	}

	result, execErr := compiled.Invoke(ctx, initialState)
	if execErr != nil {
		emit("error", map[string]string{"message": execErr.Error()})
		emit("workflow_completed", map[string]interface{}{"executionId": execID, "status": "failed", "error": execErr.Error()})
		return
	}

	finalState, convErr := toRunState(result)
	if convErr != nil {
		emit("error", map[string]string{"message": convErr.Error()})
		emit("workflow_completed", map[string]interface{}{"executionId": execID, "status": "failed", "error": convErr.Error()})
		return
	}
	if finalState.Error != "" {
		emit("workflow_completed", map[string]interface{}{"executionId": execID, "status": "failed", "error": finalState.Error})
		return
	}

	emit("state_update", map[string]interface{}{
		"executionId": execID, "status": "completed", "nodeOutputs": finalState.NodeOutputs,
	})
	emit("workflow_completed", map[string]interface{}{"executionId": execID, "status": "completed"})
}

// BuildGraph constructs a harness-go StateGraph from a WorkflowDef.
func (e *WorkflowEngine) BuildGraph(wf *WorkflowDef, eventsCh chan<- SSEEvent) (*graph.CompiledGraph, error) {
	sg := graph.NewStateGraph(&RunState{})
	sg.NodeTriggerMode = types.NodeTriggerAnyPredecessor

	startID := ""
	endID := ""
	for _, n := range wf.Nodes {
		switch n.Type {
		case "start":
			startID = n.ID
		case "end":
			endID = n.ID
		}
	}
	if startID == "" {
		return nil, fmt.Errorf("no start node found")
	}

	for i := range wf.Nodes {
		n := &wf.Nodes[i]
		if n.Type == "note" {
			continue
		}
		sg.AddNode(n.ID, e.createNodeFunc(n, eventsCh))
	}

	// Build outgoing edge groups. For conditional nodes, collect by sourceHandle.
	type edgeGroup struct {
		target string
		handle string
	}
	bySource := make(map[string][]edgeGroup)
	for _, edge := range wf.Edges {
		h := edge.SourceHandle
		if h == "" {
			h = "default"
		}
		bySource[edge.Source] = append(bySource[edge.Source], edgeGroup{target: edge.Target, handle: h})
	}

	// Add edges.
	for id, groups := range bySource {
		node := findNode(wf.Nodes, id)
		if node == nil {
			continue
		}

		if node.Type == "if-else" || node.Type == "while" {
			// Conditional edges.
			mapping := make(map[string]string)
			for _, g := range groups {
				if g.handle == "default" {
					mapping["true"] = g.target
				} else {
					mapping[g.handle] = g.target
				}
			}
			condFunc := e.createConditionFunc(node)
			sg.AddConditionalEdges(node.ID, condFunc, mapping)
		} else {
			// Regular edges.
			for _, g := range groups {
				sg.AddEdge(id, g.target)
			}
		}
	}

	// Connect constants.Start → start node.
	sg.AddEdge(constants.Start, startID)

	// Connect end node → constants.End.
	if endID != "" {
		sg.AddEdge(endID, constants.End)
	}

	return sg.Compile(graph.WithRecursionLimit(200))
}

// createNodeFunc returns a StateGraph NodeFunc with event emission captured via closure.
func (e *WorkflowEngine) createNodeFunc(node *WorkflowNode, eventsCh chan<- SSEEvent) types.NodeFunc {
	nodeCopy := *node
	return func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(*RunState)
		s.CurrentNode = nodeCopy.ID

		emitSSE := func(event string, data interface{}) {
			select {
			case eventsCh <- SSEEvent{Event: event, Data: data}:
			default:
			}
		}

		emitSSE("node_started", map[string]string{"nodeId": nodeCopy.ID, "nodeName": nodeCopy.Data.NodeName})

		var output string
		var execErr error

		switch nodeCopy.Type {
		case "start":
			output = "workflow started"
		case "end":
			output = "workflow ended"
		case "agent":
			output, execErr = e.executeAgentNode(ctx, &nodeCopy, s)
		case "if-else":
			output = e.executeIfElseNode(&nodeCopy, s)
		case "while":
			output = e.executeWhileNode(&nodeCopy, s)
		case "transform":
			output = e.executeTransformNode(&nodeCopy, s)
		case "user-approval":
			output = "awaiting approval (placeholder)"
		default:
			output = "unknown node type, skipped"
		}

		if execErr != nil {
			s.Error = execErr.Error()
			emitSSE("node_failed", map[string]string{"nodeId": nodeCopy.ID, "nodeName": nodeCopy.Data.NodeName, "error": execErr.Error()})
			return s, execErr
		}

		s.NodeOutputs[nodeCopy.ID] = output
		emitSSE("node_completed", map[string]string{"nodeId": nodeCopy.ID, "nodeName": nodeCopy.Data.NodeName, "output": output})
		return s, nil
	}
}

// createConditionFunc returns a StateGraph EdgeFunc for conditional routing.
func (e *WorkflowEngine) createConditionFunc(node *WorkflowNode) types.EdgeFunc {
	nodeCopy := *node
	return func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(*RunState)
		if s.Route != "" {
			return s.Route, nil
		}
		if nodeCopy.Type == "if-else" {
			return evaluateCondition(nodeCopy.Data.Condition, s), nil
		}
		if nodeCopy.Type == "while" {
			s.LoopCounts[nodeCopy.ID]++
			maxIters := 10
			if s.LoopCounts[nodeCopy.ID] > maxIters {
				return "break", nil
			}
			return "continue", nil
		}
		return "true", nil
	}
}

func (e *WorkflowEngine) executeAgentNode(ctx context.Context, node *WorkflowNode, s *RunState) (string, error) {
	systemPrompt := node.Data.SystemPrompt
	if systemPrompt == "" {
		systemPrompt = node.Data.Instructions
	}
	if systemPrompt == "" {
		systemPrompt = "You are a helpful assistant."
	}

	mcfg := e.registry.Default()
	if node.Data.ModelName != "" {
		if m := e.registry.Get(node.Data.ModelName); m != nil {
			mcfg = m
		}
	}
	if mcfg == nil {
		return "", fmt.Errorf("no model configured")
	}

	systemPrompt = applyVariables(systemPrompt, s)

	msgs := []ChatMessage{
		{Role: "system", Content: systemPrompt},
	}
	for _, m := range s.Messages {
		msgs = append(msgs, ChatMessage{Role: m.Role, Content: m.Content})
	}
	if len(msgs) == 1 {
		msgs = append(msgs, ChatMessage{Role: "user", Content: "Please proceed with your task."})
	}

	var resp ChatCompletionResponse
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			log.Printf("Retry %d for node %s", attempt, node.ID)
		}
		client := NewLLMClient(*mcfg)
		resp, lastErr = client.CreateChatCompletion(ctx, msgs, mcfg.Model)
		if lastErr == nil {
			break
		}
	}
	if lastErr != nil {
		return "", fmt.Errorf("LLM call failed after 3 retries: %w", lastErr)
	}

	content := resp.Choices[0].Message.Content
	s.Messages = append(s.Messages, Message{Role: "assistant", Content: content})
	s.NodeOutputs[node.ID] = content
	return content, nil
}

func (e *WorkflowEngine) executeIfElseNode(node *WorkflowNode, s *RunState) string {
	s.Route = evaluateCondition(node.Data.Condition, s)
	return s.Route
}

func (e *WorkflowEngine) executeWhileNode(node *WorkflowNode, s *RunState) string {
	s.LoopCounts[node.ID]++
	maxIters := 10
	if s.LoopCounts[node.ID] > maxIters {
		s.Route = "break"
	} else {
		s.Route = "continue"
	}
	return s.Route
}

func (e *WorkflowEngine) executeTransformNode(node *WorkflowNode, s *RunState) string {
	code := node.Data.TransformCode
	if code == "" {
		return ""
	}
	result := applyVariables(code, s)
	s.NodeOutputs[node.ID] = result
	return result
}

func evaluateCondition(cond string, s *RunState) string {
	cond = strings.TrimSpace(cond)
	if cond == "" {
		return "true"
	}
	if v, ok := s.Variables[cond]; ok {
		switch val := v.(type) {
		case bool:
			if val {
				return "true"
			}
			return "false"
		case string:
			if val != "" && val != "false" && val != "0" {
				return "true"
			}
			return "false"
		case float64:
			if val != 0 {
				return "true"
			}
			return "false"
		default:
			return "true"
		}
	}
	for _, v := range s.Variables {
		if str, ok := v.(string); ok && strings.Contains(str, cond) {
			return "true"
		}
	}
	for _, v := range s.NodeOutputs {
		if strings.Contains(v, cond) {
			return "true"
		}
	}
	return "false"
}

// applyVariables substitutes {{variable}} and {{outputs.nodeId}} placeholders.
func applyVariables(template string, s *RunState) string {
	result := template
	for k, v := range s.Variables {
		old := fmt.Sprintf("{{%s}}", k)
		result = strings.ReplaceAll(result, old, fmt.Sprintf("%v", v))
	}
	for k, v := range s.NodeOutputs {
		old := fmt.Sprintf("{{outputs.%s}}", k)
		result = strings.ReplaceAll(result, old, v)
	}
	return result
}

// validateGraph checks for reachability from start node.
func validateGraph(wf *WorkflowDef) error {
	startID := ""
	for _, n := range wf.Nodes {
		if n.Type == "start" {
			startID = n.ID
			break
		}
	}
	if startID == "" {
		return fmt.Errorf("no start node found")
	}

	reachable := make(map[string]bool)
	adj := make(map[string][]string)
	for _, e := range wf.Edges {
		adj[e.Source] = append(adj[e.Source], e.Target)
	}

	queue := []string{startID}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if reachable[id] {
			continue
		}
		reachable[id] = true
		for _, next := range adj[id] {
			queue = append(queue, next)
		}
	}
	for _, n := range wf.Nodes {
		if n.Type == "note" || n.Type == "end" {
			continue
		}
		if !reachable[n.ID] {
			log.Printf("Warning: node %s (%s) is unreachable", n.ID, n.Data.NodeName)
		}
	}
	return nil
}

// findNode returns a pointer to a node by ID.
func findNode(nodes []WorkflowNode, id string) *WorkflowNode {
	for i := range nodes {
		if nodes[i].ID == id {
			return &nodes[i]
		}
	}
	return nil
}

// toRunState converts the Invoke result to *RunState.
func toRunState(v interface{}) (*RunState, error) {
	switch s := v.(type) {
	case *RunState:
		return s, nil
	case map[string]interface{}:
		return mapToRunState(s)
	default:
		return nil, fmt.Errorf("unexpected result type: %T", v)
	}
}

func mapToRunState(m map[string]interface{}) (*RunState, error) {
	rs := &RunState{
		Messages:    make([]Message, 0),
		Variables:   make(map[string]interface{}),
		NodeOutputs: make(map[string]string),
		LoopCounts:  make(map[string]int),
	}

	// Messages: []interface{} → []Message
	if msgs, ok := m["Messages"]; ok {
		if arr, ok := msgs.([]interface{}); ok {
			for _, item := range arr {
				if msgMap, ok := item.(map[string]interface{}); ok {
					m := Message{}
					if r, ok := msgMap["Role"].(string); ok {
						m.Role = r
					}
					if c, ok := msgMap["Content"].(string); ok {
						m.Content = c
					}
					rs.Messages = append(rs.Messages, m)
				}
			}
		}
	}

	// Variables
	if vars, ok := m["Variables"]; ok {
		if vm, ok := vars.(map[string]interface{}); ok {
			rs.Variables = vm
		}
	}

	// NodeOutputs: map[string]interface{} → map[string]string
	if outputs, ok := m["NodeOutputs"]; ok {
		if om, ok := outputs.(map[string]interface{}); ok {
			for k, v := range om {
				if s, ok := v.(string); ok {
					rs.NodeOutputs[k] = s
				}
			}
		}
	}

	// LoopCounts: map[string]interface{} → map[string]int
	if counts, ok := m["LoopCounts"]; ok {
		if cm, ok := counts.(map[string]interface{}); ok {
			for k, v := range cm {
				if f, ok := v.(float64); ok {
					rs.LoopCounts[k] = int(f)
				}
			}
		}
	}

	// Error
	if errStr, ok := m["Error"].(string); ok {
		rs.Error = errStr
	}

	// CurrentNode (optional, for SSE tracking)
	if cn, ok := m["CurrentNode"].(string); ok {
		rs.CurrentNode = cn
	}

	return rs, nil
}

// UnmarshalWorkflowDef unmarshals a workflow definition from JSON bytes.
func UnmarshalWorkflowDef(data json.RawMessage) (*WorkflowDef, error) {
	var wf WorkflowDef
	if err := json.Unmarshal(data, &wf); err != nil {
		return nil, fmt.Errorf("invalid workflow definition: %w", err)
	}
	return &wf, nil
}
