package main

import (
	"context"
	"fmt"

	"github.com/infiniflow/ragflow/harness/graphengine/constants"
	"github.com/infiniflow/ragflow/harness/graphengine/graph"
)

// ---- Tool definitions ----

type MockTool struct {
	Name        string
	Description string
	Execute     func(ctx context.Context, input string) (string, error)
}

func searchTool() MockTool {
	return MockTool{
		Name:        "web_search",
		Description: "Search the web for information on a topic. Input: search query.",
		Execute: func(ctx context.Context, input string) (string, error) {
			return fmt.Sprintf("[Search results for: %s]\n- Found relevant information about %s\n- Multiple sources available\n- Further investigation recommended", input, input), nil
		},
	}
}

func pythonREPLTool() MockTool {
	return MockTool{
		Name:        "execute_python",
		Description: "Execute Python code for data processing. Input: Python code.",
		Execute: func(ctx context.Context, input string) (string, error) {
			return fmt.Sprintf("[Python execution result]\nCode executed successfully.\nOutput: Processed data for research analysis."), nil
		},
	}
}

func handToPlannerTool() MockTool {
	return MockTool{
		Name:        "hand_to_planner",
		Description: "Hand off the research request to the planning agent. Call this when the user asks a research question.",
		Execute: func(ctx context.Context, input string) (string, error) {
			return "handed to planner", nil
		},
	}
}

func planTool() MockTool {
	return MockTool{
		Name:        "create_plan",
		Description: "Create a structured research plan with steps.",
		Execute: func(ctx context.Context, input string) (string, error) {
			return fmt.Sprintf("Plan created: %s", input), nil
		},
	}
}

// ---- Prompts ----

const (
	CoordinatorPrompt = `You are a research coordinator. Classify the user's request:

- If the user is just chatting or asking a simple question, respond directly.
- If the user asks for research, investigation, or analysis on a topic, use the 'hand_to_planner' tool to route to the planning phase.

Be concise. Your response will be shown to the user.`

	PlannerPrompt = `You are a research planner. Analyze the user's request and create a detailed research plan.

Your plan should include:
1. A clear title for the research
2. A thought explaining your approach
3. A list of steps, each being either "research" (information gathering) or "processing" (data analysis)

Use the 'create_plan' tool to output your plan with the format:
Title: <title>
Thought: <your thought>
Steps:
1. [research] <step description>
2. [processing] <step description>
...`

	ResearcherPrompt = `You are a research analyst. Your job is to gather information on specific topics.

For each research step:
1. Use the 'web_search' tool to search for information
2. Review the results and summarize key findings
3. Present your findings clearly

Focus on accuracy and relevance. Cite your sources when possible.`

	CoderPrompt = `You are a data processing specialist. Your job is to analyze and process research data.

For each processing step:
1. Use the 'execute_python' tool to run analysis code
2. Review the output
3. Present your findings clearly

Focus on extracting meaningful insights from the data.`

	ReporterPrompt = `You are a research report writer. Synthesize all research findings into a comprehensive final report.

Your report should:
1. Start with an executive summary
2. Cover each research step's findings
3. Include data analysis results
4. End with conclusions and recommendations

Format the report in Markdown for readability. Use the research results provided in the context.`
)

// DeerState is the shared state for the multi-agent research graph.
type DeerState struct {
	UserInput string
	Messages  []string
	// Goto is set by each node to indicate the next routing target.
	// NOTE(Pregel bug): In the current Pregel engine (BSP + channel accumulation),
	// mutations to this field inside a node function may NOT be visible to
	// conditional edge routers or subsequent nodes in the same/following superstep.
	// This is a known limitation being tested.
	Goto string

	// Plan fields
	PlanTitle        string
	PlanThought      string
	PlanSteps        []Step
	PlanAutoApproved bool

	// Execution state
	CurrentStepIdx int
	IterationCount int
	MaxIterations  int

	// Research results keyed by step description
	ResearchResults map[string]string

	// Final report
	Report string
}

// Step represents a single research step in the plan.
type Step struct {
	Description  string
	Type         string // "research" or "processing"
	ExecutionRes string
}

// Agent name constants for routing.
const (
	NodeCoordinator  = "coordinator"
	NodePlanner      = "planner"
	NodeHuman        = "human_feedback"
	NodeResearchTeam = "research_team"
	NodeResearcher   = "researcher"
	NodeCoder        = "coder"
	NodeReporter     = "reporter"
)

// ResearchWorkflow wraps a CompiledGraph with a typed Invoke method.
type ResearchWorkflow struct {
	compiled *graph.CompiledGraph
}

func (w *ResearchWorkflow) Invoke(ctx context.Context, state *DeerState) (*DeerState, error) {
	result, err := w.compiled.Invoke(ctx, state)
	if err != nil {
		return nil, err
	}
	return stateFromMap(result), nil
}

// ---- Router functions (conditional edges) ----
//
// NOTE(Pregel bug): These routers read s.Goto, which is set by the node function.
// Due to channel-based state accumulation in the Pregel engine, the value read
// here may be the INITIAL value rather than the value set by the node function.
// This is the bug being tested.

func coordinatorRouter(ctx context.Context, state interface{}) (interface{}, error) {
	s := state.(*DeerState)
	return s.Goto, nil
}

func plannerRouter(ctx context.Context, state interface{}) (interface{}, error) {
	s := state.(*DeerState)
	return s.Goto, nil
}

func researchTeamRouter(ctx context.Context, state interface{}) (interface{}, error) {
	s := state.(*DeerState)
	return s.Goto, nil
}

// ---- Node functions ----

func coordinatorNode(llm ChatModel) func(ctx context.Context, state interface{}) (interface{}, error) {
	return func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(*DeerState)
		msgs := buildLLMMessages(s, CoordinatorPrompt)
		output, _, err := llm.GenerateWithTool(ctx, msgs, []MockTool{handToPlannerTool()})
		if err != nil {
			return nil, fmt.Errorf("coordinator: %w", err)
		}
		if toolName, toolArgs, isTool := ParseToolResult(output); isTool && toolName == "hand_to_planner" {
			s.Goto = NodePlanner
			s.Messages = append(s.Messages, fmt.Sprintf("[Coordinator] Routing to planner. Input: %s", toolArgs))
		} else {
			s.Goto = "__end__"
			s.Messages = append(s.Messages, fmt.Sprintf("[Coordinator] %s", output))
		}
		return s, nil
	}
}

func plannerNode(llm ChatModel) func(ctx context.Context, state interface{}) (interface{}, error) {
	return func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(*DeerState)
		if len(s.PlanSteps) > 0 && s.CurrentStepIdx >= len(s.PlanSteps) {
			s.IterationCount++
			if s.IterationCount >= 3 {
				s.Goto = NodeReporter
				s.Messages = append(s.Messages, "[Planner] Sufficient research collected, generating report")
				return s, nil
			}
			s.CurrentStepIdx = 0
			s.PlanSteps = generateDefaultPlan(s.UserInput)
			s.Messages = append(s.Messages, "[Planner] Additional research needed, continuing")
			s.Goto = NodeResearchTeam
			return s, nil
		}

		msgs := buildLLMMessages(s, PlannerPrompt)
		planStr, err := llm.Generate(ctx, msgs, []MockTool{planTool()})
		if err != nil {
			return nil, fmt.Errorf("planner: %w", err)
		}
		s.PlanTitle = fmt.Sprintf("Research Plan: %s", truncate(s.UserInput, 40))
		s.PlanThought = "Systematic research approach"
		s.PlanSteps = generateDefaultPlan(s.UserInput)
		s.MaxIterations = len(s.PlanSteps) * 2
		s.CurrentStepIdx = 0
		s.IterationCount = 0
		s.Messages = append(s.Messages, fmt.Sprintf("[Planner] Research Plan:\n%s", planStr))
		if s.PlanAutoApproved {
			s.Goto = NodeResearchTeam
		} else {
			s.Goto = NodeHuman
		}
		return s, nil
	}
}

func humanFeedbackNode() func(ctx context.Context, state interface{}) (interface{}, error) {
	return func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(*DeerState)
		s.Goto = NodeResearchTeam
		s.Messages = append(s.Messages, "[Human] Plan auto-approved")
		return s, nil
	}
}

func researchTeamNode() func(ctx context.Context, state interface{}) (interface{}, error) {
	return func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(*DeerState)
		s.IterationCount++

		if s.CurrentStepIdx >= len(s.PlanSteps) {
			s.Goto = NodePlanner
			s.Messages = append(s.Messages, "[ResearchTeam] All steps completed, checking if more research needed")
			return s, nil
		}
		if s.IterationCount > s.MaxIterations {
			s.Goto = NodeReporter
			s.Messages = append(s.Messages, "[ResearchTeam] Max iterations reached, compiling report")
			return s, nil
		}

		step := s.PlanSteps[s.CurrentStepIdx]
		s.Messages = append(s.Messages, fmt.Sprintf("[ResearchTeam] Executing step %d: %s", s.CurrentStepIdx+1, step.Description))

		switch step.Type {
		case "research":
			s.Goto = NodeResearcher
		case "processing":
			s.Goto = NodeCoder
		default:
			s.CurrentStepIdx++
			s.Goto = NodeResearchTeam
		}
		return s, nil
	}
}

func researcherNode(llm ChatModel) func(ctx context.Context, state interface{}) (interface{}, error) {
	return func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(*DeerState)
		if s.CurrentStepIdx >= len(s.PlanSteps) {
			s.Goto = NodeResearchTeam
			return s, nil
		}
		step := &s.PlanSteps[s.CurrentStepIdx]
		msgs := buildStepMessages(s, *step, ResearcherPrompt)
		result, err := llm.Generate(ctx, msgs, []MockTool{searchTool()})
		if err != nil {
			return nil, fmt.Errorf("researcher: %w", err)
		}
		step.ExecutionRes = result
		if s.ResearchResults == nil {
			s.ResearchResults = make(map[string]string)
		}
		s.ResearchResults[step.Description] = result
		s.Messages = append(s.Messages, fmt.Sprintf("[Researcher] Findings for '%s':\n%s", step.Description, result))
		s.CurrentStepIdx++
		s.Goto = NodeResearchTeam
		return s, nil
	}
}

func coderNode(llm ChatModel) func(ctx context.Context, state interface{}) (interface{}, error) {
	return func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(*DeerState)
		if s.CurrentStepIdx >= len(s.PlanSteps) {
			s.Goto = NodeResearchTeam
			return s, nil
		}
		step := &s.PlanSteps[s.CurrentStepIdx]
		msgs := buildStepMessages(s, *step, CoderPrompt)
		result, err := llm.Generate(ctx, msgs, []MockTool{pythonREPLTool()})
		if err != nil {
			return nil, fmt.Errorf("coder: %w", err)
		}
		step.ExecutionRes = result
		if s.ResearchResults == nil {
			s.ResearchResults = make(map[string]string)
		}
		s.ResearchResults[step.Description] = result
		s.Messages = append(s.Messages, fmt.Sprintf("[Coder] Analysis for '%s':\n%s", step.Description, result))
		s.CurrentStepIdx++
		s.Goto = NodeResearchTeam
		return s, nil
	}
}

func reporterNode(llm ChatModel) func(ctx context.Context, state interface{}) (interface{}, error) {
	return func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(*DeerState)
		msgs := buildReportMessages(s, ReporterPrompt)
		report, err := llm.Generate(ctx, msgs, nil)
		if err != nil {
			return nil, fmt.Errorf("reporter: %w", err)
		}
		s.Report = report
		s.Messages = append(s.Messages, fmt.Sprintf("[Reporter] Final Report:\n%s", report))
		s.Goto = "__end__"
		return s, nil
	}
}

// ---- Build the StateGraph ----

func BuildResearchGraph(ctx context.Context, llm ChatModel) (*ResearchWorkflow, error) {
	sg := graph.NewStateGraph(&DeerState{})

	// Add 7 nodes with their logic
	sg.AddNode(NodeCoordinator, coordinatorNode(llm))
	sg.AddNode(NodePlanner, plannerNode(llm))
	sg.AddNode(NodeHuman, humanFeedbackNode())
	sg.AddNode(NodeResearchTeam, researchTeamNode())
	sg.AddNode(NodeResearcher, researcherNode(llm))
	sg.AddNode(NodeCoder, coderNode(llm))
	sg.AddNode(NodeReporter, reporterNode(llm))

	// Edges: start → coordinator
	sg.AddEdge(constants.Start, NodeCoordinator)

	// Coordinator → conditional routing
	sg.AddConditionalEdges(NodeCoordinator, coordinatorRouter,
		makeMap([]string{NodePlanner, constants.End}))

	// Planner → conditional routing
	sg.AddConditionalEdges(NodePlanner, plannerRouter,
		makeMap([]string{NodeHuman, NodeResearchTeam, NodeReporter}))

	// Human → research_team
	sg.AddEdge(NodeHuman, NodeResearchTeam)

	// ResearchTeam → conditional routing
	sg.AddConditionalEdges(NodeResearchTeam, researchTeamRouter,
		makeMap([]string{NodeResearcher, NodeCoder, NodeResearchTeam, NodePlanner, NodeReporter}))

	// Researcher/Coder → back to research_team
	sg.AddEdge(NodeResearcher, NodeResearchTeam)
	sg.AddEdge(NodeCoder, NodeResearchTeam)

	// Reporter → end
	sg.AddEdge(NodeReporter, constants.End)

	// Mark all terminal nodes as finish points for validation
	sg.SetFinishPoint(constants.End)

	compiled, err := sg.Compile(
		graph.WithRecursionLimit(100),
	)
	if err != nil {
		return nil, fmt.Errorf("compile research graph: %w", err)
	}

	return &ResearchWorkflow{compiled: compiled}, nil
}

// ---- Helpers ----

func makeMap(targets []string) map[string]string {
	m := make(map[string]string, len(targets))
	for _, t := range targets {
		m[t] = t
	}
	return m
}

func buildLLMMessages(state *DeerState, systemPrompt string) []map[string]string {
	msgs := []map[string]string{
		{"role": "system", "content": systemPrompt},
		{"role": "user", "content": state.UserInput},
	}
	for _, m := range state.Messages {
		msgs = append(msgs, map[string]string{"role": "assistant", "content": m})
	}
	return msgs
}

func buildStepMessages(state *DeerState, step Step, systemPrompt string) []map[string]string {
	msgs := []map[string]string{
		{"role": "system", "content": systemPrompt},
		{"role": "user", "content": fmt.Sprintf("Execute step: %s\nContext: %s", step.Description, state.UserInput)},
	}
	if len(state.ResearchResults) > 0 {
		for desc, res := range state.ResearchResults {
			msgs = append(msgs, map[string]string{"role": "assistant", "content": fmt.Sprintf("Previous result for '%s': %s", desc, res)})
		}
	}
	return msgs
}

func buildReportMessages(state *DeerState, systemPrompt string) []map[string]string {
	msgs := []map[string]string{
		{"role": "system", "content": systemPrompt},
		{"role": "user", "content": fmt.Sprintf("Original request: %s\n\nTitle: %s\nThought: %s", state.UserInput, state.PlanTitle, state.PlanThought)},
	}
	for _, step := range state.PlanSteps {
		res := step.ExecutionRes
		if res == "" {
			res = "No results"
		}
		msgs = append(msgs, map[string]string{
			"role":    "assistant",
			"content": fmt.Sprintf("Step [%s]: %s\nResult: %s", step.Type, step.Description, res),
		})
	}
	return msgs
}

func truncate(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n]) + "..."
}

func generateDefaultPlan(input string) []Step {
	return []Step{
		{Description: fmt.Sprintf("Research background and context for: %s", truncate(input, 40)), Type: "research"},
		{Description: "Analyze key findings and trends", Type: "processing"},
		{Description: "Research additional sources and perspectives", Type: "research"},
		{Description: "Synthesize all findings into coherent analysis", Type: "processing"},
	}
}

// stateFromMap converts a map result back to *DeerState.
func stateFromMap(v interface{}) *DeerState {
	if s, ok := v.(*DeerState); ok {
		return s
	}
	m, ok := v.(map[string]interface{})
	if !ok {
		return nil
	}
	s := &DeerState{
		Messages:         []string{},
		ResearchResults:  make(map[string]string),
		PlanAutoApproved: true,
		MaxIterations:    10,
	}
	s.UserInput = strFromMap(m, "UserInput")
	s.Goto = strFromMap(m, "Goto")
	s.PlanTitle = strFromMap(m, "PlanTitle")
	s.PlanThought = strFromMap(m, "PlanThought")
	s.Report = strFromMap(m, "Report")
	if msgs := strsFromMapNested(m, "Messages"); msgs != nil {
		s.Messages = msgs
	}
	if steps, ok := m["PlanSteps"].([]Step); ok {
		s.PlanSteps = steps
	} else if steps := stepsFromMapNested(m, "PlanSteps"); steps != nil {
		s.PlanSteps = steps
	}
	if results, ok := m["ResearchResults"].(map[string]string); ok {
		s.ResearchResults = results
	} else if results := resultsFromMapNested(m, "ResearchResults"); results != nil {
		s.ResearchResults = results
	}
	s.CurrentStepIdx = intFromMap(m, "CurrentStepIdx")
	s.IterationCount = intFromMap(m, "IterationCount")
	return s
}

func strFromMap(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func strsFromMapNested(m map[string]interface{}, key string) []string {
	v, ok := m[key]
	if !ok {
		return nil
	}
	var result []string
	var flatten func(v interface{})
	flatten = func(v interface{}) {
		switch val := v.(type) {
		case []interface{}:
			for _, item := range val {
				flatten(item)
			}
		case []string:
			result = append(result, val...)
		case string:
			result = append(result, val)
		}
	}
	flatten(v)
	return result
}

func intFromMap(m map[string]interface{}, key string) int {
	if v, ok := m[key]; ok {
		switch n := v.(type) {
		case int:
			return n
		case float64:
			return int(n)
		}
	}
	return 0
}

func stepsFromMapNested(m map[string]interface{}, key string) []Step {
	v, ok := m[key]
	if !ok {
		return nil
	}
	var allItems []interface{}
	var flattenItems func(v interface{})
	flattenItems = func(v interface{}) {
		switch val := v.(type) {
		case []interface{}:
			for _, item := range val {
				flattenItems(item)
			}
		case map[string]interface{}:
			allItems = append(allItems, val)
		}
	}
	flattenItems(v)
	if len(allItems) == 0 {
		return nil
	}
	last := allItems[len(allItems)-1].(map[string]interface{})
	var steps []Step
	for i := 0; ; i++ {
		desc := strFromMap(last, fmt.Sprintf("Description_%d", i))
		if desc == "" {
			break
		}
		steps = append(steps, Step{
			Description:  desc,
			Type:         strFromMap(last, fmt.Sprintf("Type_%d", i)),
			ExecutionRes: strFromMap(last, fmt.Sprintf("ExecutionRes_%d", i)),
		})
	}
	return steps
}

func resultsFromMapNested(m map[string]interface{}, key string) map[string]string {
	v, ok := m[key]
	if !ok {
		return nil
	}
	var allMaps []map[string]interface{}
	var flattenResults func(v interface{})
	flattenResults = func(v interface{}) {
		switch val := v.(type) {
		case []interface{}:
			for _, item := range val {
				flattenResults(item)
			}
		case map[string]interface{}:
			allMaps = append(allMaps, val)
		}
	}
	flattenResults(v)
	if len(allMaps) == 0 {
		return nil
	}
	last := allMaps[len(allMaps)-1]
	out := make(map[string]string, len(last))
	for k, val := range last {
		if s, ok := val.(string); ok {
			out[k] = s
		}
	}
	return out
}

func isInteractive() bool {
	return true
}
