package main

// DeerState is the shared state for the multi-agent research graph.
// Each agent reads from and writes to this state as it flows through the graph.
type DeerState struct {
	// UserInput is the original research question.
	UserInput string
	// Messages holds the full conversation history.
	Messages []string
	// Goto controls dynamic routing between agents.
	// Possible values: "planner", "human", "research_team", "reporter", "__end__"
	Goto string

	// Plan fields
	PlanTitle      string
	PlanThought    string
	PlanSteps      []Step
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

// AgentName constants for routing.
const (
	NodeCoordinator = "coordinator"
	NodePlanner     = "planner"
	NodeHuman       = "human_feedback"
	NodeResearchTeam = "research_team"
	NodeResearcher  = "researcher"
	NodeCoder       = "coder"
	NodeReporter    = "reporter"
)
