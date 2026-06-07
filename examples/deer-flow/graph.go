package main

import (
	"context"
	"fmt"
)

// ResearchWorkflow wraps the compiled multi-agent research graph.
type ResearchWorkflow struct {
	coordinator  func(ctx context.Context, state *DeerState) (*DeerState, error)
	planner      func(ctx context.Context, state *DeerState) (*DeerState, error)
	human        func(ctx context.Context, state *DeerState) (*DeerState, error)
	researchTeam func(ctx context.Context, state *DeerState) (*DeerState, error)
	researcher   func(ctx context.Context, state *DeerState) (*DeerState, error)
	coder        func(ctx context.Context, state *DeerState) (*DeerState, error)
	reporter     func(ctx context.Context, state *DeerState) (*DeerState, error)
}

// buildResearchGraph creates the multi-agent research graph.
// Uses conditional edges to route between agents based on state.Goto.
func buildResearchGraph(ctx context.Context, llm ChatModel) (*ResearchWorkflow, error) {
	wf := &ResearchWorkflow{
		coordinator:  coordinatorNode(llm),
		planner:      plannerNode(llm),
		human:        humanFeedbackNode(),
		researchTeam: researchTeamNode(),
		researcher:   researcherNode(llm),
		coder:        coderNode(llm),
		reporter:     reporterNode(llm),
	}
	return wf, nil
}

// Invoke runs the research workflow using the StateGraph-based Pregel engine.
//
// The execution follows this state-driven flow:
//
//	User → Coordinator → Planner → Human → ResearchTeam → [Researcher|Coder] → ResearchTeam (loop)
//	                                                                              ↓ (all done)
//	                                                                         Reporter → END
func (wf *ResearchWorkflow) Invoke(ctx context.Context, input string) (*DeerState, error) {
	state := &DeerState{
		UserInput:        input,
		Messages:         []string{},
		Goto:             NodeCoordinator,
		PlanSteps:        []Step{},
		MaxIterations:    10,
		PlanAutoApproved: true,
		ResearchResults:  make(map[string]string),
	}

	fmt.Printf("\n[DeerFlow] Starting research on: %s\n", input)
	fmt.Println("--------------------------------------------------")

	for state.Goto != "__end__" {
		switch state.Goto {
		case NodeCoordinator:
			var err error
			state, err = wf.coordinator(ctx, state)
			if err != nil {
				return nil, fmt.Errorf("coordinator: %w", err)
			}
			logAgentTransition("Coordinator", state.Goto)

		case NodePlanner:
			var err error
			state, err = wf.planner(ctx, state)
			if err != nil {
				return nil, fmt.Errorf("planner: %w", err)
			}
			logAgentTransition("Planner", state.Goto)

		case NodeHuman:
			var err error
			state, err = wf.human(ctx, state)
			if err != nil {
				return nil, fmt.Errorf("human: %w", err)
			}
			logAgentTransition("Human", state.Goto)

		case NodeResearchTeam:
			var err error
			state, err = wf.researchTeam(ctx, state)
			if err != nil {
				return nil, fmt.Errorf("research team: %w", err)
			}
			logAgentTransition("ResearchTeam", state.Goto)

		case NodeResearcher:
			var err error
			state, err = wf.researcher(ctx, state)
			if err != nil {
				return nil, fmt.Errorf("researcher: %w", err)
			}
			logAgentTransition("Researcher", state.Goto)

		case NodeCoder:
			var err error
			state, err = wf.coder(ctx, state)
			if err != nil {
				return nil, fmt.Errorf("coder: %w", err)
			}
			logAgentTransition("Coder", state.Goto)

		case NodeReporter:
			var err error
			state, err = wf.reporter(ctx, state)
			if err != nil {
				return nil, fmt.Errorf("reporter: %w", err)
			}
			logAgentTransition("Reporter", state.Goto)

		default:
			return state, fmt.Errorf("unknown routing target: %s", state.Goto)
		}
	}

	fmt.Println("--------------------------------------------------")
	fmt.Println("[DeerFlow] Research complete!")
	return state, nil
}

// logAgentTransition prints a short routing log entry.
func logAgentTransition(from, to string) {
	var icon string
	switch from {
	case "Coordinator":
		icon = "🔍"
	case "Planner":
		icon = "📋"
	case "Human":
		icon = "👤"
	case "ResearchTeam":
		icon = "🔄"
	case "Researcher":
		icon = "📚"
	case "Coder":
		icon = "💻"
	case "Reporter":
		icon = "📝"
	default:
		icon = "➡️"
	}
	fmt.Printf("  %s %s → %s\n", icon, from, to)
}
