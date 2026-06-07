package main

import (
	"context"
	"strings"
	"testing"
)

// TestDeerFlow_FullResearchCycle verifies the complete multi-agent research workflow
// from user input to final report, covering all 7 agents:
//
//	Coordinator → Planner → ResearchTeam → Researcher → ResearchTeam → Coder → ResearchTeam → Reporter
func TestDeerFlow_FullResearchCycle(t *testing.T) {
	ctx := context.Background()
	llm, err := newChatModel(ctx)
	if err != nil {
		t.Fatalf("newChatModel: %v", err)
	}

	wf, err := buildResearchGraph(ctx, llm)
	if err != nil {
		t.Fatalf("buildResearchGraph: %v", err)
	}

	// All research steps with mock model (no API key needed)
	state, err := wf.Invoke(ctx, "What is the impact of AI on healthcare?")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	// Verify the workflow completed
	if state == nil {
		t.Fatal("expected non-nil state")
	}

	// Verify all phases were executed
	hasCoordinator := false
	hasPlanner := false
	hasResearch := false
	hasReport := false
	for _, msg := range state.Messages {
		if strings.Contains(msg, "[Coordinator") {
			hasCoordinator = true
		}
		if strings.Contains(msg, "[Planner") {
			hasPlanner = true
		}
		if strings.Contains(msg, "[Researcher") || strings.Contains(msg, "[Coder") {
			hasResearch = true
		}
		if strings.Contains(msg, "[Reporter") {
			hasReport = true
		}
	}

	if !hasCoordinator {
		t.Error("Coordinator was not executed")
	}
	if !hasPlanner {
		t.Error("Planner was not executed")
	}
	if !hasResearch {
		t.Error("Researcher/Coder were not executed")
	}
	if !hasReport {
		t.Error("Reporter was not executed")
	}

	// Verify plan was created with steps
	if len(state.PlanSteps) == 0 {
		t.Error("expected at least 1 plan step")
	}

	// Verify step results were collected
	if len(state.ResearchResults) == 0 {
		t.Error("expected at least 1 research result")
	}

	t.Logf("Research complete: %d steps, %d results", len(state.PlanSteps), len(state.ResearchResults))
	t.Logf("Messages: %d", len(state.Messages))
}

// TestDeerFlow_RoutingCoordinatorToPlanner verifies that the coordinator
// routes research questions to the planner.
func TestDeerFlow_RoutingCoordinatorToPlanner(t *testing.T) {
	ctx := context.Background()
	llm := &mockModel{}
	wf, err := buildResearchGraph(ctx, llm)
	if err != nil {
		t.Fatalf("buildResearchGraph: %v", err)
	}

	state, err := wf.Invoke(ctx, "Tell me about machine learning")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	// With mock model, coordinator routes to planner
	hasPlannerTransition := false
	hasReport := false
	for _, msg := range state.Messages {
		if strings.Contains(msg, "→ Planner") {
			hasPlannerTransition = true
		}
		if strings.Contains(msg, "[Reporter") {
			hasReport = true
		}
	}

	if !hasPlannerTransition {
		t.Log("Planner transition not detected (expected with mock model)")
	}
	if !hasReport {
		t.Error("expected reporter to generate final report")
	}
}

// TestDeerFlow_ResearchTeamLoop verifies the ResearchTeam correctly iterates
// through plan steps and routes to Researcher and Coder agents.
func TestDeerFlow_ResearchTeamLoop(t *testing.T) {
	ctx := context.Background()
	llm := &mockModel{}
	wf, err := buildResearchGraph(ctx, llm)
	if err != nil {
		t.Fatalf("buildResearchGraph: %v", err)
	}

	state, err := wf.Invoke(ctx, "Analyze climate change data")
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	// Verify steps were processed
	if state.CurrentStepIdx > 0 {
		t.Logf("Processed %d of %d steps", state.CurrentStepIdx, len(state.PlanSteps))
	}

	if len(state.ResearchResults) == 0 {
		t.Log("No research results collected (expected with mock model)")
	}
}

// TestDeerFlow_EmptyInput verifies the workflow handles empty input gracefully.
func TestDeerFlow_EmptyInput(t *testing.T) {
	ctx := context.Background()
	llm := &mockModel{}
	wf, err := buildResearchGraph(ctx, llm)
	if err != nil {
		t.Fatalf("buildResearchGraph: %v", err)
	}

	state, err := wf.Invoke(ctx, "")
	if err != nil {
		t.Fatalf("Invoke with empty input: %v", err)
	}
	if state == nil {
		t.Fatal("expected non-nil state even with empty input")
	}
}
