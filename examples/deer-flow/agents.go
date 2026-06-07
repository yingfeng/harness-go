package main

import (
	"context"
	"fmt"
	"os"
)

// ---- Coordinator Agent ----

// coordinatorNode creates the coordinator agent node function.
// It classifies the user request: chat (direct reply) or research (route to planner).
func coordinatorNode(llm ChatModel) func(ctx context.Context, state *DeerState) (*DeerState, error) {
	return func(ctx context.Context, state *DeerState) (*DeerState, error) {
		msgs := buildLLMMessages(state, CoordinatorPrompt)
		tool, result, err := llm.GenerateWithTool(ctx, msgs, []MockTool{handToPlannerTool()})
		if err != nil {
			return nil, fmt.Errorf("coordinator: %w", err)
		}

		if tool == "hand_to_planner" {
			state.Goto = NodePlanner
			state.Messages = append(state.Messages, fmt.Sprintf("[Coordinator → Planner] Routing research request"))
		} else {
			state.Goto = "__end__"
			state.Messages = append(state.Messages, fmt.Sprintf("[Coordinator] %s", result))
		}
		return state, nil
	}
}

// ---- Planner Agent ----

// plannerNode creates the planner agent node function.
// It generates a research plan with steps and routes to human feedback.
// When re-invoked after all steps are done, it routes to Reporter.
func plannerNode(llm ChatModel) func(ctx context.Context, state *DeerState) (*DeerState, error) {
	return func(ctx context.Context, state *DeerState) (*DeerState, error) {
		// If all steps were executed but we're back at planner, check if more
		// research is needed or route directly to reporter.
		if len(state.PlanSteps) > 0 && state.CurrentStepIdx >= len(state.PlanSteps) {
			state.IterationCount++
			if state.IterationCount >= 3 {
				state.Goto = NodeReporter
				state.Messages = append(state.Messages, "[Planner] Sufficient research collected, generating report")
				return state, nil
			}
			// Reset for another research round
			state.CurrentStepIdx = 0
			state.PlanSteps = generateDefaultPlan(state.UserInput)
			state.Messages = append(state.Messages, "[Planner] Additional research needed, continuing")
			state.Goto = NodeResearchTeam
			return state, nil
		}

		msgs := buildLLMMessages(state, PlannerPrompt)
		planStr, err := llm.Generate(ctx, msgs, []MockTool{planTool()})
		if err != nil {
			return nil, fmt.Errorf("planner: %w", err)
		}

		// Parse the plan (simplified)
		state.PlanTitle = fmt.Sprintf("Research Plan: %s", truncate(state.UserInput, 40))
		state.PlanThought = "Systematic research approach"
		state.PlanSteps = generateDefaultPlan(state.UserInput)
		state.MaxIterations = len(state.PlanSteps) * 2
		state.CurrentStepIdx = 0
		state.IterationCount = 0

		state.Messages = append(state.Messages, fmt.Sprintf("[Planner] Created plan with %d steps: %s", len(state.PlanSteps), truncate(planStr, 100)))

		if state.PlanAutoApproved {
			state.Goto = NodeResearchTeam
		} else {
			state.Goto = NodeHuman
		}
		return state, nil
	}
}

// ---- Human Feedback Node ----

// humanFeedbackNode creates a node that pauses for human review of the plan.
// In interactive mode, it prompts the user to approve or edit the plan.
func humanFeedbackNode() func(ctx context.Context, state *DeerState) (*DeerState, error) {
	return func(ctx context.Context, state *DeerState) (*DeerState, error) {
		// Auto-approve in non-interactive mode
		if !isInteractive() {
			state.Goto = NodeResearchTeam
			state.Messages = append(state.Messages, "[Human] Plan auto-approved (non-interactive)")
			return state, nil
		}

		fmt.Println("\n=== Plan Review ===")
		fmt.Printf("Title: %s\n", state.PlanTitle)
		fmt.Printf("Thought: %s\n", state.PlanThought)
		fmt.Println("Steps:")
		for i, step := range state.PlanSteps {
			fmt.Printf("  %d. [%s] %s\n", i+1, step.Type, step.Description)
		}
		fmt.Println("\nApprove this plan? (Y/n/e to edit): ")
		var input string
		fmt.Scanln(&input)

		switch input {
		case "n", "N":
			state.Goto = "__end__"
			state.Messages = append(state.Messages, "[Human] Research cancelled by user")
		case "e", "E":
			fmt.Println("Enter revised plan description: ")
			var edit string
			fmt.Scanln(&edit)
			if edit != "" {
				state.PlanSteps = generateDefaultPlan(edit)
				state.Messages = append(state.Messages, "[Human] Plan edited by user")
			}
			state.Goto = NodeResearchTeam
		default:
			state.Goto = NodeResearchTeam
			state.Messages = append(state.Messages, "[Human] Plan approved")
		}
		return state, nil
	}
}

// ---- Research Team Router ----

// researchTeamNode creates the research team routing node.
// It selects the next unexecuted step and routes to the appropriate agent.
func researchTeamNode() func(ctx context.Context, state *DeerState) (*DeerState, error) {
	return func(ctx context.Context, state *DeerState) (*DeerState, error) {
		state.IterationCount++

		// Check if all steps are done
		if state.CurrentStepIdx >= len(state.PlanSteps) {
			state.Goto = NodePlanner
			state.Messages = append(state.Messages, "[ResearchTeam] All steps completed, checking if more research needed")
			return state, nil
		}

		// Check iteration limit
		if state.IterationCount > state.MaxIterations {
			state.Goto = NodeReporter
			state.Messages = append(state.Messages, "[ResearchTeam] Max iterations reached, compiling report")
			return state, nil
		}

		step := state.PlanSteps[state.CurrentStepIdx]
		state.Messages = append(state.Messages, fmt.Sprintf("[ResearchTeam] Executing step %d: %s", state.CurrentStepIdx+1, step.Description))

		switch step.Type {
		case "research":
			state.Goto = NodeResearcher
		case "processing":
			state.Goto = NodeCoder
		default:
			state.CurrentStepIdx++
			state.Goto = NodeResearchTeam
		}
		return state, nil
	}
}

// ---- Researcher Agent ----

// researcherNode creates the researcher agent node function.
// It gathers information using search tools for a research step.
func researcherNode(llm ChatModel) func(ctx context.Context, state *DeerState) (*DeerState, error) {
	return func(ctx context.Context, state *DeerState) (*DeerState, error) {
		if state.CurrentStepIdx >= len(state.PlanSteps) {
			state.Goto = NodeResearchTeam
			return state, nil
		}

		step := &state.PlanSteps[state.CurrentStepIdx]
		msgs := buildStepMessages(state, *step, ResearcherPrompt)
		result, err := llm.Generate(ctx, msgs, []MockTool{searchTool()})
		if err != nil {
			return nil, fmt.Errorf("researcher: %w", err)
		}

		step.ExecutionRes = result
		if state.ResearchResults == nil {
			state.ResearchResults = make(map[string]string)
		}
		state.ResearchResults[step.Description] = result
		state.Messages = append(state.Messages, fmt.Sprintf("[Researcher] Completed: %s", step.Description))
		state.CurrentStepIdx++
		state.Goto = NodeResearchTeam
		return state, nil
	}
}

// ---- Coder Agent ----

// coderNode creates the coder agent node function.
// It processes data using Python execution tools.
func coderNode(llm ChatModel) func(ctx context.Context, state *DeerState) (*DeerState, error) {
	return func(ctx context.Context, state *DeerState) (*DeerState, error) {
		if state.CurrentStepIdx >= len(state.PlanSteps) {
			state.Goto = NodeResearchTeam
			return state, nil
		}

		step := &state.PlanSteps[state.CurrentStepIdx]
		msgs := buildStepMessages(state, *step, CoderPrompt)
		result, err := llm.Generate(ctx, msgs, []MockTool{pythonREPLTool()})
		if err != nil {
			return nil, fmt.Errorf("coder: %w", err)
		}

		step.ExecutionRes = result
		if state.ResearchResults == nil {
			state.ResearchResults = make(map[string]string)
		}
		state.ResearchResults[step.Description] = result
		state.Messages = append(state.Messages, fmt.Sprintf("[Coder] Completed: %s", step.Description))
		state.CurrentStepIdx++
		state.Goto = NodeResearchTeam
		return state, nil
	}
}

// ---- Reporter Agent ----

// reporterNode creates the reporter agent node function.
// It synthesizes all research findings into a final report.
func reporterNode(llm ChatModel) func(ctx context.Context, state *DeerState) (*DeerState, error) {
	return func(ctx context.Context, state *DeerState) (*DeerState, error) {
		msgs := buildReportMessages(state, ReporterPrompt)
		report, err := llm.Generate(ctx, msgs, nil)
		if err != nil {
			return nil, fmt.Errorf("reporter: %w", err)
		}

		state.Report = report
		state.Messages = append(state.Messages, "[Reporter] Final report generated")
		state.Goto = "__end__"
		return state, nil
	}
}

// ---- Helpers ----

// buildLLMMessages constructs the message list for a standard agent call.
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

// buildStepMessages constructs the message list for a step executor agent.
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

// buildReportMessages constructs the message list for the reporter.
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
			"role": "assistant",
			"content": fmt.Sprintf("Step [%s]: %s\nResult: %s", step.Type, step.Description, res),
		})
	}
	return msgs
}

// isInteractive checks if the program is running with a terminal attached.
func isInteractive() bool {
	info, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

// generateDefaultPlan creates a default research plan based on user input.
func generateDefaultPlan(input string) []Step {
	return []Step{
		{Description: fmt.Sprintf("Research background and context for: %s", truncate(input, 40)), Type: "research"},
		{Description: "Analyze key findings and trends", Type: "processing"},
		{Description: "Research additional sources and perspectives", Type: "research"},
		{Description: "Synthesize all findings into coherent analysis", Type: "processing"},
	}
}
