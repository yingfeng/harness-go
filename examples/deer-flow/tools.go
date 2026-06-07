package main

import (
	"context"
	"fmt"
)

// MockTool defines a simple tool for demonstration.
type MockTool struct {
	Name        string
	Description string
	Execute     func(ctx context.Context, input string) (string, error)
}

// searchTool simulates a web search tool.
func searchTool() MockTool {
	return MockTool{
		Name:        "web_search",
		Description: "Search the web for information on a topic. Input: search query.",
		Execute: func(ctx context.Context, input string) (string, error) {
			return fmt.Sprintf("[Search results for: %s]\n- Found relevant information about %s\n- Multiple sources available\n- Further investigation recommended", input, input), nil
		},
	}
}

// pythonREPLTool simulates a Python execution tool.
func pythonREPLTool() MockTool {
	return MockTool{
		Name:        "execute_python",
		Description: "Execute Python code for data processing. Input: Python code.",
		Execute: func(ctx context.Context, input string) (string, error) {
			return fmt.Sprintf("[Python execution result]\nCode executed successfully.\nOutput: Processed data for research analysis."), nil
		},
	}
}

// handToPlannerTool is used by the Coordinator to route to the Planner.
func handToPlannerTool() MockTool {
	return MockTool{
		Name:        "hand_to_planner",
		Description: "Hand off the research request to the planning agent. Call this when the user asks a research question.",
		Execute: func(ctx context.Context, input string) (string, error) {
			return "handed to planner", nil
		},
	}
}

// planTool is used by the Planner to output a structured plan.
func planTool() MockTool {
	return MockTool{
		Name:        "create_plan",
		Description: "Create a structured research plan with steps.",
		Execute: func(ctx context.Context, input string) (string, error) {
			return fmt.Sprintf("Plan created: %s", input), nil
		},
	}
}
