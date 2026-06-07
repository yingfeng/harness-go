// Package deerflow implements a multi-agent research collaboration system,
// adapted from bytedance/deer-flow using the harness-go framework.
//
// Architecture:
//
//	User Input
//	   │
//	   ▼
//	Coordinator ──(hand_to_planner)──→ Planner ──→ Human (review plan)
//	   │                                              │ accept
//	   │                                              ▼
//	   └──(chat)──→ END                     ResearchTeam
//                                              │        │
//                                              ▼        ▼
//                                         Researcher  Coder
//                                              │        │
//                                              └───┬────┘
//                                                  │
//                                                  ▼ (all steps done)
//                                             Reporter → END
package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {
	ctx := context.Background()

	// Initialize LLM from environment
	llm, err := newChatModel(ctx)
	if err != nil {
		log.Fatalf("init LLM: %v", err)
	}

	// Build and compile the multi-agent graph
	workflow, err := buildResearchGraph(ctx, llm)
	if err != nil {
		log.Fatalf("build graph: %v", err)
	}

	fmt.Println("=== DeerFlow: Multi-Agent Research System ===")
	fmt.Println("Enter a research topic, or type 'quit' to exit.")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		input := strings.TrimSpace(scanner.Text())
		if input == "" || input == "quit" {
			break
		}

		state, err := workflow.Invoke(ctx, input)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nError: %v\n", err)
			continue
		}
		if state.Report != "" {
			fmt.Printf("\n=== Final Report ===\n%s\n", state.Report)
		}
		if len(state.Messages) > 0 {
			last := state.Messages[len(state.Messages)-1]
			fmt.Printf("\nLast message: %s\n", last)
		}
		fmt.Println()
	}
}
