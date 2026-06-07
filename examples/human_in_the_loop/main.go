// Package main demonstrates Human-in-the-Loop (HITL) with harness-go.
//
// This example shows:
//   - Building a graph with interrupt points for human approval
//   - Running the graph until an interrupt fires
//   - Resuming execution after human review
//   - Using checkpoint to preserve state between run/resume cycles
//
// Scenario: An expense approval flow where a manager must approve expenses > $1000.
// The graph pauses at the "approval_check" node and waits for human input.
//
// To run: go run main.go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/infiniflow/ragflow/harness"
	"github.com/infiniflow/ragflow/harness/checkpoint"
	"github.com/infiniflow/ragflow/harness/constants"
)

// ---- Shared State ----

type ExpenseState struct {
	Amount      float64 `json:"amount"`
	Category    string  `json:"category"`
	Approved    bool    `json:"approved"`
	ReviewerNote string  `json:"reviewer_note"`
	Status      string  `json:"status"`
}

// ---- Mock Human Reviewer ----

// simulateHumanReview pretends to be a human reviewing an expense.
// In production, this would be a web UI or Slack bot waiting for actual human input.
func simulateHumanReview(state *ExpenseState) {
	fmt.Println("\n===== HUMAN REVIEW REQUIRED =====")
	fmt.Printf("Expense: $%.2f (%s)\n", state.Amount, state.Category)
	fmt.Printf("Status: %s\n", state.Status)
	fmt.Printf("Approve? (simulating approval...)\n")
	state.Approved = true
	state.ReviewerNote = "Approved by manager after review"
	fmt.Println("===== REVIEW COMPLETE =====\n")
}

func main() {
	ctx := context.Background()

	// ---- 1. Build the Approval Graph ----
	//
	// Nodes:
	//   validate_expense → approval_check → process_result → end
	//
	// The "approval_check" node is an interrupt point: the graph pauses before
	// executing it, giving the human a chance to review.

	fmt.Println("=== Building Approval Graph ===")
	sg := harness.NewStateGraph(&ExpenseState{
		Status: "pending",
	})

	// Node: Validate the expense (auto).
	sg.AddNode("validate_expense", func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(*ExpenseState)
		fmt.Printf("[validate] Expense: $%.2f (%s)\n", s.Amount, s.Category)
		if s.Amount <= 0 {
			return nil, fmt.Errorf("invalid amount: %.2f", s.Amount)
		}
		if s.Category == "" {
			s.Category = "general"
		}
		s.Status = "validated"
		return s, nil
	})

	// Node: Approval check (HITL breakpoint).
	// The graph pauses HERE because we set WithInterrupts("approval_check").
	sg.AddNode("approval_check", func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(*ExpenseState)
		fmt.Printf("[approval_check] Amount=%.2f needs human review\n", s.Amount)

		// If amount ≤ 1000, auto-approve.
		if s.Amount <= 1000 {
			s.Approved = true
			s.ReviewerNote = "Auto-approved (≤ $1000)"
			s.Status = "approved"
			fmt.Println("[approval_check] Auto-approved")
			return s, nil
		}

		// Amount > 1000: requires human approval.
		// The graph was interrupted BEFORE this node, so the human has already
		// reviewed via simulateHumanReview() by the time we reach here.
		s.Status = "manually_approved"
		fmt.Println("[approval_check] Approved by human")
		return s, nil
	})

	// Node: Process the result.
	sg.AddNode("process_result", func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(*ExpenseState)
		if s.Approved {
			s.Status = "processed"
			fmt.Printf("[process] Expense $%.2f processed: %s\n", s.Amount, s.ReviewerNote)
		} else {
			s.Status = "rejected"
			fmt.Printf("[process] Expense rejected\n")
		}
		return s, nil
	})

	// Edges.
	sg.AddEdge(constants.Start, "validate_expense")
	sg.AddEdge("validate_expense", "approval_check")
	sg.AddEdge("approval_check", "process_result")
	sg.AddEdge("process_result", constants.End)

	// Compile with checkpoint and interrupt.
	checkpointer := checkpoint.NewMemorySaver()
	compiled, err := sg.Compile(
		harness.WithCheckpointer(checkpointer),
		harness.WithInterrupts("approval_check"), // Pause before approval_check
		harness.WithRecursionLimit(10),
	)
	if err != nil {
		log.Fatalf("Compile: %v", err)
	}

	// ---- 2. First Run: Interrupt at approval_check ----

	fmt.Println("\n=== First Run (will interrupt before approval_check) ===")
	threadID := "expense-thread-001"
	config := harness.NewRunnableConfig()
	config.ThreadID = threadID

	input := &ExpenseState{
		Amount:   2500.00,
		Category: "travel",
		Status:   "pending",
	}

	result, err := compiled.Invoke(ctx, input, config)
	if err != nil {
		var gi *harness.GraphInterrupt
		if errors.As(err, &gi) {
			fmt.Printf("\n✓ Graph interrupted as expected!\n")
			fmt.Printf("  Interrupts: %v\n", gi.Interrupts)
			fmt.Printf("  Thread ID: %s\n", threadID)
			fmt.Printf("  Call Resume() to continue after review.\n")
		} else {
			log.Fatalf("Unexpected error: %v", err)
		}
	} else {
		s := result.(*ExpenseState)
		jsonBytes, _ := json.MarshalIndent(s, "", "  ")
		fmt.Printf("Graph completed (no interrupt needed):\n%s\n", jsonBytes)
		return
	}

	// ---- 3. Human Review (simulated) ----

	state := &ExpenseState{Amount: 2500.00, Category: "travel"}
	simulateHumanReview(state)

	// ---- 4. Resume: Continue from checkpoint ----

	fmt.Println("=== Resuming from Checkpoint ===")
	config.Set("checkpoint_id", threadID)

	result, err = compiled.Invoke(ctx, nil, config)
	if err != nil {
		log.Fatalf("Resume: %v", err)
	}

	s := result.(*ExpenseState)
	jsonBytes, _ := json.MarshalIndent(s, "", "  ")
	fmt.Printf("\n✓ Final State:\n%s\n", jsonBytes)
	fmt.Printf("\n=== Flow Complete ===\n")
}
