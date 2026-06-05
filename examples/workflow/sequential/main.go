// Package main demonstrates a Sequential workflow using NewSequential.
//
// A PlannerAgent first creates a research plan, then a WriterAgent expands
// it into a full report. The workflow runs the two sub-agents in order.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
	"github.com/infiniflow/ragflow/harness/examples/workflow"
	wfsub "github.com/infiniflow/ragflow/harness/examples/workflow/sequential/subagents"
)

func main() {
	ctx := context.Background()

	// Create the sequential workflow: PlannerAgent → WriterAgent
	wf, err := agentcore.NewSequential(ctx, &agentcore.SequentialConfig{
		Name:        "ResearchAgent",
		Description: "A sequential workflow for planning and writing a research report.",
		SubAgents:   []agentcore.Agent{wfsub.NewPlanAgent(), wfsub.NewWriterAgent()},
	})
	if err != nil {
		log.Fatalf("NewSequential: %v", err)
	}

	query := "The history of Large Language Models"
	fmt.Printf("Query: %s\n\n", query)

	runner := agentcore.NewTypedRunner(agentcore.RunnerConfig[*schema.Message]{
		EnableStreaming: false,
		Agent:           wf,
	})

	var lastMsg *schema.Message
	iter := runner.Query(ctx, query)
	for {
		ev, ok := iter.Next()
		if !ok {
			break
		}
		workflow.PrintEvent(ev)
		if ev.Err != nil {
			fmt.Printf("Error: %v\n", ev.Err)
			break
		}
		if ev.Output != nil && ev.Output.MessageOutput != nil {
			if !ev.Output.MessageOutput.IsStreaming {
				lastMsg = ev.Output.MessageOutput.Message
			}
		}
	}

	if lastMsg != nil {
		fmt.Printf("\nFinal output: %s\n", lastMsg.Content)
	}
}
