// Package main demonstrates a Loop workflow using NewLoop.
//
// A MainAgent attempts to solve a task while a CritiqueAgent reviews the
// output and provides feedback. When satisfied, CritiqueAgent triggers
// a break-loop action to terminate the loop.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
	"github.com/infiniflow/ragflow/harness/examples/workflow"
	wfsub "github.com/infiniflow/ragflow/harness/examples/workflow/loop/subagents"
)

func main() {
	ctx := context.Background()

	// Create the loop workflow: MainAgent → CritiqueAgent → (loop back or break)
	wf, err := agentcore.NewLoop(ctx, &agentcore.LoopConfig{
		Name:          "reflection_agent",
		Description:   "Reflection agent with main and critique agent for iterative task solving.",
		SubAgents:     []agentcore.Agent{wfsub.NewMainAgent(), wfsub.NewCritiqueAgent()},
		MaxIterations: 5,
	})
	if err != nil {
		log.Fatalf("NewLoop: %v", err)
	}

	query := "briefly introduce what a multimodal embedding model is."
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
