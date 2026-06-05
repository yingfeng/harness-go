// Package main demonstrates a Parallel workflow using NewParallel.
//
// Three data-collection agents (Stock, News, Social Media) run concurrently
// to gather information for market research.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
	"github.com/infiniflow/ragflow/harness/examples/workflow"
	wfsub "github.com/infiniflow/ragflow/harness/examples/workflow/parallel/subagents"
)

func main() {
	ctx := context.Background()

	// Create the parallel workflow: all three sub-agents run concurrently
	wf, err := agentcore.NewParallel(ctx, &agentcore.ParallelConfig{
		Name:        "DataCollectionAgent",
		Description: "Data Collection Agent could collect data from multiple sources.",
		SubAgents: []agentcore.Agent{
			wfsub.NewStockDataCollectionAgent(),
			wfsub.NewNewsDataCollectionAgent(),
			wfsub.NewSocialMediaInfoCollectionAgent(),
		},
	})
	if err != nil {
		log.Fatalf("NewParallel: %v", err)
	}

	query := "give me today's market research"
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
