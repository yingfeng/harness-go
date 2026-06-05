package workflow

import (
	"fmt"

	"github.com/infiniflow/ragflow/harness/agentcore"
)

// PrintEvent prints an AgentEvent in a human-readable format.
func PrintEvent(ev *agentcore.AgentEvent) {
	if ev == nil {
		return
	}
	if ev.Err != nil {
		fmt.Printf("[ERROR] %v\n", ev.Err)
		return
	}
	if ev.Action != nil {
		if ev.Action.Interrupted != nil {
			fmt.Printf("[INTERRUPT] %v\n", ev.Action.Interrupted.Data)
		}
		if ev.Action.Exit {
			fmt.Printf("[EXIT]\n")
		}
		if ev.Action.TransferToAgent != nil {
			fmt.Printf("[TRANSFER] -> %s\n", ev.Action.TransferToAgent.DestAgentName)
		}
		if ev.Action.BreakLoop != nil {
			fmt.Printf("[BREAK_LOOP] from=%s iter=%d done=%v\n",
				ev.Action.BreakLoop.From, ev.Action.BreakLoop.CurrentIterations, ev.Action.BreakLoop.Done)
		}
		return
	}
	if ev.Output != nil && ev.Output.MessageOutput != nil {
		mv := ev.Output.MessageOutput
		if !mv.IsStreaming && mv.Message != nil {
			role := mv.Role
			content := mv.Message.Content
			agentName := ev.AgentName
			if role == "tool" {
				fmt.Printf("[%s] TOOL(%s): %s\n", agentName, mv.ToolName, truncate(content, 120))
			} else if role == "assistant" {
				fmt.Printf("[%s] ASSISTANT: %s\n", agentName, truncate(content, 200))
			} else {
				fmt.Printf("[%s] %s: %s\n", agentName, role, truncate(content, 120))
			}
		}
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
