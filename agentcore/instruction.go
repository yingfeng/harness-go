package agentcore

import "fmt"

// GenTransferInstruction generates an instruction string for agent transfer.
func GenTransferInstruction(names []string) string {
	if len(names) == 0 { return "" }
	s := "You can transfer to the following agents:\n"
	for _, n := range names { s += fmt.Sprintf("- %s\n", n) }
	return s
}

// GenToolInstruction generates tool instruction for an agent.
func GenToolInstruction(name, desc string) string {
	return fmt.Sprintf("Agent '%s': %s", name, desc)
}
