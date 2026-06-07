package agentcore

// ResumeWithData creates a ResumeInfo with custom resume data.
// Use this to pass ReActAgentResumeData (e.g., HistoryModifier)
// when resuming an interrupted agent.
func ResumeWithData(data any) *ResumeInfo {
	return &ResumeInfo{ResumeData: data}
}

// WithHistoryModifier returns a RunOption that sets a HistoryModifier
// for modifying message history during resume.
// Deprecated: Use ResumeWithData(ReActAgentResumeData{...}) instead.
//
// Note: This is defined in options.go and re-exported here for convenience.
// The actual implementation is in options.go; this is just a documentation alias.

// ---- DeterministicTransfer helpers ----

// exactRunPathMatch checks if two run paths are exactly equal.
// This prevents sub-agents from forging paths to access restricted agents.
func exactRunPathMatch(a, b []RunStep) bool {
	if len(a) != len(b) { return false }
	for i := range a { if !a[i].Equals(b[i]) { return false } }
	return true
}
