package agentcore

import "github.com/infiniflow/ragflow/harness/agentcore/internal"

// SetLanguage sets the language for agent prompts.
func SetLanguage(lang internal.Language) { internal.SetLanguage(lang) }

const (
	LanguageEnglish = internal.LanguageEnglish
	LanguageChinese = internal.LanguageChinese
)
