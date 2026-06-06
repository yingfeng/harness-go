package summarization

const (
	DefaultSummaryPrompt = `Summarize the following conversation, focusing on:
1. Key decisions made
2. Action items and their status
3. Important context that future messages need
4. Unresolved questions

Keep the summary concise but informative.

Conversation:`

	ChineseSummaryPrompt = `总结以下对话,重点关注:
1. 作出的关键决定
2. 行动项及其状态
3. 后续消息需要的重要上下文
4. 未解决的问题

保持摘要简洁但信息丰富。

对话:`
)
