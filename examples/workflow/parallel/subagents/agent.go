// Package subagents provides data collection agents for the parallel workflow.
package subagents

import (
	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
	"github.com/infiniflow/ragflow/harness/examples/workflow"
)

// NewStockDataCollectionAgent returns a stock market data collector agent.
func NewStockDataCollectionAgent() agentcore.Agent {
	return agentcore.NewReActAgent[*schema.Message](&agentcore.ReActConfig[*schema.Message]{
		Model: workflow.MockModel("StockDataCollectionAgent"),
		Instruction: `You are a Stock Data Collection Agent. Your role is to:

- Collect accurate and up-to-date stock market data from trusted sources.
- Retrieve information such as stock prices, trading volumes, historical trends, and relevant financial indicators.
- Ensure data completeness and reliability.
- Format the collected data clearly for further analysis or user queries.
- Handle requests efficiently and verify the accuracy of the data before presenting it.
- Maintain professionalism and clarity in communication.`,
	}).WithName("StockDataCollectionAgent").WithDescription("Gathers real-time and historical stock market data.")
}

// NewNewsDataCollectionAgent returns a news data collector agent.
func NewNewsDataCollectionAgent() agentcore.Agent {
	return agentcore.NewReActAgent[*schema.Message](&agentcore.ReActConfig[*schema.Message]{
		Model: workflow.MockModel("NewsDataCollectionAgent"),
		Instruction: `You are a News Data Collection Agent. Your responsibilities include:

- Aggregating news articles and updates from diverse and credible news sources.
- Filtering and organizing news based on relevance, timeliness, and user interests.
- Providing summaries or full content as required.
- Ensuring the accuracy and authenticity of the collected news data.
- Presenting information in a clear, concise, and unbiased manner.
- Responding promptly to user requests for specific news topics or updates.`,
	}).WithName("NewsDataCollectionAgent").WithDescription("Aggregates news articles from multiple reputable outlets.")
}

// NewSocialMediaInfoCollectionAgent returns a social media data collector agent.
func NewSocialMediaInfoCollectionAgent() agentcore.Agent {
	return agentcore.NewReActAgent[*schema.Message](&agentcore.ReActConfig[*schema.Message]{
		Model: workflow.MockModel("SocialMediaInformationCollectionAgent"),
		Instruction: `You are a Social Media Information Collection Agent. Your tasks are to:

- Collect relevant and up-to-date information from multiple social media platforms.
- Monitor trends, user sentiments, and public discussions related to specified topics.
- Ensure the data collected respects privacy and platform policies.
- Organize and summarize the information to highlight key insights.
- Provide clear and objective reports based on the social media data.
- Communicate findings in a user-friendly and professional manner.`,
	}).WithName("SocialMediaInformationCollectionAgent").WithDescription("Gathers data from social media platforms.")
}
