// Package toolsearch provides dynamic tool search middleware.
// It enables the model to search and select tools at runtime rather than
// using a fixed set of tools.
package toolsearch

import (
	"context"
	"github.com/infiniflow/ragflow/harness/agentcore"
	"github.com/infiniflow/ragflow/harness/agentcore/schema"
)

type Searcher interface {
	Search(query string) ([]ToolEntry, error)
}

type ToolEntry struct {
	Name        string
	Description string
	Tool        agentcore.Tool
}

type Config struct{ Searcher Searcher }

type middleware[M agentcore.MessageType] struct {
	agentcore.BaseMiddleware[M]
	searcher Searcher
}

func New[M agentcore.MessageType](searcher Searcher) agentcore.TypedChatModelMiddleware[M] {
	return &middleware[M]{searcher: searcher}
}

func (m *middleware[M]) BeforeAgent(ctx context.Context, rc *agentcore.ChatModelAgentContext) (context.Context, *agentcore.ChatModelAgentContext, error) {
	if m.searcher != nil {
		rc.ToolSearchTool = &schema.ToolInfo{Name: "search_tools", Description: "Search for available tools"}
	}
	return ctx, rc, nil
}
