// Package toolsearch provides dynamic tool search middleware.
// Instead of passing all tools to the model, agents can search for tools
// by keyword using a meta-tool, making it suitable for large tool libraries.
package toolsearch

import (
	"context"
	"strings"

	"github.com/infiniflow/ragflow/harness/agentcore"
)

type Config struct {
	AllTools        []agentcore.Tool
	MaxResults      int
	SearchThreshold int // Number of tools beyond which search is preferred
}

type middleware[M agentcore.MessageType] struct {
	agentcore.BaseMiddleware[M]
	cfg *Config
}

func New[M agentcore.MessageType](cfg *Config) agentcore.TypedChatModelMiddleware[M] {
	if cfg == nil { cfg = &Config{MaxResults: 5, SearchThreshold: 10} }
	if cfg.MaxResults <= 0 { cfg.MaxResults = 5 }
	if cfg.SearchThreshold <= 0 { cfg.SearchThreshold = 10 }
	return &middleware[M]{cfg: cfg}
}

func (m *middleware[M]) BeforeAgent(ctx context.Context, rc *agentcore.ChatModelAgentContext) (context.Context, *agentcore.ChatModelAgentContext, error) {
	if len(m.cfg.AllTools) <= m.cfg.SearchThreshold {
		// Small toolset: pass all tools directly
		rc.Tools = append(rc.Tools, m.cfg.AllTools...)
		return ctx, rc, nil
	}

	// Large toolset: add a search meta-tool + pass last threshold tools directly
	searchTool := agentcore.NewBaseTool("tool_search",
		"Search for available tools by keyword. Args: keyword(s) to search.",
		func(ctx context.Context, args string) (string, error) {
			keywords := strings.Fields(args)
			if len(keywords) == 0 { return "Please provide keywords to search.", nil }

			var results []string
			for _, t := range m.cfg.AllTools {
				name := strings.ToLower(t.Name())
				desc := strings.ToLower(t.Description())
				for _, kw := range keywords {
					if strings.Contains(name, strings.ToLower(kw)) || strings.Contains(desc, strings.ToLower(kw)) {
						results = append(results, t.Name()+": "+t.Description())
						break
					}
				}
				if len(results) >= m.cfg.MaxResults { break }
			}
			if len(results) == 0 { return "No tools found for: " + args, nil }
			return "Available tools:\n" + strings.Join(results, "\n"), nil
		})

	rc.Tools = append(rc.Tools, searchTool)
	// Also pass a few commonly-needed tools directly
	passDirect := m.cfg.SearchThreshold / 2
	if passDirect > len(m.cfg.AllTools) { passDirect = len(m.cfg.AllTools) }
	rc.Tools = append(rc.Tools, m.cfg.AllTools[:passDirect]...)

	return ctx, rc, nil
}

func (m *middleware[M]) BeforeModelRewrite(ctx context.Context, state *agentcore.TypedChatModelAgentState[M], mc *agentcore.TypedModelContext[M]) (context.Context, *agentcore.TypedChatModelAgentState[M], error) {
	return ctx, state, nil
}
