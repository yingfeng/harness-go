package engine

var Templates []Template

type Template struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Category    string      `json:"category"`
	Difficulty  string      `json:"difficulty"`
	NodeCount   int         `json:"nodeCount"`
	Workflow    WorkflowDef `json:"workflow"`
}

func init() {
	Templates = []Template{
		simpleAgent(),                    // 3 nodes
		simpleLoopTest(),                 // 6 nodes (transform only, tests while loop)
		multiCompanyAnalysis(),           // 6 nodes (4 sequential agents)
		branchingAnalysis(),              // 7 nodes (if-else branching)
		humanApproval(),                  // 4 nodes (human-in-the-loop)
		zillowPropertyFinder(),           // 9 nodes (while loop + agents)
		webScraper(),                     // 3 nodes (agent chain)
		stockReport(),                    // 3 nodes (agent chain)
		advancedCompetitiveAnalysis(),    // 12 nodes (while + if-else + agents)
	}
}

// simpleAgent: start → agent → end
func simpleAgent() Template {
	wf := WorkflowDef{
		ID:   "simple-agent",
		Name: "Simple Agent",
		Nodes: []WorkflowNode{
			{ID: "s", Type: "start", Position: map[string]float64{"x": 250, "y": 0}, Data: NodeData{Label: "Start", NodeName: "Start"}},
			{ID: "a1", Type: "agent", Position: map[string]float64{"x": 250, "y": 150}, Data: NodeData{Label: "Answer Question", NodeName: "Answer Question", SystemPrompt: "You are a helpful assistant. Answer the user's question concisely and accurately."}},
			{ID: "e", Type: "end", Position: map[string]float64{"x": 250, "y": 350}, Data: NodeData{Label: "End", NodeName: "End"}},
		},
		Edges: []WorkflowEdge{
			{ID: "e1", Source: "s", Target: "a1"},
			{ID: "e2", Source: "a1", Target: "e"},
		},
	}
	return Template{ID: "simple-agent", Name: "Simple Agent", Description: "A single agent node that answers a question. Tests basic agent execution.", Category: "Getting Started", Difficulty: "beginner", NodeCount: 3, Workflow: wf}
}

// simpleLoopTest: start → parse → while(loop) → process → loopback → prepare → end
// Uses while node with sourceHandle "break" to exit loop.
func simpleLoopTest() Template {
	wf := WorkflowDef{
		ID:   "simple-loop-test",
		Name: "Simple Loop Test (No LLM)",
		Nodes: []WorkflowNode{
			{ID: "s", Type: "start", Position: map[string]float64{"x": 250, "y": 0}, Data: NodeData{Label: "Start", NodeName: "Start"}},
			{ID: "parse", Type: "transform", Position: map[string]float64{"x": 250, "y": 120}, Data: NodeData{Label: "Parse Items", NodeName: "Parse Items", TransformCode: "Parsed items: Red, Blue, Green"}},
			{ID: "loop", Type: "while", Position: map[string]float64{"x": 250, "y": 240}, Data: NodeData{Label: "Loop Items", NodeName: "Loop Items"}},
			{ID: "process", Type: "transform", Position: map[string]float64{"x": 80, "y": 360}, Data: NodeData{Label: "Process Item", NodeName: "Process Item", TransformCode: "Processing item..."}},
			{ID: "prepare", Type: "transform", Position: map[string]float64{"x": 420, "y": 360}, Data: NodeData{Label: "Prepare Results", NodeName: "Prepare Results", TransformCode: "All items processed successfully."}},
			{ID: "e", Type: "end", Position: map[string]float64{"x": 250, "y": 500}, Data: NodeData{Label: "End", NodeName: "End"}},
		},
		Edges: []WorkflowEdge{
			{ID: "e1", Source: "s", Target: "parse"},
			{ID: "e2", Source: "parse", Target: "loop"},
			{ID: "e3", Source: "loop", Target: "process", SourceHandle: "continue"},
			{ID: "e4", Source: "process", Target: "loop"},
			{ID: "e5", Source: "loop", Target: "prepare", SourceHandle: "break"},
			{ID: "e6", Source: "prepare", Target: "e"},
		},
	}
	return Template{ID: "simple-loop-test", Name: "Simple Loop Test (No LLM)", Description: "Pure transform nodes with a while loop. Tests while-loop routing and iteration without LLM calls.", Category: "Testing", Difficulty: "simple", NodeCount: 6, Workflow: wf}
}

// multiCompanyAnalysis: start → 4 sequential agents → end
func multiCompanyAnalysis() Template {
	wf := WorkflowDef{
		ID:   "multi-company-analysis",
		Name: "Multi-Company Analysis",
		Nodes: []WorkflowNode{
			{ID: "s", Type: "start", Position: map[string]float64{"x": 250, "y": 0}, Data: NodeData{Label: "Start", NodeName: "Start"}},
			{ID: "a1", Type: "agent", Position: map[string]float64{"x": 250, "y": 130}, Data: NodeData{Label: "Research TSLA", NodeName: "Research TSLA", SystemPrompt: "You are a financial analyst. Research and summarize the latest known information about Tesla (TSLA) including stock trends, recent news, and business outlook. Keep it under 200 words."}},
			{ID: "a2", Type: "agent", Position: map[string]float64{"x": 250, "y": 260}, Data: NodeData{Label: "Research AAPL", NodeName: "Research AAPL", SystemPrompt: "You are a financial analyst. Research and summarize the latest known information about Apple (AAPL) including stock trends, recent news, and business outlook. Keep it under 200 words."}},
			{ID: "a3", Type: "agent", Position: map[string]float64{"x": 250, "y": 390}, Data: NodeData{Label: "Research MSFT", NodeName: "Research MSFT", SystemPrompt: "You are a financial analyst. Research and summarize the latest known information about Microsoft (MSFT) including stock trends, recent news, and business outlook. Keep it under 200 words."}},
			{ID: "report", Type: "agent", Position: map[string]float64{"x": 250, "y": 520}, Data: NodeData{Label: "Write Summary Report", NodeName: "Write Summary Report", SystemPrompt: "You are a financial report writer. Based on the research provided, write a concise comparative analysis report covering the three companies. Highlight key differences and similarities. Keep it under 300 words."}},
			{ID: "e", Type: "end", Position: map[string]float64{"x": 250, "y": 660}, Data: NodeData{Label: "End", NodeName: "End"}},
		},
		Edges: []WorkflowEdge{
			{ID: "e1", Source: "s", Target: "a1"},
			{ID: "e2", Source: "a1", Target: "a2"},
			{ID: "e3", Source: "a2", Target: "a3"},
			{ID: "e4", Source: "a3", Target: "report"},
			{ID: "e5", Source: "report", Target: "e"},
		},
	}
	return Template{ID: "multi-company-analysis", Name: "Multi-Company Analysis", Description: "4 sequential agents researching TSLA, AAPL, MSFT then writing a summary report. Tests agent chain with conversation history accumulation.", Category: "Finance", Difficulty: "intermediate", NodeCount: 6, Workflow: wf}
}

// branchingAnalysis: start → classify → if-else → {tech, creative} → merge → end
func branchingAnalysis() Template {
	wf := WorkflowDef{
		ID:   "branching-analysis",
		Name: "Advanced Analysis with Branching",
		Nodes: []WorkflowNode{
			{ID: "s", Type: "start", Position: map[string]float64{"x": 250, "y": 0}, Data: NodeData{Label: "Start", NodeName: "Start"}},
			{ID: "classify", Type: "agent", Position: map[string]float64{"x": 250, "y": 130}, Data: NodeData{Label: "Classify Input", NodeName: "Classify Input", SystemPrompt: "Analyze the conversation and determine if the topic is technical or creative. Respond with exactly one word: 'technical' or 'creative'."}},
			{ID: "branch", Type: "if-else", Position: map[string]float64{"x": 250, "y": 260}, Data: NodeData{Label: "Technical or Creative?", NodeName: "Technical or Creative?", Condition: "technical"}},
			{ID: "tech", Type: "agent", Position: map[string]float64{"x": 80, "y": 390}, Data: NodeData{Label: "Technical Analysis", NodeName: "Technical Analysis", SystemPrompt: "You are a technical expert. Provide a detailed technical analysis of the topic, covering architecture, implementation details, and best practices."}},
			{ID: "creative", Type: "agent", Position: map[string]float64{"x": 420, "y": 390}, Data: NodeData{Label: "Creative Analysis", NodeName: "Creative Analysis", SystemPrompt: "You are a creative director. Provide a creative analysis of the topic, covering design thinking, user experience, and innovative approaches."}},
			{ID: "merge", Type: "agent", Position: map[string]float64{"x": 250, "y": 530}, Data: NodeData{Label: "Final Synthesis", NodeName: "Final Synthesis", SystemPrompt: "You are an editor. Synthesize the analysis into a coherent, well-structured response that addresses the user's original question."}},
			{ID: "e", Type: "end", Position: map[string]float64{"x": 250, "y": 670}, Data: NodeData{Label: "End", NodeName: "End"}},
		},
		Edges: []WorkflowEdge{
			{ID: "e1", Source: "s", Target: "classify"},
			{ID: "e2", Source: "classify", Target: "branch"},
			{ID: "e3", Source: "branch", Target: "tech", SourceHandle: "true"},
			{ID: "e4", Source: "branch", Target: "creative", SourceHandle: "false"},
			{ID: "e5", Source: "tech", Target: "merge"},
			{ID: "e6", Source: "creative", Target: "merge"},
			{ID: "e7", Source: "merge", Target: "e"},
		},
	}
	return Template{ID: "branching-analysis", Name: "Advanced Analysis with Branching", Description: "Classifies input as technical/creative, routes to different agents, then merges results. Tests if-else routing and parallel branch execution.", Category: "Testing", Difficulty: "advanced", NodeCount: 7, Workflow: wf}
}

// humanApproval: start → analyze → user-approval → execute → end
func humanApproval() Template {
	wf := WorkflowDef{
		ID:   "human-approval",
		Name: "Human-in-the-Loop Approval",
		Nodes: []WorkflowNode{
			{ID: "s", Type: "start", Position: map[string]float64{"x": 250, "y": 0}, Data: NodeData{Label: "Start", NodeName: "Start"}},
			{ID: "analyze", Type: "transform", Position: map[string]float64{"x": 250, "y": 130}, Data: NodeData{Label: "Analyze Task", NodeName: "Analyze Task", TransformCode: "Task: Send 100 emails to customers. Estimated effort: 2 hours."}},
			{ID: "approve", Type: "user-approval", Position: map[string]float64{"x": 250, "y": 260}, Data: NodeData{Label: "Request Approval", NodeName: "Request Approval"}},
			{ID: "execute", Type: "transform", Position: map[string]float64{"x": 250, "y": 390}, Data: NodeData{Label: "Execute Task", NodeName: "Execute Task", TransformCode: "Task approved and executed successfully."}},
			{ID: "e", Type: "end", Position: map[string]float64{"x": 250, "y": 530}, Data: NodeData{Label: "End", NodeName: "End"}},
		},
		Edges: []WorkflowEdge{
			{ID: "e1", Source: "s", Target: "analyze"},
			{ID: "e2", Source: "analyze", Target: "approve"},
			{ID: "e3", Source: "approve", Target: "execute"},
			{ID: "e4", Source: "execute", Target: "e"},
		},
	}
	return Template{ID: "human-approval", Name: "Human-in-the-Loop Approval", Description: "Demonstrates human approval workflow. A task is analyzed, then paused for approval before execution.", Category: "Demo", Difficulty: "simple", NodeCount: 5, Workflow: wf}
}

// zillowPropertyFinder: start → search → parse → while(loop) → analyze → loopback → compare → report → end
func zillowPropertyFinder() Template {
	wf := WorkflowDef{
		ID:   "zillow-property-finder",
		Name: "Property Finder",
		Nodes: []WorkflowNode{
			{ID: "s", Type: "start", Position: map[string]float64{"x": 250, "y": 0}, Data: NodeData{Label: "Start", NodeName: "Start"}},
			{ID: "search", Type: "agent", Position: map[string]float64{"x": 250, "y": 120}, Data: NodeData{Label: "Search Properties", NodeName: "Search Properties", SystemPrompt: "You are a real estate analyst. Search and list available properties in Austin, TX with max price $500,000 and minimum 3 beds. List up to 5 properties with address, price, beds, baths, and sqft."}},
			{ID: "parse", Type: "transform", Position: map[string]float64{"x": 250, "y": 240}, Data: NodeData{Label: "Parse Properties", NodeName: "Parse Properties", TransformCode: "Parsed {{outputs.search}}"}},
			{ID: "loop", Type: "while", Position: map[string]float64{"x": 250, "y": 360}, Data: NodeData{Label: "Loop Properties", NodeName: "Loop Properties"}},
			{ID: "analyze", Type: "agent", Position: map[string]float64{"x": 80, "y": 480}, Data: NodeData{Label: "Analyze Property", NodeName: "Analyze Property", SystemPrompt: "You are a property analyst. Analyze this property's value, neighborhood quality, and investment potential. Provide a brief assessment."}},
			{ID: "collect", Type: "transform", Position: map[string]float64{"x": 80, "y": 600}, Data: NodeData{Label: "Collect Result", NodeName: "Collect Result", TransformCode: "Collected property analysis."}},
			{ID: "compare", Type: "transform", Position: map[string]float64{"x": 420, "y": 480}, Data: NodeData{Label: "Prepare Comparison", NodeName: "Prepare Comparison", TransformCode: "All properties analyzed. Preparing comparison table."}},
			{ID: "report", Type: "agent", Position: map[string]float64{"x": 420, "y": 600}, Data: NodeData{Label: "Generate Report", NodeName: "Generate Report", SystemPrompt: "You are a real estate report writer. Generate a comprehensive comparison report of all analyzed properties, highlighting the best value option."}},
			{ID: "e", Type: "end", Position: map[string]float64{"x": 250, "y": 740}, Data: NodeData{Label: "End", NodeName: "End"}},
		},
		Edges: []WorkflowEdge{
			{ID: "e1", Source: "s", Target: "search"},
			{ID: "e2", Source: "search", Target: "parse"},
			{ID: "e3", Source: "parse", Target: "loop"},
			{ID: "e4", Source: "loop", Target: "analyze", SourceHandle: "continue"},
			{ID: "e5", Source: "analyze", Target: "collect"},
			{ID: "e6", Source: "collect", Target: "loop"},
			{ID: "e7", Source: "loop", Target: "compare", SourceHandle: "break"},
			{ID: "e8", Source: "compare", Target: "report"},
			{ID: "e9", Source: "report", Target: "e"},
		},
	}
	return Template{ID: "zillow-property-finder", Name: "Property Finder", Description: "Real estate workflow with while-loop. Searches properties, loops to analyze each one, then generates a comparison report. Tests while loop + agents.", Category: "Real Estate", Difficulty: "intermediate", NodeCount: 9, Workflow: wf}
}

// webScraper: start → scrape agent → analyze agent → end
func webScraper() Template {
	wf := WorkflowDef{
		ID:   "web-scraper",
		Name: "Web Scraper",
		Nodes: []WorkflowNode{
			{ID: "s", Type: "start", Position: map[string]float64{"x": 250, "y": 0}, Data: NodeData{Label: "Start", NodeName: "Start"}},
			{ID: "scrape", Type: "agent", Position: map[string]float64{"x": 250, "y": 130}, Data: NodeData{Label: "Scrape Website", NodeName: "Scrape Website", SystemPrompt: "You are a web research assistant. Describe the typical content and structure of a modern SaaS landing page. Include common sections like hero, features, pricing, and testimonials."}},
			{ID: "analyze", Type: "agent", Position: map[string]float64{"x": 250, "y": 260}, Data: NodeData{Label: "Analyze Content", NodeName: "Analyze Content", SystemPrompt: "You are a content analyst. Analyze the scraped website content and provide insights about the business model, target audience, and key value propositions."}},
			{ID: "e", Type: "end", Position: map[string]float64{"x": 250, "y": 400}, Data: NodeData{Label: "End", NodeName: "End"}},
		},
		Edges: []WorkflowEdge{
			{ID: "e1", Source: "s", Target: "scrape"},
			{ID: "e2", Source: "scrape", Target: "analyze"},
			{ID: "e3", Source: "analyze", Target: "e"},
		},
	}
	return Template{ID: "web-scraper", Name: "Web Scraper", Description: "Two sequential agent nodes: one scrapes a website, the other analyzes the content. Tests 2-step agent chain.", Category: "Utilities", Difficulty: "beginner", NodeCount: 4, Workflow: wf}
}

// stockReport: start → research agent → report agent → end
func stockReport() Template {
	wf := WorkflowDef{
		ID:   "stock-report",
		Name: "Stock Report",
		Nodes: []WorkflowNode{
			{ID: "s", Type: "start", Position: map[string]float64{"x": 250, "y": 0}, Data: NodeData{Label: "Start", NodeName: "Start"}},
			{ID: "research", Type: "agent", Position: map[string]float64{"x": 250, "y": 130}, Data: NodeData{Label: "Research Stock", NodeName: "Research Stock", SystemPrompt: "You are a financial analyst. Research and provide the latest known information about NVDA (NVIDIA) stock including recent performance, news, and analyst ratings."}},
			{ID: "report", Type: "agent", Position: map[string]float64{"x": 250, "y": 260}, Data: NodeData{Label: "Write Report", NodeName: "Write Report", SystemPrompt: "You are a financial writer. Based on the research provided, write a professional stock report with sections: Executive Summary, Financial Analysis, Risk Assessment, and Recommendation."}},
			{ID: "e", Type: "end", Position: map[string]float64{"x": 250, "y": 400}, Data: NodeData{Label: "End", NodeName: "End"}},
		},
		Edges: []WorkflowEdge{
			{ID: "e1", Source: "s", Target: "research"},
			{ID: "e2", Source: "research", Target: "report"},
			{ID: "e3", Source: "report", Target: "e"},
		},
	}
	return Template{ID: "stock-report", Name: "Stock Report", Description: "Two sequential agent nodes: researches a stock ticker then writes a professional report. Tests agent chain with different roles.", Category: "Finance", Difficulty: "beginner", NodeCount: 4, Workflow: wf}
}

// advancedCompetitiveAnalysis: start → parse → while(loop) → research → quality-check → if-else → {extract/insufficient} → merge → loopback → report → approve → end
func advancedCompetitiveAnalysis() Template {
	wf := WorkflowDef{
		ID:   "advanced-competitive-analysis",
		Name: "Advanced Competitive Analysis",
		Nodes: []WorkflowNode{
			{ID: "s", Type: "start", Position: map[string]float64{"x": 250, "y": 0}, Data: NodeData{Label: "Start", NodeName: "Start"}},
			{ID: "parse", Type: "transform", Position: map[string]float64{"x": 250, "y": 100}, Data: NodeData{Label: "Parse Companies", NodeName: "Parse Companies", TransformCode: "Analyzing companies: OpenAI, Anthropic, Google DeepMind"}},
			{ID: "loop", Type: "while", Position: map[string]float64{"x": 250, "y": 200}, Data: NodeData{Label: "For Each Company", NodeName: "For Each Company"}},
			{ID: "research", Type: "agent", Position: map[string]float64{"x": 250, "y": 300}, Data: NodeData{Label: "Research Company", NodeName: "Research Company", SystemPrompt: "You are a market research analyst. Research the current company and provide: key products, market position, recent developments, and competitive advantages. Be specific and data-driven."}},
			{ID: "quality", Type: "agent", Position: map[string]float64{"x": 250, "y": 400}, Data: NodeData{Label: "Quality Check", NodeName: "Quality Check", SystemPrompt: "You are a quality assurance analyst. Check if the research provided has sufficient detail. If it has specific data points and is well-structured, respond with 'PASS'. If it's too vague, respond with 'INSUFFICIENT'."}},
			{ID: "branch", Type: "if-else", Position: map[string]float64{"x": 250, "y": 500}, Data: NodeData{Label: "Quality Check Result", NodeName: "Quality Check Result", Condition: "PASS"}},
			{ID: "extract", Type: "agent", Position: map[string]float64{"x": 80, "y": 600}, Data: NodeData{Label: "Extract Structured Data", NodeName: "Extract Structured Data", SystemPrompt: "Extract structured data from the research in JSON format with fields: company_name, products, market_position, strengths, weaknesses."}},
			{ID: "insufficient", Type: "transform", Position: map[string]float64{"x": 420, "y": 600}, Data: NodeData{Label: "Mark Insufficient", NodeName: "Mark Insufficient", TransformCode: "Research marked as insufficient. Skipping this company."}},
			{ID: "merge", Type: "transform", Position: map[string]float64{"x": 250, "y": 700}, Data: NodeData{Label: "Merge Results", NodeName: "Merge Results", TransformCode: "Merging company data into analysis."}},
			{ID: "report", Type: "agent", Position: map[string]float64{"x": 250, "y": 800}, Data: NodeData{Label: "Generate Final Report", NodeName: "Generate Final Report", SystemPrompt: "You are a competitive analysis expert. Based on all the company research collected, generate a comprehensive competitive analysis report. Include: market overview, company-by-company analysis, competitive landscape, and strategic recommendations."}},
			{ID: "approve", Type: "user-approval", Position: map[string]float64{"x": 250, "y": 900}, Data: NodeData{Label: "Review Report", NodeName: "Review Report"}},
			{ID: "e", Type: "end", Position: map[string]float64{"x": 250, "y": 1020}, Data: NodeData{Label: "End", NodeName: "End"}},
		},
		Edges: []WorkflowEdge{
			{ID: "e1", Source: "s", Target: "parse"},
			{ID: "e2", Source: "parse", Target: "loop"},
			{ID: "e3", Source: "loop", Target: "research", SourceHandle: "continue"},
			{ID: "e4", Source: "research", Target: "quality"},
			{ID: "e5", Source: "quality", Target: "branch"},
			{ID: "e6", Source: "branch", Target: "extract", SourceHandle: "true"},
			{ID: "e7", Source: "branch", Target: "insufficient", SourceHandle: "false"},
			{ID: "e8", Source: "extract", Target: "merge"},
			{ID: "e9", Source: "insufficient", Target: "merge"},
			{ID: "e10", Source: "merge", Target: "loop"},
			{ID: "e11", Source: "loop", Target: "report", SourceHandle: "break"},
			{ID: "e12", Source: "report", Target: "approve"},
			{ID: "e13", Source: "approve", Target: "e"},
		},
	}
	return Template{ID: "advanced-competitive-analysis", Name: "Advanced Competitive Analysis", Description: "Complete workflow with while loop, if-else branching, quality checks, and human approval. Tests all engine features together.", Category: "Testing", Difficulty: "advanced", NodeCount: 13, Workflow: wf}
}
