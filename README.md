# Agent Harness Go

[![Go Reference](https://pkg.go.dev/badge/github.com/infiniflow/ragflow/harness.svg)](https://pkg.go.dev/github.com/infiniflow/ragflow/harness)
[![Go Report Card](https://goreportcard.com/badge/github.com/infiniflow/ragflow/harness)](https://goreportcard.com/report/github.com/infiniflow/ragflow/harness)

Agent Harness is a Go framework for building stateful, multi-agent applications with LLMs. It provides a graph-based execution model that supports cycles, branching, persistence, and human-in-the-loop workflows.

## Features

- **Agent Harness**: A complete agent development kit with ChatModelAgent, middleware system, and tool execution
- **Stateful Computation**: Nodes communicate by reading and writing to shared state channels
- **Cyclic Graphs**: Support for loops and recursion with configurable limits
- **Checkpointing**: Persistent state storage with in-memory, SQLite, and PostgreSQL backends
- **Human-in-the-Loop**: Interrupt flows for human approval or input
- **Streaming**: Real-time output streaming during graph execution
- **Type-Safe**: Strong typing with Go generics support
- **OpenTelemetry Integration**: Distributed tracing and metrics for observability
- **Middleware Ecosystem**: Summarization, reduction, filesystem, skill, and more

## Installation

```bash
go get github.com/infiniflow/ragflow/harness
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/infiniflow/ragflow/harness"
)

// Define your state
type State struct {
    Messages []string
    Counter  int
}

func main() {
    ctx := context.Background()

    // Create a graph builder
    builder := harness.NewStateGraph(State{})

    // Add nodes
    builder.AddNode("agent", func(ctx context.Context, state interface{}) (interface{}, error) {
        s := state.(State)
        s.Messages = append(s.Messages, "Hello from agent")
        s.Counter++
        return s, nil
    })

    // Add edges
    builder.AddEdge(harness.Start, "agent")
    builder.AddEdge("agent", harness.End)

    // Compile the graph
    graph, err := builder.Compile()
    if err != nil {
        log.Fatal(err)
    }

    // Run the graph
    result, err := graph.Invoke(ctx, State{
        Messages: []string{"Starting..."},
        Counter:  0,
    })
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Result: %+v\n", result)
}
```

## Core Concepts

### StateGraph

The `StateGraph` is the main class for building graphs. Nodes in the graph communicate by reading and writing to a shared state object.

```go
builder := harness.NewStateGraph(State{})
```

### Nodes

Nodes are functions that read the current state and return updates to the state.

```go
builder.AddNode("my_node", func(ctx context.Context, state interface{}) (interface{}, error) {
    s := state.(MyState)
    // Process state...
    return s, nil
})
```

### Edges

Edges define the flow between nodes.

```go
// Simple edge
builder.AddEdge("node_a", "node_b")

// Conditional edges
builder.AddConditionalEdges("decision", conditionFunc, map[string]string{
    "yes": "node_a",
    "no":  "node_b",
})
```

### Channels

Channels define how state is stored and updated. Different channel types support different update semantics:

- **LastValue**: Stores only the most recent value
- **Topic**: Accumulates values in a list
- **BinaryOperatorAggregate**: Reduces values using a binary operator
- **EphemeralValue**: Clears after being read once

```go
builder.AddChannel("messages", harness.NewTopic(string, true))
```

### Checkpoints

Checkpoints enable persistence and resumption of graph execution.

```go
// In-memory checkpointer
saver := harness.NewMemorySaver()

// SQLite checkpointer
saver, err := harness.NewSqliteSaver("checkpoints.db")

// Compile with checkpointer
graph, err := builder.Compile(harness.WithCheckpointer(saver))
```

## Examples

### Conditional Routing

```go
builder.AddConditionalEdges("router", func(ctx context.Context, state interface{}) (interface{}, error) {
    s := state.(MyState)
    if s.Value > threshold {
        return "high", nil
    }
    return "low", nil
}, map[string]string{
    "high": "high_value_node",
    "low":  "low_value_node",
})
```

### Retry Policy

```go
retryPolicy := harness.RetryPolicy{
    MaxAttempts:     3,
    InitialInterval: 500 * time.Millisecond,
    BackoffFactor:   2.0,
}

builder.AddNodeWithOptions("risky_node", nodeFunc, harness.NodeOptions{
    RetryPolicy: &retryPolicy,
})
```

### Interrupts (Human-in-the-Loop)

```go
// Enable interrupts for specific nodes
graph, err := builder.Compile(harness.WithInterrupts("human_review"))

// In your node, use interrupt
func humanReviewNode(ctx context.Context, state interface{}) (interface{}, error) {
    // This will pause execution and return to the client
    result, err := harness.InterruptFunc("Please review and approve")
    if err != nil {
        return nil, err
    }
    // Resume with the result
    return processResult(result), nil
}

// Resume with a command
result, err := graph.Invoke(ctx, harness.NewCommand().WithResume(approval), config)
```

## Agent Harness (agentcore)

The `agentcore` package provides a complete Agent Development Kit (ADK):

### ChatModelAgent (ReAct Loop)

```go
import "github.com/infiniflow/ragflow/harness/agentcore"

model := // your LLM model implementing agentcore.ChatModel[*schema.Message]
agent := agentcore.NewChatModelAgent[*schema.Message](&agentcore.ChatModelConfig[*schema.Message]{
    Model:     model,
    Tools:     []agentcore.Tool{myTool},
    MaxIterations: 10,
}).WithName("my_agent")

runner := agentcore.NewTypedRunner(agentcore.RunnerConfig[*schema.Message]{
    Agent: agent,
})

iter := runner.Query(ctx, "Hello!")
for { ev, ok := iter.Next(); if !ok { break }; /* handle ev */ }
```

### Middleware System

```go
agent := agentcore.NewChatModelAgent[*schema.Message](&agentcore.ChatModelConfig[*schema.Message]{
    Model: model,
    Middlewares: []agentcore.ChatModelMiddleware{
        summarization.New(model, &summarization.Config{MaxTokens: 4096}),
        reduction.New(&reduction.Config{MaxToolOutputLen: 2000}),
        myCustomMiddleware,
    },
})
```

### Workflow Agents

```go
// Sequential workflow
wf, _ := agentcore.NewSequential(ctx, &agentcore.SequentialConfig{
    Name: "pipeline", SubAgents: []agentcore.Agent{agentA, agentB},
})

// Parallel workflow
wf, _ := agentcore.NewParallel(ctx, &agentcore.ParallelConfig{
    Name: "collectors", SubAgents: []agentcore.Agent{agentC, agentD, agentE},
})

// Loop workflow
wf, _ := agentcore.NewLoop(ctx, &agentcore.LoopConfig{
    Name: "reflection", SubAgents: []agentcore.Agent{mainAgent, critiqueAgent},
    MaxIterations: 5,
})
```

## Project Structure

```
harness/
├── agentcore/     # Agent Development Kit (ChatModelAgent, middleware, workflow)
├── channels/      # Channel implementations for state management
├── checkpoint/    # Checkpoint savers (memory, sqlite, postgres)
├── constants/     # Constants and reserved keys
├── errors/        # Error types
├── examples/      # Example applications
├── graph/         # Graph building and execution
├── interrupt/     # Human-in-the-loop functionality
├── telemetry/     # OpenTelemetry observability
├── types/         # Core type definitions
└── utils/         # Utility functions
```

## Architecture

This project follows the [Pregel](https://research.google.com/pubs/pub37252.html) execution model:

1. **Build Phase**: Define nodes, edges, and state channels
2. **Compile Phase**: Validate and prepare the graph for execution
3. **Execution Phase**: Run the Pregel loop:
   - Determine which nodes are ready to execute
   - Execute nodes in parallel (when possible)
   - Apply writes to channels
   - Check for interrupts
   - Repeat until complete or recursion limit reached

## Observability with OpenTelemetry

This project provides comprehensive OpenTelemetry support for distributed tracing and metrics collection.

### Basic Setup

```go
import "github.com/infiniflow/ragflow/harness/telemetry"

// Initialize for development
shutdown, err := telemetry.InitForDevelopment("my-app")
if err != nil {
    log.Fatal(err)
}
defer shutdown(context.Background())
```

For more details, see the [telemetry README](telemetry/README.md) and [examples](examples/telemetry/).

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
