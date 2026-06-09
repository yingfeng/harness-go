# AgentCore Architecture

## Overview

AgentCore is a production-grade Agent Development Kit (ADK) layer built on top of a StateGraph/Pregel
execution engine. It provides high-level Agent abstractions (ReActAgent, Runner, Middleware, Tools)
for building complex LLM-powered agents.

The code lives under `agentcore/` and is imported as `github.com/infiniflow/ragflow/harness/agentcore`.
Public types are re-exported from the top-level `harness` package in `harness.go`.

---

## Core Architecture

### Layer 1: Agent Interface

The base interface `Agent` (type alias for `TypedAgent[*schema.Message]`, defined in `interface.go`)
is the contract all agents implement:

- `Name(ctx)` / `Description(ctx)` — agent metadata
- `Run(ctx, input, opts...)` — returns `AsyncIterator[*AgentEvent]` for event streaming
- `Resume(ctx, info, opts...)` — for agents implementing `ResumableAgent`

**Key types:**

| Type | Description |
|---|---|
| `TypedAgent[M]` | Generic agent interface (M = `*schema.Message` or `*schema.AgenticMessage`) |
| `Agent` | Type alias for `TypedAgent[*schema.Message]` |
| `ResumableAgent` | Extends Agent with Resume for interrupt handling |
| `AgentInput` | Input with Messages + EnableStreaming flag |
| `AgentEvent` | Event with Output (Message or MessageStream), Action, AgentName, RunPath |
| `AgentAction` | Action emitted by agent: Exit, Interrupted, TransferToAgent, BreakLoop |

### Layer 2: Execution Engine

**ReActAgent** (defined in `react_agent.go`) implements the ReAct (Reasoning + Acting) pattern:

1. `freeze()` — after first Run/Resume, configuration is frozen via atomic flag to prevent mutation during execution.

2. `buildRunFunc` — lazy-init that detects whether tools are configured and selects the appropriate runner:
   - `buildReActRunFunc` — full ReAct loop with tool execution
   - `buildNoToolsRunFunc` — single model call without tool dispatch
   - `buildGraphReActRunFunc` — graph-based ReAct using the project's own StateGraph/Pregel engine

3. **ReAct loop flow** (buildReActRunFunc):
   ```
   BeforeAgent (middleware)
     └─> Loop (RemainingIterations > 0):
           ├─ BeforeModelRewrite (middleware)
           ├─ StateModifier (optional)
           ├─ GenModelInput (build input messages)
           ├─ model.Generate (with wrapper chain)
           ├─ AfterModelRewrite (middleware)
           ├─ extractToolCalls
           ├─ if tool calls:
           │    └─ ToolsNode.Execute (or executeInlineTools)
           └─ if no tool calls: break
   AfterAgent (middleware)
   ```

4. **Model Wrapper Chain** (built in `BuildModelWrapperChain` in `model_chain.go`):
   ```
   base model
     → EventSender (emits model output events)
     → Retry (with backoff and ShouldRetry callback)
     → Failover (backup models on failure)
     → User Middleware.WrapModel (custom wrappers)
     → State Wrapper (deep copy + ID injection + cancel check)
     → Callback Injection (tracing/monitoring)
   ```

5. **ToolsNode** (defined in `tools_node.go`):
   - Dispatches tool calls extracted from model response
   - Supports both standard `Tool` (string I/O) and `EnhancedTool` (structured `*schema.ToolResult`)
   - Applies `ToolCallMiddleware` chain for each tool call
   - Handles `ReturnDirectly` — tools that cause immediate agent exit
   - Parses tool call JSON arguments

### Layer 3: Orchestration

**flowAgent** (defined in `flow.go`):
- Wraps any `Agent` with sub-agent management, history rewriting, transfer routing
- `SetSubAgents` — registers child agents with parent tracking
- `genInput` — rebuilds agent input from accumulated session events with history rewriting
- `runLoop` — processes agent events, handles transfer-to-sub-agent routing
- Supports both `*schema.Message` (flowAgent) and `*schema.AgenticMessage` (typedFlowAgent) paths

**workflowAgent** (defined in `workflow.go`):
- Orchestrates multiple agents in predefined patterns:
  - **Sequential** — runs sub-agents one after another
  - **Parallel** — runs sub-agents concurrently with `sync.WaitGroup`; each branch gets its
    own `BranchEvents` for event isolation; on join, events are merged chronologically
  - **Loop** — repeats a sequence of sub-agents up to MaxIterations
- All patterns support interrupt/resume with state serialization via gob
- Constructors: `NewSequential`, `NewParallel`, `NewLoop`

**Runner** (defined in `runner.go`):
- Primary entry point for agent execution
- `Run(ctx, msgs, opts...)` → event stream
- `Query(ctx, query, opts...)` — convenience for single-query runs
- `Resume(ctx, cid, opts...)` / `ResumeWithParams` — checkpoint-based resume
- `handleIter` — goroutine that drains events, saves checkpoints on interrupt, wraps cancel
- Routes through `flowAgent` (Message) or `typedFlowAgent` (AgenticMessage) based on type

### Layer 4: Middleware System

Defined in `contracts.go`, middlewares implement `ReActMiddleware` interface with 9 hook points:

| Hook | Signature | Purpose |
|---|---|---|
| BeforeAgent | `(ctx, *ReActAgentContext)` | Modify instruction, tools, return-directly map |
| AfterAgent | `(ctx, state)` | Post-execution cleanup |
| BeforeModelRewrite | `(ctx, state, *ModelContext)` | Transform state before model call |
| AfterModelRewrite | `(ctx, state, *ModelContext)` | Transform state after model call |
| WrapModel | `(ctx, ChatModel[M], *ModelContext)` → ChatModel[M] | Wrap model call |
| WrapToolInvoke | `(ctx, InvokableToolEndpoint, *ToolContext)` | Wrap sync tool invoke |
| WrapToolStream | `(ctx, StreamableToolEndpoint, *ToolContext)` | Wrap streaming tool invoke |
| WrapEnhancedInvokableToolCall | `(ctx, EnhancedInvokableToolEndpoint, *ToolContext)` | Wrap enhanced sync tool |
| WrapEnhancedStreamableToolCall | `(ctx, EnhancedStreamableToolEndpoint, *ToolContext)` | Wrap enhanced streaming tool |

**BaseMiddleware** provides default no-op implementations for all hooks. Embed it and override only needed methods.

**Prebuilt middlewares** (in `agentcore/middlewares/`):
- `agentsmd` — inject Agents.md file contents into model input
- `filesystem` — provide filesystem tools (ls, read, write, edit, grep, execute)
- `patchtoolcalls` — fix dangling tool calls in message history
- `plantask` — task management tools (CRUD) for coding sessions
- `reduction` — offload large tool results to backend when token limits are exceeded
- `skill` — load and execute skills from SKILL.md files
- `summarization` — auto-summarize conversation history on token overflow
- `telemetry` — OpenTelemetry tracing/monitoring

### Layer 5: AgentLoop (Push-based Execution)

Defined in `agent_loop.go`, AgentLoop enables push-based agent interaction where external events
can be injected while the agent is running — useful for chat/streaming applications.

**Lifecycle:**
```
idle ──beginPlanningTurn──▶ planning ──beginActiveTurn──▶ active ──endActiveTurn──▶ idle
                             │                                                      ▲
                             └────────abortPlanningTurn─────────────────────────────┘
```

**Key components:**
- `preemptController` — turn-targeted preempt with snapshot/ack mechanism
- `stopController` — global terminal stop with optional active-turn cancel
- `bridgeStore` — bridges AgentLoop checkpoints with Runner checkpoints
- `TurnContext[T]` — provides per-turn Preempted/Stopped channels, StopCause
- `GenInput` callback — decides which buffered items to consume
- `GenResume` callback — plans resume from checkpoint state
- `PrepareAgent` callback — creates agent for each turn's consumed items
- `OnAgentEvents` callback — handles agent events, with automatic CancelError handling

**Push options:**
- `WithPreempt` — cancel current turn at safe point
- `WithPreemptTimeout` — preempt with timeout and recursive cancel
- `WithPreemptDelay` — delayed preempt resolution

**Stop options:**
- `WithGraceful` — wait for safe point (after tool calls / after model call)
- `WithImmediate` — abort as soon as possible
- `WithGracefulTimeout` — graceful with timeout escalation to immediate
- `UntilIdleFor` — defer stop until loop is continuously idle
- `WithSkipCheckpoint` — skip checkpoint save
- `WithStopCause` — attach business reason string

---

## Cancellation System

Defined in `cancel.go`, the cancel system supports 3 modes:

| Mode | Behavior |
|---|---|
| `CancelImmediate` | Stop immediately (closes immediateChan) |
| `CancelAfterChatModel` | Stop after current model call completes |
| `CancelAfterToolCalls` | Stop after current tool calls complete |

**State machine:** `cancelContext` transitions:
```
Running → Cancelling → Done / Handled
```

**Key features:**
- Children derive from parents with configurable recursive propagation
- `deriveAgentToolCancelContext` — creates child cancel context for nested agent tools
- `timeoutEscalation` — timeout triggers escalation from CancelAfterChatModel/ToolCalls to CancelImmediate
- `cancelMonitoredToolHandler` — checks cancel state before tool dispatch
- `cancelMonitoredModel` — wraps streaming model output with cancel detection
- `wrapStreamWithCancel` — goroutine that bridges StreamReader to cancel channel
- `InterruptFromGraph` — coordinates graph-level interrupts with the cancel state machine

**Error types:**
- `CancelError` — contains AgentCancelInfo (Mode, Escalated, Timeout), InterruptContexts
- `StreamCanceledError` — returned when stream is canceled mid-flight
- `ErrCancelTimeout` — timeout expired during graceful cancel
- `ErrExecutionEnded` — cancel attempted after execution already finished

---

## Checkpoint / Resume

Checkpoints are serialized via gob encoding with type registration (`schema.RegisterType`).

**Checkpoint payload includes:**
- Run context (run path, session values)
- Interrupt info (state data, interrupt signal)
- Agent state (`*ReActAgentState[M]`)

**Resume flow:**
1. `Runner.Resume` → loads checkpoint from store via `loadCheckpoint`
2. Reconstructs run context from checkpoint data
3. Calls `ResumableAgent.Resume` with `ResumeInfo`
4. `ReActAgent.Resume` restores state from `InterruptState` and re-enters run function
5. `ReActAgentResumeData.HistoryModifier` allows input modification on resume

**Gob encodability check:**
- `checkGobEncodability` in `callback.go` proactively validates values at `SetRunLocalValue` time
- Catches unregistered types early with actionable error message including `RegisterType` code snippet

**AgentLoop checkpoint** (`agentLoopCheckpoint[T]`):
- Stores runner checkpoint bytes, has-runner-state flag, unhandled items, canceled items
- Saved during stop/business-interrupt, cleaned up on normal exit

---

## Event System

**AsyncIterator / AsyncGenerator** — async pull/push event mechanism.

**Event types emitted during agent execution:**
- `Model output events` — emitted by EventSenderModelWrapper after model.Generate
- `Tool result events` — emitted by EventSenderToolWrapper after tool.Invoke
- `Error events` — on errors during execution
- `Action events` — on interrupt, transfer, exit, break-loop

**Event constructors** (in `event_sender.go`):
- `ToolInvokeEvent` / `ToolStreamEvent` — for standard tools
- `EnhancedToolInvokeEvent` / `EnhancedToolStreamEvent` — for enhanced tools (preserve Extra metadata for multimodal)

---

## Session & Branch Events System

Defined in `session.go`, the session system manages per-execution mutable state:

- **`runSession`** — stores agent metadata, events, values, and branch events
- **`branchEvents`** — per-parallel-branch event isolation with parent-linked list
- **`forkRunCtx`** — creates a child session with its own `BranchEvents` for parallel lanes
- **`joinRunCtxs`** — collects events from child branches, sorts by timestamp, commits to parent
- **`AddSessionValues`** / `getSession` — context-based value access

When `BranchEvents` is set on a session:
- `addEvent()` appends to the branch's local event slice (lock-free)
- `getEvents()` merges committed + branch events and sorts chronologically

---

## ReActGraph (Graph-level Integration)

Defined in `react_graph.go`, wraps a ReActAgent's loop into StateGraph nodes:

```
prepare_input → model_generate → execute_tools → check_done
                                                 ↘ [end]
```

- Interrupt set at "execute_tools" node for human-in-the-loop
- With Checkpointer, each node transition saves a checkpoint automatically
- Middleware hooks (BeforeAgent, BeforeModelRewrite, AfterModelRewrite) fire at appropriate nodes
- Accessible via `NewReActGraph(agent, checkpointer)`

---

## Retry & Failover

**Retry** (in `retry.go`):
- `ModelRetryConfig[M]` — MaxRetries, ShouldRetry callback, IsRetryAble, BackoffFunc
- Default backoff: exponential with jitter (100ms base, up to 10s + 5s jitter)
- Legacy path (no ShouldRetry) and modern path (with ShouldRetry decision)
- Stream retry: first-chunk verification, retry signal propagation via `retrySignal`
- `RetryExhaustedError` / `WillRetryError`

**Failover** (in `failover.go`):
- `FailoverConfig[M]` — backup models tried in order
- `ShouldFailover` callback for decision control
- `GetFailoverModel` for dynamic model selection
- Wraps retry so each failover attempt gets retry behavior

---

## Tool Invocation System

### ToolInvocationContext (`tool_invoke.go`)

Unified context object for tool invocations, replacing separate endpoint function
signatures with a single struct:

| Field | Type | Purpose |
|---|---|---|
| `Name` | `string` | Tool name |
| `CallID` | `string` | LLM call identifier |
| `Arguments` | `*schema.ToolArgument` | Structured input |
| `Result` | `*schema.ToolResult` | Execution output |
| `Timeout` | `time.Duration` | Per-invocation timeout |
| `RetryConfig` | `*ToolRetryConfig` | Per-invocation retry |

### ToolWrapper Chain (`tool_invoke.go`)

`ToolInvokeMiddleware` chains composable behaviors:

```go
tool := ToolWrapperChain(
    ToolToInvokeFn(myTool),
    NewTimeoutToolMiddleware(5*time.Second),
    NewRetryToolMiddleware(&ToolRetryConfig{MaxAttempts: 3}),
    NewFallbackToolMiddleware(fallbackFn),
)
```

Built-in wrappers:
- **Timeout** — context-based deadline per invocation
- **Retry** — exponential backoff with configurable IsRetryable predicate
- **Fallback** — secondary function on primary failure

### Approval Mechanism (`tool_invoke.go`)

`ApprovalMiddleware` enables human-in-the-loop for tool calls:

```go
chain := ToolWrapperChain(
    ToolToInvokeFn(myTool),
    ApprovalMiddleware(func(ctx, ictx) (*ApprovalRequest, error) {
        return &ApprovalRequest{
            ToolName: ictx.Name,
            ApproveChan: make(chan bool, 1),
        }, nil
    }),
)
```

`AutoApprovalMiddleware()` skips approval (useful for testing).

### ToolRegistry (`tool_registry.go`)

Centralized tool management replacing raw `[]Tool` slices:

| Method | Purpose |
|---|---|
| `Register(tool, opts...)` | Register with aliases and categories |
| `Lookup(name)` | Find by name or alias |
| `LookupByCategory(cat)` | Filter by category |
| `AllTools()` / `ToSlice()` | List all tools |
| `Filter(predicate)` | Create filtered subset |
| `Merge(other)` | Combine registries |
| `Unregister(name)` | Remove tool |

Options: `WithAlias`, `WithCategory`.

### Reflection Schema Generation (`tool_schema.go`)

`ReflectTool[T]` creates a Tool from any function using reflection:

```go
type WeatherArgs struct {
    City string `json:"city" description:"The city name"`
}

tool, _ := ReflectTool("get_weather", "Get current weather",
    func(ctx context.Context, args *WeatherArgs) (string, error) {
        return fmt.Sprintf("Weather in %s: sunny", args.City), nil
    })
```

`GenerateToolInfo[T]` generates `*schema.ToolInfo` from struct types, reading
`json`, `description`, and `enum` struct tags for schema metadata.

---

## File Map

| File | Purpose |
|---|---|
| `interface.go` | Agent interface, event types, type aliases, MessageType constraint |
| `react_agent.go` | ReActAgent: ReAct loop, freeze, run/resume, model wrapper chain builder |
| `react.go` | ReAct loop implementation (for-loop + graph-based) |
| `contracts.go` | Middleware interface (ReActMiddleware), ChatModel/Tool interfaces, BaseMiddleware, BaseTool |
| `contracts_conversion.go` | Middleware conversion between generic types |
| `cancel.go` | CancelContext state machine, cancel modes, stream cancel wrapping |
| `callback.go` | Callback handler, run-local values, gob encodability check |
| `options.go` | RunOption types (session values, checkpoint ID, cancel, callbacks, etc.) |
| `tools_node.go` | ToolsNode: tool dispatch, middleware chain, standard/enhanced tool paths |
| `event_sender.go` | Event sender middlewares (model + tool), event constructors |
| `model_chain.go` | BuildModelWrapperChain, EventSenderModelWrapper, CallbackModelWrapper |
| `retry.go` | Model retry with backoff, ShouldRetry, stream retry |
| `failover.go` | Model failover across backup models |
| `flow.go` | flowAgent: sub-agent management, history rewriting, transfer routing |
| `workflow.go` | workflowAgent: Sequential/Parallel/Loop orchestration |
| `runner.go` | Runner: entry point, run/resume, checkpoint save, event handling |
| `agent_loop.go` | AgentLoop: push-based agent execution, preempt/stop controllers |
| `state_wrapper.go` | StateModelWrapper: message deep copy, ID injection, cancel check |
| `config.go` | Agent option types, configuration |
| `instruction.go` | Instruction management |
| `interrupt.go` | Interrupt types and signals |
| `resume_data.go` | Resume data types |
| `agent_handoff.go` | Deterministic agent-to-agent transfer, message ID utilities |
| `turn_buffer.go` | AgentLoop buffer implementation |
| `session.go` | Run context, session management, BranchEvents for parallel isolation |
| `react_graph.go` | ReActGraph: graph-level ReAct loop with StateGraph |
| `utils.go` | Utility functions (AsyncIterator, AsyncGenerator) |
| `tool.go` | Tool-related helpers |
| **`tool_invoke.go`** | ToolInvocationContext, ToolInvokeMiddleware, Timeout/Retry/Fallback wrappers, Approval mechanism |
| **`tool_registry.go`** | ToolRegistry: aliases, categories, filtering, merge |
| **`tool_schema.go`** | Reflection-based ToolInfo generation, ReflectTool[T] |
| `session.go` | Run context, session management, BranchEvents for parallel isolation |
| `react_graph.go` | ReActGraph: graph-level ReAct loop with StateGraph |
| `utils.go` | Utility functions (AsyncIterator, AsyncGenerator) |
| `tool.go` | Tool-related helpers, AgentTool (sub-agent as Tool), recursion depth guard |

**Test files:**

| File | Purpose |
|---|---|
| `contracts_test.go` | Middleware/interface tests |
| `session_test.go` | Session, RunPath, BranchEvents, Fork/Join tests |
| `model_chain_test.go` | Wrapper chain tests |
| `model_chain_retry_failover_test.go` | Retry+failover combined integration tests |
| `agent_loop_test.go` | AgentLoop lifecycle tests |
| `agent_loop_edge_test.go` | AgentLoop edge cases |
| `agent_loop_ctrl_test.go` | AgentLoop controller tests |
| `agentic_integration_test.go` | Workflow + agentic integration tests |
| `agent_tool_test.go` | AgentTool tests |
| `agent_tool_depth_test.go` | Recursion depth error message test |
| `cancel_full_test.go` | Cancel system full suite (75 tests) |
| `chatmodel_retry_test.go` | Chat model retry tests |
| `concurrency_test.go` | AgentCore concurrency tests |
| `deterministic_transfer_test.go` | Deterministic transfer tests |
| `event_sender_test.go` | Event sender tests |
| `graph_integration_test.go` | Graph-based ReAct integration tests |
| `react_graph_test.go` | ReActGraph tests |
| `turn_loop_edge_test.go` | AgentLoop edge tests (ported) |

**Sub-packages:**

| Directory | Purpose |
|---|---|
| `backend/` | Filesystem backend abstraction (Backend interface, InMemoryBackend) |
| `internal/` | Internal helpers (default system prompt) |
| `middlewares/` | 10 middleware implementations (agentsmd, filesystem, patchtoolcalls, plantask, reduction, skill, subagent, summarization, telemetry, dynamictool) |
| `prebuilt/` | Prebuilt agent components (deep, supervisor, planexecute) |
| `schema/` | Schema types (Message, ToolCall, ToolResult, StreamReader, etc.) |

---

## SubAgentMiddleware — Dynamic Sub-Agent Invocation

Defined in `middlewares/subagent/subagent.go`, the SubAgentMiddleware injects sub-agents as
callable Tools that the parent LLM can invoke dynamically via tool calls.

### Architecture

```
Parent Agent (ReActAgent)
  ├─ Tools: [..., researcher_AgentTool, coder_AgentTool]  ← injected by SubAgentMiddleware
  ├─ Middlewares: [SubAgentMiddleware, ...]
  └─ Tool dispatch: executeInlineTools (ToolsConfig = nil)

  When LLM calls "researcher":
    └─ researcher_AgentTool.Invoke(ctx, args)
         └─ Runner.Run(runCtx_with_depth_1)
              └─ Researcher Agent (independent ReActLoop)
```

### Key Design Decisions

1. **AgentTool wrapping**: Each sub-agent is wrapped via `NewAgentTool(ctx, agent)` into a
   standard `Tool`. The sub-agent runs independently with its own Runner.Run().

2. **BindToConfig()**: Adds AgentTool wrappers to `config.Tools` and sets `config.ToolsConfig = nil`,
   forcing inline tool dispatch (`executeInlineTools` in `react_loop.go`). This is required because
   middleware-injected tools in `rc.Tools` are only found by `executeInlineTools`, not by `ToolsNode`.

3. **BeforeModelRewrite hook**: Injects sub-agent `ToolInfo` entries so the LLM sees them.

4. **Inheritance filtering**: Uses a marker interface (`subAgentMarker`) to exclude the
   SubAgentMiddleware itself when copying parent middlewares to the sub-agent's config.
   Additional exclusions by type name via `ExcludedParentMiddlewareNames`.

### Three Agent Sources

```go
// 1. Pre-built Agent (backward compatible)
spec := SubAgentSpec{
    Name: "researcher", Description: "Research",
    Agent: agentcore.NewReActAgent(cfg).WithName("researcher"),
}

// 2. Declarative AgentConfig (recommended)
spec := SubAgentSpec{
    Name: "researcher", Description: "Research",
    AgentConfig: &AgentConfig{
        Model:        anthropicModel,
        Tools:        []agentcore.Tool{searchTool},
        SystemPrompt: "You are a research assistant.",
    },
    InheritParentMiddlewares: true,
    ExcludedParentMiddlewareNames: []string{
        "*filesystem.middleware[*schema.Message]",
    },
}

// 3. AgentFactory (legacy)
spec := SubAgentSpec{
    Name: "researcher", Description: "Research",
    AgentFactory: func(ctx context.Context) (agentcore.Agent, error) {
        return agentcore.NewReActAgent(cfg).WithName("researcher"), nil
    },
}
```

### Recursion Depth Guard

MaxDepth limits nested sub-agent calls. Depth is tracked via `context.Context` using an
unexported key (`subAgentDepthKey` in `tool.go`). Every `AgentTool.Invoke` reads the
current depth from the parent context, checks the limit, and propagates depth+1 to the
child context.

```go
mw := subagent.New(specs, &subagent.Config{
    MaxDepth: 3, // allow parent → child → grandchild, block deeper
})
```

**Error handling**: When ToolsNode is used (default), recursion errors are converted to
tool result strings (not Go errors). The agent continues execution with the error text
visible to the LLM. For inline dispatch, the error propagates as a Go error to the
ReAct loop, which converts it to a tool message.

### Implementation Details

- `tool.go`: `subAgentDepthKey{}` context key, `MaxDepth` field in `AgentToolOptions`,
  `WithMaxDepth()` option. Depth is always propagated regardless of whether MaxDepth > 0,
  so nested tools from different middleware instances see the real depth.
- `subagent.go`: Marker interface for self-exclusion during inheritance.
  `BindToConfig` is idempotent via `m.built` flag.

---

## Sub-Agent Architecture: flowAgent vs SubAgentMiddleware

AgentCore provides two distinct sub-agent mechanisms:

| Aspect | flowAgent (deterministic) | SubAgentMiddleware (LLM-driven) |
|---|---|---|
| Invocation | Code-driven via `TransferToAgent` action | LLM-driven via tool call |
| Control flow | `flowAgent.runLoop` detects TransferToAgent, routes to child | Parent LLM decides when to invoke sub-agent |
| Sub-agent selection | Pre-registered via `SetSubAgents()` | Declared in `SubAgentSpec`, auto-wrapped as Tool |
| Execution context | Shares parent session, events accumulated | Independent Runner, no session sharing |
| Middleware inheritance | N/A | Optional via `InheritParentMiddlewares` |
| Orchestration patterns | Sequential / Parallel / Loop (workflowAgent) | LLM decides sequencing |
| Best for | Predictable multi-step pipelines | Dynamic task decomposition by LLM |

Both can be combined: a workflowAgent step can use SubAgentMiddleware to give the LLM
dynamic sub-agent capabilities within a structured pipeline.

---

## Quick Start — Building an Agent

### Minimal ReAct Agent

```go
package main

import (
    "context"
    "github.com/infiniflow/ragflow/harness/agentcore"
    "github.com/infiniflow/ragflow/harness/agentcore/schema"
)

func main() {
    // 1. Create a chat model (implement agentcore.Model[*schema.Message]).
    model := myChatModel{}

    // 2. Create tools.
    tool := &myTool{}

    // 3. Build ReAct agent.
    agent := agentcore.NewReActAgent(&agentcore.ReActConfig[*schema.Message]{
        Model:       model,
        Tools:       []agentcore.Tool{tool},
        Instruction: "You are a helpful assistant.",
    }).WithName("my_agent")

    // 4. Run via Runner.
    runner := agentcore.NewTypedRunner(agentcore.RunnerConfig[*schema.Message]{Agent: agent})
    iter := runner.Run(context.Background(), []*schema.Message{
        schema.UserMessage("Hello!"),
    })

    for {
        ev, ok := iter.Next()
        if !ok { break }
        if ev.Err != nil { /* handle */ }
        if ev.Output != nil && ev.Output.MessageOutput != nil {
            // consume output
        }
    }
}
```

### Agent with Middleware Stack

```go
import (
    "github.com/infiniflow/ragflow/harness/agentcore"
    "github.com/infiniflow/ragflow/harness/agentcore/middlewares/filesystem"
    "github.com/infiniflow/ragflow/harness/agentcore/middlewares/summarization"
    "github.com/infiniflow/ragflow/harness/agentcore/middlewares/subagent"
)

// Middleware chain order:
// 1. SubAgentMiddleware (injects sub-agent tools)
// 2. FilesystemMiddleware (injects read/write/edit tools)
// 3. SummarizationMiddleware (auto-compresses long conversations)
agent := agentcore.NewReActAgent(&agentcore.ReActConfig[*schema.Message]{
    Model:       model,
    Middlewares: []agentcore.ReActMiddleware{
        subAgentMW,
        filesystem.New(&filesystem.Config{Backend: fsBackend}),
        summarization.New(&summarization.Config{
            TokenLimit: 100000,
            Model:      summaryModel,
        }),
    },
    Instruction: "You are a coding assistant.",
})
```

### With Sub-Agents

```go
// Declare sub-agents.
spec := subagent.SubAgentSpec{
    Name:        "researcher",
    Description: "Research a topic using web search",
    AgentConfig: &subagent.AgentConfig{
        Model:        claudeModel,
        Tools:        []agentcore.Tool{webSearchTool},
        SystemPrompt: "You are a research assistant.",
        Middlewares:  []agentcore.ReActMiddleware{ownMiddleware},
    },
    InheritParentMiddlewares: true, // inherit filesystem, summarization, etc.
}

// Create middleware.
saMW := subagent.New([]subagent.SubAgentSpec{spec}, &subagent.Config{
    EmitInternalEvents: true, // forward sub-agent events to parent stream
    MaxDepth:           5,    // guard against infinite nesting
})

// Build parent agent.
cfg := &agentcore.ReActConfig[*schema.Message]{
    Model:       parentModel,
    Middlewares: []agentcore.ReActMiddleware{saMW, filesystem.New(...)},
}
saMW.BindToConfig(cfg) // mandatory: injects tools, forces inline dispatch
agent := agentcore.NewReActAgent(cfg)
```

### Cancellation

```go
opt, cancel := agentcore.WithCancel()
defer cancel(agentcore.WithCancelMode(agentcore.CancelAfterChatModel))

iter := runner.Run(ctx, msgs, opt)

// Later, to cancel:
handle, ok := cancel(agentcore.WithCancelMode(agentcore.CancelImmediate))
if ok { handle.Wait() }
```

### Checkpoint / Resume

```go
store := &myCheckpointStore{}

// Run with checkpoint ID.
iter := runner.Run(ctx, msgs, agentcore.WithCheckPointID("run-001"))

// Resume from checkpoint.
iter, err := runner.Resume(ctx, "run-001")
```

### Custom Middleware

```go
type LoggingMiddleware struct {
    agentcore.BaseMiddleware[*schema.Message]
}
func (m *LoggingMiddleware) BeforeModelRewrite(
    ctx context.Context,
    state *agentcore.ReActAgentState,
    mc *agentcore.ModelContext,
) (context.Context, *agentcore.ReActAgentState, error) {
    log.Printf("model input: %d messages", len(state.Messages))
    return ctx, state, nil
}
func (m *LoggingMiddleware) AfterModelRewrite(
    ctx context.Context,
    state *agentcore.ReActAgentState,
    mc *agentcore.ModelContext,
) (context.Context, *agentcore.ReActAgentState, error) {
    log.Printf("model output: %d messages", len(state.Messages))
    return ctx, state, nil
}
```

### Tool Implementation

```go
type WeatherTool struct{}

func (t *WeatherTool) Name() string { return "get_weather" }
func (t *WeatherTool) Description() string { return "Get weather for a city. Args: city name." }
func (t *WeatherTool) Invoke(ctx context.Context, args string, opts ...agentcore.ToolOption) (string, error) {
    return fmt.Sprintf("Weather in %s: sunny, 25°C", args), nil
}
func (t *WeatherTool) Stream(ctx context.Context, args string, opts ...agentcore.ToolOption) (*schema.StreamReader[string], error) {
    return schema.StreamReaderFromArray([]string{t.Invoke(ctx, args)}), nil
}
```

### Key API Patterns

1. **Agent construction**: `NewReActAgent(cfg)` — config is frozen after first Run call.
2. **Middleware**: Embed `BaseMiddleware[*schema.Message]`, override only needed hooks.
3. **Tools**: Implement `Tool` interface; for structured results, also implement `EnhancedTool`.
4. **Runner**: Primary entry point; wraps agent with flowAgent for session/transfer/checkpoint.
5. **Sub-agents**: Use `SubAgentMiddleware` for LLM-driven delegation or `flowAgent`/`workflowAgent`
   for deterministic orchestration.
6. **Event streaming**: `AsyncIterator[*AgentEvent]` — pull-based; events carry output, actions, errors.
7. **Cancellation**: Three-mode system with timeout escalation; supports recursive cancel for sub-agents.
