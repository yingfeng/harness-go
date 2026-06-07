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
| `middlewares/` | 9 middleware implementations (agentsmd, filesystem, patchtoolcalls, plantask, reduction, skill, summarization, telemetry, dynamictool) |
| `prebuilt/` | Prebuilt agent components (deep, supervisor, planexecute) |
| `schema/` | Schema types (Message, ToolCall, ToolResult, StreamReader, etc.) |
