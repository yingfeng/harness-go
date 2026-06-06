# AgentCore Architecture

## Overview

AgentCore is the production-grade Agent Development Kit (ADK) layer of the Harness-Go framework.
It wraps a StateGraph/Pregel execution engine with high-level Agent abstractions (ChatModelAgent,
Runner, Middleware, Tools) for building complex LLM-powered agents.

The code lives under `agentcore/` and is imported as `github.com/infiniflow/ragflow/harness/agentcore`.
Public types are re-exported from the top-level `harness` package in `/home/infominer/codebase/harness-go/harness.go`.

---

## Core Architecture

### Layer 1: Agent Interface

The base interface `TypedAgent[M]` (defined in `interface.go`) is the contract all agents implement:

- `Name(ctx)` / `Description(ctx)` — agent metadata
- `Run(ctx, input, opts...)` — returns `AsyncIterator[*TypedAgentEvent[M]]` for event streaming
- `Resume(ctx, info, opts...)` — for agents implementing `TypedResumableAgent[M]`

**Key types:**

| Type | Description |
|---|---|
| `TypedAgent[M]` | Generic agent interface (M = `*schema.Message` or `*schema.AgenticMessage`) |
| `Agent` | Type alias for `TypedAgent[*schema.Message]` |
| `TypedResumableAgent[M]` | Extends TypedAgent with Resume for interrupt handling |
| `TypedAgentInput[M]` | Input with Messages + EnableStreaming flag |
| `TypedAgentEvent[M]` | Event with Output (Message or MessageStream), Action, AgentName, RunPath |
| `AgentAction` | Action emitted by agent: Exit, Interrupted, TransferToAgent, BreakLoop |

### Layer 2: Execution Engine

**ChatModelAgent** (defined in `chatmodel.go`) implements the ReAct (Reasoning + Acting) pattern:

1. `freeze()` — after first Run/Resume, configuration is frozen via atomic flag to prevent mutation during execution.

2. `buildRunFunc` — lazy-init that detects whether tools are configured and selects the appropriate runner:
   - `buildReActRunFunc` — full ReAct loop with tool execution
   - `buildNoToolsRunFunc` — single model call without tool dispatch

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

4. **Model Wrapper Chain** (built in `BuildModelWrapperChain` in `wrappers.go`):
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
  - **Parallel** — runs sub-agents concurrently with `sync.WaitGroup`
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

Defined in `handler.go`, middlewares implement `TypedChatModelMiddleware[M]` with 9 hook points:

| Hook | Signature | Purpose |
|---|---|---|
| BeforeAgent | `(ctx, *ChatModelAgentContext)` | Modify instruction, tools, return-directly map |
| AfterAgent | `(ctx, state)` | Post-execution cleanup |
| BeforeModelRewrite | `(ctx, state, *ModelContext)` | Transform state before model call |
| AfterModelRewrite | `(ctx, state, *ModelContext)` | Transform state after model call |
| WrapModel | `(ctx, ChatModel[M], *ModelContext)` → ChatModel[M] | Wrap model call |
| WrapToolInvoke | `(ctx, InvokableToolEndpoint, *ToolContext)` | Wrap sync tool invoke |
| WrapToolStream | `(ctx, StreamableToolEndpoint, *ToolContext)` | Wrap streaming tool invoke |
| WrapEnhancedInvokableToolCall | `(ctx, EnhancedInvokableToolEndpoint, *ToolContext)` | Wrap enhanced sync tool |
| WrapEnhancedStreamableToolCall | `(ctx, EnhancedStreamableToolEndpoint, *ToolContext)` | Wrap enhanced streaming tool |

**BaseMiddleware** provides default no-op implementations for all hooks. Embed it and override only needed methods.

**Event Sender Middlewares** (in `event_sender.go`):
- `NewEventSenderModelWrapper` — emits model output events (position in chain controls event timing)
- `NewEventSenderToolWrapper` — emits tool result events
- Detection via `HasUserEventSenderToolWrapper` / `HasUserEventSenderModelWrapper` — skips built-in sender to avoid duplicates

### Layer 5: TurnLoop (Push-based Execution)

Defined in `turn_loop.go`, TurnLoop enables push-based agent interaction where external events
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
- `bridgeStore` — bridges TurnLoop checkpoints with Runner checkpoints
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
- Agent state (`*TypedChatModelAgentState[M]`)

**Resume flow:**
1. `Runner.Resume` → loads checkpoint from store via `loadCheckpoint`
2. Reconstructs run context from checkpoint data
3. Calls `ResumableAgent.Resume` with `ResumeInfo`
4. `ChatModelAgent.Resume` restores state from `InterruptState` and re-enters run function
5. `ChatModelAgentResumeData.HistoryModifier` allows input modification on resume

**Gob encodability check:**
- `checkGobEncodability` in `callback.go` proactively validates values at `SetRunLocalValue` time
- Catches unregistered types early with actionable error message including `RegisterType` code snippet

**TurnLoop checkpoint** (`turnLoopCheckpoint[T]`):
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
- `TypedToolInvokeEvent` / `TypedToolStreamEvent` — for standard tools
- `TypedEnhancedToolInvokeEvent` / `TypedEnhancedToolStreamEvent` — for enhanced tools (preserve Extra metadata for multimodal)

---

## ReActGraph (Graph-level Integration)

Defined in `react_graph.go`, wraps a ChatModelAgent's loop into StateGraph nodes:

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
- `TypedModelRetryConfig[M]` — MaxRetries, ShouldRetry callback, IsRetryAble, BackoffFunc
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
| `chatmodel.go` | ChatModelAgent: ReAct loop, freeze, run/resume, model wrapper chain builder |
| `handler.go` | Middleware interface (TypedChatModelMiddleware), ChatModel/Tool interfaces, BaseMiddleware |
| `handler_conversion.go` | Middleware conversion between generic types |
| `cancel.go` | CancelContext state machine, cancel modes, stream cancel wrapping |
| `callback.go` | Callback handler, run-local values, gob encodability check |
| `options.go` | RunOption types (session values, checkpoint ID, cancel, callbacks, etc.) |
| `tools_node.go` | ToolsNode: tool dispatch, middleware chain, standard/enhanced tool paths |
| `event_sender.go` | Event sender middlewares (model + tool), event constructors |
| `wrappers.go` | EventSenderModelWrapper, CallbackModelWrapper, BuildModelWrapperChain |
| `retry.go` | Model retry with backoff, ShouldRetry, stream retry |
| `failover.go` | Model failover across backup models |
| `flow.go` | flowAgent: sub-agent management, history rewriting, transfer routing |
| `workflow.go` | workflowAgent: Sequential/Parallel/Loop orchestration |
| `runner.go` | Runner: entry point, run/resume, checkpoint save, event handling |
| `turn_loop.go` | TurnLoop: push-based agent execution, preempt/stop controllers |
| `state_wrapper.go` | StateModelWrapper: message deep copy, ID injection, cancel check |
| `config.go` | Agent option types, configuration |
| `instruction.go` | Instruction management |
| `interrupt.go` | Interrupt types and signals |
| `resume_data.go` | Resume data types |
| `transfer.go` | Agent transfer logic |
| `turn_buffer.go` | TurnLoop buffer implementation |
| `runctx.go` | Run context, session management |
| `react_graph.go` | ReActGraph: graph-level ReAct loop with StateGraph |
| `utils.go` | Utility functions |
| `tool.go` | Tool-related helpers |
| `react.go` | ReAct loop helpers |
| `deterministic_transfer_test.go` | Deterministic transfer tests |
| `agentcore_test.go` | Mock implementations (mockModel, mockTool, memStore, etc.) |
| `agentcore_full_test.go` | Comprehensive integration tests |
| `cancel_full_test.go` | Cancel system tests |
| `event_sender_test.go` | Event sender tests |
| `retry_test.go` | Retry tests |
| `handler_test.go` | Handler/middleware tests |
| `tools_node_test.go` | ToolsNode tests |
| `turn_loop_test.go` | TurnLoop tests |
| `wrappers_test.go` | Wrapper tests |
| `failover_test.go` | Failover tests |
| `callback_test.go` | Callback tests |

**Sub-packages:**
- `filesystem/` — filesystem utilities
- `internal/` — internal helpers (default system prompt)
- `middlewares/` — middleware implementations (25 files)
- `prebuilt/` — prebuilt agent components
- `schema/` — schema types (Message, ToolCall, ToolResult, StreamReader, etc.)
