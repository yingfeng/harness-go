// Package langgraph is the main package for LangGraph Go.
//
// LangGraph is a library for building stateful, multi-agent applications with LLMs.
// It provides a graph-based execution model that supports:
//
//   - Stateful computation with channels and reducers
//   - Multi-agent workflows with subgraphs
//   - Human-in-the-loop with interrupts
//   - Persistence with checkpoints
//   - Streaming and debugging
//
// Basic Usage:
//
//	import (
//	    "context"
//	    "github.com/infiniflow/ragflow/agent"
//	    "github.com/infiniflow/ragflow/agent/channels"
//	)
//
//	// Define state schema
//	type State struct {
//	    Messages []string
//	    Counter  int
//	}
//
//	// Create graph
//	builder := langgraph.NewStateGraph(State{})
//
//	// Add nodes
//	builder.AddNode("agent", func(ctx context.Context, state interface{}) (interface{}, error) {
//	    s := state.(State)
//	    s.Messages = append(s.Messages, "Hello from agent")
//	    s.Counter++
//	    return s, nil
//	})
//
//	// Add edges
//	builder.AddEdge("__start__", "agent")
//	builder.AddEdge("agent", "__end__")
//
//	// Compile and run
//	graph, err := builder.Compile()
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	result, err := graph.Invoke(context.Background(), State{
//	    Messages: []string{"Hello"},
//	    Counter:  0,
//	})
//
// For more examples and documentation, visit:
// https://github.com/infiniflow/ragflow/agent
package langgraph

import (
	"github.com/infiniflow/ragflow/agent/agentcore"
	"github.com/infiniflow/ragflow/agent/channels"
	"github.com/infiniflow/ragflow/agent/checkpoint"
	"github.com/infiniflow/ragflow/agent/constants"
	"github.com/infiniflow/ragflow/agent/errors"
	"github.com/infiniflow/ragflow/agent/graph"
	"github.com/infiniflow/ragflow/agent/interrupt"
	"github.com/infiniflow/ragflow/agent/prebuilt"
	"github.com/infiniflow/ragflow/agent/types"
)

// Re-export main types for convenience.
type (
	// StateGraph is a graph whose nodes communicate by reading and writing to a shared state.
	StateGraph = graph.StateGraph
	
	// CompiledGraph is a compiled, executable graph.
	CompiledGraph = graph.CompiledGraph
	
	// Node represents a node in the graph.
	Node = graph.Node
	
	// Edge represents an edge in the graph.
	Edge = graph.Edge
	
	// Send represents a dynamic node invocation.
	Send = graph.Send
	
	// Checkpointer is the interface for checkpoint savers.
	Checkpointer = graph.Checkpointer
	
	// MemorySaver is an in-memory checkpoint saver.
	MemorySaver = checkpoint.MemorySaver
	
	// SqliteSaver is a SQLite-based checkpoint saver.
	SqliteSaver = checkpoint.SqliteSaver

	// PostgresSaver is a PostgreSQL-based checkpoint saver.
	PostgresSaver = checkpoint.PostgresSaver
	// PostgresConfig holds configuration for PostgreSQL connection.
	PostgresConfig = checkpoint.PostgresConfig

	// Channel is the base interface for all channels.
	Channel = channels.Channel
	
	// BaseChannel provides a base implementation of Channel.
	BaseChannel = channels.BaseChannel
	
	// LastValue stores the last value received.
	LastValue = channels.LastValue
	
	// Topic is a configurable PubSub Topic.
	Topic = channels.Topic
	
	// BinaryOperatorAggregate stores the result of applying a binary operator.
	BinaryOperatorAggregate = channels.BinaryOperatorAggregate
	
	// BinaryOperator is a function that combines two values into one.
	BinaryOperator = channels.BinaryOperator
	
	// EphemeralValue stores a value that is cleared after being read once.
	EphemeralValue = channels.EphemeralValue
	
	// NamedBarrierValue waits until all named nodes have written a value.
	NamedBarrierValue = channels.NamedBarrierValue
	
	// NamedBarrierValueAfterFinish waits for all named nodes, available only after finish.
	NamedBarrierValueAfterFinish = channels.NamedBarrierValueAfterFinish
	
	// LastValueAfterFinish stores last value, available only after finish.
	LastValueAfterFinish = channels.LastValueAfterFinish
	
	// UntrackedValue stores a value but does not track it for checkpointing.
	UntrackedValue = channels.UntrackedValue
	
	// AnyValue stores any value received.
	AnyValue = channels.AnyValue
	
	// RunnableConfig is the configuration for a runnable.
	RunnableConfig = types.RunnableConfig
	
	// StreamMode defines how the stream method should emit outputs.
	StreamMode = types.StreamMode
	
	// RetryPolicy configures retrying nodes.
	RetryPolicy = types.RetryPolicy
	
	// CachePolicy configures caching nodes.
	CachePolicy = types.CachePolicy
	
	// Command is used to update the graph's state and send messages to nodes.
	Command = types.Command

	// Interrupt represents information about an interrupt.
	Interrupt = types.Interrupt

	// NodeFunc is the signature of a node function.
	NodeFunc = types.NodeFunc

	// EdgeFunc is the signature of an edge/condition function.
	EdgeFunc = types.EdgeFunc

	// StreamWriter writes data to the output stream.
	StreamWriter = types.StreamWriter

	// Prebuilt types
	ReactAgentConfig = prebuilt.ReactAgentConfig
	ReActState       = prebuilt.ReActState
	Tool             = prebuilt.Tool
	ToolCall         = prebuilt.ToolCall
	LLM              = prebuilt.LLM
)

// AgentCore types (selectively re-exported).
// Generic types like ChatModel[M] and RunnerConfig[M] must be imported directly.
type (
	// Agent is the core agent interface (Message type).
	Agent = agentcore.Agent
	// ResumableAgent supports interrupt/resume.
	ResumableAgent = agentcore.ResumableAgent
	// Runner executes agents.
	Runner = agentcore.Runner
	// AgentEvent represents an event during agent execution.
	AgentEvent = agentcore.AgentEvent
	// AgentAction represents actions an agent can emit.
	AgentAction = agentcore.AgentAction
	// AgentInput is the input to an agent.
	AgentInput = agentcore.AgentInput
	// AgentOutput is the output from an agent event.
	AgentOutput = agentcore.AgentOutput
	// RunOption configures agent execution.
	RunOption = agentcore.RunOption
	// InterruptInfo holds interrupt metadata.
	InterruptInfo = agentcore.InterruptInfo
	// InterruptCtx provides structured interrupt context.
	InterruptCtx = agentcore.InterruptCtx
	// InterruptSignal is the internal interrupt signal.
	InterruptSignal = agentcore.InterruptSignal
	// CancelMode defines when an agent should be canceled.
	CancelMode = agentcore.CancelMode
	// CancelError indicates an agent was canceled.
	CancelError = agentcore.CancelError
	// CancelHandle allows waiting for cancel completion.
	CancelHandle = agentcore.CancelHandle
	// AgentCancelFunc cancels a running agent.
	AgentCancelFunc = agentcore.AgentCancelFunc
	// BaseTool provides a simple Tool implementation.
	BaseTool = agentcore.BaseTool
	// ToolContext provides tool metadata.
	ToolContext = agentcore.ToolContext
	// ChatModelAgentState holds agent state for middlewares.
	ChatModelAgentState = agentcore.ChatModelAgentState
	// ChatModelAgentContext is passed to BeforeAgent middlewares.
	ChatModelAgentContext = agentcore.ChatModelAgentContext
	// ModelContext wraps model call context.
	ModelContext = agentcore.ModelContext
	// TurnLoop implements priority-based scheduling.
	TurnLoop = agentcore.TurnLoop
	// CheckPointStore persists execution checkpoints.
	CheckPointStore = agentcore.CheckPointStore
	// ChatModelMiddleware allows customizing agent behavior.
	ChatModelMiddleware = agentcore.ChatModelMiddleware
	// Workflow types
	SequentialConfig = agentcore.SequentialConfig
	ParallelConfig   = agentcore.ParallelConfig
	LoopConfig       = agentcore.LoopConfig
)

// Cancel constants.
const (
	CancelImmediate      = agentcore.CancelImmediate
	CancelAfterChatModel = agentcore.CancelAfterChatModel
	CancelAfterToolCalls = agentcore.CancelAfterToolCalls
)

// AgentCore functions.
var (
	// NewRunner creates an agent Runner (Message type).
	NewRunner = agentcore.NewRunner
	// NewAgentTool wraps an Agent as a Tool.
	NewAgentTool = agentcore.NewAgentTool
	// NewSequential creates a sequential workflow agent.
	NewSequential = agentcore.NewSequential
	// NewParallel creates a parallel workflow agent.
	NewParallel = agentcore.NewParallel
	// NewLoop creates a loop workflow agent.
	NewLoop = agentcore.NewLoop
	// NewTurnLoop creates a TurnLoop scheduler.
	NewTurnLoop = agentcore.NewTurnLoop
	// SetSubAgents configures sub-agents.
	SetSubAgents = agentcore.SetSubAgents
	// WithCancel creates a cancel option and cancel function.
	WithCancel = agentcore.WithCancel

	// Run option constructors
	WithSessionValues       = agentcore.WithSessionValues
	WithCheckPointID        = agentcore.WithCheckPointID
	WithSkipTransferMessages = agentcore.WithSkipTransferMessages
	WithCallbacks            = agentcore.WithCallbacks
	WithAgentNames           = agentcore.WithAgentNames
	WithSharedParentSession  = agentcore.WithSharedParentSession
	WithChatModelOptions     = agentcore.WithChatModelOptions
	WithToolOptions          = agentcore.WithToolOptions
	WithAgentToolOptions     = agentcore.WithAgentToolOptions
	WithHistoryModifier      = agentcore.WithHistoryModifier
	WithCancelMode           = agentcore.WithCancelMode
	WithCancelTimeout        = agentcore.WithCancelTimeout
	WithRecursiveCancel      = agentcore.WithRecursiveCancel

	// Event helpers
	StatefulInterrupt   = agentcore.StatefulInterrupt
	CompositeInterrupt  = agentcore.CompositeInterrupt
	SendEvent           = agentcore.SendEvent
	SetRunLocalValue    = agentcore.SetRunLocalValue
	GetRunLocalValue    = agentcore.GetRunLocalValue
	DeleteRunLocalValue = agentcore.DeleteRunLocalValue

	// Transfer and middleware
	AgentWithOptions           = agentcore.AgentWithOptions
	AgentWithDeterministicTransfer = agentcore.AgentWithDeterministicTransfer
	SetLanguage                = agentcore.SetLanguage

	// Errors
	ErrCancelTimeout  = agentcore.ErrCancelTimeout
	ErrExecutionEnded = agentcore.ErrExecutionEnded
	ErrStreamCanceled = agentcore.ErrStreamCanceled

)

// Prebuilt component functions.
var (
	// CreateReactAgent creates a new ReAct (Reasoning + Acting) agent.
	CreateReactAgent = prebuilt.CreateReactAgent
	// ToolNode creates a node that executes a tool.
	ToolNode = prebuilt.ToolNode
	// ValidationNode creates a node that validates input.
	ValidationNode = prebuilt.ValidationNode
	// ConditionalNode creates a node that routes based on a condition.
	ConditionalNode = prebuilt.ConditionalNode
	// TransformNode creates a node that transforms input.
	TransformNode = prebuilt.TransformNode
)

// Re-export constants.
const (
	// Start is the first (virtual) node in the graph.
	Start = constants.Start
	
	// End is the last (virtual) node in the graph.
	End = constants.End
	
	// TagNoStream is a tag to disable streaming.
	TagNoStream = constants.TagNoStream
	
	// TagHidden is a tag to hide a node/edge from tracing.
	TagHidden = constants.TagHidden
)

// Re-export stream modes.
const (
	StreamModeValues      = types.StreamModeValues
	StreamModeUpdates     = types.StreamModeUpdates
	StreamModeCustom      = types.StreamModeCustom
	StreamModeMessages    = types.StreamModeMessages
	StreamModeCheckpoints = types.StreamModeCheckpoints
	StreamModeTasks       = types.StreamModeTasks
	StreamModeDebug       = types.StreamModeDebug
)

// Re-export error types.
type (
	GraphRecursionError   = errors.GraphRecursionError
	InvalidUpdateError    = errors.InvalidUpdateError
	GraphInterrupt        = errors.GraphInterrupt
	EmptyChannelError     = errors.EmptyChannelError
	EmptyInputError       = errors.EmptyInputError
	NodeNotFoundError     = errors.NodeNotFoundError
	InvalidNodeError      = errors.InvalidNodeError
	InvalidEdgeError      = errors.InvalidEdgeError
	ChannelNotFoundError  = errors.ChannelNotFoundError
)

// NewStateGraph creates a new StateGraph with the given state schema.
func NewStateGraph(stateSchema interface{}) *StateGraph {
	return graph.NewStateGraph(stateSchema)
}

// NewMemorySaver creates a new in-memory checkpoint saver.
func NewMemorySaver() *MemorySaver {
	return checkpoint.NewMemorySaver()
}

// NewSqliteSaver creates a new SQLite checkpoint saver.
func NewSqliteSaver(dbPath string) (*SqliteSaver, error) {
	return checkpoint.NewSqliteSaver(dbPath)
}

// NewPostgresSaver creates a new PostgreSQL checkpoint saver.
func NewPostgresSaver(connString string) (*PostgresSaver, error) {
	return checkpoint.NewPostgresSaver(connString)
}

// NewPostgresSaverWithConfig creates a new PostgreSQL checkpoint saver with explicit config.
func NewPostgresSaverWithConfig(config *PostgresConfig) (*PostgresSaver, error) {
	return checkpoint.NewPostgresSaverWithConfig(config)
}

// Compile options.
var (
	// WithCheckpointer sets the checkpointer for the compiled graph.
	WithCheckpointer = graph.WithCheckpointer
	
	// WithInterrupts sets the nodes that should trigger interrupts.
	WithInterrupts = graph.WithInterrupts
	
	// WithRecursionLimit sets the recursion limit.
	WithRecursionLimit = graph.WithRecursionLimit
	
	// WithDebug enables debug mode.
	WithDebug = graph.WithDebug
)

// Interrupt functions.
var (
	// InterruptFunc interrupts the graph with a resumable exception.
	InterruptFunc = interrupt.Interrupt
	
	// IsInterrupt checks if an error is a GraphInterrupt.
	IsInterrupt = interrupt.IsInterrupt
	
	// GetInterruptValue extracts the interrupt value from a GraphInterrupt error.
	GetInterruptValue = interrupt.GetInterruptValue
)

// Channel constructors.
var (
	// NewLastValue creates a new LastValue channel.
	NewLastValue = channels.NewLastValue
	
	// NewTopic creates a new Topic channel.
	NewTopic = channels.NewTopic
	
	// NewBinaryOperatorAggregate creates a new BinaryOperatorAggregate channel.
	NewBinaryOperatorAggregate = channels.NewBinaryOperatorAggregate
	
	// NewEphemeralValue creates a new EphemeralValue channel.
	NewEphemeralValue = channels.NewEphemeralValue
	
	// NewNamedBarrierValue creates a new NamedBarrierValue channel.
	NewNamedBarrierValue = channels.NewNamedBarrierValue
	
	// NewNamedBarrierValueAfterFinish creates a new NamedBarrierValueAfterFinish channel.
	NewNamedBarrierValueAfterFinish = channels.NewNamedBarrierValueAfterFinish
	
	// NewLastValueAfterFinish creates a new LastValueAfterFinish channel.
	NewLastValueAfterFinish = channels.NewLastValueAfterFinish
	
	// NewUntrackedValue creates a new UntrackedValue channel.
	NewUntrackedValue = channels.NewUntrackedValue
	
	// NewAnyValue creates a new AnyValue channel.
	NewAnyValue = channels.NewAnyValue
)

// BinaryOperator functions.
var (
	// ListAppend appends two lists.
	ListAppend = channels.ListAppend
	
	// IntAdd adds two integers.
	IntAdd = channels.IntAdd
	
	// StringConcat concatenates two strings.
	StringConcat = channels.StringConcat
)

// DefaultRetryPolicy returns a default retry policy.
func DefaultRetryPolicy() RetryPolicy {
	return types.DefaultRetryPolicy()
}

// NewRunnableConfig creates a new RunnableConfig.
func NewRunnableConfig() *RunnableConfig {
	return types.NewRunnableConfig()
}

// NewCommand creates a new Command.
func NewCommand() *Command {
	return types.NewCommand()
}

// NewSend creates a new Send.
func NewSend(node string, arg interface{}) *Send {
	return &Send{Node: node, Arg: arg}
}
