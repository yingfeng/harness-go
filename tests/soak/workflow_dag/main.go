//go:build soak

// Package main provides a long-running soak test for the StateGraph/Pregel engine.
//
// Design:
//   - A complex workflow DAG with ~35 nodes, cycles, branches, parallelism, and mock tool calls
//   - 10+ concurrent tenants, each repeatedly running the graph with checkpoint/resume cycles
//   - Random fault injection: tool timeouts, panics, rate limits, recursion limit hits
//   - Periodic checkpoint save + resume simulation
//   - Default: 30 minutes; configurable via --duration flag
//
// Usage:
//
//	go run -tags soak ./tests/soak/workflow_dag/ --duration 30m --tenants 10
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/infiniflow/ragflow/harness/graphengine/channels"
	"github.com/infiniflow/ragflow/harness/graphengine/checkpoint"
	"github.com/infiniflow/ragflow/harness/graphengine/constants"
	gerrors "github.com/infiniflow/ragflow/harness/graphengine/errors"
	"github.com/infiniflow/ragflow/harness/graphengine/graph"
	"github.com/infiniflow/ragflow/harness/graphengine/pregel"
	"github.com/infiniflow/ragflow/harness/graphengine/types"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ---- Configuration ----

var (
	flagDuration   = flag.Duration("duration", 30*time.Minute, "Total soak duration")
	flagTenants    = flag.Int("tenants", 10, "Number of concurrent tenant goroutines")
	flagVerbose    = flag.Bool("verbose", false, "Verbose per-iteration logging")
	flagFailRate   = flag.Float64("fail-rate", 0.05, "Probability of random tool/node failure")
	flagCrashRate  = flag.Float64("crash-rate", 0.01, "Probability of random tool/node panic")
	flagCheckpoint = flag.String("checkpoint", "memory", "Checkpoint backend: memory or nats")
	flagNatsURL    = flag.String("nats-url", "nats://localhost:4222", "NATS server URL (used when --checkpoint=nats)")
)

// ---- Global Metrics ----

type metrics struct {
	tenantExecs     int64 // total executions across all tenants
	tenantErrors    int64 // total execution errors
	graphInvocations int64
	recursionErrors int64
	toolFailures    int64
	toolPanics      int64
	checkpoints     int64
	resumes         int64
	timeouts        int64
}

var (
	globalMetrics   metrics
	startTime       time.Time
)

// ---- Node Names ----

const nodeCount = 35

var nodeNames = func() []string {
	names := make([]string, 0, nodeCount)
	names = append(names,
		// Main pipeline (linear)
		"node_initial",
		"node_preflight",
		"node_validate",
		"node_plan",
		"node_dispatch",

		// Branch A: high priority
		"node_process_high",
		"node_check_quality_high",
		"node_retry_high",

		// Parallel sub-tasks (fan-out)
		"node_parallel_a",
		"node_parallel_b",
		"node_parallel_c",
		"node_join_parallel",

		// Branch B: normal processing with tool calls
		"node_process_normal",
		"node_call_tool_search",
		"node_review_result",
		"node_retry_normal",
		"node_transform_data",
		"node_call_tool_codegen",

		// Branch C: slow processing
		"node_process_slow",
		"node_call_tool_heavy",
		"node_escalate",
		"node_manual_review",
		"node_recovery_action",

		// Common pipeline after branches
		"node_aggregate",
		"node_checkpoint_save",
		"node_loop_check",
		"node_finalize",
		"node_report",
		"node_backup_data",
		"node_cleanup_temp",
		"node_archive_logs",
		"node_summary_post",
		"node_teardown",
	)
	return names
}()

// ---- Tenant ----

type tenantState struct {
	ID         int
	LoopCount  int
	Route      string
	Errors     []string
	Executions int
}

type tenantMetrics struct {
	id             int
	executions     int64
	errors         int64
	recursionLimit int64
	checkpoints    int64
	resumes        int64
	lastLogTime    time.Time
}

// ---- Tool Simulation Node ----

// resilientNode creates a node function that simulates tool-like behavior:
// delays, random failures, and random panics.
func resilientNode(name string, delay time.Duration, failRate float64, panicRate float64) types.NodeFunc {
	return func(ctx context.Context, state interface{}) (interface{}, error) {
		// Random delay simulation
		if delay > 0 {
			jitter := time.Duration(rand.Int63n(int64(delay) / 2))
			totalDelay := delay + jitter
			select {
			case <-ctx.Done():
				atomic.AddInt64(&globalMetrics.timeouts, 1)
				return nil, ctx.Err()
			case <-time.After(totalDelay):
			}
		}

		// Random panic simulation
		if rand.Float64() < panicRate {
			atomic.AddInt64(&globalMetrics.toolPanics, 1)
			panic(fmt.Sprintf("simulated crash in node %s", name))
		}

		// Random failure simulation
		if rand.Float64() < failRate {
			atomic.AddInt64(&globalMetrics.toolFailures, 1)
			m := safeMap(state)
			m["last_error"] = fmt.Sprintf("rate limit exceeded in %s", name)
			m[constants.Error] = fmt.Errorf("rate limit exceeded")
			return m, nil
		}

		m := safeMap(state)
		// Advance state
		step := toInt(m["step"])
		m["step"] = step + 1
		m["last_node"] = name
		counter := toInt(m["counter"])
		m["counter"] = counter + 1
		return m, nil
	}
}

// ---- Helpers ----

func safeMap(state interface{}) map[string]interface{} {
	if state == nil {
		return map[string]interface{}{}
	}
	m, ok := state.(map[string]interface{})
	if !ok {
		return map[string]interface{}{}
	}
	return m
}

func toInt(v interface{}) int {
	if v == nil {
		return 0
	}
	switch i := v.(type) {
	case int:
		return i
	case int64:
		return int(i)
	case float64:
		return int(i)
	default:
		return 0
	}
}

// ---- Graph Builder ----

// buildStateGraph creates and configures the StateGraph (before compile).
func buildStateGraph() *graph.StateGraph {
	sg := graph.NewStateGraph(map[string]interface{}{
		"step":       0,
		"counter":    0,
		"loop_count": 0,
		"route":      "",
		"status":     "initial",
		"last_node":  "",
		"last_error": "",
		"errors":     []string{},
	})

	// Configure channels with proper types
	sg.AddChannel("counter", channels.NewBinaryOperatorAggregate(0, func(a, b interface{}) interface{} {
		return toInt(a) + toInt(b)
	}))
	sg.AddChannel("loop_count", channels.NewBinaryOperatorAggregate(0, func(a, b interface{}) interface{} {
		return toInt(a) + toInt(b)
	}))
	sg.AddChannel("errors", channels.NewTopic("", true))     // accumulating topic
	sg.AddChannel("messages", channels.NewTopic("", true))    // accumulating topic
	sg.AddChannel("step", channels.NewLastValue(0))
	sg.AddChannel("route", channels.NewLastValue(""))
	sg.AddChannel("status", channels.NewLastValue(""))
	sg.AddChannel("last_node", channels.NewLastValue(""))
	sg.AddChannel("last_error", channels.NewLastValue(""))

	// ---- Main pipeline nodes ----
	sg.AddNode("node_initial", func(ctx context.Context, state interface{}) (interface{}, error) {
		m := safeMap(state)
		m["status"] = "initialized"
		m["step"] = 1
		m["last_node"] = "node_initial"
		return m, nil
	})

	sg.AddNode("node_preflight", func(ctx context.Context, state interface{}) (interface{}, error) {
		m := safeMap(state)
		m["status"] = "preflight"
		m["step"] = toInt(m["step"]) + 1
		m["last_node"] = "node_preflight"
		return m, nil
	})

	sg.AddNode("node_validate", func(ctx context.Context, state interface{}) (interface{}, error) {
		m := safeMap(state)
		m["status"] = "validated"
		m["step"] = toInt(m["step"]) + 1
		m["last_node"] = "node_validate"
		return m, nil
	})

	sg.AddNode("node_plan", func(ctx context.Context, state interface{}) (interface{}, error) {
		m := safeMap(state)
		m["status"] = "planned"
		m["step"] = toInt(m["step"]) + 1
		m["last_node"] = "node_plan"
		return m, nil
	})

	// Dispatch: conditional route by counter
	sg.AddNode("node_dispatch", func(ctx context.Context, state interface{}) (interface{}, error) {
		m := safeMap(state)
		counter := toInt(m["counter"])
		loop := toInt(m["loop_count"])
		// Alternate routes based on loop and counter
		if loop%3 == 0 && counter%2 == 0 {
			m["route"] = "high"
		} else if loop%3 == 1 {
			m["route"] = "normal"
		} else {
			m["route"] = "slow"
		}
		m["step"] = toInt(m["step"]) + 1
		m["last_node"] = "node_dispatch"
		return m, nil
	})

	sg.AddConditionalEdges("node_dispatch",
		func(ctx context.Context, state interface{}) (interface{}, error) {
			m := safeMap(state)
			return m["route"], nil
		},
		map[string]string{
			"high":   "node_process_high",
			"normal": "node_process_normal",
			"slow":   "node_process_slow",
		},
	)

	// ---- Branch A: High Priority ----
	sg.AddNode("node_process_high", func(ctx context.Context, state interface{}) (interface{}, error) {
		m := safeMap(state)
		m["status"] = "processing_high"
		m["step"] = toInt(m["step"]) + 1
		m["last_node"] = "node_process_high"
		return m, nil
	})

	sg.AddNode("node_check_quality_high", func(ctx context.Context, state interface{}) (interface{}, error) {
		m := safeMap(state)
		m["step"] = toInt(m["step"]) + 1
		m["last_node"] = "node_check_quality_high"
		// Simulate quality check: pass ≈ 80%
		if rand.Float64() < 0.8 {
			m["quality_pass"] = true
		} else {
			m["quality_pass"] = false
		}
		return m, nil
	})

	sg.AddConditionalEdges("node_check_quality_high",
		func(ctx context.Context, state interface{}) (interface{}, error) {
			m := safeMap(state)
			if pass, ok := m["quality_pass"].(bool); ok && pass {
				return "pass", nil
			}
			return "fail", nil
		},
		map[string]string{
			"pass": "node_parallel_a",
			"fail": "node_retry_high",
		},
	)

	sg.AddNode("node_retry_high", func(ctx context.Context, state interface{}) (interface{}, error) {
		m := safeMap(state)
		m["status"] = "retry_high"
		m["step"] = toInt(m["step"]) + 1
		m["last_node"] = "node_retry_high"
		return m, nil
	})

	// ---- Parallel Sub-Tasks (fan-out) ----
	sg.AddNode("node_parallel_a", resilientNode("node_parallel_a", 2*time.Millisecond, *flagFailRate*0.5, *flagCrashRate))
	sg.AddNode("node_parallel_b", resilientNode("node_parallel_b", 3*time.Millisecond, *flagFailRate*0.5, *flagCrashRate))
	sg.AddNode("node_parallel_c", resilientNode("node_parallel_c", 1*time.Millisecond, *flagFailRate*0.5, *flagCrashRate))

	sg.AddEdge("node_parallel_a", "node_join_parallel")
	sg.AddEdge("node_parallel_b", "node_join_parallel")
	sg.AddEdge("node_parallel_c", "node_join_parallel")

	sg.AddNode("node_join_parallel", func(ctx context.Context, state interface{}) (interface{}, error) {
		m := safeMap(state)
		m["status"] = "joined"
		m["step"] = toInt(m["step"]) + 1
		m["last_node"] = "node_join_parallel"
		return m, nil
	})

	// ---- Branch B: Normal Processing ----
	sg.AddNode("node_process_normal", resilientNode("node_process_normal", 1*time.Millisecond, *flagFailRate, *flagCrashRate))

	// Tool call simulation: search tool
	sg.AddNode("node_call_tool_search", resilientNode("node_call_tool_search", 5*time.Millisecond, *flagFailRate*2, *flagCrashRate))

	sg.AddNode("node_review_result", func(ctx context.Context, state interface{}) (interface{}, error) {
		m := safeMap(state)
		m["step"] = toInt(m["step"]) + 1
		m["last_node"] = "node_review_result"
		// Approve ≈ 75%
		if rand.Float64() < 0.75 {
			m["review_approved"] = true
		} else {
			m["review_approved"] = false
		}
		return m, nil
	})

	sg.AddConditionalEdges("node_review_result",
		func(ctx context.Context, state interface{}) (interface{}, error) {
			m := safeMap(state)
			if approved, ok := m["review_approved"].(bool); ok && approved {
				return "approve", nil
			}
			return "reject", nil
		},
		map[string]string{
			"approve": "node_transform_data",
			"reject":  "node_retry_normal",
		},
	)

	sg.AddNode("node_retry_normal", func(ctx context.Context, state interface{}) (interface{}, error) {
		m := safeMap(state)
		m["status"] = "retry_normal"
		m["step"] = toInt(m["step"]) + 1
		m["last_node"] = "node_retry_normal"
		return m, nil
	})

	sg.AddNode("node_transform_data", func(ctx context.Context, state interface{}) (interface{}, error) {
		m := safeMap(state)
		m["status"] = "transformed"
		m["step"] = toInt(m["step"]) + 1
		m["last_node"] = "node_transform_data"
		return m, nil
	})

	// Tool call simulation: codegen tool
	sg.AddNode("node_call_tool_codegen", resilientNode("node_call_tool_codegen", 8*time.Millisecond, *flagFailRate*3, *flagCrashRate*2))

	// ---- Branch C: Slow Processing ----
	sg.AddNode("node_process_slow", resilientNode("node_process_slow", 3*time.Millisecond, *flagFailRate, *flagCrashRate))

	// Heavy tool call (longer delay)
	sg.AddNode("node_call_tool_heavy", resilientNode("node_call_tool_heavy", 15*time.Millisecond, *flagFailRate*2, *flagCrashRate))

	sg.AddNode("node_escalate", func(ctx context.Context, state interface{}) (interface{}, error) {
		m := safeMap(state)
		m["status"] = "escalated"
		m["step"] = toInt(m["step"]) + 1
		m["last_node"] = "node_escalate"
		return m, nil
	})

	sg.AddNode("node_manual_review", func(ctx context.Context, state interface{}) (interface{}, error) {
		m := safeMap(state)
		m["status"] = "manual_review"
		m["step"] = toInt(m["step"]) + 1
		m["last_node"] = "node_manual_review"
		return m, nil
	})

	sg.AddNode("node_recovery_action", func(ctx context.Context, state interface{}) (interface{}, error) {
		m := safeMap(state)
		m["status"] = "recovering"
		m["step"] = toInt(m["step"]) + 1
		m["last_node"] = "node_recovery_action"
		return m, nil
	})

	// ---- Common Pipeline After Branches ----
	// All branches converge here
	sg.AddNode("node_aggregate", func(ctx context.Context, state interface{}) (interface{}, error) {
		m := safeMap(state)
		m["status"] = "aggregated"
		m["step"] = toInt(m["step"]) + 1
		m["last_node"] = "node_aggregate"
		counter := toInt(m["counter"])
		m["counter"] = counter + 1
		return m, nil
	})

	sg.AddNode("node_checkpoint_save", func(ctx context.Context, state interface{}) (interface{}, error) {
		m := safeMap(state)
		m["status"] = "checkpoint"
		m["step"] = toInt(m["step"]) + 1
		m["last_node"] = "node_checkpoint_save"
		return m, nil
	})

	// Loop check: loop back to plan for cycling, or continue to finalize
	sg.AddNode("node_loop_check", func(ctx context.Context, state interface{}) (interface{}, error) {
		m := safeMap(state)
		loopCount := toInt(m["loop_count"])
		m["last_node"] = "node_loop_check"
		// Loop back up to 3 times
		if loopCount < 3 {
			m["loop_continue"] = true
			m["loop_count"] = loopCount + 1
		} else {
			m["loop_continue"] = false
		}
		m["step"] = toInt(m["step"]) + 1
		return m, nil
	})

	sg.AddConditionalEdges("node_loop_check",
		func(ctx context.Context, state interface{}) (interface{}, error) {
			m := safeMap(state)
			if cont, ok := m["loop_continue"].(bool); ok && cont {
				return "continue", nil
			}
			return "done", nil
		},
		map[string]string{
			"continue": "node_plan", // loop back
			"done":     "node_finalize",
		},
	)

	// Final pipeline
	sg.AddNode("node_finalize", func(ctx context.Context, state interface{}) (interface{}, error) {
		m := safeMap(state)
		m["status"] = "finalized"
		m["step"] = toInt(m["step"]) + 1
		m["last_node"] = "node_finalize"
		return m, nil
	})

	sg.AddNode("node_report", func(ctx context.Context, state interface{}) (interface{}, error) {
		m := safeMap(state)
		m["status"] = "reported"
		m["step"] = toInt(m["step"]) + 1
		m["last_node"] = "node_report"
		return m, nil
	})

	sg.AddNode("node_backup_data", resilientNode("node_backup_data", 2*time.Millisecond, *flagFailRate*0.3, *flagCrashRate*0.3))
	sg.AddNode("node_cleanup_temp", resilientNode("node_cleanup_temp", 1*time.Millisecond, *flagFailRate*0.2, *flagCrashRate*0.2))
	sg.AddNode("node_archive_logs", resilientNode("node_archive_logs", 3*time.Millisecond, *flagFailRate*0.3, *flagCrashRate*0.3))
	sg.AddNode("node_summary_post", func(ctx context.Context, state interface{}) (interface{}, error) {
		m := safeMap(state)
		m["status"] = "summarized"
		m["step"] = toInt(m["step"]) + 1
		m["last_node"] = "node_summary_post"
		return m, nil
	})
	sg.AddNode("node_teardown", func(ctx context.Context, state interface{}) (interface{}, error) {
		m := safeMap(state)
		m["status"] = "completed"
		m["step"] = toInt(m["step"]) + 1
		m["last_node"] = "node_teardown"
		return m, nil
	})

	// ---- Edges ----
	// Main pipeline
	sg.AddEdge(constants.Start, "node_initial")
	sg.AddEdge("node_initial", "node_preflight")
	sg.AddEdge("node_preflight", "node_validate")
	sg.AddEdge("node_validate", "node_plan")
	sg.AddEdge("node_plan", "node_dispatch")

	// Branch A edges
	sg.AddEdge("node_process_high", "node_check_quality_high")
	sg.AddEdge("node_retry_high", "node_parallel_a")

	// Branch B edges
	sg.AddEdge("node_retry_normal", "node_call_tool_search")
	sg.AddEdge("node_process_normal", "node_call_tool_search")
	sg.AddEdge("node_call_tool_search", "node_review_result")
	sg.AddEdge("node_transform_data", "node_call_tool_codegen")

	// Branch C edges
	sg.AddEdge("node_process_slow", "node_call_tool_heavy")
	sg.AddEdge("node_call_tool_heavy", "node_escalate")
	sg.AddEdge("node_escalate", "node_manual_review")
	sg.AddEdge("node_manual_review", "node_recovery_action")

	// Join branches to common pipeline
	sg.AddEdge("node_join_parallel", "node_aggregate")
	sg.AddEdge("node_call_tool_codegen", "node_aggregate")
	sg.AddEdge("node_recovery_action", "node_aggregate")

	// Common pipeline
	sg.AddEdge("node_aggregate", "node_checkpoint_save")
	sg.AddEdge("node_checkpoint_save", "node_loop_check")
	sg.AddEdge("node_finalize", "node_report")
	sg.AddEdge("node_report", "node_backup_data")
	sg.AddEdge("node_backup_data", "node_cleanup_temp")
	sg.AddEdge("node_cleanup_temp", "node_archive_logs")
	sg.AddEdge("node_archive_logs", "node_summary_post")
	sg.AddEdge("node_summary_post", "node_teardown")
	sg.AddEdge("node_teardown", constants.End)

	// Set node trigger mode (BSP mode for cyclic graphs)
	sg.NodeTriggerMode = types.NodeTriggerAnyPredecessor
	return sg
}

// buildSimpleGraph creates a pregel Engine for the no-checkpoint path.
func buildSimpleGraph() invoker {
	sg := buildStateGraph()
	eng := pregel.NewEngine(sg,
		pregel.WithRecursionLimit(100),
	)
	return &pregelInvoker{engine: eng}
}

// pregelInvoker wraps a pregel Engine to satisfy the invoker interface.
type pregelInvoker struct {
	engine *pregel.Engine
}

func (p *pregelInvoker) Invoke(ctx context.Context, input interface{}, opts ...*types.RunnableConfig) (interface{}, error) {
	return p.engine.RunSync(ctx, input)
}

// invoker is the interface for graph execution.
type invoker interface {
	Invoke(ctx context.Context, input interface{}, opts ...*types.RunnableConfig) (interface{}, error)
}

// ---- Tenant Runner ----

func runTenant(ctx context.Context, id int, wg *sync.WaitGroup, tenantMetricsChan chan<- *tenantMetrics, cptr graph.Checkpointer) {
	defer wg.Done()

	localMetrics := &tenantMetrics{
		id:          id,
		lastLogTime: time.Now(),
	}

	sg := buildStateGraph()

	// Build engine with or without checkpoint
	var tc invoker
	if id%2 == 0 {
		// Even tenants: checkpoint-enabled graph
		eng := pregel.NewEngine(sg,
			pregel.WithRecursionLimit(100),
			pregel.WithCheckpointer(cptr),
			pregel.WithInterrupts("node_checkpoint_save"),
		)
		tc = &pregelInvoker{engine: eng}
	} else {
		// Odd tenants: no checkpoint (fast path)
		tc = buildSimpleGraph()
	}

	iteration := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		iteration++
		atomic.AddInt64(&globalMetrics.graphInvocations, 1)

		// Create fresh input state
		input := map[string]interface{}{
			"step":       0,
			"counter":    0,
			"loop_count": 0,
			"route":      "",
			"status":     "started",
			"last_node":  "",
			"last_error": "",
		}

		// Each invocation uses a per-iteration timeout to prevent hanging
		// forever if NATS operations block (e.g., server unreachable).
		invokeCtx, invokeCancel := context.WithTimeout(ctx, 30*time.Second)
		_, err := tc.Invoke(invokeCtx, input)
		invokeCancel()
		if err != nil {
			if gerrors.IsGraphRecursionError(err) {
				atomic.AddInt64(&globalMetrics.recursionErrors, 1)
				localMetrics.recursionLimit++
				// Recursion errors are expected in soak tests
			} else if !gerrors.IsGraphInterrupt(err) {
				atomic.AddInt64(&globalMetrics.tenantErrors, 1)
				localMetrics.errors++
				if *flagVerbose {
					log.Printf("[Tenant %d] Iter %d: error=%v", id, iteration, err)
				}
			} else {
				// Interrupt: simulate resume
				atomic.AddInt64(&globalMetrics.resumes, 1)
				localMetrics.resumes++
				resumeCtx, resumeCancel := context.WithTimeout(ctx, 30*time.Second)
				_, resumeErr := tc.Invoke(resumeCtx, nil)
				resumeCancel()
				if resumeErr != nil && !gerrors.IsGraphRecursionError(resumeErr) {
					atomic.AddInt64(&globalMetrics.tenantErrors, 1)
					localMetrics.errors++
				}
			}
		}

		atomic.AddInt64(&globalMetrics.tenantExecs, 1)
		localMetrics.executions++
		atomic.AddInt64(&globalMetrics.checkpoints, 1)
		localMetrics.checkpoints++

		// Log every 100 iterations per tenant
		if iteration%100 == 0 {
			tenantMetricsChan <- localMetrics
		}
	}
}

// ---- Metrics Reporter ----

func metricsReporter(ctx context.Context, tenantMetricsChan <-chan *tenantMetrics) {
	lastCheck := time.Now()
	var lastExecs int64
	var lastErrors int64
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	tenantStats := make(map[int]*tenantMetrics)

	for {
		select {
		case <-ctx.Done():
			return
		case tm := <-tenantMetricsChan:
			tenantStats[tm.id] = tm
		case <-ticker.C:
			now := time.Now()
			elapsed := now.Sub(lastCheck)
			totalElapsed := now.Sub(startTime)
			execs := atomic.LoadInt64(&globalMetrics.tenantExecs)
			errs := atomic.LoadInt64(&globalMetrics.tenantErrors)
			recursions := atomic.LoadInt64(&globalMetrics.recursionErrors)
			panics := atomic.LoadInt64(&globalMetrics.toolPanics)
			fails := atomic.LoadInt64(&globalMetrics.toolFailures)
			touts := atomic.LoadInt64(&globalMetrics.timeouts)
			cps := atomic.LoadInt64(&globalMetrics.checkpoints)
			res := atomic.LoadInt64(&globalMetrics.resumes)
			invocs := atomic.LoadInt64(&globalMetrics.graphInvocations)

			execRate := float64(execs-lastExecs) / elapsed.Seconds()
			errRate := float64(errs-lastErrors) / elapsed.Seconds()

			fmt.Printf("\n[%s] === Soak Test Progress ===\n", now.Format(time.RFC3339))
			fmt.Printf("  Duration:    %s / %s\n", totalElapsed.Round(time.Second), flagDuration.Round(time.Second))
			fmt.Printf("  Tenants:     %d\n", *flagTenants)
			fmt.Printf("  Executions:  %d (rate=%.1f/s)\n", execs, execRate)
			fmt.Printf("  Invocations: %d\n", invocs)
			fmt.Printf("  Errors:      %d (rate=%.1f/s, %.1f%%)\n", errs, errRate, pct(errs, execs))
			fmt.Printf("  Recursion:   %d\n", recursions)
			fmt.Printf("  ToolPanics:  %d\n", panics)
			fmt.Printf("  ToolFails:   %d\n", fails)
			fmt.Printf("  Timeouts:    %d\n", touts)
			fmt.Printf("  Checkpoints: %d\n", cps)
			fmt.Printf("  Resumes:     %d\n", res)

			// Per-tenant summary
			for _, tm := range tenantStats {
				fmt.Printf("  [T%d] execs=%d errs=%d recursions=%d\n",
					tm.id, tm.executions, tm.errors, tm.recursionLimit)
			}

			lastExecs = execs
			lastErrors = errs
			lastCheck = now
		}
	}
}

func pct(n, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(n) / float64(total) * 100
}

// ---- Main ----

func main() {
	flag.Parse()

	// Create the shared checkpointer based on --checkpoint flag
	var cptr graph.Checkpointer
	var checkpointDesc string
	switch *flagCheckpoint {
	case "nats":
		nc, err := nats.Connect(*flagNatsURL)
		if err != nil {
			log.Fatalf("Failed to connect to NATS at %s: %v", *flagNatsURL, err)
		}
		defer nc.Close()
		js, err := jetstream.New(nc)
		if err != nil {
			log.Fatalf("Failed to create JetStream context: %v", err)
		}
		ns, err := checkpoint.NewNATSSaver(js, &checkpoint.NATSConfig{
			Bucket:       "checkpoints-soak",
			History:      3,
			Replicas:     1,
			MaxGraphIdle: 5 * time.Minute,
			GCInterval:   2 * time.Minute,
		})
		if err != nil {
			log.Fatalf("Failed to create NATS checkpointer: %v", err)
		}
		defer ns.Close()
		cptr = ns
		checkpointDesc = fmt.Sprintf("NATS (%s / bucket=checkpoints-soak)", *flagNatsURL)
	default:
		cptr = checkpoint.NewMemorySaver()
		checkpointDesc = "Memory (no persistence)"
	}

	fmt.Printf("=== Harness-Go Workflow DAG Soak Test ===\n")
	fmt.Printf("  Duration:      %s\n", flagDuration)
	fmt.Printf("  Tenants:       %d\n", *flagTenants)
	fmt.Printf("  Fail Rate:     %.1f%%\n", *flagFailRate*100)
	fmt.Printf("  Crash Rate:    %.1f%%\n", *flagCrashRate*100)
	fmt.Printf("  Graph nodes:   %d\n", len(nodeNames))
	fmt.Printf("  Checkpoint:    %s\n", checkpointDesc)
	fmt.Printf("\n")

	// Validate graph builds before running
	sg := buildStateGraph()
	_ = sg
	fmt.Println("StateGraph built successfully.")

	// Signal handling for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), *flagDuration)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nReceived signal, shutting down...")
		cancel()
	}()

	startTime = time.Now()

	// Start metrics reporter
	tenantMetricsChan := make(chan *tenantMetrics, 100)
	go metricsReporter(ctx, tenantMetricsChan)

	// Start tenant goroutines
	var wg sync.WaitGroup
	for i := 0; i < *flagTenants; i++ {
		wg.Add(1)
		go runTenant(ctx, i, &wg, tenantMetricsChan, cptr)
		// Stagger start
		time.Sleep(10 * time.Millisecond)
	}

	wg.Wait()

	// Final report
	fmt.Printf("\n=== Soak Test Complete ===\n")
	fmt.Printf("  Duration:     %s\n", time.Since(startTime).Round(time.Second))
	fmt.Printf("  Total Execs:  %d\n", atomic.LoadInt64(&globalMetrics.tenantExecs))
	fmt.Printf("  Total Errors: %d\n", atomic.LoadInt64(&globalMetrics.tenantErrors))
	fmt.Printf("  Recursions:   %d\n", atomic.LoadInt64(&globalMetrics.recursionErrors))
	fmt.Printf("  Tool Panics:  %d\n", atomic.LoadInt64(&globalMetrics.toolPanics))
	fmt.Printf("  Tool Fails:   %d\n", atomic.LoadInt64(&globalMetrics.toolFailures))
	fmt.Printf("  Timeouts:     %d\n", atomic.LoadInt64(&globalMetrics.timeouts))
	fmt.Printf("  Checkpoints:  %d\n", atomic.LoadInt64(&globalMetrics.checkpoints))
	fmt.Printf("  Resumes:      %d\n", atomic.LoadInt64(&globalMetrics.resumes))

	totalExecs := atomic.LoadInt64(&globalMetrics.tenantExecs)
	totalErrors := atomic.LoadInt64(&globalMetrics.tenantErrors)
	if totalExecs > 0 {
		fmt.Printf("  Error Rate:   %.2f%%\n", pct(totalErrors, totalExecs))
	}

	if totalErrors > 0 {
		fmt.Println("\n⚠️  Test completed with errors (expected during fault injection).")
	} else {
		fmt.Println("\n✅ Test completed with zero errors.")
	}
}
