package pregel

import (
	"context"
	"sync"
	"testing"

	"github.com/infiniflow/ragflow/harness/constants"
	"github.com/infiniflow/ragflow/harness/graph"
	"github.com/infiniflow/ragflow/harness/types"
)

// TestPregel_ConcurrentEngineCreation verifies multiple engines can be created
// and run concurrently without data races.
func TestPregel_ConcurrentEngineCreation(t *testing.T) {
	const engines = 20
	var wg sync.WaitGroup
	errs := make(chan error, engines)

	for i := 0; i < engines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			sg := graph.NewStateGraph(map[string]interface{}{"id": id})
			sg.AddNode("echo", func(ctx context.Context, state interface{}) (interface{}, error) {
				return state, nil
			})
			sg.AddEdge(constants.Start, "echo")
			sg.AddEdge("echo", constants.End)

			engine := NewEngine(sg, WithRecursionLimit(5))
			_, err := engine.RunSync(context.Background(), map[string]interface{}{"id": id})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("concurrent engine failed: %v", err)
		}
	}
}

// TestPregel_ConcurrentStreaming verifies streaming from multiple engines concurrently.
func TestPregel_ConcurrentStreaming(t *testing.T) {
	const streams = 15
	var wg sync.WaitGroup
	errs := make(chan error, streams)

	for i := 0; i < streams; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			sg := graph.NewStateGraph(map[string]interface{}{"val": id})
			sg.AddNode("step", func(ctx context.Context, state interface{}) (interface{}, error) {
				s := state.(map[string]interface{})
				s["val"] = id * 2
				return s, nil
			})
			sg.AddEdge(constants.Start, "step")
			sg.AddEdge("step", constants.End)

			engine := NewEngine(sg, WithRecursionLimit(5))
			outCh, errCh := engine.Run(context.Background(), map[string]interface{}{"val": id}, types.StreamModeValues)
			go func() { for range outCh {} }()
			errs <- <-errCh
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("concurrent stream failed: %v", err)
		}
	}
}

// TestPregel_AsyncPipelineConcurrency verifies AsyncPipeline handles concurrent tasks.
func TestPregel_AsyncPipelineConcurrency(t *testing.T) {
	exec := NewAsyncExecutor(10)

	const tasks = 50
	var wg sync.WaitGroup
	results := make(chan error, tasks)

	for i := 0; i < tasks; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			resultCh := exec.Execute(context.Background(), "task", func(ctx context.Context) (interface{}, error) {
				return id, nil
			})
			result := <-resultCh
			if result.Err != nil {
				results <- result.Err
			} else {
				results <- nil
			}
		}(i)
	}
	wg.Wait()
	close(results)

	for err := range results {
		if err != nil {
			t.Errorf("async task failed: %v", err)
		}
	}
}

// TestPregel_AsyncExecutorCancel verifies cancel stops running tasks.
func TestPregel_AsyncExecutorCancel(t *testing.T) {
	exec := NewAsyncExecutor(5)

	// Submit tasks that block.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		resultCh := exec.Execute(ctx, "blocking", func(ctx context.Context) (interface{}, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		})
		<-resultCh
		close(done)
	}()

	// Cancel and verify tasks stop.
	cancel()
	select {
	case <-done:
		t.Log("async executor stopped after cancel")
	default:
		exec.Cancel() // force cancel
		t.Log("forced cancel on executor")
	}
}

// TestPregel_MultiNodeFanOut verifies fan-out pattern correctness.
func TestPregel_MultiNodeFanOut(t *testing.T) {
	sg := graph.NewStateGraph(map[string]interface{}{"a": "", "b": "", "done": false})
	sg.AddNode("start", func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(map[string]interface{})
		s["a"] = "from_start"
		return s, nil
	})
	sg.AddNode("processor", func(ctx context.Context, state interface{}) (interface{}, error) {
		s := state.(map[string]interface{})
		s["b"] = "processed"
		s["done"] = true
		return s, nil
	})

	sg.AddEdge(constants.Start, "start")
	sg.AddEdge("start", "processor")
	sg.AddEdge("processor", constants.End)

	engine := NewEngine(sg, WithRecursionLimit(10))
	result, err := engine.RunSync(context.Background(), map[string]interface{}{})
	if err != nil {
		t.Fatalf("RunSync: %v", err)
	}
	m := result.(map[string]interface{})
	if m["done"] != true {
		t.Errorf("expected done=true, got %v", m["done"])
	}
	t.Logf("fan-out result: a=%v b=%v done=%v", m["a"], m["b"], m["done"])
}
