# Agent Harness Go Server

A Agent Harness API server implementation for Go that is compatible with the Python `agent-harness-sdk`.

## Features

- **Assistants API**: Create, manage, and search graph assistants
- **Threads API**: Manage conversation threads
- **Runs API**: Execute graph runs with async support
- **Store API**: Persistent storage interface (pluggable)
- **Authentication**: Optional API key authentication
- **CORS**: Built-in CORS support for web clients

## Installation

```bash
go get github.com/infiniflow/ragflow/harness/server
```

## Quick Start

```go
package main

import (
    "context"
    "log"

    "github.com/infiniflow/ragflow/harness/constants"
    "github.com/infiniflow/ragflow/harness/graph"
    "github.com/infiniflow/ragflow/harness/server"
)

// Define your state
type MyState struct {
    Message string `json:"message"`
}

func main() {
    // Create a graph
    builder := graph.NewStateGraph(&MyState{})
    
    builder.AddNode("process", func(ctx context.Context, state interface{}) (interface{}, error) {
        // Process the state
        return state, nil
    })
    
    builder.SetEntryPoint("process")
    builder.AddEdge("process", constants.End)
    
    compiledGraph, err := builder.Compile()
    if err != nil {
        log.Fatal(err)
    }
    
    // Create server
    srv := server.NewServer(&server.ServerConfig{
        Host: "0.0.0.0",
        Port: 8123,
    })
    
    // Register graph
    srv.RegisterGraph("my-graph", compiledGraph)
    
    // Start server
    log.Println("Server starting on http://localhost:8123")
    if err := srv.Start(); err != nil {
        log.Fatal(err)
    }
}
```

## API Endpoints

### Health
- `GET /health` - Health check

### Assistants
- `GET /assistants` - List assistants
- `POST /assistants` - Create assistant
- `GET /assistants/{assistant_id}` - Get assistant
- `PATCH /assistants/{assistant_id}` - Update assistant
- `DELETE /assistants/{assistant_id}` - Delete assistant
- `POST /assistants/search` - Search assistants
- `POST /assistants/count` - Count assistants

### Threads
- `GET /threads` - List threads
- `POST /threads` - Create thread
- `GET /threads/{thread_id}` - Get thread
- `PATCH /threads/{thread_id}` - Update thread
- `DELETE /threads/{thread_id}` - Delete thread
- `POST /threads/search` - Search threads
- `POST /threads/count` - Count threads

### Runs
- `POST /runs` - Create run
- `GET /runs/{run_id}` - Get run
- `DELETE /runs/{run_id}` - Delete run
- `POST /threads/{thread_id}/runs` - Create run in thread
- `GET /threads/{thread_id}/runs/{run_id}` - Get run in thread
- `POST /threads/{thread_id}/state` - Get/update thread state
- `POST /threads/{thread_id}/history` - Get thread history

### Store
- `POST /store` - Search store
- `GET /store/namespaces` - List namespaces

## Using with Python SDK

Once the server is running, you can use the Python `agent-harness-sdk` to interact with it:

```python
from agent_harness_sdk import get_client

# Connect to the Go server
client = get_client(url="http://localhost:8123")

# Create an assistant (graphs are auto-registered)
assistants = await client.assistants.search()
agent = assistants[0]

# Create a thread
thread = await client.threads.create()

# Run the graph
input_data = {"message": "Hello, world!"}
run = await client.runs.create(
    thread_id=thread['thread_id'],
    assistant_id=agent['assistant_id'],
    input=input_data
)

# Stream results
async for chunk in client.runs.stream(
    thread_id=thread['thread_id'],
    assistant_id=agent['assistant_id'],
    input=input_data
):
    print(chunk)
```

## Configuration

### Authentication

Enable API key authentication:

```go
srv := server.NewServer(&server.ServerConfig{
    AuthToken: "your-secret-api-key",
})
```

Clients must provide the key via header:
- `X-API-Key: your-secret-api-key`
- Or `Authorization: Bearer your-secret-api-key`

### Custom Store

Implement the `Store` interface for persistent storage:

```go
type MyStore struct {
    // Your implementation
}

func (s *MyStore) Get(ctx context.Context, namespace []string, key string) (server.Item, error) {
    // Implementation
}

func (s *MyStore) Put(ctx context.Context, namespace []string, key string, value server.Item) error {
    // Implementation
}

// ... implement other methods

srv := server.NewServer(&server.ServerConfig{
    Store: &MyStore{},
})
```

## Architecture

The server is designed to be compatible with the Agent Harness Python SDK:

1. **REST API**: Follows the same endpoint structure as the Python server
2. **JSON Schema**: Uses compatible request/response formats
3. **Async Execution**: Graph runs are executed asynchronously
4. **State Management**: Thread state is managed per conversation

## Limitations

Current limitations (to be implemented):
- Streaming responses (SSE) - partially implemented
- Batch runs
- Cron jobs
- Checkpoint persistence (requires custom Store implementation)
- Graph schemas endpoint

## Testing

Run the server tests:

```bash
go test ./server -v
```

Run the example server:

```bash
go run ./server/example/main.go
```

Then test with curl:

```bash
# Health check
curl http://localhost:8123/health

# Create assistant
curl -X POST http://localhost:8123/assistants \
  -H "Content-Type: application/json" \
  -d '{"graph_id": "simple-graph", "name": "Test Assistant"}'

# Create thread
curl -X POST http://localhost:8123/threads

# Create run
curl -X POST http://localhost:8123/runs \
  -H "Content-Type: application/json" \
  -d '{"assistant_id": "<assistant_id>", "input": {"message": "Hello"}}'
```
