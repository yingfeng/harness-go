# DeerFlow — Multi-Agent Research System

A multi-agent research collaboration system implemented using the **harness-go** framework, ported from the [bytedance/deer-flow](https://github.com/bytedance/deer-flow) design.

## Architecture

Seven specialized agents collaborate through a state-driven pipeline:

```
User → Coordinator → Planner → ResearchTeam → [Researcher|Coder] → ResearchTeam (loop)
                                                                       ↓ (all done)
                                                                  Planner → Reporter → END
```

| Agent | Role | Tools |
|-------|------|-------|
| **Coordinator** | Classifies requests: chat or research | `hand_to_planner` |
| **Planner** | Creates a research plan with steps | `create_plan` |
| **Human** | Interactive plan review (approve/edit/cancel) | — |
| **ResearchTeam** | Routes steps to the right executor | — |
| **Researcher** | Gathers information via search | `web_search` |
| **Coder** | Processes data via code execution | `execute_python` |
| **Reporter** | Writes the final research report | — |

## Usage

### With a real LLM (OpenAI-compatible):

```bash
export OPENAI_API_KEY="your-api-key"
export OPENAI_MODEL="gpt-4o"
export OPENAI_BASE_URL="https://api.openai.com/v1"
go run ./examples/deer-flow
```

### With mock model (no API key needed):

```bash
go run ./examples/deer-flow
```

### Run tests:

```bash
cd examples/deer-flow && go test -v ./...
```

## Configuration via Environment

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENAI_API_KEY` | — | OpenAI API key |
| `OPENAI_BASE_URL` | `https://api.openai.com/v1` | API endpoint |
| `OPENAI_MODEL` | — | Model name (required with API key) |

Without `OPENAI_API_KEY`, the system uses a mock model that simulates agent responses for demonstration.
