# DeerFlow — Multi-Agent Research System

A multi-agent research collaboration system ported from [bytedance/deer-flow](https://github.com/bytedance/deer-flow) to the **harness-go** framework.

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

## Quick Start

### 1. Start the Backend Server

```bash
cd examples/deer-flow/backend

# 使用内置 Mock 模型（无需 API key）：
make server

# 使用真实 LLM：
# OPENAI_API_KEY="sk-xxx" OPENAI_MODEL="gpt-4o" make server
```

服务器监听 `http://0.0.0.0:8001`（局域网可访问）。

### 2. Start the Frontend

```bash
cd examples/deer-flow/frontend

# 安装依赖（需要 pnpm。如未安装：npm install -g pnpm）
pnpm install

# 启动（无需任何配置，已跳过登录认证，直接进入 workspace）
#
#   dev 模式:  pnpm dev                # 自动绑定 0.0.0.0:3000（无 HMR WebSocket 问题）
#   生产模式:  pnpm build && pnpm start -H 0.0.0.0   # 推荐远程访问方式
pnpm dev
```

前端监听 `http://0.0.0.0:3000`，打开浏览器访问即可直接使用。

> **注意**：使用 `pnpm`，非 `npm`。

### 运行测试

```bash
cd examples/deer-flow/backend && go test -v ./...
```

## Configuration

### 后端环境变量

| 变量 | 默认值 | 说明 |
|----------|---------|-------------|
| `OPENAI_API_KEY` | — | OpenAI API key（必填，否则使用 mock） |
| `OPENAI_BASE_URL` | `https://api.openai.com/v1` | API 端点 |
| `OPENAI_MODEL` | — | 模型名称（使用 real LLM 时必填） |
| `PORT` | `8001` | 服务器监听端口 |

### 前端环境变量（`.env` 文件）

| 变量 | 说明 |
|------|------|
| `NEXT_PUBLIC_BACKEND_BASE_URL` | 后端 API 地址：`http://localhost:8001` |
| `NEXT_PUBLIC_LANGGRAPH_BASE_URL` | LangGraph API 地址：`http://localhost:8001/api` |
