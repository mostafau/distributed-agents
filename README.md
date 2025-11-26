# Distributed Agents Framework

A Python framework for building distributed agentic AI workflows, similar to LangGraph but designed for multi-machine deployments.

## Features

- **Graph-based Workflows**: Define agent workflows as directed graphs with nodes and edges
- **Conditional Routing**: Route execution based on state using conditional edges
- **Distributed Execution**: Scale across multiple machines using Redis for coordination
- **Fault Tolerance**: Checkpointing and retry policies for resilient execution
- **State Management**: Persistent state with checkpoint/restore capabilities
- **Event-Driven**: Pub/sub messaging for real-time coordination

## Installation

```bash
# Basic installation (local execution only)
pip install distributed-agents

# With Redis support for distributed execution
pip install distributed-agents[redis]

# Development installation
pip install distributed-agents[dev]
```

## Quick Start

### Local Execution

```python
import asyncio
from distributed_agents import Graph, State, Orchestrator, NodeRegistry

# Create registry and register node functions
registry = NodeRegistry()

@registry.register("researcher")
async def researcher(state: State) -> dict:
    query = state.get("query", "")
    return {
        "findings": [f"Finding about {query}"],
        "sources": ["source1.com"]
    }

@registry.register("writer")
async def writer(state: State) -> dict:
    findings = state.get("findings", [])
    return {
        "report": f"Report with {len(findings)} findings"
    }

# Create workflow graph
graph = Graph(name="research_workflow")
graph.add_node("researcher")
graph.add_node("writer")
graph.set_entry_point("researcher")
graph.add_edge("researcher", "writer")
graph.set_finish_point("writer")
graph.compile()

# Execute
async def main():
    orchestrator = Orchestrator(registry=registry)
    result = await orchestrator.execute(
        graph,
        initial_state={"query": "AI trends"}
    )
    print(result.get("report"))

asyncio.run(main())
```

### Conditional Routing

```python
from distributed_agents import Graph
from distributed_agents.core.graph import END

graph = Graph(name="review_workflow")
graph.add_node("writer")
graph.add_node("reviewer")
graph.add_node("publisher")

graph.set_entry_point("writer")
graph.add_edge("writer", "reviewer")

# Route based on review decision
def route_after_review(state: dict) -> str:
    return state.get("decision", "revise")

graph.add_conditional_edge(
    "reviewer",
    route_after_review,
    {
        "approve": "publisher",
        "revise": "writer",  # Loop back
    }
)

graph.set_finish_point("publisher")
graph.compile()
```

### Distributed Execution

Run workflows across multiple machines using Redis:

**Orchestrator (machine 1):**

```python
from distributed_agents.distributed import DistributedOrchestrator

orchestrator = DistributedOrchestrator(
    redis_url="redis://localhost:6379/0"
)
await orchestrator.connect()

result = await orchestrator.execute(
    graph,
    initial_state={"query": "AI trends"}
)
```

**Worker (machine 2):**

```python
from distributed_agents.distributed import DistributedWorker

worker = DistributedWorker(
    redis_url="redis://localhost:6379/0",
    node_types={"researcher", "writer"}
)

@worker.register("researcher")
async def researcher(state):
    return {"findings": [...]}

@worker.register("writer")
async def writer(state):
    return {"report": "..."}

await worker.start()
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                       Orchestrator                          │
│  - Manages workflow lifecycle                               │
│  - Distributes tasks to workers                             │
│  - Tracks execution state                                   │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    Redis (Coordination)                     │
│  - Task Queue (priority-based)                              │
│  - State Storage (persistent)                               │
│  - Node Registry (worker discovery)                         │
│  - Message Bus (events)                                     │
└────────────────────────┬────────────────────────────────────┘
                         │
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  Worker 1   │  │  Worker 2   │  │  Worker 3   │
│ researcher  │  │   writer    │  │  analyzer   │
└─────────────┘  └─────────────┘  └─────────────┘
```

## Core Concepts

### Graph

A `Graph` defines the workflow structure:

```python
graph = Graph(name="my_workflow", description="...")

# Add nodes
graph.add_node("step1", step1_func)
graph.add_node("step2", step2_func)

# Define flow
graph.set_entry_point("step1")
graph.add_edge("step1", "step2")
graph.set_finish_point("step2")

# Must compile before execution
graph.compile()
```

### State

`State` flows through the graph and accumulates data:

```python
state = State(workflow_id="wf_123")
state["query"] = "AI trends"
state["results"] = []

# Access data
query = state.get("query")
state.update({"new_key": "value"})

# Checkpoint for fault tolerance
checkpoint = state.checkpoint("current_node")
```

### Nodes

Nodes are functions that transform state:

```python
async def my_node(state: State) -> dict:
    # Read from state
    input_data = state.get("input")
    
    # Do work
    result = await process(input_data)
    
    # Return updates (merged into state)
    return {"output": result}
```

### Workers

Workers execute node functions:

```python
worker = Worker(
    node_types={"researcher", "writer"},
    capacity=4  # Concurrent tasks
)

# Register implementations
worker.register_node("researcher", researcher_func)
worker.register_node("writer", writer_func)

await worker.start()
```

## Kubernetes Deployment

Example deployment for running distributed workers:

```yaml
# worker-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: agent-worker
spec:
  replicas: 3
  selector:
    matchLabels:
      app: agent-worker
  template:
    metadata:
      labels:
        app: agent-worker
    spec:
      containers:
      - name: worker
        image: your-registry/agent-worker:latest
        env:
        - name: REDIS_URL
          value: "redis://redis-service:6379/0"
        - name: NODE_TYPES
          value: "researcher,writer,analyzer"
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
```

## Configuration

### Redis Connection

```python
# State Manager
state_manager = RedisStateManager(
    redis_url="redis://localhost:6379/0",
    prefix="myapp",           # Key prefix
    state_ttl=86400,          # State expiry (seconds)
)

# Node Registry
registry = RedisNodeRegistry(
    redis_url="redis://localhost:6379/0",
    heartbeat_ttl=30,         # Worker timeout
)

# Task Queue
queue = RedisTaskQueue(
    redis_url="redis://localhost:6379/0",
    result_ttl=3600,          # Result expiry
)
```

### Worker Configuration

```python
worker = DistributedWorker(
    redis_url="redis://localhost:6379/0",
    worker_id="worker-1",           # Unique ID
    node_types={"researcher"},      # Handled node types
    capacity=4,                     # Concurrent tasks
    heartbeat_interval=10.0,        # Heartbeat frequency
)
```

## Examples

See the `examples/` directory for complete examples:

- `simple_workflow.py` - Basic local workflow
- `conditional_workflow.py` - Workflows with routing
- `distributed_workflow.py` - Multi-machine execution
- `llm_agents.py` - LLM-powered agents with tools

## API Reference

### Graph

| Method | Description |
|--------|-------------|
| `add_node(name, func, node_type)` | Add a node to the graph |
| `add_edge(source, target)` | Add a directed edge |
| `add_conditional_edge(source, condition, targets)` | Add conditional routing |
| `set_entry_point(node)` | Set the starting node |
| `set_finish_point(node)` | Add edge to END |
| `compile()` | Validate and compile the graph |

### State

| Method | Description |
|--------|-------------|
| `get(key, default)` | Get a value from state |
| `update(data)` | Update state with dict |
| `checkpoint(node_name)` | Create a checkpoint |
| `restore_from_checkpoint(cp)` | Restore from checkpoint |
| `to_dict()` / `from_dict()` | Serialization |

### Orchestrator

| Method | Description |
|--------|-------------|
| `execute(graph, initial_state)` | Execute a workflow |
| `get_workflow_status(id)` | Get workflow status |
| `cancel_workflow(id)` | Cancel a running workflow |
| `list_workflows()` | List active workflows |

## License

MIT License - see LICENSE file for details.
