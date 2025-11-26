"""
Distributed Agents Framework
============================

A distributed agentic AI framework for building scalable workflows.

Quick Start
-----------

Local execution (no Redis required):

    from distributed_agents import Graph, State, Orchestrator, NodeRegistry
    
    # Create registry and register nodes
    registry = NodeRegistry()
    
    @registry.register("researcher")
    async def researcher(state):
        return {"results": ["finding1", "finding2"]}
    
    @registry.register("writer")
    async def writer(state):
        return {"report": "Final report..."}
    
    # Create graph
    graph = Graph(name="my_workflow")
    graph.add_node("researcher")
    graph.add_node("writer")
    graph.set_entry_point("researcher")
    graph.add_edge("researcher", "writer")
    graph.set_finish_point("writer")
    graph.compile()
    
    # Execute
    orchestrator = Orchestrator(registry=registry)
    result = await orchestrator.execute(graph, {"query": "AI trends"})

Distributed execution (requires Redis):

    from distributed_agents.distributed import (
        DistributedOrchestrator,
        DistributedWorker,
    )
    
    # On orchestrator machine:
    orchestrator = DistributedOrchestrator(redis_url="redis://localhost:6379")
    await orchestrator.connect()
    result = await orchestrator.execute(graph, {"query": "AI trends"})
    
    # On worker machines:
    worker = DistributedWorker(
        redis_url="redis://localhost:6379",
        node_types={"researcher", "writer"}
    )
    worker.register_node("researcher", researcher_func)
    await worker.start()
"""

__version__ = "0.1.0"

# Core exports
from distributed_agents.core import (
    Graph,
    Node,
    Edge,
    ConditionalEdge,
    State,
    StateManager,
    Worker,
    WorkerPool,
    Orchestrator,
    NodeRegistry,
)

from distributed_agents.core.graph import START, END, NodeType
from distributed_agents.core.state import ExecutionStatus, StateCheckpoint
from distributed_agents.core.worker import Task, TaskResult
from distributed_agents.core.orchestrator import run_workflow

__all__ = [
    # Version
    "__version__",
    # Graph
    "Graph",
    "Node",
    "Edge",
    "ConditionalEdge",
    "START",
    "END",
    "NodeType",
    # State
    "State",
    "StateManager",
    "ExecutionStatus",
    "StateCheckpoint",
    # Worker
    "Worker",
    "WorkerPool",
    "Task",
    "TaskResult",
    # Orchestrator
    "Orchestrator",
    "run_workflow",
    # Registry
    "NodeRegistry",
]
