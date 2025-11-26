"""
Example: Distributed Workflow Execution

This example demonstrates:
- Multi-machine distributed execution using Redis
- Multiple workers processing different node types
- Fault tolerance with checkpointing

Prerequisites:
- Redis running at localhost:6379
- pip install redis

Run this example with multiple terminals:
1. Terminal 1 (Orchestrator): python distributed_workflow.py orchestrator
2. Terminal 2 (Worker 1): python distributed_workflow.py worker researcher
3. Terminal 3 (Worker 2): python distributed_workflow.py worker writer analyzer
"""

import asyncio
import sys
import logging
from typing import Set

from distributed_agents.core import Graph, State
from distributed_agents.distributed.distributed_orchestrator import (
    DistributedOrchestrator,
    DistributedWorker,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

REDIS_URL = "redis://localhost:6379/0"


# Node implementations
async def researcher(state: State) -> dict:
    """Research node - simulates gathering information."""
    query = state.get("query", "default topic")
    logger.info(f"[Researcher] Starting research on: {query}")
    
    # Simulate work
    await asyncio.sleep(2)
    
    return {
        "research_results": [
            {"title": f"Study on {query}", "summary": "Key findings..."},
            {"title": f"Analysis of {query}", "summary": "Important insights..."},
        ],
        "research_complete": True,
    }


async def analyzer(state: State) -> dict:
    """Analyzer node - processes research results."""
    results = state.get("research_results", [])
    logger.info(f"[Analyzer] Analyzing {len(results)} research items")
    
    await asyncio.sleep(1)
    
    return {
        "analysis_summary": f"Analyzed {len(results)} items. Key themes identified.",
        "sentiment": "positive",
        "analysis_complete": True,
    }


async def writer(state: State) -> dict:
    """Writer node - creates final report."""
    query = state.get("query", "")
    analysis = state.get("analysis_summary", "")
    logger.info(f"[Writer] Writing report for: {query}")
    
    await asyncio.sleep(1.5)
    
    return {
        "final_report": f"""
# Report: {query}

## Summary
{analysis}

## Conclusion
Based on the research and analysis, we have identified key trends and insights.
""",
        "report_complete": True,
    }


async def run_orchestrator():
    """Run the orchestrator that manages workflow execution."""
    logger.info("Starting orchestrator...")
    
    # Create distributed orchestrator
    orchestrator = DistributedOrchestrator(
        redis_url=REDIS_URL,
        orchestrator_id="main-orchestrator",
    )
    
    await orchestrator.connect()
    
    # Define workflow graph
    graph = Graph(name="distributed_research")
    graph.add_node("researcher")
    graph.add_node("analyzer")
    graph.add_node("writer")
    
    graph.set_entry_point("researcher")
    graph.add_edge("researcher", "analyzer")
    graph.add_edge("analyzer", "writer")
    graph.set_finish_point("writer")
    graph.compile()
    
    logger.info("Graph compiled. Waiting for workers...")
    logger.info("Run workers in separate terminals:")
    logger.info("  python distributed_workflow.py worker researcher")
    logger.info("  python distributed_workflow.py worker writer analyzer")
    
    # Wait a bit for workers to register
    await asyncio.sleep(3)
    
    # Check for workers
    workers = await orchestrator.get_workers()
    logger.info(f"Found {len(workers)} registered workers")
    
    if not workers:
        logger.warning("No workers found! Make sure workers are running.")
        logger.info("Continuing anyway - tasks will queue until workers connect...")
    
    # Execute workflow
    try:
        logger.info("Submitting workflow...")
        result = await orchestrator.execute(
            graph,
            initial_state={"query": "machine learning trends 2024"},
            timeout=120.0,  # 2 minute timeout
        )
        
        logger.info("Workflow completed!")
        logger.info(f"Status: {result.status}")
        logger.info(f"Final report:\n{result.get('final_report', 'No report')}")
        
    except asyncio.TimeoutError:
        logger.error("Workflow timed out!")
    except Exception as e:
        logger.error(f"Workflow failed: {e}")
    finally:
        await orchestrator.disconnect()


async def run_worker(node_types: Set[str]):
    """Run a worker that processes specific node types."""
    logger.info(f"Starting worker for node types: {node_types}")
    
    # Create distributed worker
    worker = DistributedWorker(
        redis_url=REDIS_URL,
        node_types=node_types,
        capacity=2,  # Can run 2 tasks concurrently
    )
    
    # Register node implementations based on what this worker handles
    if "researcher" in node_types:
        worker.register_node("researcher", researcher)
    if "analyzer" in node_types:
        worker.register_node("analyzer", analyzer)
    if "writer" in node_types:
        worker.register_node("writer", writer)
    
    await worker.start()
    
    logger.info(f"Worker {worker.worker_id} ready and waiting for tasks...")
    logger.info("Press Ctrl+C to stop")
    
    # Keep running until interrupted
    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    finally:
        await worker.stop()
        logger.info("Worker stopped")


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python distributed_workflow.py orchestrator")
        print("  python distributed_workflow.py worker <node_types...>")
        print()
        print("Examples:")
        print("  python distributed_workflow.py orchestrator")
        print("  python distributed_workflow.py worker researcher")
        print("  python distributed_workflow.py worker writer analyzer")
        sys.exit(1)
    
    role = sys.argv[1]
    
    if role == "orchestrator":
        asyncio.run(run_orchestrator())
    elif role == "worker":
        if len(sys.argv) < 3:
            print("Error: Specify node types for worker")
            print("Example: python distributed_workflow.py worker researcher writer")
            sys.exit(1)
        node_types = set(sys.argv[2:])
        asyncio.run(run_worker(node_types))
    else:
        print(f"Unknown role: {role}")
        sys.exit(1)


if __name__ == "__main__":
    main()
