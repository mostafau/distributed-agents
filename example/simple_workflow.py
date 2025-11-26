"""
Example: Simple Local Workflow Execution

This example demonstrates how to create and run a basic workflow
without distributed infrastructure (no Redis required).
"""

import asyncio
from distributed_agents.core import (
    Graph,
    State,
    Orchestrator,
    NodeRegistry,
)


# Define node functions
async def researcher(state: State) -> dict:
    """Research node that gathers information."""
    query = state.get("query", "default topic")
    print(f"[Researcher] Researching: {query}")
    
    # Simulate research
    await asyncio.sleep(0.5)
    
    return {
        "research_results": [
            f"Finding 1 about {query}",
            f"Finding 2 about {query}",
            f"Finding 3 about {query}",
        ],
        "sources": ["source1.com", "source2.com"],
    }


async def analyzer(state: State) -> dict:
    """Analyzer node that processes research results."""
    results = state.get("research_results", [])
    print(f"[Analyzer] Analyzing {len(results)} findings")
    
    await asyncio.sleep(0.3)
    
    return {
        "analysis": f"Analysis of {len(results)} findings: Key insights identified.",
        "confidence": 0.85,
    }


async def writer(state: State) -> dict:
    """Writer node that produces final output."""
    analysis = state.get("analysis", "")
    query = state.get("query", "")
    print(f"[Writer] Writing report for: {query}")
    
    await asyncio.sleep(0.3)
    
    return {
        "report": f"""
# Research Report: {query}

## Analysis
{analysis}

## Findings
{chr(10).join('- ' + f for f in state.get('research_results', []))}

## Sources
{chr(10).join('- ' + s for s in state.get('sources', []))}
""",
        "status": "complete",
    }


async def main():
    # Create registry and register nodes
    registry = NodeRegistry()
    registry.register_node("researcher", researcher)
    registry.register_node("analyzer", analyzer)
    registry.register_node("writer", writer)
    
    # Create workflow graph
    graph = Graph(name="research_workflow", description="A simple research workflow")
    
    # Add nodes
    graph.add_node("researcher")
    graph.add_node("analyzer")
    graph.add_node("writer")
    
    # Define flow
    graph.set_entry_point("researcher")
    graph.add_edge("researcher", "analyzer")
    graph.add_edge("analyzer", "writer")
    graph.set_finish_point("writer")
    
    # Compile
    graph.compile()
    
    print("Graph structure:")
    print(graph.visualize())
    print()
    
    # Create orchestrator and execute
    orchestrator = Orchestrator(registry=registry)
    
    print("Executing workflow...")
    print("-" * 40)
    
    result = await orchestrator.execute(
        graph,
        initial_state={"query": "artificial intelligence trends 2024"}
    )
    
    print("-" * 40)
    print("\nFinal State:")
    print(f"  Status: {result.status}")
    print(f"  Workflow ID: {result.workflow_id}")
    print(f"\nReport:\n{result.get('report', 'No report generated')}")
    
    print("\nExecution History:")
    for execution in result.get_execution_history():
        print(f"  - {execution['node_name']}: {execution['status']}")


if __name__ == "__main__":
    asyncio.run(main())
