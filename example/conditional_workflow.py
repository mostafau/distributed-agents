"""
Example: Workflow with Conditional Routing

This example demonstrates:
- Conditional edges that route based on state
- Loops in workflows (revision cycles)
- Human-in-the-loop pattern (simulated)
"""

import asyncio
import random
from distributed_agents.core import Graph, State, Orchestrator, NodeRegistry
from distributed_agents.core.graph import END


async def draft_writer(state: State) -> dict:
    """Write initial draft."""
    topic = state.get("topic", "unknown")
    revision_count = state.get("revision_count", 0)
    
    print(f"[Writer] Writing draft #{revision_count + 1} for: {topic}")
    await asyncio.sleep(0.3)
    
    # Quality improves with revisions
    quality_score = min(0.5 + (revision_count * 0.2), 0.95)
    
    return {
        "draft": f"Draft #{revision_count + 1} about {topic}. Quality: {quality_score:.2f}",
        "quality_score": quality_score,
        "revision_count": revision_count + 1,
    }


async def reviewer(state: State) -> dict:
    """Review the draft and decide if it needs revision."""
    quality = state.get("quality_score", 0)
    revision_count = state.get("revision_count", 0)
    
    print(f"[Reviewer] Reviewing draft (quality: {quality:.2f}, revisions: {revision_count})")
    await asyncio.sleep(0.2)
    
    # Approve if quality is high enough or max revisions reached
    approved = quality >= 0.8 or revision_count >= 3
    
    return {
        "approved": approved,
        "review_feedback": "Approved!" if approved else "Needs improvement in clarity and depth.",
        "review_decision": "approve" if approved else "revise",
    }


async def publisher(state: State) -> dict:
    """Publish the approved content."""
    draft = state.get("draft", "")
    
    print("[Publisher] Publishing content...")
    await asyncio.sleep(0.2)
    
    return {
        "published": True,
        "published_content": draft,
        "publication_url": "https://example.com/article/123",
    }


def route_after_review(state: dict) -> str:
    """Routing function for conditional edge after review."""
    decision = state.get("review_decision", "revise")
    return decision


async def main():
    # Create registry
    registry = NodeRegistry()
    registry.register_node("writer", draft_writer)
    registry.register_node("reviewer", reviewer)
    registry.register_node("publisher", publisher)
    
    # Create graph with conditional routing
    graph = Graph(
        name="content_pipeline",
        description="Content creation with review loop"
    )
    
    # Add nodes
    graph.add_node("writer")
    graph.add_node("reviewer")
    graph.add_node("publisher")
    
    # Define flow
    graph.set_entry_point("writer")
    graph.add_edge("writer", "reviewer")
    
    # Conditional routing: reviewer decides to approve or request revision
    graph.add_conditional_edge(
        "reviewer",
        route_after_review,
        {
            "approve": "publisher",  # If approved, go to publisher
            "revise": "writer",      # If needs revision, back to writer
        },
        condition_name="review_decision"
    )
    
    graph.set_finish_point("publisher")
    
    # Compile
    graph.compile()
    
    print("Graph structure:")
    print(graph.visualize())
    print()
    
    # Execute
    orchestrator = Orchestrator(registry=registry)
    
    print("Executing content pipeline...")
    print("-" * 50)
    
    result = await orchestrator.execute(
        graph,
        initial_state={"topic": "The Future of AI"}
    )
    
    print("-" * 50)
    print("\nFinal Results:")
    print(f"  Revisions made: {result.get('revision_count', 0)}")
    print(f"  Published: {result.get('published', False)}")
    print(f"  URL: {result.get('publication_url', 'N/A')}")
    
    print("\nExecution path:")
    for i, execution in enumerate(result.get_execution_history()):
        print(f"  {i+1}. {execution['node_name']}")


if __name__ == "__main__":
    asyncio.run(main())
