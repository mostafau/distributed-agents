"""
Example: LLM-Powered Agentic Workflow

This example demonstrates how to build agentic workflows with:
- LLM-powered decision making
- Tool use patterns
- Multi-step reasoning

Note: This example uses mock LLM calls. Replace with actual LLM API calls
(e.g., OpenAI, Anthropic) for real use.
"""

import asyncio
import json
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from distributed_agents.core import Graph, State, Orchestrator, NodeRegistry
from distributed_agents.core.graph import END, NodeType


# Mock LLM for demonstration (replace with real LLM client)
class MockLLM:
    """Mock LLM that simulates responses."""
    
    async def complete(self, prompt: str, tools: Optional[List[dict]] = None) -> dict:
        """Simulate LLM completion."""
        await asyncio.sleep(0.3)
        
        # Simple pattern matching for demo
        if "search" in prompt.lower():
            if tools:
                return {
                    "content": None,
                    "tool_calls": [
                        {"name": "web_search", "arguments": {"query": "AI trends 2024"}}
                    ]
                }
            return {"content": "I would search for that information."}
        
        if "analyze" in prompt.lower():
            return {"content": "Based on my analysis, the key findings are: 1) AI is advancing rapidly, 2) LLMs are becoming more capable, 3) Agents are the next frontier."}
        
        if "write" in prompt.lower() or "summarize" in prompt.lower():
            return {"content": "# Summary Report\n\nAI is transforming industries through advanced language models and autonomous agents."}
        
        if "decide" in prompt.lower() or "route" in prompt.lower():
            return {"content": "continue", "decision": "continue"}
        
        return {"content": "I understand. How can I help?"}


# Tools that agents can use
class Tools:
    """Collection of tools for agents."""
    
    @staticmethod
    async def web_search(query: str) -> List[dict]:
        """Simulate web search."""
        await asyncio.sleep(0.5)
        return [
            {"title": f"Result 1 for '{query}'", "snippet": "Important finding about the topic..."},
            {"title": f"Result 2 for '{query}'", "snippet": "Another relevant piece of information..."},
        ]
    
    @staticmethod
    async def calculator(expression: str) -> float:
        """Evaluate a mathematical expression."""
        try:
            return eval(expression)
        except:
            return 0.0
    
    @staticmethod
    async def save_document(content: str, filename: str) -> str:
        """Simulate saving a document."""
        await asyncio.sleep(0.2)
        return f"Document saved: {filename}"


# Agent node implementations
class ResearchAgent:
    """Agent that conducts research using tools."""
    
    def __init__(self, llm: MockLLM):
        self.llm = llm
        self.tools = [
            {
                "name": "web_search",
                "description": "Search the web for information",
                "parameters": {"query": "string"}
            }
        ]
    
    async def __call__(self, state: State) -> dict:
        """Execute research agent."""
        query = state.get("query", "")
        print(f"[ResearchAgent] Researching: {query}")
        
        # Get LLM to decide what to search
        response = await self.llm.complete(
            f"Search for information about: {query}",
            tools=self.tools
        )
        
        # Execute tool calls
        results = []
        if response.get("tool_calls"):
            for tool_call in response["tool_calls"]:
                if tool_call["name"] == "web_search":
                    search_results = await Tools.web_search(
                        tool_call["arguments"]["query"]
                    )
                    results.extend(search_results)
        
        return {
            "search_results": results,
            "research_complete": True,
        }


class AnalysisAgent:
    """Agent that analyzes research results."""
    
    def __init__(self, llm: MockLLM):
        self.llm = llm
    
    async def __call__(self, state: State) -> dict:
        """Execute analysis agent."""
        results = state.get("search_results", [])
        print(f"[AnalysisAgent] Analyzing {len(results)} results")
        
        # Format results for LLM
        results_text = "\n".join(
            f"- {r['title']}: {r['snippet']}"
            for r in results
        )
        
        # Get LLM analysis
        response = await self.llm.complete(
            f"Analyze these search results:\n{results_text}"
        )
        
        return {
            "analysis": response["content"],
            "analysis_complete": True,
        }


class WriterAgent:
    """Agent that writes the final report."""
    
    def __init__(self, llm: MockLLM):
        self.llm = llm
    
    async def __call__(self, state: State) -> dict:
        """Execute writer agent."""
        query = state.get("query", "")
        analysis = state.get("analysis", "")
        print(f"[WriterAgent] Writing report")
        
        # Get LLM to write report
        response = await self.llm.complete(
            f"Write a summary report about '{query}' based on this analysis:\n{analysis}"
        )
        
        return {
            "final_report": response["content"],
            "report_complete": True,
        }


class RouterAgent:
    """Agent that decides workflow routing."""
    
    def __init__(self, llm: MockLLM):
        self.llm = llm
    
    async def __call__(self, state: State) -> dict:
        """Make routing decision."""
        analysis = state.get("analysis", "")
        print("[RouterAgent] Deciding next step")
        
        # Check if we have enough information
        response = await self.llm.complete(
            f"Based on this analysis, decide if we should 'continue' to write a report or 'research_more' to gather additional information:\n{analysis}"
        )
        
        decision = response.get("decision", "continue")
        
        return {
            "routing_decision": decision,
        }


def create_agentic_workflow(llm: MockLLM) -> tuple[Graph, NodeRegistry]:
    """Create an agentic workflow graph."""
    
    # Create agents
    research_agent = ResearchAgent(llm)
    analysis_agent = AnalysisAgent(llm)
    writer_agent = WriterAgent(llm)
    router_agent = RouterAgent(llm)
    
    # Create registry
    registry = NodeRegistry()
    registry.register_node("researcher", research_agent)
    registry.register_node("analyzer", analysis_agent)
    registry.register_node("router", router_agent)
    registry.register_node("writer", writer_agent)
    
    # Create graph
    graph = Graph(
        name="agentic_research",
        description="LLM-powered research workflow"
    )
    
    # Add nodes with types
    graph.add_node("researcher", research_agent, node_type=NodeType.AGENT)
    graph.add_node("analyzer", analysis_agent, node_type=NodeType.AGENT)
    graph.add_node("router", router_agent, node_type=NodeType.ROUTER)
    graph.add_node("writer", writer_agent, node_type=NodeType.AGENT)
    
    # Define flow
    graph.set_entry_point("researcher")
    graph.add_edge("researcher", "analyzer")
    graph.add_edge("analyzer", "router")
    
    # Conditional routing based on router decision
    def route_decision(state: dict) -> str:
        return state.get("routing_decision", "continue")
    
    graph.add_conditional_edge(
        "router",
        route_decision,
        {
            "continue": "writer",
            "research_more": "researcher",
        }
    )
    
    graph.set_finish_point("writer")
    graph.compile()
    
    return graph, registry


async def main():
    # Create LLM (use real LLM client in production)
    llm = MockLLM()
    
    # Create workflow
    graph, registry = create_agentic_workflow(llm)
    
    print("Agentic Workflow Structure:")
    print(graph.visualize())
    print()
    
    # Create orchestrator
    orchestrator = Orchestrator(registry=registry)
    
    # Execute workflow
    print("Executing agentic workflow...")
    print("-" * 50)
    
    result = await orchestrator.execute(
        graph,
        initial_state={"query": "latest developments in AI agents"}
    )
    
    print("-" * 50)
    print("\nWorkflow Results:")
    print(f"  Status: {result.status}")
    print(f"\nFinal Report:\n{result.get('final_report', 'No report')}")
    
    print("\nAgent Execution Path:")
    for i, execution in enumerate(result.get_execution_history()):
        print(f"  {i+1}. {execution['node_name']} ({execution['status']})")


if __name__ == "__main__":
    asyncio.run(main())
