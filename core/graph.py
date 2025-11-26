"""
Graph definition module for distributed agentic workflows.

A Graph represents a workflow as a directed graph where:
- Nodes are agent functions that process state
- Edges define the flow between nodes
- Conditional edges allow branching based on state
"""

from dataclasses import dataclass, field
from typing import Callable, Any, Optional, Dict, List, Set
from enum import Enum
import json
import hashlib


class NodeType(Enum):
    """Types of nodes in the graph."""
    AGENT = "agent"           # LLM-powered agent node
    TOOL = "tool"             # Tool execution node
    ROUTER = "router"         # Routing/decision node
    HUMAN = "human"           # Human-in-the-loop node
    SUBGRAPH = "subgraph"     # Nested graph node


@dataclass
class Node:
    """
    A node in the workflow graph.
    
    Attributes:
        name: Unique identifier for the node
        func: The function to execute (can be None for remote execution)
        node_type: Type of node (agent, tool, router, etc.)
        retry_policy: Number of retries on failure
        timeout: Execution timeout in seconds
        metadata: Additional node configuration
    """
    name: str
    func: Optional[Callable] = None
    node_type: NodeType = NodeType.AGENT
    retry_policy: int = 3
    timeout: float = 300.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __hash__(self):
        return hash(self.name)
    
    def __eq__(self, other):
        if isinstance(other, Node):
            return self.name == other.name
        return False
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize node for distribution."""
        return {
            "name": self.name,
            "node_type": self.node_type.value,
            "retry_policy": self.retry_policy,
            "timeout": self.timeout,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Node":
        """Deserialize node from dict."""
        return cls(
            name=data["name"],
            node_type=NodeType(data["node_type"]),
            retry_policy=data.get("retry_policy", 3),
            timeout=data.get("timeout", 300.0),
            metadata=data.get("metadata", {}),
        )


@dataclass
class Edge:
    """
    A directed edge between two nodes.
    
    Attributes:
        source: Source node name
        target: Target node name
        metadata: Additional edge configuration
    """
    source: str
    target: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "target": self.target,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Edge":
        return cls(
            source=data["source"],
            target=data["target"],
            metadata=data.get("metadata", {}),
        )


@dataclass
class ConditionalEdge:
    """
    A conditional edge that routes based on state.
    
    Attributes:
        source: Source node name
        condition_func: Function that takes state and returns target node name
        targets: Possible target node names
        condition_name: Name of the condition for serialization
    """
    source: str
    targets: List[str]
    condition_func: Optional[Callable] = None
    condition_name: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "targets": self.targets,
            "condition_name": self.condition_name,
            "type": "conditional",
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ConditionalEdge":
        return cls(
            source=data["source"],
            targets=data["targets"],
            condition_name=data.get("condition_name"),
        )


# Special node names
START = "__start__"
END = "__end__"


class Graph:
    """
    A directed graph representing an agentic workflow.
    
    The graph defines how agents are connected and how state flows
    between them. It can be serialized and distributed across machines.
    
    Example:
        graph = Graph(name="research_workflow")
        graph.add_node("researcher", researcher_agent)
        graph.add_node("writer", writer_agent)
        graph.add_node("reviewer", reviewer_agent)
        
        graph.set_entry_point("researcher")
        graph.add_edge("researcher", "writer")
        graph.add_conditional_edge(
            "reviewer",
            should_revise,
            {"revise": "writer", "approve": END}
        )
    """
    
    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self.nodes: Dict[str, Node] = {}
        self.edges: List[Edge] = []
        self.conditional_edges: List[ConditionalEdge] = []
        self.entry_point: Optional[str] = None
        self._compiled = False
        self._adjacency: Dict[str, List[str]] = {}
        
    def add_node(
        self,
        name: str,
        func: Optional[Callable] = None,
        node_type: NodeType = NodeType.AGENT,
        **kwargs
    ) -> "Graph":
        """Add a node to the graph."""
        if name in self.nodes:
            raise ValueError(f"Node '{name}' already exists")
        
        self.nodes[name] = Node(
            name=name,
            func=func,
            node_type=node_type,
            **kwargs
        )
        self._compiled = False
        return self
    
    def add_edge(self, source: str, target: str, **metadata) -> "Graph":
        """Add a directed edge between two nodes."""
        if source != START and source not in self.nodes:
            raise ValueError(f"Source node '{source}' not found")
        if target != END and target not in self.nodes:
            raise ValueError(f"Target node '{target}' not found")
        
        self.edges.append(Edge(source=source, target=target, metadata=metadata))
        self._compiled = False
        return self
    
    def add_conditional_edge(
        self,
        source: str,
        condition: Callable,
        targets: Dict[str, str],
        condition_name: Optional[str] = None
    ) -> "Graph":
        """
        Add a conditional edge that routes based on state.
        
        Args:
            source: Source node name
            condition: Function(state) -> str that returns the routing key
            targets: Dict mapping routing keys to target node names
        """
        if source not in self.nodes:
            raise ValueError(f"Source node '{source}' not found")
        
        for target in targets.values():
            if target != END and target not in self.nodes:
                raise ValueError(f"Target node '{target}' not found")
        
        # Wrap the condition to map keys to actual targets
        def wrapped_condition(state):
            key = condition(state)
            return targets.get(key, END)
        
        self.conditional_edges.append(ConditionalEdge(
            source=source,
            targets=list(targets.values()),
            condition_func=wrapped_condition,
            condition_name=condition_name or condition.__name__,
        ))
        self._compiled = False
        return self
    
    def set_entry_point(self, node_name: str) -> "Graph":
        """Set the entry point of the graph."""
        if node_name not in self.nodes:
            raise ValueError(f"Node '{node_name}' not found")
        self.entry_point = node_name
        self._compiled = False
        return self
    
    def set_finish_point(self, node_name: str) -> "Graph":
        """Add an edge from node to END."""
        return self.add_edge(node_name, END)
    
    def compile(self) -> "Graph":
        """Compile the graph and validate its structure."""
        if not self.entry_point:
            raise ValueError("Entry point not set")
        
        # Build adjacency list
        self._adjacency = {name: [] for name in self.nodes}
        self._adjacency[START] = [self.entry_point]
        
        for edge in self.edges:
            if edge.source in self._adjacency:
                self._adjacency[edge.source].append(edge.target)
        
        for cond_edge in self.conditional_edges:
            if cond_edge.source in self._adjacency:
                self._adjacency[cond_edge.source].extend(cond_edge.targets)
        
        # Validate: check all nodes are reachable
        reachable = self._find_reachable(START)
        unreachable = set(self.nodes.keys()) - reachable
        if unreachable:
            raise ValueError(f"Unreachable nodes: {unreachable}")
        
        # Validate: check END is reachable
        if END not in reachable:
            raise ValueError("END node is not reachable from any path")
        
        self._compiled = True
        return self
    
    def _find_reachable(self, start: str) -> Set[str]:
        """Find all nodes reachable from start."""
        visited = set()
        stack = [start]
        
        while stack:
            node = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            
            if node in self._adjacency:
                stack.extend(self._adjacency[node])
        
        return visited
    
    def get_next_nodes(self, current: str, state: Dict[str, Any]) -> List[str]:
        """Get the next nodes to execute given current node and state."""
        if not self._compiled:
            self.compile()
        
        # Check conditional edges first
        for cond_edge in self.conditional_edges:
            if cond_edge.source == current and cond_edge.condition_func:
                target = cond_edge.condition_func(state)
                return [target] if target else []
        
        # Fall back to regular edges
        next_nodes = []
        for edge in self.edges:
            if edge.source == current:
                next_nodes.append(edge.target)
        
        return next_nodes
    
    def get_graph_hash(self) -> str:
        """Get a hash representing the graph structure."""
        data = self.to_dict()
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()[:16]
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize the graph to a dictionary."""
        return {
            "name": self.name,
            "description": self.description,
            "nodes": {name: node.to_dict() for name, node in self.nodes.items()},
            "edges": [edge.to_dict() for edge in self.edges],
            "conditional_edges": [ce.to_dict() for ce in self.conditional_edges],
            "entry_point": self.entry_point,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Graph":
        """Deserialize a graph from a dictionary."""
        graph = cls(name=data["name"], description=data.get("description", ""))
        
        for name, node_data in data.get("nodes", {}).items():
            node = Node.from_dict(node_data)
            graph.nodes[name] = node
        
        for edge_data in data.get("edges", []):
            graph.edges.append(Edge.from_dict(edge_data))
        
        for ce_data in data.get("conditional_edges", []):
            graph.conditional_edges.append(ConditionalEdge.from_dict(ce_data))
        
        graph.entry_point = data.get("entry_point")
        return graph
    
    def visualize(self) -> str:
        """Generate a simple ASCII visualization of the graph."""
        lines = [f"Graph: {self.name}", "=" * 40]
        
        if self.entry_point:
            lines.append(f"[START] -> {self.entry_point}")
        
        for edge in self.edges:
            lines.append(f"  {edge.source} -> {edge.target}")
        
        for ce in self.conditional_edges:
            targets = ", ".join(ce.targets)
            lines.append(f"  {ce.source} -> [{targets}] (conditional)")
        
        return "\n".join(lines)
