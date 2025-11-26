"""
Node registry for distributed node implementations.

The registry allows:
- Workers to register their capabilities
- Orchestrator to discover available node implementations
- Dynamic routing of tasks to appropriate workers
"""

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Any, Set
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import json


class WorkerStatus(Enum):
    """Status of a registered worker."""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DRAINING = "draining"  # Not accepting new work
    OFFLINE = "offline"


@dataclass
class WorkerInfo:
    """Information about a registered worker."""
    worker_id: str
    hostname: str
    port: int
    node_types: Set[str]  # Which node types this worker can handle
    capacity: int  # Max concurrent tasks
    current_load: int = 0
    status: WorkerStatus = WorkerStatus.HEALTHY
    last_heartbeat: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def available_capacity(self) -> int:
        return max(0, self.capacity - self.current_load)
    
    @property
    def is_available(self) -> bool:
        return (
            self.status == WorkerStatus.HEALTHY
            and self.available_capacity > 0
        )
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            "hostname": self.hostname,
            "port": self.port,
            "node_types": list(self.node_types),
            "capacity": self.capacity,
            "current_load": self.current_load,
            "status": self.status.value,
            "last_heartbeat": self.last_heartbeat.isoformat(),
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WorkerInfo":
        return cls(
            worker_id=data["worker_id"],
            hostname=data["hostname"],
            port=data["port"],
            node_types=set(data["node_types"]),
            capacity=data["capacity"],
            current_load=data.get("current_load", 0),
            status=WorkerStatus(data.get("status", "healthy")),
            last_heartbeat=datetime.fromisoformat(data["last_heartbeat"]),
            metadata=data.get("metadata", {}),
        )


@dataclass 
class NodeRegistration:
    """Registration of a node implementation."""
    node_name: str
    func: Optional[Callable] = None  # Local function if registered locally
    worker_ids: Set[str] = field(default_factory=set)  # Workers that can handle this node
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_name": self.node_name,
            "worker_ids": list(self.worker_ids),
            "metadata": self.metadata,
        }


class NodeRegistry:
    """
    Registry for node implementations and workers.
    
    This is a local registry - for distributed scenarios,
    use RedisNodeRegistry or similar.
    
    Example:
        registry = NodeRegistry()
        
        # Register a local node implementation
        @registry.register("researcher")
        async def researcher(state):
            # ... do research
            return {"findings": [...]}
        
        # Or register programmatically
        registry.register_node("writer", writer_func)
    """
    
    def __init__(self):
        self._nodes: Dict[str, NodeRegistration] = {}
        self._workers: Dict[str, WorkerInfo] = {}
        self._conditions: Dict[str, Callable] = {}
        self._lock = asyncio.Lock()
    
    def register(self, node_name: str) -> Callable:
        """Decorator to register a node implementation."""
        def decorator(func: Callable) -> Callable:
            self.register_node(node_name, func)
            return func
        return decorator
    
    def register_node(
        self,
        node_name: str,
        func: Optional[Callable] = None,
        **metadata
    ) -> None:
        """Register a node implementation."""
        if node_name in self._nodes:
            # Update existing registration
            self._nodes[node_name].func = func
            self._nodes[node_name].metadata.update(metadata)
        else:
            self._nodes[node_name] = NodeRegistration(
                node_name=node_name,
                func=func,
                metadata=metadata,
            )
    
    def register_condition(self, name: str, func: Callable) -> None:
        """Register a condition function for conditional edges."""
        self._conditions[name] = func
    
    def get_condition(self, name: str) -> Optional[Callable]:
        """Get a registered condition function."""
        return self._conditions.get(name)
    
    def get_node(self, node_name: str) -> Optional[NodeRegistration]:
        """Get a registered node."""
        return self._nodes.get(node_name)
    
    def get_node_func(self, node_name: str) -> Optional[Callable]:
        """Get the function for a registered node."""
        reg = self._nodes.get(node_name)
        return reg.func if reg else None
    
    def has_node(self, node_name: str) -> bool:
        """Check if a node is registered."""
        return node_name in self._nodes
    
    def list_nodes(self) -> List[str]:
        """List all registered node names."""
        return list(self._nodes.keys())
    
    # Worker management
    async def register_worker(self, worker: WorkerInfo) -> None:
        """Register a worker."""
        async with self._lock:
            self._workers[worker.worker_id] = worker
            
            # Update node registrations with this worker
            for node_type in worker.node_types:
                if node_type not in self._nodes:
                    self._nodes[node_type] = NodeRegistration(node_name=node_type)
                self._nodes[node_type].worker_ids.add(worker.worker_id)
    
    async def unregister_worker(self, worker_id: str) -> None:
        """Unregister a worker."""
        async with self._lock:
            if worker_id in self._workers:
                worker = self._workers[worker_id]
                
                # Remove from node registrations
                for node_type in worker.node_types:
                    if node_type in self._nodes:
                        self._nodes[node_type].worker_ids.discard(worker_id)
                
                del self._workers[worker_id]
    
    async def update_worker_heartbeat(self, worker_id: str) -> None:
        """Update worker's last heartbeat time."""
        async with self._lock:
            if worker_id in self._workers:
                self._workers[worker_id].last_heartbeat = datetime.utcnow()
    
    async def update_worker_load(self, worker_id: str, load: int) -> None:
        """Update worker's current load."""
        async with self._lock:
            if worker_id in self._workers:
                self._workers[worker_id].current_load = load
    
    async def set_worker_status(self, worker_id: str, status: WorkerStatus) -> None:
        """Set worker status."""
        async with self._lock:
            if worker_id in self._workers:
                self._workers[worker_id].status = status
    
    def get_worker(self, worker_id: str) -> Optional[WorkerInfo]:
        """Get worker info."""
        return self._workers.get(worker_id)
    
    def list_workers(self, status: Optional[WorkerStatus] = None) -> List[WorkerInfo]:
        """List workers, optionally filtered by status."""
        workers = list(self._workers.values())
        if status:
            workers = [w for w in workers if w.status == status]
        return workers
    
    def get_workers_for_node(self, node_name: str) -> List[WorkerInfo]:
        """Get available workers that can handle a node type."""
        reg = self._nodes.get(node_name)
        if not reg:
            return []
        
        workers = []
        for worker_id in reg.worker_ids:
            worker = self._workers.get(worker_id)
            if worker and worker.is_available:
                workers.append(worker)
        
        return workers
    
    def select_worker(
        self,
        node_name: str,
        strategy: str = "least_loaded"
    ) -> Optional[WorkerInfo]:
        """
        Select a worker for a node using the specified strategy.
        
        Strategies:
        - "least_loaded": Worker with lowest current load
        - "round_robin": Cycle through workers
        - "random": Random selection
        """
        workers = self.get_workers_for_node(node_name)
        if not workers:
            return None
        
        if strategy == "least_loaded":
            return min(workers, key=lambda w: w.current_load)
        elif strategy == "random":
            import random
            return random.choice(workers)
        else:
            # Default to least loaded
            return min(workers, key=lambda w: w.current_load)
    
    async def cleanup_stale_workers(self, timeout: timedelta = timedelta(minutes=5)) -> List[str]:
        """Remove workers that haven't sent a heartbeat recently."""
        cutoff = datetime.utcnow() - timeout
        stale = []
        
        async with self._lock:
            for worker_id, worker in list(self._workers.items()):
                if worker.last_heartbeat < cutoff:
                    stale.append(worker_id)
        
        for worker_id in stale:
            await self.unregister_worker(worker_id)
        
        return stale
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize registry state."""
        return {
            "nodes": {name: reg.to_dict() for name, reg in self._nodes.items()},
            "workers": {wid: w.to_dict() for wid, w in self._workers.items()},
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NodeRegistry":
        """Deserialize registry state."""
        registry = cls()
        
        for name, reg_data in data.get("nodes", {}).items():
            registry._nodes[name] = NodeRegistration(
                node_name=reg_data["node_name"],
                worker_ids=set(reg_data.get("worker_ids", [])),
                metadata=reg_data.get("metadata", {}),
            )
        
        for wid, worker_data in data.get("workers", {}).items():
            registry._workers[wid] = WorkerInfo.from_dict(worker_data)
        
        return registry


# Global registry instance
_global_registry: Optional[NodeRegistry] = None


def get_registry() -> NodeRegistry:
    """Get the global registry instance."""
    global _global_registry
    if _global_registry is None:
        _global_registry = NodeRegistry()
    return _global_registry


def set_registry(registry: NodeRegistry) -> None:
    """Set the global registry instance."""
    global _global_registry
    _global_registry = registry
