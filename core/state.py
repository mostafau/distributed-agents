"""
State management for distributed agentic workflows.

State flows through the graph and can be:
- Serialized for transmission between machines
- Checkpointed for fault tolerance
- Merged when parallel branches converge
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TypeVar, Generic
from datetime import datetime
from enum import Enum
import json
import copy
import uuid
import hashlib


class ExecutionStatus(Enum):
    """Status of workflow execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"
    CANCELLED = "cancelled"


@dataclass
class StateCheckpoint:
    """A checkpoint of state at a specific point in execution."""
    checkpoint_id: str
    workflow_id: str
    node_name: str
    state_data: Dict[str, Any]
    timestamp: datetime
    parent_checkpoint_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "checkpoint_id": self.checkpoint_id,
            "workflow_id": self.workflow_id,
            "node_name": self.node_name,
            "state_data": self.state_data,
            "timestamp": self.timestamp.isoformat(),
            "parent_checkpoint_id": self.parent_checkpoint_id,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StateCheckpoint":
        return cls(
            checkpoint_id=data["checkpoint_id"],
            workflow_id=data["workflow_id"],
            node_name=data["node_name"],
            state_data=data["state_data"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            parent_checkpoint_id=data.get("parent_checkpoint_id"),
        )


@dataclass
class NodeExecution:
    """Record of a single node execution."""
    node_name: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: ExecutionStatus = ExecutionStatus.PENDING
    input_state: Dict[str, Any] = field(default_factory=dict)
    output_state: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    retry_count: int = 0
    worker_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_name": self.node_name,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "status": self.status.value,
            "input_state": self.input_state,
            "output_state": self.output_state,
            "error": self.error,
            "retry_count": self.retry_count,
            "worker_id": self.worker_id,
        }


class State:
    """
    Workflow state that flows through the graph.
    
    State is a mutable container that:
    - Holds arbitrary key-value data
    - Tracks execution history
    - Supports checkpointing
    - Can be serialized for distribution
    
    Example:
        state = State(workflow_id="wf_123")
        state["query"] = "What is the weather?"
        state["results"] = []
        
        # After node execution
        state.record_execution("researcher", input_state, output_state)
    """
    
    def __init__(
        self,
        workflow_id: Optional[str] = None,
        graph_name: Optional[str] = None,
        initial_data: Optional[Dict[str, Any]] = None,
    ):
        self.workflow_id = workflow_id or str(uuid.uuid4())
        self.graph_name = graph_name
        self._data: Dict[str, Any] = initial_data or {}
        self._execution_history: List[NodeExecution] = []
        self._checkpoints: List[StateCheckpoint] = []
        self._current_node: Optional[str] = None
        self._status: ExecutionStatus = ExecutionStatus.PENDING
        self._created_at: datetime = datetime.utcnow()
        self._updated_at: datetime = datetime.utcnow()
        self._metadata: Dict[str, Any] = {}
    
    # Dict-like access
    def __getitem__(self, key: str) -> Any:
        return self._data[key]
    
    def __setitem__(self, key: str, value: Any) -> None:
        self._data[key] = value
        self._updated_at = datetime.utcnow()
    
    def __delitem__(self, key: str) -> None:
        del self._data[key]
        self._updated_at = datetime.utcnow()
    
    def __contains__(self, key: str) -> bool:
        return key in self._data
    
    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)
    
    def update(self, data: Dict[str, Any]) -> None:
        """Update state with new data."""
        self._data.update(data)
        self._updated_at = datetime.utcnow()
    
    def keys(self):
        return self._data.keys()
    
    def values(self):
        return self._data.values()
    
    def items(self):
        return self._data.items()
    
    @property
    def data(self) -> Dict[str, Any]:
        """Get a copy of the state data."""
        return copy.deepcopy(self._data)
    
    @property
    def status(self) -> ExecutionStatus:
        return self._status
    
    @status.setter
    def status(self, value: ExecutionStatus) -> None:
        self._status = value
        self._updated_at = datetime.utcnow()
    
    @property
    def current_node(self) -> Optional[str]:
        return self._current_node
    
    @current_node.setter
    def current_node(self, value: Optional[str]) -> None:
        self._current_node = value
        self._updated_at = datetime.utcnow()
    
    def record_execution(
        self,
        node_name: str,
        input_state: Dict[str, Any],
        output_state: Optional[Dict[str, Any]] = None,
        status: ExecutionStatus = ExecutionStatus.COMPLETED,
        error: Optional[str] = None,
        worker_id: Optional[str] = None,
    ) -> None:
        """Record a node execution."""
        execution = NodeExecution(
            node_name=node_name,
            started_at=datetime.utcnow(),
            completed_at=datetime.utcnow(),
            status=status,
            input_state=input_state,
            output_state=output_state or {},
            error=error,
            worker_id=worker_id,
        )
        self._execution_history.append(execution)
        self._updated_at = datetime.utcnow()
    
    def checkpoint(self, node_name: str) -> StateCheckpoint:
        """Create a checkpoint of current state."""
        parent_id = self._checkpoints[-1].checkpoint_id if self._checkpoints else None
        
        checkpoint = StateCheckpoint(
            checkpoint_id=str(uuid.uuid4()),
            workflow_id=self.workflow_id,
            node_name=node_name,
            state_data=copy.deepcopy(self._data),
            timestamp=datetime.utcnow(),
            parent_checkpoint_id=parent_id,
        )
        self._checkpoints.append(checkpoint)
        return checkpoint
    
    def restore_from_checkpoint(self, checkpoint: StateCheckpoint) -> None:
        """Restore state from a checkpoint."""
        self._data = copy.deepcopy(checkpoint.state_data)
        self._current_node = checkpoint.node_name
        self._updated_at = datetime.utcnow()
    
    def get_execution_history(self) -> List[Dict[str, Any]]:
        """Get execution history as list of dicts."""
        return [ex.to_dict() for ex in self._execution_history]
    
    def get_last_execution(self, node_name: Optional[str] = None) -> Optional[NodeExecution]:
        """Get the last execution, optionally filtered by node name."""
        for execution in reversed(self._execution_history):
            if node_name is None or execution.node_name == node_name:
                return execution
        return None
    
    def merge(self, other: "State", strategy: str = "update") -> "State":
        """
        Merge another state into this one.
        
        Strategies:
        - "update": Other state overwrites conflicts
        - "keep": This state keeps conflicts
        - "list": Conflicts become lists of both values
        """
        if strategy == "update":
            self._data.update(other._data)
        elif strategy == "keep":
            for key, value in other._data.items():
                if key not in self._data:
                    self._data[key] = value
        elif strategy == "list":
            for key, value in other._data.items():
                if key in self._data:
                    existing = self._data[key]
                    if isinstance(existing, list):
                        existing.append(value)
                    else:
                        self._data[key] = [existing, value]
                else:
                    self._data[key] = value
        
        # Merge execution history
        self._execution_history.extend(other._execution_history)
        self._updated_at = datetime.utcnow()
        return self
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize state to dictionary."""
        return {
            "workflow_id": self.workflow_id,
            "graph_name": self.graph_name,
            "data": self._data,
            "execution_history": [ex.to_dict() for ex in self._execution_history],
            "checkpoints": [cp.to_dict() for cp in self._checkpoints],
            "current_node": self._current_node,
            "status": self._status.value,
            "created_at": self._created_at.isoformat(),
            "updated_at": self._updated_at.isoformat(),
            "metadata": self._metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "State":
        """Deserialize state from dictionary."""
        state = cls(
            workflow_id=data["workflow_id"],
            graph_name=data.get("graph_name"),
            initial_data=data.get("data", {}),
        )
        state._current_node = data.get("current_node")
        state._status = ExecutionStatus(data.get("status", "pending"))
        state._created_at = datetime.fromisoformat(data["created_at"])
        state._updated_at = datetime.fromisoformat(data["updated_at"])
        state._metadata = data.get("metadata", {})
        
        # Restore checkpoints
        for cp_data in data.get("checkpoints", []):
            state._checkpoints.append(StateCheckpoint.from_dict(cp_data))
        
        return state
    
    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> "State":
        """Deserialize from JSON string."""
        return cls.from_dict(json.loads(json_str))
    
    def get_state_hash(self) -> str:
        """Get a hash of the current state data."""
        return hashlib.sha256(
            json.dumps(self._data, sort_keys=True).encode()
        ).hexdigest()[:16]
    
    def __repr__(self) -> str:
        return f"State(workflow_id={self.workflow_id}, status={self._status.value}, keys={list(self._data.keys())})"


class StateManager:
    """
    Manager for state persistence and retrieval.
    
    This is an abstract interface - concrete implementations
    can use Redis, PostgreSQL, S3, etc.
    """
    
    async def save_state(self, state: State) -> None:
        """Save state to storage."""
        raise NotImplementedError
    
    async def load_state(self, workflow_id: str) -> Optional[State]:
        """Load state from storage."""
        raise NotImplementedError
    
    async def delete_state(self, workflow_id: str) -> None:
        """Delete state from storage."""
        raise NotImplementedError
    
    async def list_states(
        self,
        graph_name: Optional[str] = None,
        status: Optional[ExecutionStatus] = None,
        limit: int = 100,
    ) -> List[State]:
        """List states matching criteria."""
        raise NotImplementedError
    
    async def save_checkpoint(self, checkpoint: StateCheckpoint) -> None:
        """Save a checkpoint."""
        raise NotImplementedError
    
    async def load_checkpoint(self, checkpoint_id: str) -> Optional[StateCheckpoint]:
        """Load a checkpoint."""
        raise NotImplementedError
    
    async def get_checkpoints(self, workflow_id: str) -> List[StateCheckpoint]:
        """Get all checkpoints for a workflow."""
        raise NotImplementedError
