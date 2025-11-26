"""
Worker implementation for distributed task execution.

Workers:
- Register with the orchestrator
- Pull tasks from the queue
- Execute node functions
- Report results back
"""

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Any, Set
from datetime import datetime
import asyncio
import uuid
import socket
import signal
import traceback
import logging

from .state import State, ExecutionStatus
from .registry import NodeRegistry, WorkerInfo, WorkerStatus, get_registry
from .graph import Node

logger = logging.getLogger(__name__)


@dataclass
class Task:
    """A task to be executed by a worker."""
    task_id: str
    workflow_id: str
    node_name: str
    state: State
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    priority: int = 0
    retry_count: int = 0
    max_retries: int = 3
    timeout: float = 300.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "workflow_id": self.workflow_id,
            "node_name": self.node_name,
            "state": self.state.to_dict(),
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "priority": self.priority,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "timeout": self.timeout,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Task":
        return cls(
            task_id=data["task_id"],
            workflow_id=data["workflow_id"],
            node_name=data["node_name"],
            state=State.from_dict(data["state"]),
            created_at=datetime.fromisoformat(data["created_at"]),
            started_at=datetime.fromisoformat(data["started_at"]) if data.get("started_at") else None,
            priority=data.get("priority", 0),
            retry_count=data.get("retry_count", 0),
            max_retries=data.get("max_retries", 3),
            timeout=data.get("timeout", 300.0),
            metadata=data.get("metadata", {}),
        )


@dataclass
class TaskResult:
    """Result of a task execution."""
    task_id: str
    workflow_id: str
    node_name: str
    status: ExecutionStatus
    output_state: State
    error: Optional[str] = None
    error_traceback: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: datetime = field(default_factory=datetime.utcnow)
    worker_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "workflow_id": self.workflow_id,
            "node_name": self.node_name,
            "status": self.status.value,
            "output_state": self.output_state.to_dict(),
            "error": self.error,
            "error_traceback": self.error_traceback,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat(),
            "worker_id": self.worker_id,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskResult":
        return cls(
            task_id=data["task_id"],
            workflow_id=data["workflow_id"],
            node_name=data["node_name"],
            status=ExecutionStatus(data["status"]),
            output_state=State.from_dict(data["output_state"]),
            error=data.get("error"),
            error_traceback=data.get("error_traceback"),
            started_at=datetime.fromisoformat(data["started_at"]) if data.get("started_at") else None,
            completed_at=datetime.fromisoformat(data["completed_at"]),
            worker_id=data.get("worker_id"),
        )


class Worker:
    """
    A worker that executes node tasks.
    
    Workers pull tasks from a queue, execute the corresponding
    node function, and report results back.
    
    Example:
        worker = Worker(
            node_types={"researcher", "writer"},
            capacity=4
        )
        
        # Register node implementations
        worker.register("researcher", researcher_func)
        worker.register("writer", writer_func)
        
        # Start processing
        await worker.start()
    """
    
    def __init__(
        self,
        worker_id: Optional[str] = None,
        node_types: Optional[Set[str]] = None,
        capacity: int = 4,
        hostname: Optional[str] = None,
        port: int = 8000,
        registry: Optional[NodeRegistry] = None,
    ):
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.node_types = node_types or set()
        self.capacity = capacity
        self.hostname = hostname or socket.gethostname()
        self.port = port
        self.registry = registry or get_registry()
        
        self._running = False
        self._current_tasks: Dict[str, Task] = {}
        self._task_queue: asyncio.Queue[Task] = asyncio.Queue()
        self._result_queue: asyncio.Queue[TaskResult] = asyncio.Queue()
        self._shutdown_event = asyncio.Event()
        self._workers: List[asyncio.Task] = []
    
    def register(self, node_name: str) -> Callable:
        """Decorator to register a node implementation."""
        def decorator(func: Callable) -> Callable:
            self.register_node(node_name, func)
            return func
        return decorator
    
    def register_node(self, node_name: str, func: Callable) -> None:
        """Register a node implementation."""
        self.node_types.add(node_name)
        self.registry.register_node(node_name, func)
    
    def get_info(self) -> WorkerInfo:
        """Get worker info for registration."""
        return WorkerInfo(
            worker_id=self.worker_id,
            hostname=self.hostname,
            port=self.port,
            node_types=self.node_types,
            capacity=self.capacity,
            current_load=len(self._current_tasks),
        )
    
    async def submit_task(self, task: Task) -> None:
        """Submit a task for execution."""
        await self._task_queue.put(task)
    
    async def get_result(self, timeout: Optional[float] = None) -> Optional[TaskResult]:
        """Get a completed task result."""
        try:
            return await asyncio.wait_for(
                self._result_queue.get(),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            return None
    
    async def execute_task(self, task: Task) -> TaskResult:
        """Execute a single task."""
        task.started_at = datetime.utcnow()
        self._current_tasks[task.task_id] = task
        
        try:
            # Get the node function
            func = self.registry.get_node_func(task.node_name)
            if not func:
                raise ValueError(f"No implementation for node '{task.node_name}'")
            
            # Create a copy of state for execution
            input_state = task.state.data
            
            # Execute with timeout
            if asyncio.iscoroutinefunction(func):
                result = await asyncio.wait_for(
                    func(task.state),
                    timeout=task.timeout
                )
            else:
                # Run sync function in thread pool
                loop = asyncio.get_event_loop()
                result = await asyncio.wait_for(
                    loop.run_in_executor(None, func, task.state),
                    timeout=task.timeout
                )
            
            # Update state with result if it's a dict
            if isinstance(result, dict):
                task.state.update(result)
            elif isinstance(result, State):
                task.state = result
            
            # Record execution
            task.state.record_execution(
                node_name=task.node_name,
                input_state=input_state,
                output_state=task.state.data,
                status=ExecutionStatus.COMPLETED,
                worker_id=self.worker_id,
            )
            
            return TaskResult(
                task_id=task.task_id,
                workflow_id=task.workflow_id,
                node_name=task.node_name,
                status=ExecutionStatus.COMPLETED,
                output_state=task.state,
                started_at=task.started_at,
                worker_id=self.worker_id,
            )
            
        except asyncio.TimeoutError:
            error_msg = f"Task timed out after {task.timeout}s"
            logger.error(f"Task {task.task_id} timed out: {error_msg}")
            
            task.state.record_execution(
                node_name=task.node_name,
                input_state=task.state.data,
                status=ExecutionStatus.FAILED,
                error=error_msg,
                worker_id=self.worker_id,
            )
            
            return TaskResult(
                task_id=task.task_id,
                workflow_id=task.workflow_id,
                node_name=task.node_name,
                status=ExecutionStatus.FAILED,
                output_state=task.state,
                error=error_msg,
                started_at=task.started_at,
                worker_id=self.worker_id,
            )
            
        except Exception as e:
            error_msg = str(e)
            tb = traceback.format_exc()
            logger.error(f"Task {task.task_id} failed: {error_msg}\n{tb}")
            
            task.state.record_execution(
                node_name=task.node_name,
                input_state=task.state.data,
                status=ExecutionStatus.FAILED,
                error=error_msg,
                worker_id=self.worker_id,
            )
            
            return TaskResult(
                task_id=task.task_id,
                workflow_id=task.workflow_id,
                node_name=task.node_name,
                status=ExecutionStatus.FAILED,
                output_state=task.state,
                error=error_msg,
                error_traceback=tb,
                started_at=task.started_at,
                worker_id=self.worker_id,
            )
        
        finally:
            self._current_tasks.pop(task.task_id, None)
    
    async def _worker_loop(self, worker_num: int) -> None:
        """Main worker loop that processes tasks."""
        logger.info(f"Worker loop {worker_num} started")
        
        while not self._shutdown_event.is_set():
            try:
                # Get task with timeout to check shutdown periodically
                try:
                    task = await asyncio.wait_for(
                        self._task_queue.get(),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue
                
                # Execute task
                logger.info(f"Worker {worker_num} executing task {task.task_id}")
                result = await self.execute_task(task)
                
                # Put result in queue
                await self._result_queue.put(result)
                logger.info(f"Worker {worker_num} completed task {task.task_id}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker loop {worker_num} error: {e}")
        
        logger.info(f"Worker loop {worker_num} stopped")
    
    async def start(self) -> None:
        """Start the worker."""
        if self._running:
            return
        
        self._running = True
        self._shutdown_event.clear()
        
        # Register with registry
        await self.registry.register_worker(self.get_info())
        
        # Start worker loops
        for i in range(self.capacity):
            task = asyncio.create_task(self._worker_loop(i))
            self._workers.append(task)
        
        logger.info(f"Worker {self.worker_id} started with {self.capacity} workers")
    
    async def stop(self, timeout: float = 30.0) -> None:
        """Stop the worker gracefully."""
        if not self._running:
            return
        
        logger.info(f"Stopping worker {self.worker_id}...")
        
        # Signal shutdown
        self._shutdown_event.set()
        
        # Wait for workers to finish current tasks
        if self._workers:
            done, pending = await asyncio.wait(
                self._workers,
                timeout=timeout
            )
            
            # Cancel any still running
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        # Unregister from registry
        await self.registry.unregister_worker(self.worker_id)
        
        self._running = False
        self._workers.clear()
        
        logger.info(f"Worker {self.worker_id} stopped")
    
    @property
    def is_running(self) -> bool:
        return self._running
    
    @property
    def current_load(self) -> int:
        return len(self._current_tasks)


class WorkerPool:
    """
    Pool of workers for local parallel execution.
    
    Useful for testing or single-machine deployments.
    """
    
    def __init__(
        self,
        num_workers: int = 4,
        registry: Optional[NodeRegistry] = None,
    ):
        self.num_workers = num_workers
        self.registry = registry or get_registry()
        self._workers: List[Worker] = []
        self._running = False
    
    def register(self, node_name: str) -> Callable:
        """Decorator to register a node implementation."""
        def decorator(func: Callable) -> Callable:
            self.registry.register_node(node_name, func)
            return func
        return decorator
    
    async def start(self) -> None:
        """Start all workers."""
        if self._running:
            return
        
        for i in range(self.num_workers):
            worker = Worker(
                worker_id=f"pool-worker-{i}",
                registry=self.registry,
                capacity=1,
            )
            # Copy registered node types
            worker.node_types = set(self.registry.list_nodes())
            await worker.start()
            self._workers.append(worker)
        
        self._running = True
        logger.info(f"Worker pool started with {self.num_workers} workers")
    
    async def stop(self) -> None:
        """Stop all workers."""
        for worker in self._workers:
            await worker.stop()
        self._workers.clear()
        self._running = False
        logger.info("Worker pool stopped")
    
    async def submit_task(self, task: Task) -> None:
        """Submit a task to the least loaded worker."""
        if not self._workers:
            raise RuntimeError("Worker pool not started")
        
        # Find least loaded worker
        worker = min(self._workers, key=lambda w: w.current_load)
        await worker.submit_task(task)
    
    async def get_result(self, timeout: Optional[float] = None) -> Optional[TaskResult]:
        """Get a result from any worker."""
        if not self._workers:
            return None
        
        # Create tasks to get from all worker result queues
        async def get_from_worker(worker: Worker) -> Optional[TaskResult]:
            return await worker.get_result(timeout=0.1)
        
        end_time = datetime.utcnow().timestamp() + (timeout or float('inf'))
        
        while datetime.utcnow().timestamp() < end_time:
            for worker in self._workers:
                result = await get_from_worker(worker)
                if result:
                    return result
            await asyncio.sleep(0.1)
        
        return None
