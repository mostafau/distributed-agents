"""
Distributed orchestrator for multi-machine workflow execution.

This orchestrator uses Redis for:
- State persistence
- Task queue
- Worker registry
- Event messaging

It can scale horizontally with multiple orchestrator instances
coordinating through Redis.
"""

import asyncio
from typing import Dict, List, Optional, Any, Set
from datetime import datetime, timedelta
import uuid
import logging

from ..core.graph import Graph, START, END
from ..core.state import State, ExecutionStatus
from ..core.registry import NodeRegistry, WorkerInfo
from ..core.worker import Task, TaskResult
from ..core.orchestrator import WorkflowExecution

from .redis_backend import RedisStateManager, RedisNodeRegistry, RedisTaskQueue
from .message_bus import RedisMessageBus, Message, MessageType

logger = logging.getLogger(__name__)


class DistributedOrchestrator:
    """
    Distributed orchestrator for multi-machine workflow execution.
    
    Features:
    - Horizontal scaling with multiple orchestrators
    - Redis-backed state and task coordination
    - Automatic worker discovery
    - Event-driven execution
    
    Example:
        orchestrator = DistributedOrchestrator(
            redis_url="redis://localhost:6379/0"
        )
        await orchestrator.connect()
        
        # Define graph
        graph = Graph(name="research")
        graph.add_node("researcher")
        graph.add_node("writer")
        graph.set_entry_point("researcher")
        graph.add_edge("researcher", "writer")
        graph.set_finish_point("writer")
        graph.compile()
        
        # Execute workflow
        result = await orchestrator.execute(
            graph,
            initial_state={"query": "AI trends"}
        )
    """
    
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        orchestrator_id: Optional[str] = None,
        prefix: str = "dagent",
        task_timeout: float = 300.0,
        result_poll_interval: float = 0.5,
        max_concurrent_workflows: int = 100,
    ):
        self.redis_url = redis_url
        self.orchestrator_id = orchestrator_id or f"orch-{uuid.uuid4().hex[:8]}"
        self.prefix = prefix
        self.task_timeout = task_timeout
        self.result_poll_interval = result_poll_interval
        self.max_concurrent_workflows = max_concurrent_workflows
        
        # Initialize backends
        self.state_manager = RedisStateManager(redis_url, prefix)
        self.registry = RedisNodeRegistry(redis_url, prefix)
        self.task_queue = RedisTaskQueue(redis_url, prefix)
        self.message_bus = RedisMessageBus(redis_url, prefix)
        
        # Tracking
        self._workflows: Dict[str, WorkflowExecution] = {}
        self._graphs: Dict[str, Graph] = {}
        self._running = False
        self._lock = asyncio.Lock()
        self._result_watchers: Dict[str, asyncio.Event] = {}
    
    async def connect(self) -> None:
        """Connect to all Redis backends."""
        await self.state_manager.connect()
        await self.registry.connect()
        await self.task_queue.connect()
        await self.message_bus.connect()
        
        # Subscribe to result events
        await self.message_bus.subscribe(
            ["tasks"],
            self._handle_task_event
        )
        
        self._running = True
        logger.info(f"Distributed orchestrator {self.orchestrator_id} connected")
    
    async def disconnect(self) -> None:
        """Disconnect from all backends."""
        self._running = False
        
        await self.message_bus.disconnect()
        await self.task_queue.disconnect()
        await self.registry.disconnect()
        await self.state_manager.disconnect()
        
        logger.info(f"Distributed orchestrator {self.orchestrator_id} disconnected")
    
    async def _handle_task_event(self, message: Message) -> None:
        """Handle task events from message bus."""
        if message.message_type == MessageType.TASK_COMPLETED:
            workflow_id = message.payload.get("workflow_id")
            if workflow_id in self._result_watchers:
                self._result_watchers[workflow_id].set()
        
        elif message.message_type == MessageType.TASK_FAILED:
            workflow_id = message.payload.get("workflow_id")
            if workflow_id in self._result_watchers:
                self._result_watchers[workflow_id].set()
    
    def register_graph(self, graph: Graph) -> None:
        """Register a graph for execution."""
        if not graph._compiled:
            graph.compile()
        self._graphs[graph.name] = graph
    
    async def execute(
        self,
        graph: Graph,
        initial_state: Optional[Dict[str, Any]] = None,
        workflow_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> State:
        """
        Execute a workflow.
        
        Args:
            graph: The graph to execute
            initial_state: Initial state data
            workflow_id: Optional workflow ID
            timeout: Overall workflow timeout
            
        Returns:
            Final state after execution
        """
        # Compile graph if needed
        if not graph._compiled:
            graph.compile()
        
        # Create workflow
        workflow_id = workflow_id or str(uuid.uuid4())
        state = State(
            workflow_id=workflow_id,
            graph_name=graph.name,
            initial_data=initial_state or {},
        )
        
        execution = WorkflowExecution(
            workflow_id=workflow_id,
            graph=graph,
            state=state,
        )
        
        async with self._lock:
            if len(self._workflows) >= self.max_concurrent_workflows:
                raise RuntimeError("Maximum concurrent workflows reached")
            self._workflows[workflow_id] = execution
        
        # Create result watcher
        self._result_watchers[workflow_id] = asyncio.Event()
        
        logger.info(f"Starting distributed workflow {workflow_id}")
        
        try:
            return await asyncio.wait_for(
                self._run_workflow(execution),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            execution.status = ExecutionStatus.FAILED
            execution.error = "Workflow timed out"
            state.status = ExecutionStatus.FAILED
            raise
        finally:
            # Cleanup
            self._result_watchers.pop(workflow_id, None)
            async with self._lock:
                self._workflows.pop(workflow_id, None)
    
    async def _run_workflow(self, execution: WorkflowExecution) -> State:
        """Run a workflow to completion."""
        execution.status = ExecutionStatus.RUNNING
        execution.started_at = datetime.utcnow()
        execution.state.status = ExecutionStatus.RUNNING
        
        graph = execution.graph
        state = execution.state
        workflow_id = execution.workflow_id
        
        try:
            # Save initial state
            await self.state_manager.save_state(state)
            
            # Broadcast workflow started
            await self.message_bus.publish("workflows", Message(
                message_type=MessageType.WORKFLOW_STARTED,
                payload={
                    "workflow_id": workflow_id,
                    "graph_name": graph.name,
                },
                source=self.orchestrator_id,
            ))
            
            # Start from entry point
            current_nodes = [graph.entry_point]
            
            while current_nodes:
                # Filter out END nodes
                current_nodes = [n for n in current_nodes if n != END]
                if not current_nodes:
                    break
                
                execution.current_nodes = set(current_nodes)
                
                # Submit tasks for current nodes
                task_ids = []
                for node_name in current_nodes:
                    task = Task(
                        task_id=str(uuid.uuid4()),
                        workflow_id=workflow_id,
                        node_name=node_name,
                        state=State(
                            workflow_id=workflow_id,
                            graph_name=graph.name,
                            initial_data=state.data,
                        ),
                        timeout=self.task_timeout,
                    )
                    
                    # Get node config
                    node = graph.nodes.get(node_name)
                    if node:
                        task.timeout = node.timeout
                        task.max_retries = node.retry_policy
                    
                    # Submit to queue
                    await self.task_queue.submit_task(task)
                    task_ids.append(task.task_id)
                    execution.pending_tasks[task.task_id] = task
                    
                    # Broadcast task submitted
                    await self.message_bus.publish_task_submitted(
                        task.task_id,
                        workflow_id,
                        node_name,
                    )
                    
                    logger.debug(f"Task {task.task_id} submitted for node {node_name}")
                
                # Wait for all results
                results = await self._wait_for_results(workflow_id, task_ids)
                
                # Check for failures
                failed = [r for r in results if r.status == ExecutionStatus.FAILED]
                if failed:
                    error_msg = "; ".join(f"{r.node_name}: {r.error}" for r in failed)
                    raise RuntimeError(f"Node execution failed: {error_msg}")
                
                # Merge results
                for result in results:
                    state.update(result.output_state.data)
                    execution.completed_nodes.add(result.node_name)
                    execution.pending_tasks.pop(result.task_id, None)
                
                # Checkpoint
                checkpoint = state.checkpoint(current_nodes[-1])
                await self.state_manager.save_checkpoint(checkpoint)
                await self.state_manager.save_state(state)
                
                # Determine next nodes
                next_nodes = []
                for node_name in current_nodes:
                    next_nodes.extend(graph.get_next_nodes(node_name, state.data))
                
                # Deduplicate
                seen = set()
                current_nodes = []
                for n in next_nodes:
                    if n not in seen:
                        seen.add(n)
                        current_nodes.append(n)
            
            # Success
            execution.status = ExecutionStatus.COMPLETED
            execution.completed_at = datetime.utcnow()
            state.status = ExecutionStatus.COMPLETED
            
            await self.state_manager.save_state(state)
            await self.message_bus.publish_workflow_completed(
                workflow_id,
                graph.name,
            )
            
            logger.info(f"Workflow {workflow_id} completed successfully")
            
        except Exception as e:
            execution.status = ExecutionStatus.FAILED
            execution.completed_at = datetime.utcnow()
            execution.error = str(e)
            state.status = ExecutionStatus.FAILED
            
            await self.state_manager.save_state(state)
            
            logger.error(f"Workflow {workflow_id} failed: {e}")
            raise
        
        return state
    
    async def _wait_for_results(
        self,
        workflow_id: str,
        task_ids: List[str],
    ) -> List[TaskResult]:
        """Wait for task results."""
        results = []
        pending = set(task_ids)
        
        event = self._result_watchers.get(workflow_id)
        
        while pending:
            # Check for results
            for task_id in list(pending):
                result = await self.task_queue.get_result(task_id)
                if result:
                    results.append(result)
                    pending.remove(task_id)
            
            if pending:
                # Wait for event or poll interval
                if event:
                    event.clear()
                    try:
                        await asyncio.wait_for(
                            event.wait(),
                            timeout=self.result_poll_interval
                        )
                    except asyncio.TimeoutError:
                        pass
                else:
                    await asyncio.sleep(self.result_poll_interval)
        
        return results
    
    async def get_workflow_status(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Get workflow status."""
        # Check active workflows
        async with self._lock:
            execution = self._workflows.get(workflow_id)
            if execution:
                return execution.to_dict()
        
        # Check persistent state
        state = await self.state_manager.load_state(workflow_id)
        if state:
            return {
                "workflow_id": workflow_id,
                "status": state.status.value,
                "state": state.to_dict(),
            }
        
        return None
    
    async def list_active_workflows(self) -> List[Dict[str, Any]]:
        """List active workflows on this orchestrator."""
        async with self._lock:
            return [ex.to_dict() for ex in self._workflows.values()]
    
    async def get_queue_status(self) -> Dict[str, int]:
        """Get task queue lengths."""
        return await self.task_queue.get_all_queue_lengths()
    
    async def get_workers(self) -> List[Dict[str, Any]]:
        """Get registered workers."""
        # Get from registry
        nodes = await self.registry.list_all_nodes()
        workers = {}
        
        for node in nodes:
            node_workers = await self.registry.get_workers_for_node_async(node)
            for worker in node_workers:
                if worker.worker_id not in workers:
                    workers[worker.worker_id] = worker.to_dict()
        
        return list(workers.values())


class DistributedWorker:
    """
    Distributed worker that pulls tasks from Redis queue.
    
    Example:
        worker = DistributedWorker(
            redis_url="redis://localhost:6379/0",
            node_types={"researcher", "writer"},
        )
        
        @worker.register("researcher")
        async def researcher(state):
            # Do research
            return {"findings": [...]}
        
        await worker.start()
    """
    
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        worker_id: Optional[str] = None,
        node_types: Optional[Set[str]] = None,
        capacity: int = 4,
        prefix: str = "dagent",
        heartbeat_interval: float = 10.0,
    ):
        self.redis_url = redis_url
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.node_types = node_types or set()
        self.capacity = capacity
        self.prefix = prefix
        self.heartbeat_interval = heartbeat_interval
        
        # Initialize backends
        self.registry = RedisNodeRegistry(redis_url, prefix)
        self.task_queue = RedisTaskQueue(redis_url, prefix)
        self.message_bus = RedisMessageBus(redis_url, prefix)
        
        # Local node functions
        self._node_funcs: Dict[str, Any] = {}
        
        # Tracking
        self._running = False
        self._current_tasks: Dict[str, Task] = {}
        self._worker_tasks: List[asyncio.Task] = []
    
    def register(self, node_name: str):
        """Decorator to register a node implementation."""
        def decorator(func):
            self.register_node(node_name, func)
            return func
        return decorator
    
    def register_node(self, node_name: str, func) -> None:
        """Register a node implementation."""
        self.node_types.add(node_name)
        self._node_funcs[node_name] = func
    
    async def connect(self) -> None:
        """Connect to Redis backends."""
        await self.registry.connect()
        await self.task_queue.connect()
        await self.message_bus.connect()
        
        # Register worker
        import socket
        worker_info = WorkerInfo(
            worker_id=self.worker_id,
            hostname=socket.gethostname(),
            port=0,
            node_types=self.node_types,
            capacity=self.capacity,
        )
        await self.registry.register_worker(worker_info)
        
        logger.info(f"Distributed worker {self.worker_id} connected")
    
    async def disconnect(self) -> None:
        """Disconnect from backends."""
        await self.registry.unregister_worker(self.worker_id)
        await self.message_bus.disconnect()
        await self.task_queue.disconnect()
        await self.registry.disconnect()
        
        logger.info(f"Distributed worker {self.worker_id} disconnected")
    
    async def start(self) -> None:
        """Start the worker."""
        if self._running:
            return
        
        await self.connect()
        self._running = True
        
        # Start worker loops
        for i in range(self.capacity):
            task = asyncio.create_task(self._worker_loop(i))
            self._worker_tasks.append(task)
        
        # Start heartbeat
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._worker_tasks.append(heartbeat_task)
        
        logger.info(f"Worker {self.worker_id} started with {self.capacity} workers")
    
    async def stop(self, timeout: float = 30.0) -> None:
        """Stop the worker."""
        if not self._running:
            return
        
        self._running = False
        
        # Wait for tasks to complete
        if self._worker_tasks:
            done, pending = await asyncio.wait(
                self._worker_tasks,
                timeout=timeout
            )
            
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        await self.disconnect()
        self._worker_tasks.clear()
        
        logger.info(f"Worker {self.worker_id} stopped")
    
    async def _worker_loop(self, worker_num: int) -> None:
        """Main worker loop."""
        logger.info(f"Worker loop {worker_num} started")
        
        while self._running:
            try:
                # Get task from queue
                task = await self.task_queue.get_task(
                    self.node_types,
                    timeout=1.0
                )
                
                if not task:
                    continue
                
                self._current_tasks[task.task_id] = task
                
                try:
                    # Execute task
                    result = await self._execute_task(task)
                    
                    # Submit result
                    await self.task_queue.submit_result(result)
                    
                    # Broadcast event
                    if result.status == ExecutionStatus.COMPLETED:
                        await self.message_bus.publish_task_completed(
                            task.task_id,
                            task.workflow_id,
                            task.node_name,
                            self.worker_id,
                        )
                    else:
                        await self.message_bus.publish_task_failed(
                            task.task_id,
                            task.workflow_id,
                            task.node_name,
                            result.error or "Unknown error",
                        )
                
                finally:
                    self._current_tasks.pop(task.task_id, None)
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker loop {worker_num} error: {e}")
        
        logger.info(f"Worker loop {worker_num} stopped")
    
    async def _execute_task(self, task: Task) -> TaskResult:
        """Execute a single task."""
        func = self._node_funcs.get(task.node_name)
        
        if not func:
            return TaskResult(
                task_id=task.task_id,
                workflow_id=task.workflow_id,
                node_name=task.node_name,
                status=ExecutionStatus.FAILED,
                output_state=task.state,
                error=f"No implementation for node '{task.node_name}'",
                worker_id=self.worker_id,
            )
        
        try:
            task.started_at = datetime.utcnow()
            input_state = task.state.data
            
            # Execute with timeout
            if asyncio.iscoroutinefunction(func):
                output = await asyncio.wait_for(
                    func(task.state),
                    timeout=task.timeout
                )
            else:
                loop = asyncio.get_event_loop()
                output = await asyncio.wait_for(
                    loop.run_in_executor(None, func, task.state),
                    timeout=task.timeout
                )
            
            # Update state
            if isinstance(output, dict):
                task.state.update(output)
            elif isinstance(output, State):
                task.state = output
            
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
            error = f"Task timed out after {task.timeout}s"
            logger.error(f"Task {task.task_id} timed out")
            
            return TaskResult(
                task_id=task.task_id,
                workflow_id=task.workflow_id,
                node_name=task.node_name,
                status=ExecutionStatus.FAILED,
                output_state=task.state,
                error=error,
                started_at=task.started_at,
                worker_id=self.worker_id,
            )
        
        except Exception as e:
            import traceback
            error = str(e)
            tb = traceback.format_exc()
            logger.error(f"Task {task.task_id} failed: {error}\n{tb}")
            
            return TaskResult(
                task_id=task.task_id,
                workflow_id=task.workflow_id,
                node_name=task.node_name,
                status=ExecutionStatus.FAILED,
                output_state=task.state,
                error=error,
                error_traceback=tb,
                started_at=task.started_at,
                worker_id=self.worker_id,
            )
    
    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats."""
        while self._running:
            try:
                await self.registry.update_worker_heartbeat(self.worker_id)
                await self.registry.update_worker_load(
                    self.worker_id,
                    len(self._current_tasks)
                )
                
                await self.message_bus.publish_worker_heartbeat(
                    self.worker_id,
                    len(self._current_tasks),
                    "healthy",
                )
                
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
            
            await asyncio.sleep(self.heartbeat_interval)
    
    @property
    def current_load(self) -> int:
        return len(self._current_tasks)
