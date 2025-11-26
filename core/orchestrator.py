"""
Orchestrator for distributed workflow execution.

The orchestrator:
- Manages workflow lifecycle
- Distributes tasks to workers
- Tracks execution state
- Handles failures and retries
"""

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Any, Set
from datetime import datetime
import asyncio
import uuid
import logging

from .graph import Graph, START, END
from .state import State, StateManager, ExecutionStatus
from .registry import NodeRegistry, get_registry
from .worker import Worker, WorkerPool, Task, TaskResult

logger = logging.getLogger(__name__)


@dataclass
class WorkflowExecution:
    """Tracks the execution of a workflow."""
    workflow_id: str
    graph: Graph
    state: State
    status: ExecutionStatus = ExecutionStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    current_nodes: Set[str] = field(default_factory=set)
    pending_tasks: Dict[str, Task] = field(default_factory=dict)
    completed_nodes: Set[str] = field(default_factory=set)
    error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "graph_name": self.graph.name,
            "state": self.state.to_dict(),
            "status": self.status.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "current_nodes": list(self.current_nodes),
            "completed_nodes": list(self.completed_nodes),
            "error": self.error,
        }


class Orchestrator:
    """
    Central orchestrator for workflow execution.
    
    The orchestrator coordinates workflow execution across
    distributed workers, handling:
    - Task distribution
    - State management
    - Failure recovery
    - Checkpointing
    
    Example:
        orchestrator = Orchestrator()
        
        # Define and compile graph
        graph = Graph(name="research")
        graph.add_node("researcher", researcher_func)
        graph.add_node("writer", writer_func)
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
        registry: Optional[NodeRegistry] = None,
        state_manager: Optional[StateManager] = None,
        worker_pool: Optional[WorkerPool] = None,
        max_concurrent_workflows: int = 100,
    ):
        self.registry = registry or get_registry()
        self.state_manager = state_manager
        self.worker_pool = worker_pool
        self.max_concurrent_workflows = max_concurrent_workflows
        
        self._workflows: Dict[str, WorkflowExecution] = {}
        self._graphs: Dict[str, Graph] = {}
        self._running = False
        self._lock = asyncio.Lock()
    
    def register_graph(self, graph: Graph) -> None:
        """Register a graph for execution."""
        if not graph._compiled:
            graph.compile()
        self._graphs[graph.name] = graph
    
    async def start(self) -> None:
        """Start the orchestrator."""
        if self._running:
            return
        
        # Start worker pool if we have one
        if self.worker_pool:
            await self.worker_pool.start()
        
        self._running = True
        logger.info("Orchestrator started")
    
    async def stop(self) -> None:
        """Stop the orchestrator."""
        if not self._running:
            return
        
        # Stop worker pool
        if self.worker_pool:
            await self.worker_pool.stop()
        
        self._running = False
        logger.info("Orchestrator stopped")
    
    async def execute(
        self,
        graph: Graph,
        initial_state: Optional[Dict[str, Any]] = None,
        workflow_id: Optional[str] = None,
        wait: bool = True,
        checkpoint_interval: int = 1,
    ) -> State:
        """
        Execute a workflow.
        
        Args:
            graph: The graph to execute
            initial_state: Initial state data
            workflow_id: Optional workflow ID
            wait: If True, wait for completion
            checkpoint_interval: Checkpoint every N nodes
            
        Returns:
            Final state after execution
        """
        # Compile graph if needed
        if not graph._compiled:
            graph.compile()
        
        # Create workflow execution
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
        
        logger.info(f"Starting workflow {workflow_id} for graph '{graph.name}'")
        
        if wait:
            return await self._run_workflow(execution, checkpoint_interval)
        else:
            asyncio.create_task(self._run_workflow(execution, checkpoint_interval))
            return state
    
    async def _run_workflow(
        self,
        execution: WorkflowExecution,
        checkpoint_interval: int,
    ) -> State:
        """Run a workflow to completion."""
        execution.status = ExecutionStatus.RUNNING
        execution.started_at = datetime.utcnow()
        execution.state.status = ExecutionStatus.RUNNING
        
        graph = execution.graph
        state = execution.state
        
        try:
            # Start from entry point
            current_nodes = [graph.entry_point]
            nodes_executed = 0
            
            while current_nodes:
                # Filter out END nodes
                current_nodes = [n for n in current_nodes if n != END]
                if not current_nodes:
                    break
                
                # Execute all current nodes (could be parallel)
                execution.current_nodes = set(current_nodes)
                
                # Execute nodes
                results = await self._execute_nodes(
                    execution,
                    current_nodes,
                )
                
                # Check for failures
                failed = [r for r in results if r.status == ExecutionStatus.FAILED]
                if failed:
                    error_msg = "; ".join(f"{r.node_name}: {r.error}" for r in failed)
                    raise RuntimeError(f"Node execution failed: {error_msg}")
                
                # Merge results back into state
                for result in results:
                    state.update(result.output_state.data)
                    execution.completed_nodes.add(result.node_name)
                    nodes_executed += 1
                
                # Checkpoint periodically
                if checkpoint_interval and nodes_executed % checkpoint_interval == 0:
                    checkpoint = state.checkpoint(current_nodes[-1])
                    if self.state_manager:
                        await self.state_manager.save_checkpoint(checkpoint)
                
                # Determine next nodes
                next_nodes = []
                for node_name in current_nodes:
                    next_nodes.extend(graph.get_next_nodes(node_name, state.data))
                
                # Remove duplicates while preserving order
                seen = set()
                current_nodes = []
                for n in next_nodes:
                    if n not in seen:
                        seen.add(n)
                        current_nodes.append(n)
            
            # Workflow completed successfully
            execution.status = ExecutionStatus.COMPLETED
            execution.completed_at = datetime.utcnow()
            state.status = ExecutionStatus.COMPLETED
            
            logger.info(f"Workflow {execution.workflow_id} completed successfully")
            
        except Exception as e:
            execution.status = ExecutionStatus.FAILED
            execution.completed_at = datetime.utcnow()
            execution.error = str(e)
            state.status = ExecutionStatus.FAILED
            
            logger.error(f"Workflow {execution.workflow_id} failed: {e}")
            raise
        
        finally:
            # Save final state
            if self.state_manager:
                await self.state_manager.save_state(state)
            
            # Cleanup
            async with self._lock:
                self._workflows.pop(execution.workflow_id, None)
        
        return state
    
    async def _execute_nodes(
        self,
        execution: WorkflowExecution,
        node_names: List[str],
    ) -> List[TaskResult]:
        """Execute a list of nodes."""
        results = []
        
        # Create tasks for each node
        tasks = []
        for node_name in node_names:
            task = Task(
                task_id=str(uuid.uuid4()),
                workflow_id=execution.workflow_id,
                node_name=node_name,
                state=State(
                    workflow_id=execution.workflow_id,
                    graph_name=execution.graph.name,
                    initial_data=execution.state.data,
                ),
            )
            
            # Get node config for timeout/retries
            node = execution.graph.nodes.get(node_name)
            if node:
                task.timeout = node.timeout
                task.max_retries = node.retry_policy
            
            tasks.append(task)
            execution.pending_tasks[task.task_id] = task
        
        # Execute tasks
        if self.worker_pool:
            # Distributed execution via worker pool
            results = await self._execute_via_pool(tasks)
        else:
            # Local execution
            results = await self._execute_locally(execution.graph, tasks)
        
        # Clean up pending tasks
        for task in tasks:
            execution.pending_tasks.pop(task.task_id, None)
        
        return results
    
    async def _execute_via_pool(self, tasks: List[Task]) -> List[TaskResult]:
        """Execute tasks via worker pool."""
        # Submit all tasks
        for task in tasks:
            await self.worker_pool.submit_task(task)
        
        # Collect results
        results = []
        pending = len(tasks)
        
        while pending > 0:
            result = await self.worker_pool.get_result(timeout=60.0)
            if result:
                results.append(result)
                pending -= 1
        
        return results
    
    async def _execute_locally(
        self,
        graph: Graph,
        tasks: List[Task],
    ) -> List[TaskResult]:
        """Execute tasks locally."""
        results = []
        
        for task in tasks:
            # Get node function from registry or graph
            func = self.registry.get_node_func(task.node_name)
            if not func:
                node = graph.nodes.get(task.node_name)
                if node:
                    func = node.func
            
            if not func:
                results.append(TaskResult(
                    task_id=task.task_id,
                    workflow_id=task.workflow_id,
                    node_name=task.node_name,
                    status=ExecutionStatus.FAILED,
                    output_state=task.state,
                    error=f"No implementation for node '{task.node_name}'",
                ))
                continue
            
            # Execute function
            try:
                input_state = task.state.data
                
                if asyncio.iscoroutinefunction(func):
                    output = await func(task.state)
                else:
                    output = func(task.state)
                
                # Update state with output
                if isinstance(output, dict):
                    task.state.update(output)
                elif isinstance(output, State):
                    task.state = output
                
                task.state.record_execution(
                    node_name=task.node_name,
                    input_state=input_state,
                    output_state=task.state.data,
                    status=ExecutionStatus.COMPLETED,
                )
                
                results.append(TaskResult(
                    task_id=task.task_id,
                    workflow_id=task.workflow_id,
                    node_name=task.node_name,
                    status=ExecutionStatus.COMPLETED,
                    output_state=task.state,
                ))
                
            except Exception as e:
                logger.error(f"Node {task.node_name} failed: {e}")
                
                task.state.record_execution(
                    node_name=task.node_name,
                    input_state=task.state.data,
                    status=ExecutionStatus.FAILED,
                    error=str(e),
                )
                
                results.append(TaskResult(
                    task_id=task.task_id,
                    workflow_id=task.workflow_id,
                    node_name=task.node_name,
                    status=ExecutionStatus.FAILED,
                    output_state=task.state,
                    error=str(e),
                ))
        
        return results
    
    async def get_workflow_status(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Get the status of a workflow."""
        async with self._lock:
            execution = self._workflows.get(workflow_id)
            if execution:
                return execution.to_dict()
        
        # Check state manager for completed workflows
        if self.state_manager:
            state = await self.state_manager.load_state(workflow_id)
            if state:
                return {
                    "workflow_id": workflow_id,
                    "status": state.status.value,
                    "state": state.to_dict(),
                }
        
        return None
    
    async def cancel_workflow(self, workflow_id: str) -> bool:
        """Cancel a running workflow."""
        async with self._lock:
            execution = self._workflows.get(workflow_id)
            if execution:
                execution.status = ExecutionStatus.CANCELLED
                execution.state.status = ExecutionStatus.CANCELLED
                return True
        return False
    
    async def list_workflows(
        self,
        status: Optional[ExecutionStatus] = None,
    ) -> List[Dict[str, Any]]:
        """List workflows, optionally filtered by status."""
        async with self._lock:
            workflows = list(self._workflows.values())
        
        if status:
            workflows = [w for w in workflows if w.status == status]
        
        return [w.to_dict() for w in workflows]


# Convenience function for simple execution
async def run_workflow(
    graph: Graph,
    initial_state: Optional[Dict[str, Any]] = None,
    registry: Optional[NodeRegistry] = None,
) -> State:
    """
    Simple workflow execution without explicit orchestrator.
    
    Example:
        graph = Graph(name="simple")
        graph.add_node("step1", step1_func)
        graph.add_node("step2", step2_func)
        graph.set_entry_point("step1")
        graph.add_edge("step1", "step2")
        graph.set_finish_point("step2")
        
        result = await run_workflow(graph, {"input": "data"})
    """
    orchestrator = Orchestrator(registry=registry)
    return await orchestrator.execute(graph, initial_state)
