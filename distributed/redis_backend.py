"""
Redis backend for distributed state and coordination.

Provides:
- State persistence
- Distributed node registry
- Task queue for distributed execution
"""

import json
import asyncio
from typing import Dict, List, Optional, Any, Set
from datetime import datetime, timedelta
import logging

try:
    import redis.asyncio as redis
except ImportError:
    redis = None

from ..core.state import State, StateManager, StateCheckpoint, ExecutionStatus
from ..core.registry import NodeRegistry, WorkerInfo, WorkerStatus, NodeRegistration
from ..core.worker import Task, TaskResult

logger = logging.getLogger(__name__)


class RedisStateManager(StateManager):
    """
    Redis-backed state manager for distributed persistence.
    
    Features:
    - Persistent state storage
    - Checkpoint management
    - State queries by status/graph
    
    Example:
        state_manager = RedisStateManager("redis://localhost:6379/0")
        await state_manager.connect()
        
        await state_manager.save_state(state)
        loaded = await state_manager.load_state(workflow_id)
    """
    
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        prefix: str = "dagent",
        state_ttl: Optional[int] = None,  # Seconds, None = no expiry
    ):
        self.redis_url = redis_url
        self.prefix = prefix
        self.state_ttl = state_ttl
        self._client: Optional[redis.Redis] = None
    
    async def connect(self) -> None:
        """Connect to Redis."""
        if redis is None:
            raise ImportError("redis package required: pip install redis")
        self._client = redis.from_url(self.redis_url)
        await self._client.ping()
        logger.info(f"Connected to Redis at {self.redis_url}")
    
    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self._client:
            await self._client.close()
            self._client = None
    
    def _state_key(self, workflow_id: str) -> str:
        return f"{self.prefix}:state:{workflow_id}"
    
    def _checkpoint_key(self, checkpoint_id: str) -> str:
        return f"{self.prefix}:checkpoint:{checkpoint_id}"
    
    def _workflow_checkpoints_key(self, workflow_id: str) -> str:
        return f"{self.prefix}:workflow_checkpoints:{workflow_id}"
    
    def _status_index_key(self, status: ExecutionStatus) -> str:
        return f"{self.prefix}:index:status:{status.value}"
    
    def _graph_index_key(self, graph_name: str) -> str:
        return f"{self.prefix}:index:graph:{graph_name}"
    
    async def save_state(self, state: State) -> None:
        """Save state to Redis."""
        key = self._state_key(state.workflow_id)
        data = json.dumps(state.to_dict())
        
        if self.state_ttl:
            await self._client.setex(key, self.state_ttl, data)
        else:
            await self._client.set(key, data)
        
        # Update indexes
        await self._client.sadd(
            self._status_index_key(state.status),
            state.workflow_id
        )
        
        if state.graph_name:
            await self._client.sadd(
                self._graph_index_key(state.graph_name),
                state.workflow_id
            )
    
    async def load_state(self, workflow_id: str) -> Optional[State]:
        """Load state from Redis."""
        key = self._state_key(workflow_id)
        data = await self._client.get(key)
        
        if data:
            return State.from_dict(json.loads(data))
        return None
    
    async def delete_state(self, workflow_id: str) -> None:
        """Delete state from Redis."""
        state = await self.load_state(workflow_id)
        if state:
            # Remove from indexes
            await self._client.srem(
                self._status_index_key(state.status),
                workflow_id
            )
            if state.graph_name:
                await self._client.srem(
                    self._graph_index_key(state.graph_name),
                    workflow_id
                )
        
        # Delete state
        await self._client.delete(self._state_key(workflow_id))
        
        # Delete checkpoints
        checkpoint_ids = await self._client.smembers(
            self._workflow_checkpoints_key(workflow_id)
        )
        for cp_id in checkpoint_ids:
            await self._client.delete(self._checkpoint_key(cp_id.decode()))
        await self._client.delete(self._workflow_checkpoints_key(workflow_id))
    
    async def list_states(
        self,
        graph_name: Optional[str] = None,
        status: Optional[ExecutionStatus] = None,
        limit: int = 100,
    ) -> List[State]:
        """List states matching criteria."""
        workflow_ids: Set[str] = set()
        
        if status:
            ids = await self._client.smembers(self._status_index_key(status))
            workflow_ids = {wid.decode() for wid in ids}
        
        if graph_name:
            ids = await self._client.smembers(self._graph_index_key(graph_name))
            graph_ids = {wid.decode() for wid in ids}
            
            if status:
                workflow_ids &= graph_ids
            else:
                workflow_ids = graph_ids
        
        # If no filters, scan for all states
        if not status and not graph_name:
            cursor = 0
            pattern = f"{self.prefix}:state:*"
            while True:
                cursor, keys = await self._client.scan(
                    cursor=cursor,
                    match=pattern,
                    count=100
                )
                for key in keys:
                    # Extract workflow_id from key
                    wid = key.decode().split(":")[-1]
                    workflow_ids.add(wid)
                if cursor == 0:
                    break
        
        # Load states
        states = []
        for wid in list(workflow_ids)[:limit]:
            state = await self.load_state(wid)
            if state:
                states.append(state)
        
        return states
    
    async def save_checkpoint(self, checkpoint: StateCheckpoint) -> None:
        """Save a checkpoint."""
        key = self._checkpoint_key(checkpoint.checkpoint_id)
        data = json.dumps(checkpoint.to_dict())
        
        await self._client.set(key, data)
        
        # Add to workflow's checkpoint list
        await self._client.sadd(
            self._workflow_checkpoints_key(checkpoint.workflow_id),
            checkpoint.checkpoint_id
        )
    
    async def load_checkpoint(self, checkpoint_id: str) -> Optional[StateCheckpoint]:
        """Load a checkpoint."""
        key = self._checkpoint_key(checkpoint_id)
        data = await self._client.get(key)
        
        if data:
            return StateCheckpoint.from_dict(json.loads(data))
        return None
    
    async def get_checkpoints(self, workflow_id: str) -> List[StateCheckpoint]:
        """Get all checkpoints for a workflow."""
        checkpoint_ids = await self._client.smembers(
            self._workflow_checkpoints_key(workflow_id)
        )
        
        checkpoints = []
        for cp_id in checkpoint_ids:
            cp = await self.load_checkpoint(cp_id.decode())
            if cp:
                checkpoints.append(cp)
        
        # Sort by timestamp
        checkpoints.sort(key=lambda c: c.timestamp)
        return checkpoints


class RedisNodeRegistry(NodeRegistry):
    """
    Redis-backed distributed node registry.
    
    Features:
    - Distributed worker registration
    - Automatic worker health checks
    - Node type discovery
    
    Example:
        registry = RedisNodeRegistry("redis://localhost:6379/0")
        await registry.connect()
        
        await registry.register_worker(worker_info)
        workers = registry.get_workers_for_node("researcher")
    """
    
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        prefix: str = "dagent",
        heartbeat_ttl: int = 30,  # Seconds before worker considered dead
    ):
        super().__init__()
        self.redis_url = redis_url
        self.prefix = prefix
        self.heartbeat_ttl = heartbeat_ttl
        self._client: Optional[redis.Redis] = None
    
    async def connect(self) -> None:
        """Connect to Redis."""
        if redis is None:
            raise ImportError("redis package required: pip install redis")
        self._client = redis.from_url(self.redis_url)
        await self._client.ping()
        logger.info(f"Registry connected to Redis at {self.redis_url}")
    
    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self._client:
            await self._client.close()
            self._client = None
    
    def _worker_key(self, worker_id: str) -> str:
        return f"{self.prefix}:worker:{worker_id}"
    
    def _workers_set_key(self) -> str:
        return f"{self.prefix}:workers"
    
    def _node_workers_key(self, node_name: str) -> str:
        return f"{self.prefix}:node_workers:{node_name}"
    
    def _nodes_set_key(self) -> str:
        return f"{self.prefix}:nodes"
    
    async def register_worker(self, worker: WorkerInfo) -> None:
        """Register a worker."""
        # Save worker info
        key = self._worker_key(worker.worker_id)
        data = json.dumps(worker.to_dict())
        await self._client.setex(key, self.heartbeat_ttl, data)
        
        # Add to workers set
        await self._client.sadd(self._workers_set_key(), worker.worker_id)
        
        # Register node capabilities
        for node_type in worker.node_types:
            await self._client.sadd(
                self._node_workers_key(node_type),
                worker.worker_id
            )
            await self._client.sadd(self._nodes_set_key(), node_type)
        
        # Also update local cache
        await super().register_worker(worker)
    
    async def unregister_worker(self, worker_id: str) -> None:
        """Unregister a worker."""
        # Get worker info first
        worker = await self._get_worker_from_redis(worker_id)
        
        if worker:
            # Remove from node sets
            for node_type in worker.node_types:
                await self._client.srem(
                    self._node_workers_key(node_type),
                    worker_id
                )
        
        # Remove worker
        await self._client.delete(self._worker_key(worker_id))
        await self._client.srem(self._workers_set_key(), worker_id)
        
        # Update local cache
        await super().unregister_worker(worker_id)
    
    async def _get_worker_from_redis(self, worker_id: str) -> Optional[WorkerInfo]:
        """Get worker info from Redis."""
        key = self._worker_key(worker_id)
        data = await self._client.get(key)
        
        if data:
            return WorkerInfo.from_dict(json.loads(data))
        return None
    
    async def update_worker_heartbeat(self, worker_id: str) -> None:
        """Update worker heartbeat (extends TTL)."""
        worker = await self._get_worker_from_redis(worker_id)
        if worker:
            worker.last_heartbeat = datetime.utcnow()
            key = self._worker_key(worker_id)
            data = json.dumps(worker.to_dict())
            await self._client.setex(key, self.heartbeat_ttl, data)
    
    async def update_worker_load(self, worker_id: str, load: int) -> None:
        """Update worker's current load."""
        worker = await self._get_worker_from_redis(worker_id)
        if worker:
            worker.current_load = load
            key = self._worker_key(worker_id)
            data = json.dumps(worker.to_dict())
            # Preserve existing TTL
            ttl = await self._client.ttl(key)
            if ttl > 0:
                await self._client.setex(key, ttl, data)
            else:
                await self._client.setex(key, self.heartbeat_ttl, data)
    
    def get_workers_for_node(self, node_name: str) -> List[WorkerInfo]:
        """Get available workers for a node type (sync version uses cache)."""
        return super().get_workers_for_node(node_name)
    
    async def get_workers_for_node_async(self, node_name: str) -> List[WorkerInfo]:
        """Get available workers for a node type from Redis."""
        worker_ids = await self._client.smembers(self._node_workers_key(node_name))
        
        workers = []
        for wid in worker_ids:
            worker = await self._get_worker_from_redis(wid.decode())
            if worker and worker.is_available:
                workers.append(worker)
        
        return workers
    
    async def select_worker_async(
        self,
        node_name: str,
        strategy: str = "least_loaded"
    ) -> Optional[WorkerInfo]:
        """Select a worker for a node using the specified strategy."""
        workers = await self.get_workers_for_node_async(node_name)
        if not workers:
            return None
        
        if strategy == "least_loaded":
            return min(workers, key=lambda w: w.current_load)
        elif strategy == "random":
            import random
            return random.choice(workers)
        else:
            return min(workers, key=lambda w: w.current_load)
    
    async def list_all_nodes(self) -> List[str]:
        """List all registered node types."""
        nodes = await self._client.smembers(self._nodes_set_key())
        return [n.decode() for n in nodes]
    
    async def cleanup_stale_workers(
        self,
        timeout: timedelta = timedelta(minutes=5)
    ) -> List[str]:
        """
        Cleanup stale workers.
        Note: With TTL-based keys, this mainly cleans up the sets.
        """
        worker_ids = await self._client.smembers(self._workers_set_key())
        stale = []
        
        for wid in worker_ids:
            worker_id = wid.decode()
            worker = await self._get_worker_from_redis(worker_id)
            
            if not worker:  # Key expired
                stale.append(worker_id)
                await self._client.srem(self._workers_set_key(), worker_id)
        
        return stale


class RedisTaskQueue:
    """
    Redis-backed task queue for distributed task distribution.
    
    Features:
    - Priority queue support
    - Task deduplication
    - Result collection
    - Dead letter queue for failed tasks
    
    Example:
        queue = RedisTaskQueue("redis://localhost:6379/0")
        await queue.connect()
        
        # Submit task
        await queue.submit_task(task)
        
        # Worker pulls task
        task = await queue.get_task(node_types={"researcher"})
        
        # Submit result
        await queue.submit_result(result)
    """
    
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        prefix: str = "dagent",
        result_ttl: int = 3600,  # Results expire after 1 hour
    ):
        self.redis_url = redis_url
        self.prefix = prefix
        self.result_ttl = result_ttl
        self._client: Optional[redis.Redis] = None
    
    async def connect(self) -> None:
        """Connect to Redis."""
        if redis is None:
            raise ImportError("redis package required: pip install redis")
        self._client = redis.from_url(self.redis_url)
        await self._client.ping()
        logger.info(f"Task queue connected to Redis at {self.redis_url}")
    
    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self._client:
            await self._client.close()
            self._client = None
    
    def _task_queue_key(self, node_name: str) -> str:
        return f"{self.prefix}:queue:{node_name}"
    
    def _task_data_key(self, task_id: str) -> str:
        return f"{self.prefix}:task:{task_id}"
    
    def _result_key(self, task_id: str) -> str:
        return f"{self.prefix}:result:{task_id}"
    
    def _workflow_results_key(self, workflow_id: str) -> str:
        return f"{self.prefix}:workflow_results:{workflow_id}"
    
    def _processing_set_key(self) -> str:
        return f"{self.prefix}:processing"
    
    def _dlq_key(self) -> str:
        return f"{self.prefix}:dlq"
    
    async def submit_task(self, task: Task) -> None:
        """Submit a task to the queue."""
        # Store task data
        data_key = self._task_data_key(task.task_id)
        await self._client.set(data_key, json.dumps(task.to_dict()))
        
        # Add to priority queue (sorted set by priority)
        queue_key = self._task_queue_key(task.node_name)
        score = -task.priority  # Negative so higher priority = lower score = first
        await self._client.zadd(queue_key, {task.task_id: score})
        
        logger.debug(f"Task {task.task_id} submitted to queue {task.node_name}")
    
    async def get_task(
        self,
        node_types: Set[str],
        timeout: float = 0,
    ) -> Optional[Task]:
        """
        Get a task from the queue.
        
        Args:
            node_types: Set of node types this worker can handle
            timeout: How long to block waiting (0 = non-blocking)
        """
        # Try each queue in order
        for node_name in node_types:
            queue_key = self._task_queue_key(node_name)
            
            # Pop highest priority task (lowest score)
            result = await self._client.zpopmin(queue_key, count=1)
            
            if result:
                task_id = result[0][0].decode()
                
                # Get task data
                data_key = self._task_data_key(task_id)
                task_data = await self._client.get(data_key)
                
                if task_data:
                    # Mark as processing
                    await self._client.sadd(self._processing_set_key(), task_id)
                    
                    task = Task.from_dict(json.loads(task_data))
                    task.started_at = datetime.utcnow()
                    
                    logger.debug(f"Task {task_id} retrieved from queue")
                    return task
        
        # If blocking requested, wait and retry
        if timeout > 0:
            await asyncio.sleep(min(timeout, 1.0))
            remaining = timeout - 1.0
            if remaining > 0:
                return await self.get_task(node_types, remaining)
        
        return None
    
    async def submit_result(self, result: TaskResult) -> None:
        """Submit a task result."""
        # Store result
        result_key = self._result_key(result.task_id)
        await self._client.setex(
            result_key,
            self.result_ttl,
            json.dumps(result.to_dict())
        )
        
        # Add to workflow results
        await self._client.sadd(
            self._workflow_results_key(result.workflow_id),
            result.task_id
        )
        
        # Remove from processing
        await self._client.srem(self._processing_set_key(), result.task_id)
        
        # Clean up task data
        await self._client.delete(self._task_data_key(result.task_id))
        
        logger.debug(f"Result for task {result.task_id} submitted")
    
    async def get_result(self, task_id: str) -> Optional[TaskResult]:
        """Get a task result."""
        result_key = self._result_key(task_id)
        data = await self._client.get(result_key)
        
        if data:
            return TaskResult.from_dict(json.loads(data))
        return None
    
    async def get_workflow_results(self, workflow_id: str) -> List[TaskResult]:
        """Get all results for a workflow."""
        task_ids = await self._client.smembers(
            self._workflow_results_key(workflow_id)
        )
        
        results = []
        for tid in task_ids:
            result = await self.get_result(tid.decode())
            if result:
                results.append(result)
        
        return results
    
    async def requeue_task(self, task: Task) -> None:
        """Requeue a task (for retries)."""
        task.retry_count += 1
        
        if task.retry_count > task.max_retries:
            # Move to dead letter queue
            await self._client.lpush(
                self._dlq_key(),
                json.dumps(task.to_dict())
            )
            logger.warning(f"Task {task.task_id} moved to DLQ after {task.max_retries} retries")
        else:
            # Requeue with same priority
            await self.submit_task(task)
            logger.info(f"Task {task.task_id} requeued (retry {task.retry_count})")
    
    async def get_dlq_tasks(self, limit: int = 100) -> List[Task]:
        """Get tasks from dead letter queue."""
        tasks_data = await self._client.lrange(self._dlq_key(), 0, limit - 1)
        return [Task.from_dict(json.loads(d)) for d in tasks_data]
    
    async def get_queue_length(self, node_name: str) -> int:
        """Get the length of a queue."""
        return await self._client.zcard(self._task_queue_key(node_name))
    
    async def get_all_queue_lengths(self) -> Dict[str, int]:
        """Get lengths of all queues."""
        pattern = f"{self.prefix}:queue:*"
        cursor = 0
        lengths = {}
        
        while True:
            cursor, keys = await self._client.scan(
                cursor=cursor,
                match=pattern,
                count=100
            )
            for key in keys:
                node_name = key.decode().split(":")[-1]
                lengths[node_name] = await self._client.zcard(key)
            if cursor == 0:
                break
        
        return lengths
