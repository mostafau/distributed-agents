# Distributed Agents Framework - Architecture Deep Dive

## Overview

This document explains how the distributed components work together to enable multi-machine workflow execution.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              MACHINE 1                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    Distributed Orchestrator                          │   │
│  │  - Receives workflow execution requests                              │   │
│  │  - Breaks workflow into tasks                                        │   │
│  │  - Submits tasks to Redis Queue                                      │   │
│  │  - Waits for results                                                 │   │
│  │  - Determines next steps based on graph                              │   │
│  └──────────────────────────────┬──────────────────────────────────────┘   │
│                                 │                                           │
└─────────────────────────────────┼───────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              REDIS CLUSTER                                   │
│                                                                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐            │
│  │   Task Queue    │  │  State Manager  │  │  Node Registry  │            │
│  │                 │  │                 │  │                 │            │
│  │ - Priority queue│  │ - Workflow state│  │ - Worker list   │            │
│  │ - Per-node-type │  │ - Checkpoints   │  │ - Capabilities  │            │
│  │ - Results store │  │ - History       │  │ - Health status │            │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘            │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         Message Bus (Pub/Sub)                        │   │
│  │  Channels: [tasks] [workers] [workflows]                            │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└──────────────┬────────────────────────────────────┬─────────────────────────┘
               │                                    │
               ▼                                    ▼
┌──────────────────────────────┐    ┌──────────────────────────────┐
│         MACHINE 2            │    │         MACHINE 3            │
│  ┌────────────────────────┐  │    │  ┌────────────────────────┐  │
│  │   Distributed Worker   │  │    │   Distributed Worker   │  │
│  │                        │  │    │  │                        │  │
│  │  Node Types:           │  │    │  │  Node Types:           │  │
│  │  - researcher          │  │    │  │  - writer              │  │
│  │  - analyzer            │  │    │  │  - reviewer            │  │
│  │                        │  │    │  │                        │  │
│  │  Pulls tasks from queue│  │    │  │  Pulls tasks from queue│  │
│  │  Executes node funcs   │  │    │  │  Executes node funcs   │  │
│  │  Submits results       │  │    │  │  Submits results       │  │
│  └────────────────────────┘  │    │  └────────────────────────┘  │
└──────────────────────────────┘    └──────────────────────────────┘
```

---

## Component Deep Dive

### 1. Redis Task Queue (`RedisTaskQueue`)

The task queue is the **work distribution mechanism**. It's how tasks get from the orchestrator to workers.

#### Data Structures in Redis

```
┌─────────────────────────────────────────────────────────────────┐
│                      REDIS TASK QUEUE                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  SORTED SETS (Priority Queues) - One per node type:            │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ dagent:queue:researcher                                  │   │
│  │   task_001 -> score: -10 (high priority)                │   │
│  │   task_002 -> score: -5  (medium priority)              │   │
│  │   task_003 -> score: 0   (normal priority)              │   │
│  └─────────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ dagent:queue:writer                                      │   │
│  │   task_004 -> score: 0                                   │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  STRINGS (Task Data):                                          │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ dagent:task:task_001 -> {                                │   │
│  │   "task_id": "task_001",                                 │   │
│  │   "workflow_id": "wf_123",                               │   │
│  │   "node_name": "researcher",                             │   │
│  │   "state": { ... serialized state ... },                 │   │
│  │   "timeout": 300,                                        │   │
│  │   "retry_count": 0                                       │   │
│  │ }                                                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  STRINGS (Results) - with TTL:                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ dagent:result:task_001 -> {                              │   │
│  │   "task_id": "task_001",                                 │   │
│  │   "status": "completed",                                 │   │
│  │   "output_state": { ... },                               │   │
│  │   "worker_id": "worker-abc123"                           │   │
│  │ }                                                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  SET (Processing Tracker):                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ dagent:processing -> { task_001, task_002 }              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  LIST (Dead Letter Queue):                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ dagent:dlq -> [ failed_task_data, ... ]                  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### Task Queue Operations

```python
# SUBMIT TASK (Orchestrator side)
async def submit_task(task):
    # 1. Store task data
    await redis.set(f"dagent:task:{task.task_id}", task.to_json())
    
    # 2. Add to priority queue (sorted set)
    # Lower score = higher priority (pulled first)
    score = -task.priority  
    await redis.zadd(f"dagent:queue:{task.node_name}", {task.task_id: score})

# GET TASK (Worker side)
async def get_task(node_types):
    for node_type in node_types:
        # Pop highest priority task (lowest score)
        result = await redis.zpopmin(f"dagent:queue:{node_type}")
        if result:
            task_id = result[0]
            task_data = await redis.get(f"dagent:task:{task_id}")
            
            # Mark as processing
            await redis.sadd("dagent:processing", task_id)
            return Task.from_json(task_data)
    return None

# SUBMIT RESULT (Worker side)
async def submit_result(result):
    # Store result with TTL
    await redis.setex(
        f"dagent:result:{result.task_id}",
        3600,  # 1 hour TTL
        result.to_json()
    )
    
    # Remove from processing
    await redis.srem("dagent:processing", result.task_id)
    
    # Cleanup task data
    await redis.delete(f"dagent:task:{result.task_id}")
```

---

### 2. Redis State Manager (`RedisStateManager`)

The state manager provides **persistent storage for workflow state**. This allows:
- Resuming workflows after failures
- Sharing state between orchestrator instances
- Historical queries

#### Data Structures

```
┌─────────────────────────────────────────────────────────────────┐
│                     REDIS STATE MANAGER                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  STRINGS (Workflow State):                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ dagent:state:wf_123 -> {                                 │   │
│  │   "workflow_id": "wf_123",                               │   │
│  │   "graph_name": "research_pipeline",                     │   │
│  │   "status": "running",                                   │   │
│  │   "data": {                                              │   │
│  │     "query": "AI trends",                                │   │
│  │     "findings": [...],                                   │   │
│  │     "analysis": "..."                                    │   │
│  │   },                                                     │   │
│  │   "current_node": "analyzer",                            │   │
│  │   "execution_history": [...]                             │   │
│  │ }                                                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  STRINGS (Checkpoints):                                        │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ dagent:checkpoint:cp_456 -> {                            │   │
│  │   "checkpoint_id": "cp_456",                             │   │
│  │   "workflow_id": "wf_123",                               │   │
│  │   "node_name": "researcher",                             │   │
│  │   "state_data": { ... snapshot ... },                    │   │
│  │   "timestamp": "2024-01-15T10:30:00"                     │   │
│  │ }                                                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  SETS (Indexes for queries):                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ dagent:index:status:running -> { wf_123, wf_124 }        │   │
│  │ dagent:index:status:completed -> { wf_100, wf_101 }      │   │
│  │ dagent:index:graph:research_pipeline -> { wf_123 }       │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  SETS (Workflow Checkpoints):                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ dagent:workflow_checkpoints:wf_123 -> { cp_456, cp_457 } │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

### 3. Redis Node Registry (`RedisNodeRegistry`)

The registry tracks **which workers exist and what they can do**. This enables:
- Dynamic worker discovery
- Load balancing
- Automatic failover

#### Data Structures

```
┌─────────────────────────────────────────────────────────────────┐
│                     REDIS NODE REGISTRY                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  STRINGS with TTL (Worker Info) - Auto-expire if no heartbeat: │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ dagent:worker:worker-abc123 [TTL: 30s] -> {              │   │
│  │   "worker_id": "worker-abc123",                          │   │
│  │   "hostname": "k8s-node-1",                              │   │
│  │   "node_types": ["researcher", "analyzer"],              │   │
│  │   "capacity": 4,                                         │   │
│  │   "current_load": 2,                                     │   │
│  │   "status": "healthy",                                   │   │
│  │   "last_heartbeat": "2024-01-15T10:30:00"                │   │
│  │ }                                                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  SETS (Worker Discovery):                                      │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ dagent:workers -> { worker-abc123, worker-def456 }       │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  SETS (Node Type to Workers mapping):                          │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ dagent:node_workers:researcher -> { worker-abc123 }      │   │
│  │ dagent:node_workers:writer -> { worker-def456 }          │   │
│  │ dagent:node_workers:analyzer -> { worker-abc123 }        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  SET (All Node Types):                                         │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ dagent:nodes -> { researcher, writer, analyzer }         │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### Worker Registration Flow

```python
# Worker starts up
async def register_worker(worker_info):
    # 1. Store worker info with TTL (auto-expire)
    await redis.setex(
        f"dagent:worker:{worker_info.worker_id}",
        30,  # 30 second TTL
        worker_info.to_json()
    )
    
    # 2. Add to workers set
    await redis.sadd("dagent:workers", worker_info.worker_id)
    
    # 3. Register capabilities
    for node_type in worker_info.node_types:
        await redis.sadd(f"dagent:node_workers:{node_type}", worker_info.worker_id)
        await redis.sadd("dagent:nodes", node_type)

# Worker sends heartbeat every 10 seconds
async def heartbeat():
    # Refresh TTL by re-setting the key
    worker_info.last_heartbeat = now()
    await redis.setex(
        f"dagent:worker:{worker_id}",
        30,
        worker_info.to_json()
    )
    # If worker dies, key expires after 30s -> auto-cleanup
```

---

### 4. Message Bus (`RedisMessageBus`)

The message bus provides **real-time event notifications** via Redis Pub/Sub. This is for:
- Fast notification when results are ready (instead of polling)
- Worker status broadcasts
- Workflow lifecycle events

#### Channels and Messages

```
┌─────────────────────────────────────────────────────────────────┐
│                       REDIS MESSAGE BUS                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  CHANNEL: dagent:channel:tasks                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Messages:                                                │   │
│  │                                                          │   │
│  │ → TASK_SUBMITTED: {                                      │   │
│  │     "task_id": "task_001",                               │   │
│  │     "workflow_id": "wf_123",                             │   │
│  │     "node_name": "researcher"                            │   │
│  │   }                                                      │   │
│  │                                                          │   │
│  │ → TASK_COMPLETED: {                                      │   │
│  │     "task_id": "task_001",                               │   │
│  │     "workflow_id": "wf_123",                             │   │
│  │     "worker_id": "worker-abc"                            │   │
│  │   }                                                      │   │
│  │                                                          │   │
│  │ → TASK_FAILED: {                                         │   │
│  │     "task_id": "task_001",                               │   │
│  │     "error": "Timeout after 300s"                        │   │
│  │   }                                                      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  CHANNEL: dagent:channel:workers                                │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Messages:                                                │   │
│  │                                                          │   │
│  │ → WORKER_HEARTBEAT: {                                    │   │
│  │     "worker_id": "worker-abc",                           │   │
│  │     "current_load": 2,                                   │   │
│  │     "status": "healthy"                                  │   │
│  │   }                                                      │   │
│  │                                                          │   │
│  │ → WORKER_REGISTERED: { ... }                             │   │
│  │ → WORKER_UNREGISTERED: { ... }                           │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  CHANNEL: dagent:channel:workflows                              │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Messages:                                                │   │
│  │                                                          │   │
│  │ → WORKFLOW_STARTED: {                                    │   │
│  │     "workflow_id": "wf_123",                             │   │
│  │     "graph_name": "research_pipeline"                    │   │
│  │   }                                                      │   │
│  │                                                          │   │
│  │ → WORKFLOW_COMPLETED: { ... }                            │   │
│  │ → WORKFLOW_FAILED: { ... }                               │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### Why Pub/Sub Instead of Just Polling?

```
WITHOUT MESSAGE BUS (Polling):
┌─────────────┐                      ┌───────┐
│ Orchestrator│ ──poll every 500ms─→ │ Redis │
└─────────────┘     "is task done?"  └───────┘
                    "is task done?"
                    "is task done?"
                    "YES!" (after 10 seconds = 20 requests)

WITH MESSAGE BUS:
┌─────────────┐                      ┌───────┐
│ Orchestrator│ ←─subscribe─────────→│ Redis │
└─────────────┘                      └───────┘
       │                                  │
       │   (waiting, no requests)         │
       │                                  │
       │◄──────TASK_COMPLETED────────────│ (instant notification)
```

---

## State Flow: Complete Example

Let's trace a workflow execution step by step:

```
WORKFLOW: researcher → analyzer → writer

Initial State: { "query": "AI trends" }
```

### Step 1: Orchestrator Starts Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│ ORCHESTRATOR (Machine 1)                                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ 1. Create State object:                                         │
│    state = State(workflow_id="wf_123", data={"query": "AI"})   │
│                                                                 │
│ 2. Save initial state to Redis:                                 │
│    StateManager.save_state(state)                               │
│                                                                 │
│ 3. Create Task for first node:                                  │
│    task = Task(                                                 │
│      task_id="task_001",                                        │
│      workflow_id="wf_123",                                      │
│      node_name="researcher",                                    │
│      state=state.copy()  ← STATE SERIALIZED INTO TASK          │
│    )                                                            │
│                                                                 │
│ 4. Submit to Redis Queue:                                       │
│    TaskQueue.submit_task(task)                                  │
│                                                                 │
│ 5. Publish event:                                               │
│    MessageBus.publish("tasks", TASK_SUBMITTED)                  │
│                                                                 │
│ 6. Wait for result (subscribed to message bus)                  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ REDIS                                                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ dagent:state:wf_123 = { "query": "AI", "status": "running" }   │
│                                                                 │
│ dagent:queue:researcher = [ task_001 ]                          │
│                                                                 │
│ dagent:task:task_001 = {                                        │
│   "state": { "query": "AI" },  ← FULL STATE SNAPSHOT           │
│   "node_name": "researcher"                                     │
│ }                                                               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Step 2: Worker Picks Up Task

```
┌─────────────────────────────────────────────────────────────────┐
│ WORKER (Machine 2) - handles "researcher" node type             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ 1. Pull task from queue:                                        │
│    task = TaskQueue.get_task(node_types={"researcher"})        │
│                                                                 │
│ 2. Deserialize state from task:                                 │
│    state = task.state  → { "query": "AI" }                     │
│                                                                 │
│ 3. Execute node function:                                       │
│    result = await researcher_func(state)                        │
│    → returns { "findings": ["finding1", "finding2"] }          │
│                                                                 │
│ 4. Merge result into state:                                     │
│    state.update(result)                                         │
│    → state = { "query": "AI", "findings": [...] }              │
│                                                                 │
│ 5. Create TaskResult:                                           │
│    result = TaskResult(                                         │
│      task_id="task_001",                                        │
│      status=COMPLETED,                                          │
│      output_state=state  ← UPDATED STATE                       │
│    )                                                            │
│                                                                 │
│ 6. Submit result to Redis:                                      │
│    TaskQueue.submit_result(result)                              │
│                                                                 │
│ 7. Publish completion event:                                    │
│    MessageBus.publish("tasks", TASK_COMPLETED)                  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ REDIS (After researcher completes)                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ dagent:result:task_001 = {                                      │
│   "status": "completed",                                        │
│   "output_state": {                                             │
│     "query": "AI",                                              │
│     "findings": ["finding1", "finding2"]  ← NEW DATA           │
│   }                                                             │
│ }                                                               │
│                                                                 │
│ dagent:queue:researcher = [ ]  ← EMPTY NOW                     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Step 3: Orchestrator Gets Result & Continues

```
┌─────────────────────────────────────────────────────────────────┐
│ ORCHESTRATOR (Machine 1)                                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ 1. Receives TASK_COMPLETED message via pub/sub                  │
│                                                                 │
│ 2. Fetch result from Redis:                                     │
│    result = TaskQueue.get_result("task_001")                   │
│                                                                 │
│ 3. Merge output state into workflow state:                      │
│    state.update(result.output_state)                            │
│    → state = { "query": "AI", "findings": [...] }              │
│                                                                 │
│ 4. Save checkpoint:                                             │
│    StateManager.save_checkpoint(state, "researcher")            │
│                                                                 │
│ 5. Determine next node from graph:                              │
│    next_nodes = graph.get_next_nodes("researcher", state)      │
│    → ["analyzer"]                                               │
│                                                                 │
│ 6. Create next task with CURRENT STATE:                         │
│    task = Task(                                                 │
│      task_id="task_002",                                        │
│      node_name="analyzer",                                      │
│      state=state.copy()  ← INCLUDES FINDINGS NOW               │
│    )                                                            │
│                                                                 │
│ 7. Submit to Redis Queue for "analyzer"                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ REDIS                                                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ dagent:state:wf_123 = {                                         │
│   "query": "AI",                                                │
│   "findings": ["finding1", "finding2"]  ← UPDATED              │
│ }                                                               │
│                                                                 │
│ dagent:checkpoint:cp_001 = { snapshot after researcher }        │
│                                                                 │
│ dagent:queue:analyzer = [ task_002 ]  ← NEW TASK               │
│                                                                 │
│ dagent:task:task_002 = {                                        │
│   "state": {                                                    │
│     "query": "AI",                                              │
│     "findings": ["finding1", "finding2"]  ← HAS ALL DATA       │
│   }                                                             │
│ }                                                               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Step 4: Different Worker Picks Up Analyzer Task

```
┌─────────────────────────────────────────────────────────────────┐
│ WORKER (Machine 3) - handles "analyzer" node type               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ 1. Pull task from analyzer queue:                               │
│    task = TaskQueue.get_task({"analyzer"})                     │
│                                                                 │
│ 2. State arrives with all previous data:                        │
│    state = { "query": "AI", "findings": [...] }                │
│                                                                 │
│ 3. Execute analyzer function:                                   │
│    result = await analyzer_func(state)                          │
│    → Can access state["findings"] from previous node!          │
│    → returns { "analysis": "Key insights..." }                 │
│                                                                 │
│ 4. Submit result with updated state:                            │
│    state = {                                                    │
│      "query": "AI",                                             │
│      "findings": [...],                                         │
│      "analysis": "Key insights..."  ← NEW                      │
│    }                                                            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Key Design Decisions

### Why State is Copied into Each Task?

```
Option A: Reference-based (NOT what we do)
- Task just has workflow_id
- Worker fetches state from Redis
- Problem: Race conditions, stale reads

Option B: Snapshot-based (What we do) ✓
- Task contains full state snapshot
- Worker has everything it needs
- Benefits:
  - No race conditions
  - Worker can operate offline
  - Clear versioning
  - Parallel branches get isolated copies
```

### Why Separate Queue Per Node Type?

```
Single Queue (Bad):
┌────────────────────────────────────────┐
│ queue: [researcher_task, writer_task]  │
└────────────────────────────────────────┘
- Worker that only handles "writer" would have to skip tasks
- Inefficient, complex logic

Queue Per Type (Good): ✓
┌──────────────────────────┐  ┌─────────────────────────┐
│ queue:researcher: [t1]   │  │ queue:writer: [t2]      │
└──────────────────────────┘  └─────────────────────────┘
- Worker subscribes only to queues it can handle
- Clean separation, efficient routing
```

### Why Both Queue + Pub/Sub?

```
Queue alone:
- Reliable delivery (tasks don't disappear)
- But: Orchestrator must poll for results

Pub/Sub alone:
- Real-time notifications
- But: If receiver offline, message lost

Both together: ✓
- Queue: Reliable task/result storage
- Pub/Sub: Fast "wake up" notifications
- Best of both worlds
```

---

## Failure Scenarios & Recovery

### Worker Dies Mid-Task

```
1. Worker crashes while processing task_001
2. Task is in "processing" set, result never submitted
3. Orchestrator timeout triggers
4. Orchestrator can:
   - Resubmit task to queue (retry)
   - Or mark workflow as failed
5. Worker's Redis key expires (TTL), auto-cleanup
```

### Orchestrator Dies Mid-Workflow

```
1. Orchestrator crashes after "researcher" but before "analyzer"
2. State is checkpointed in Redis
3. New orchestrator instance starts
4. Can query: "find workflows with status=running"
5. Resume from last checkpoint
```

### Redis Down

```
All components fail gracefully:
- Submit operations fail with exception
- Application can retry or alert
- No data corruption (Redis handles persistence)
```

---

## Summary Table

| Component | Redis Structure | Purpose |
|-----------|----------------|---------|
| Task Queue | Sorted Sets + Strings | Work distribution with priority |
| State Manager | Strings + Sets | Workflow state persistence |
| Node Registry | Strings (TTL) + Sets | Worker discovery & health |
| Message Bus | Pub/Sub Channels | Real-time event notifications |

| Data Flow | Mechanism |
|-----------|-----------|
| Orchestrator → Worker | Task serialized to Redis Queue |
| Worker → Orchestrator | Result serialized to Redis, Pub/Sub notification |
| State between nodes | Copied into each Task, merged after completion |
| Worker health | Heartbeat refreshes TTL key |
