"""
Sequence Diagram: Distributed Workflow Execution

This file contains ASCII sequence diagrams showing how components interact.
"""

BASIC_FLOW = """
┌──────────────┐     ┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│ Orchestrator │     │    Redis    │     │   Worker 1   │     │   Worker 2   │
│  (Machine 1) │     │   Cluster   │     │  (Machine 2) │     │  (Machine 3) │
└──────┬───────┘     └──────┬──────┘     └──────┬───────┘     └──────┬───────┘
       │                    │                   │                    │
       │  ══════════════════════════════════════════════════════════════════
       │                    WORKFLOW STARTS                          
       │  ══════════════════════════════════════════════════════════════════
       │                    │                   │                    │
       │ 1. Save State      │                   │                    │
       │ ──────────────────>│                   │                    │
       │                    │                   │                    │
       │ 2. Submit Task     │                   │                    │
       │    (researcher)    │                   │                    │
       │ ──────────────────>│                   │                    │
       │                    │                   │                    │
       │ 3. Publish         │                   │                    │
       │    TASK_SUBMITTED  │                   │                    │
       │ ──────────────────>│                   │                    │
       │                    │                   │                    │
       │ 4. Subscribe &     │                   │                    │
       │    Wait            │                   │                    │
       │ <─ ─ ─ ─ ─ ─ ─ ─ ─ │                   │                    │
       │                    │                   │                    │
       │                    │ 5. Pull Task      │                    │
       │                    │<──────────────────│                    │
       │                    │                   │                    │
       │                    │ 6. Return Task +  │                    │
       │                    │    State Snapshot │                    │
       │                    │──────────────────>│                    │
       │                    │                   │                    │
       │                    │                   │ 7. Execute         │
       │                    │                   │    researcher()    │
       │                    │                   │    ┌───────┐       │
       │                    │                   │    │ Input:│       │
       │                    │                   │    │{query}│       │
       │                    │                   │    └───┬───┘       │
       │                    │                   │        │           │
       │                    │                   │        ▼           │
       │                    │                   │    ┌───────┐       │
       │                    │                   │    │Output:│       │
       │                    │                   │    │{query,│       │
       │                    │                   │    │finding│       │
       │                    │                   │    └───────┘       │
       │                    │                   │                    │
       │                    │ 8. Submit Result  │                    │
       │                    │    + Updated State│                    │
       │                    │<──────────────────│                    │
       │                    │                   │                    │
       │                    │ 9. Publish        │                    │
       │                    │    TASK_COMPLETED │                    │
       │                    │<──────────────────│                    │
       │                    │                   │                    │
       │ 10. Receive Event  │                   │                    │
       │<───────────────────│                   │                    │
       │                    │                   │                    │
       │ 11. Fetch Result   │                   │                    │
       │ ──────────────────>│                   │                    │
       │<───────────────────│                   │                    │
       │                    │                   │                    │
       │ 12. Merge State    │                   │                    │
       │     {query,        │                   │                    │
       │      findings}     │                   │                    │
       │                    │                   │                    │
       │ 13. Checkpoint     │                   │                    │
       │ ──────────────────>│                   │                    │
       │                    │                   │                    │
       │  ══════════════════════════════════════════════════════════════════
       │                    NEXT NODE (writer) - DIFFERENT WORKER    
       │  ══════════════════════════════════════════════════════════════════
       │                    │                   │                    │
       │ 14. Submit Task    │                   │                    │
       │     (writer) with  │                   │                    │
       │     full state     │                   │                    │
       │ ──────────────────>│                   │                    │
       │                    │                   │                    │
       │                    │                   │ 15. Pull Task      │
       │                    │                   │     (writer)       │
       │                    │<───────────────────────────────────────│
       │                    │                   │                    │
       │                    │ 16. Return Task   │                    │
       │                    │     with state:   │                    │
       │                    │     {query,       │                    │
       │                    │      findings}    │                    │
       │                    │────────────────────────────────────────>│
       │                    │                   │                    │
       │                    │                   │                    │ 17. Execute
       │                    │                   │                    │     writer()
       │                    │                   │                    │  ┌─────────┐
       │                    │                   │                    │  │ Has all │
       │                    │                   │                    │  │ previous│
       │                    │                   │                    │  │  data!  │
       │                    │                   │                    │  └────┬────┘
       │                    │                   │                    │       │
       │                    │                   │                    │       ▼
       │                    │                   │                    │  ┌─────────┐
       │                    │                   │                    │  │ Output: │
       │                    │                   │                    │  │ {query, │
       │                    │                   │                    │  │ findings│
       │                    │                   │                    │  │ report} │
       │                    │                   │                    │  └─────────┘
       │                    │                   │                    │
       │                    │ 18. Submit Result │                    │
       │                    │<───────────────────────────────────────│
       │                    │                   │                    │
       │ 19. Receive Event  │                   │                    │
       │<───────────────────│                   │                    │
       │                    │                   │                    │
       │ 20. Merge Final    │                   │                    │
       │     State          │                   │                    │
       │                    │                   │                    │
       │  ══════════════════════════════════════════════════════════════════
       │                    WORKFLOW COMPLETE                        
       │  ══════════════════════════════════════════════════════════════════
       │                    │                   │                    │
       │ 21. Save Final     │                   │                    │
       │     State          │                   │                    │
       │ ──────────────────>│                   │                    │
       │                    │                   │                    │
       │ 22. Return to      │                   │                    │
       │     Caller         │                   │                    │
       │                    │                   │                    │
       ▼                    ▼                   ▼                    ▼
"""

WORKER_HEARTBEAT = """
┌──────────────┐                    ┌─────────────┐
│    Worker    │                    │    Redis    │
└──────┬───────┘                    └──────┬──────┘
       │                                   │
       │  1. Register (with TTL=30s)       │
       │ ─────────────────────────────────>│
       │   SET worker:abc {info} EX 30     │
       │                                   │
       │                                   │  Key expires in 30s
       │                                   │  unless refreshed
       │                                   │
       │        ... 10 seconds ...         │
       │                                   │
       │  2. Heartbeat (refresh TTL)       │
       │ ─────────────────────────────────>│
       │   SETEX worker:abc 30 {info}      │
       │                                   │  TTL reset to 30s
       │                                   │
       │        ... 10 seconds ...         │
       │                                   │
       │  3. Heartbeat                     │
       │ ─────────────────────────────────>│
       │                                   │
       │        ... worker crashes ...     │
       │                                   │
       │           X                       │
       │                                   │
       │        ... 30 seconds ...         │
       │                                   │
       │                                   │  4. Key auto-expires!
       │                                   │     Worker considered dead
       │                                   │
       ▼                                   ▼
"""

STATE_SNAPSHOT_EXPLANATION = """
══════════════════════════════════════════════════════════════════════════════
                         WHY STATE IS COPIED INTO TASKS
══════════════════════════════════════════════════════════════════════════════

The key insight: Each task carries a COMPLETE SNAPSHOT of state.

Step 1: Orchestrator creates task for "researcher"
┌─────────────────────────────────────┐
│ Task {                              │
│   node: "researcher",               │
│   state: {                          │
│     "query": "AI trends"            │  ← Initial state only
│   }                                 │
│ }                                   │
└─────────────────────────────────────┘

Step 2: Worker executes, returns result
┌─────────────────────────────────────┐
│ TaskResult {                        │
│   output_state: {                   │
│     "query": "AI trends",           │
│     "findings": ["f1", "f2"]        │  ← Added by researcher
│   }                                 │
│ }                                   │
└─────────────────────────────────────┘

Step 3: Orchestrator merges into workflow state
┌─────────────────────────────────────┐
│ WorkflowState = {                   │
│   "query": "AI trends",             │
│   "findings": ["f1", "f2"]          │  ← Accumulated
│ }                                   │
└─────────────────────────────────────┘

Step 4: Orchestrator creates task for "writer" with FULL state
┌─────────────────────────────────────┐
│ Task {                              │
│   node: "writer",                   │
│   state: {                          │
│     "query": "AI trends",           │  ← Everything from before
│     "findings": ["f1", "f2"]        │  ← Writer can access this!
│   }                                 │
│ }                                   │
└─────────────────────────────────────┘

This means:
✓ Workers are stateless - they just need the task
✓ Any worker can pick up any task (of matching type)
✓ No shared memory between machines needed
✓ State "flows" through the graph via task payloads
✓ Fault tolerance: checkpoint = serialized state snapshot
"""

REDIS_DATA_STRUCTURES = """
══════════════════════════════════════════════════════════════════════════════
                            REDIS DATA STRUCTURES
══════════════════════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────────────────┐
│                              TASK QUEUE                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Sorted Sets (one per node type):                                          │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │ dagent:queue:researcher                                               │ │
│  │                                                                       │ │
│  │   Member      │ Score │ Notes                                         │ │
│  │   ────────────┼───────┼─────────────────────────────────────────────  │ │
│  │   task_001    │  -10  │ High priority (lower score = first)          │ │
│  │   task_002    │   0   │ Normal priority                              │ │
│  │   task_003    │   5   │ Low priority                                 │ │
│  │                                                                       │ │
│  │   ZPOPMIN returns task_001 first                                      │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  Strings (task data):                                                      │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │ dagent:task:task_001 = JSON {                                         │ │
│  │   "task_id": "task_001",                                              │ │
│  │   "workflow_id": "wf_123",                                            │ │
│  │   "node_name": "researcher",                                          │ │
│  │   "state": { "query": "AI" },  ← Full state snapshot                  │ │
│  │   "priority": 10,                                                     │ │
│  │   "timeout": 300                                                      │ │
│  │ }                                                                     │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  Strings with TTL (results):                                               │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │ dagent:result:task_001 [TTL: 3600s] = JSON {                          │ │
│  │   "status": "completed",                                              │ │
│  │   "output_state": { "query": "AI", "findings": [...] }               │ │
│  │ }                                                                     │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                             NODE REGISTRY                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Strings with TTL (worker info):                                           │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │ dagent:worker:worker-abc [TTL: 30s] = JSON {                          │ │
│  │   "worker_id": "worker-abc",                                          │ │
│  │   "hostname": "k8s-pod-xyz",                                          │ │
│  │   "node_types": ["researcher", "analyzer"],                           │ │
│  │   "capacity": 4,                                                      │ │
│  │   "current_load": 2                                                   │ │
│  │ }                                                                     │ │
│  │                                                                       │ │
│  │ ⚠️  If worker stops sending heartbeats, key expires automatically!   │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  Sets (capability mapping):                                                │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │ dagent:node_workers:researcher = { worker-abc, worker-def }           │ │
│  │ dagent:node_workers:writer = { worker-ghi }                           │ │
│  │                                                                       │ │
│  │ → "Which workers can handle 'researcher' tasks?"                      │ │
│  │   SMEMBERS dagent:node_workers:researcher                             │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                             MESSAGE BUS                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Pub/Sub Channels:                                                         │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                                                                       │ │
│  │  dagent:channel:tasks                                                 │ │
│  │  ├── TASK_SUBMITTED  { task_id, node_name }                          │ │
│  │  ├── TASK_COMPLETED  { task_id, worker_id }                          │ │
│  │  └── TASK_FAILED     { task_id, error }                              │ │
│  │                                                                       │ │
│  │  dagent:channel:workers                                               │ │
│  │  ├── WORKER_HEARTBEAT { worker_id, load }                            │ │
│  │  └── WORKER_REGISTERED { worker_id, capabilities }                   │ │
│  │                                                                       │ │
│  │  dagent:channel:workflows                                             │ │
│  │  ├── WORKFLOW_STARTED { workflow_id }                                │ │
│  │  └── WORKFLOW_COMPLETED { workflow_id }                              │ │
│  │                                                                       │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  Note: Pub/Sub is fire-and-forget. Used for notifications, not storage.    │
│        Actual data is in the queue/state structures above.                 │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
"""

if __name__ == "__main__":
    print("=" * 80)
    print("DISTRIBUTED WORKFLOW SEQUENCE DIAGRAM")
    print("=" * 80)
    print(BASIC_FLOW)
    
    print("\n" + "=" * 80)
    print("WORKER HEARTBEAT MECHANISM")
    print("=" * 80)
    print(WORKER_HEARTBEAT)
    
    print("\n" + STATE_SNAPSHOT_EXPLANATION)
    
    print("\n" + REDIS_DATA_STRUCTURES)
