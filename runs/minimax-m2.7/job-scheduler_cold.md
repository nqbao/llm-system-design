---

Design a distributed job scheduler.


---

# Distributed Job Scheduler System Design

## 1. System Overview

```mermaid
graph TB
    subgraph "API Layer"
        API[API Server<br/>:8080]
        Auth[Auth Service]
    end
    
    subgraph "Scheduler Cluster"
        S1[Scheduler Primary]
        S2[Scheduler Replica]
        S3[Scheduler Replica]
        
        subgraph "Scheduler Leader Election"
            ZK[ZooKeeper/Raft]
        end
    end
    
    subgraph "Storage Layer"
        DB[(SQL Database<br/>Job Metadata)]
        Redis[(Redis Cluster<br/>Locks/Cache)]
        S3FS[(Object Storage<br/>Artifacts)]
    end
    
    subgraph "Message Bus"
        MQ[Message Queue<br/>Kafka/Pulsar]
    end
    
    subgraph "Executor Cluster"
        W1[Worker Node 1]
        W2[Worker Node 2]
        W3[Worker Node N]
    end
    
    API --> Auth
    API --> DB
    S1 <--> ZK
    S2 <--> ZK
    S3 <--> ZK
    S1 --> MQ
    MQ --> W1
    MQ --> W2
    MQ --> W3
    W1 --> DB
    W2 --> DB
    W3 --> DB
```

## 2. Core Data Models

### Job Definition
```
Job {
    job_id: UUID (partition key)
    name: string
    namespace: string
    job_type: enum[SIMPLE, DAG, CRON, MAP_REDUCE]
    priority: int8 (0-255, higher = more urgent)
    payload: JSON
    owner: string
    created_at: timestamp
    updated_at: timestamp
    
    // Scheduling constraints
    schedule: CronExpression | null
    max_retries: int
    timeout_seconds: int
    dependencies: []UUID
    
    // Resource requirements
    cpu_units: int
    memory_mb: int
    disk_mb: int
    
    // Execution config
    executor_type: string
    image: string
    command: string
}
```

### Job State Machine
```mermaid
stateDiagram-v2
    [*] --> PENDING: Create
    PENDING --> PENDING: Schedule retry
    PENDING --> ASSIGNED: Scheduler assigns
    ASSIGNED --> RUNNING: Worker starts
    RUNNING --> SUCCEEDED: Exit 0
    RUNNING --> FAILED: Exit != 0
    RUNNING --> ASSIGNED: Worker timeout → reassign
    FAILED --> PENDING: Retry
    SUCCEEDED --> [*]
    FAILED --> DEAD_LETTER: Max retries exceeded
    DEAD_LETTER --> PENDING: Manual retry
```

## 3. Scheduler Architecture

### Leader Election (Raft-based)

```mermaid
sequenceDiagram
    participant S1 as Scheduler-1
    participant S2 as Scheduler-2
    participant S3 as Scheduler-3
    participant R as Raft Log
    
    S1->>R: RequestVote(term=5)
    S2->>R: RequestVote(term=5)
    S3->>R: RequestVote(term=5)
    
    Note over R: Majority voted for S1
    
    R-->>S1: VoteGranted(term=5)
    R-->>S2: VoteGranted(term=5)
    
    S1->>S1: Become Leader
    S1->>S2: AppendEntries(heartbeat)
    S1->>S3: AppendEntries(heartbeat)
    
    Note over S1: Leader heartbeat every 1s
```

### Fencing Token for Exactly-Once Execution

```mermaid
sequenceDiagram
    participant Sched as Scheduler
    participant DB as Database
    participant W1 as Worker-1
    participant W2 as Worker-2
    
    Sched->>DB: SELECT job WHERE status=PENDING<br/>ORDER BY priority LIMIT 1<br/>FOR UPDATE SKIP LOCKED
    DB-->>Sched: job_id=123, version=5
    
    Sched->>DB: UPDATE job SET status=ASSIGNED,<br/>assigned_to=W1, fence_token=7,<br/>version=6 WHERE job_id=123 AND version=5
    DB-->>Sched: 1 row updated ✓
    
    Sched->>W1: Execute(job_id=123, fence_token=7)
    
    Note over W1: Store fence_token=7 locally
    
    alt Worker-1 crashes, Worker-2 picks up
        Sched->>DB: SELECT job WHERE assigned_to=W1 AND<br/>heartbeat < now - 30s FOR UPDATE
        Sched->>DB: UPDATE job SET status=ASSIGNED,<br/>assigned_to=W2, fence_token=8,<br/>version=7 WHERE job_id=123 AND fence_token=7
        DB-->>Sched: 0 rows (fence token mismatch) ✓
        
        Note over Sched: Job already completed or<br/>Worker-1 still owns it
    end
```

## 4. Scheduling Algorithm

### Priority + Fair Sharing Implementation

```
Scheduling Loop (every 100ms):
┌─────────────────────────────────────────────────────────────┐
│ 1. Acquire distributed lock: "scheduler:tick"               │
│    - If failed, another scheduler active, skip              │
│    - Lock TTL: 5 seconds                                    │
├─────────────────────────────────────────────────────────────┤
│ 2. Query pending jobs (batch of 1000):                      │
│    SELECT * FROM jobs                                       │
│    WHERE status = PENDING                                   │
│    AND schedule_time <= NOW()                               │
│    AND namespace IN (user's accessible namespaces)          │
│    ORDER BY priority DESC, created_at ASC                   │
│    LIMIT 1000                                               │
├─────────────────────────────────────────────────────────────┤
│ 3. Check namespace quotas:                                  │
│    For each namespace, calculate:                           │
│      running_count = count(status=RUNNING)                  │
│      quota = namespace_quota[namespace]                     │
│      eligible_jobs = jobs in namespace where                │
│        running_count < quota                                │
├─────────────────────────────────────────────────────────────┤
│ 4. Assign jobs to workers:                                  │
│    For each eligible job:                                   │
│      worker = select_worker_by_load(job.resource_req)       │
│      publish_to_queue(job, worker_id, fence_token++)        │
│      update_job_status(job_id, ASSIGNED, worker_id)         │
└─────────────────────────────────────────────────────────────┘
```

### Worker Selection Strategy

```python
class WorkerSelector:
    def select(self, job: Job, workers: List[Worker]) -> Optional[Worker]:
        # Filter workers with sufficient capacity
        eligible = [
            w for w in workers
            if w.available_cpu >= job.cpu_units
            and w.available_memory >= job.memory_mb
            and w.has_executor(job.executor_type)
        ]
        
        if not eligible:
            return None
        
        # Score based on multiple factors
        scored = [
            (self._score(w, job), w) for w in eligible
        ]
        
        # Pick worker with highest score (lowest load)
        scored.sort(key=lambda x: x[0], reverse=True)
        return scored[0][1]
    
    def _score(self, worker: Worker, job: Job) -> float:
        # Lower score = better choice
        cpu_score = worker.available_cpu / worker.total_cpu
        mem_score = worker.available_memory / worker.total_memory
        affinity = 1.0 if worker.has_recent_success(job.executor_type) else 0.95
        
        return (cpu_score * 0.4 + mem_score * 0.4 + affinity * 0.2)
```

## 5. DAG Scheduling (Job Dependencies)

```mermaid
graph TD
    A[start] --> B[step_1]
    A --> C[step_2]
    A --> D[step_3]
    B --> E[merge_1]
    C --> E
    D --> F[merge_2]
    E --> F
    F --> G[finalize]
    G --> H[complete]
    
    style A fill:#90EE90
    style H fill:#90EE90
    style E fill:#FFE4B5
    style F fill:#FFE4B5
```

```sql
-- DAG-aware scheduling query
WITH ready_jobs AS (
    SELECT j.job_id, j.dependencies
    FROM jobs j
    WHERE j.status = PENDING
    AND j.dag_id = ? -- same DAG group
)
SELECT r.job_id
FROM ready_jobs r
WHERE NOT EXISTS (
    SELECT 1 FROM jobs dep
    WHERE dep.job_id = ANY(r.dependencies)
    AND dep.status NOT IN (SUCCEEDED, SKIPPED)
)
ORDER BY r.priority DESC, r.created_at ASC;
```

## 6. Storage Schema

```sql
-- Core jobs table (PostgreSQL with row-level locking)
CREATE TABLE jobs (
    job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    namespace VARCHAR(128) NOT NULL,
    job_type SMALLINT NOT NULL,
    priority SMALLINT DEFAULT 128,
    
    payload JSONB NOT NULL,
    result_payload JSONB,
    error_message TEXT,
    
    status SMALLINT NOT NULL DEFAULT 0,
    version INTEGER NOT NULL DEFAULT 0,  -- optimistic lock
    fence_token BIGINT,
    
    assigned_worker VARCHAR(255),
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    timeout_seconds INTEGER DEFAULT 3600,
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    schedule_time TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    
    dag_id UUID,
    dependencies UUID[],
    
    -- Execution requirements
    cpu_units INTEGER DEFAULT 1,
    memory_mb INTEGER DEFAULT 512,
    executor_type VARCHAR(64) DEFAULT 'docker',
    
    -- Partition by namespace for query performance
    CONSTRAINT jobs_namespace_idx UNIQUE (namespace, job_id)
);

CREATE INDEX idx_jobs_status_scheduled 
ON jobs (status, schedule_time) 
WHERE status IN (0, 1);  -- PENDING, ASSIGNED only

CREATE INDEX idx_jobs_namespace_status 
ON jobs (namespace, status, priority DESC);

CREATE INDEX idx_jobs_dag 
ON jobs (dag_id) WHERE dag_id IS NOT NULL;
```

## 7. Failure Handling

### Worker Failure Detection

```
┌─────────────────────────────────────────────────────────────┐
│ Worker Heartbeat Protocol                                   │
├─────────────────────────────────────────────────────────────┤
│ Worker → Scheduler: HEARTBEAT every 10s                     │
│   - worker_id, current_jobs[], cpu_usage, memory_usage      │
│                                                             │
│ Scheduler tracks last_heartbeat per worker                  │
│                                                             │
│ If last_heartbeat > 30s ago:                                │
│   1. Mark worker as UNHEALTHY                               │
│   2. Stop assigning new jobs                                │
│   3. Wait 60s for recovery                                  │
│   4. If still unhealthy: re-assign running jobs             │
└─────────────────────────────────────────────────────────────┘
```

### Job Re-assignment Flow

```mermaid
sequenceDiagram
    participant Sched as Scheduler
    participant DB as JobStore
    participant W1 as Worker-1 (crashed)
    participant W2 as Worker-2
    
    Sched->>DB: SELECT jobs WHERE<br/>assigned_worker=W1<br/>AND status=ASSIGNED<br/>AND updated_at < now - 30s<br/>FOR UPDATE SKIP LOCKED
    DB-->>Sched: [job_123]
    
    Sched->>DB: UPDATE job SET<br/>status=PENDING,<br/>assigned_worker=NULL,<br/>version=version+1<br/>WHERE job_id=123<br/>AND version=?<br/>AND fence_token=?
    
    alt Version/fence check passed
        DB-->>Sched: 1 row updated
        Sched->>W2: Execute(job_123)
        Sched->>Sched: Re-schedule in loop
    else Version conflict (job already completed)
        DB-->>Sched: 0 rows updated
        Note over Sched: Skip, job already done
    end
```

### Dead Letter Queue

```python
class JobFailureHandler:
    def handle_failure(self, job: Job, error: ExecutionError):
        if job.retry_count < job.max_retries:
            # Exponential backoff with jitter
            backoff = min(300, 2 ** job.retry_count) + random(0, 30)
            next_schedule = datetime.now() + timedelta(seconds=backoff)
            
            job.status = PENDING
            job.retry_count += 1
            job.schedule_time = next_schedule
            job.error_message = str(error)
            self.job_store.save(job)
        else:
            job.status = DEAD_LETTER
            job.error_message = f"Max retries ({job.max_retries}) exceeded: {error}"
            self.job_store.save(job)
            self.publish_to_dlq(job)  # Separate Kafka topic for manual review
```

## 8. Scaling Considerations

### Horizontal Scaling of Schedulers

```
Capacity Calculation:

Assume:
- 100,000 jobs/second throughput target
- 10ms average schedule decision time
- 5 scheduler instances

Calculation:
- Each scheduler can do: 100 decisions/second (10ms each)
- With 5 schedulers: 500 decisions/second
- To achieve 100,000 jobs/second → need 1000 schedulers

Solution: Partition jobs by namespace

Each scheduler:
  - Responsible for subset of namespaces
  - Schedules jobs only within assigned namespaces
  - Achieves locality and reduces coordination
  
Redis-based namespace ownership:
  - SET scheduler:ownership:{namespace} = scheduler_id
  - TTL with renewal heartbeat
  - On failure, namespace reassigned
```

### Database Sharding Strategy

```mermaid
graph LR
    subgraph "Shard 0"
        DB0[(jobs_0)]
    end
    subgraph "Shard 1"
        DB1[(jobs_1)]
    end
    subgraph "Shard N"
        DBN[(jobs_n)]
    end
    
    Router[Sharding Router] --> DB0
    Router --> DB1
    Router --> DBN
    
    Router -->|"job_id % 4 == X"| DB0
    Router -->|"job_id % 4 == Y"| DB1
```

```
Shard Key Selection:
- job_id (UUID v1 contains timestamp) provides temporal locality
- Ensures jobs created around same time land on same shard
- Simplifies queries within time windows

Alternative: namespace-based sharding
- All jobs of a namespace on same shard
- Good for namespace-level isolation
- Risk of hot namespaces
```

## 9. Monitoring & Observability

### Key Metrics

```yaml
scheduler:
  jobs_scheduled_total: counter
  jobs_completed_total: counter[by_status, by_namespace]
  schedule_latency_seconds: histogram
  scheduling_queue_depth: gauge
  
  leader_election:
    is_leader: gauge
    election_failures_total: counter
    term_changes_total: counter

worker:
  jobs_running: gauge[by_type, by_namespace]
  jobs_completed_total: counter[by_status, by_executor]
  cpu_utilization: gauge
  memory_utilization: gauge
  heartbeat_lag_seconds: gauge
  
  execution_latency_seconds: histogram[by_job_type]
  
dead_letter:
  jobs_total: gauge[by_namespace, by_error_type]
  retry_queue_depth: gauge
```

### Distributed Tracing

```
Trace structure for job execution:

Span: Submit Job
  Span: Schedule Job (scheduler)
    Span: Acquire Lock (distributed)
    Span: Query Pending Jobs (database)
    Span: Assign to Worker (messaging)
  Span: Execute Job (worker)
    Span: Pull from Queue
    Span: Prepare Environment
    Span: Run Container/Process
    Span: Collect Result
  Span: Update Status (database)
```

## 10. API Specification

### Job Submission

```yaml
POST /api/v1/namespaces/{namespace}/jobs
Content-Type: application/json

Request:
{
  "name": "data-processing-pipeline",
  "job_type": "DAG",
  "priority": 200,
  "payload": {
    "dag_definition": {
      "steps": [...],
      "dependencies": [...]
    }
  },
  "timeout_seconds": 7200,
  "max_retries": 3,
  "resources": {
    "cpu_units": 4,
    "memory_mb": 8192
  }
}

Response (201 Created):
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "PENDING",
  "created_at": "2024-01-15T10:30:00Z",
  "self": "/api/v1/namespaces/prod/jobs/550e8400-e29b-41d4-a716-446655440000"
}
```

### Job Status Query

```yaml
GET /api/v1/namespaces/{namespace}/jobs/{job_id}

Response:
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "data-processing-pipeline",
  "status": "RUNNING",
  "progress": 0.65,
  "started_at": "2024-01-15T10:30:05Z",
  "estimated_completion": "2024-01-15T10:45:00Z",
  "attempts": [
    {
      "worker_id": "worker-12",
      "started_at": "2024-01-15T10:30:05Z",
      "logs": "/api/v1/jobs/.../attempts/0/logs"
    }
  ],
  "metadata": {
    "dag_nodes_total": 10,
    "dag_nodes_completed": 6
  }
}
```

## 11. Tradeoffs & Design Decisions

| Decision | Tradeoff | Chosen Approach |
|----------|----------|-----------------|
| **Scheduling granularity** | Fine (per-job) vs Coarse (per-batch) | Per-job with 100-job batch queries |
| **Locking strategy** | Pessimistic (row lock) vs Optimistic (version) | Optimistic with fence tokens |
| **State storage** | Single DB vs Distributed | Single PostgreSQL with read replicas |
| **Job distribution** | Push (scheduler→worker) vs Pull (worker→queue) | Pull with prefetch |
| **Leader election** | External (ZooKeeper) vs Internal (Raft) | External for simplicity; can migrate |
| **DAG handling** | Pre-schedule all vs Lazy scheduling | Lazy with dependency checking |

### Failure Modes Analysis

| Failure | Detection | Recovery | RTO |
|---------|-----------|----------|-----|
| Scheduler leader crash | Raft heartbeat timeout | 3-5s election | ~5s |
| Worker node crash | 30s heartbeat gap | Job reassignment | ~60s |
| Database unavailable | Connection pool exhausted | Queue jobs in memory, retry | ~5min |
| Message queue lag | Consumer lag metric | Scale consumers | ~30s |
| Split-brain | Majority quorum | Fence tokens prevent duplicate work | N/A |

## 12. Recommended Technology Stack

- **Scheduler**: Go or Rust (for performance and low GC)
- **Database**: PostgreSQL 15+ (row-level locking, JSONB, partitioning)
- **Cache/Locks**: Redis Cluster (distributed locks, rate limiting)
- **Message Queue**: Apache Pulsar or Kafka (durable, replay support)
- **Leader Election**: etcd or ZooKeeper
- **Workers**: Docker/Kubernetes for isolation
- **Monitoring**: Prometheus + Grafana + Jaeger