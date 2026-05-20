---

Design a distributed job scheduler.


---

# Distributed Job Scheduler System Design

## 1. Overview
A distributed job scheduler accepts job submissions (one‑off, cron, dependency‑based), persists them durably, and reliably delegates execution across a pool of workers. The system must be horizontally scalable, fault‑tolerant, and provide at‑least‑once execution semantics with exactly‑once semantics as an optional feature. It must handle backpressure, priorities, and varied resource requirements.

## 2. System Architecture

```mermaid
graph TD
    subgraph Clients
        C1[Client App]
        C2[CLI]
        C3[Admin UI]
    end

    subgraph API Layer
        LB[Load Balancer]
        API1[Scheduler API 1]
        API2[Scheduler API 2]
    end

    subgraph Core Services
        Leader[/Scheduler Leader/]
        Followers[/Scheduler Follower(s)/]
        Cron[Recurring Job Generator]
    end

    subgraph Storage
        DB[(Metadata Store<br/>(etcd / PostgreSQL))]
        MQ[[Message Queue<br/>(Apache Kafka)]]
    end

    subgraph Workers
        W1[Worker 1]
        W2[Worker 2]
        W3[Worker N]
    end

    C1 --> LB
    C2 --> LB
    C3 --> LB
    LB --> API1 & API2
    
    API1 --> MQ
    API2 --> MQ
    API1 --> DB
    API2 --> DB
    
    Leader --> DB
    Leader --> MQ
    Followers --> DB
    Cron --> DB
    
    W1 --> MQ
    W2 --> MQ
    W3 --> MQ
    W1 --> DB
    W2 --> DB
    W3 --> DB
    
    Leader -. state sync .-> DB
    Leader -. allocation decisions .-> MQ
    Workers -. status updates .-> DB
    Workers -. acknowledge .-> MQ
```

**Components**:
- **Scheduler API**: Accepts job definitions, validates, persists metadata, enqueues initial jobs.
- **Metadata Store (DB)**: Stores job definitions, state (PENDING, RUNNING, SUCCESS, FAILED), worker heartbeats, leader lock, cron expressions.
- **Message Queue (MQ)**: Durable, partitioned log that holds job instances awaiting execution. Kafka is chosen for durability, replayability, and partitioning keyed by job type or worker group.
- **Scheduler Leader**: Elected via etcd/ZooKeeper lease. Handles cron triggering, dependency resolution, and high‑level orchestration.
- **Cron Service**: Runs as part of the leader; periodically evaluates cron expressions to enqueue new job instances.
- **Workers**: Poll (or receive via push) job messages from MQ, execute them, and report final status back to the DB. Workers also update heartbeats periodically.

## 3. Detailed Design

### 3.1 Job Submission
- Client submits job definition (type, payload, priority, deadline, dependencies, resource hints).
- API validates and writes to `jobs` table with state `PENDING`.
- If the job has no dependencies or is immediately runnable, API publishes a message to Kafka topic `scheduled-jobs`. The message key is `worker_group` (default) or a specific affinity key; payload includes `job_id`.

### 3.2 Job Queue (Kafka)
- **Partitioning**: Partitions by `worker_group` to ensure jobs for a group are ordered (important for sequential dependency within group). Number of partitions = expected maximum parallelism of workers.
- **Durability**: replication factor ≥ 3, `acks=all`, `min.insync.replicas=2` → can survive loss of a broker.
- **Retention**: 7 days for debugging, but consumer offsets track progress.
- Workers commit offsets after successful execution. If worker crashes, message is reprocessed (at‑least‑once).

### 3.3 Metadata Store (Relational DB or etcd)
- **Jobs Table**: `job_id, type, payload, state, priority, deadline, retry_count, max_retries, created_at, updated_at, assigned_worker, worker_group, depends_on[]`.
- **Worker Heartbeats**: `worker_id, last_heartbeat, group, capacity (slots)` – stored in lightweight KV (etcd) or DB with TTL.
- **Leader Lock**: etcd lease or table `leader_lock` with unique key, heartbeat every 5s.

### 3.4 Scheduler (Leader)
- Responsible for:
  1. Evaluating cron expressions every minute (or on defined schedule) → inserts new job records, enqueues them.
  2. Dependency resolution: periodically (or triggered by job completions) scans for jobs whose dependencies are now `SUCCESS` → enqueue those jobs.
  3. Handling overdue jobs (deadline exceeded) → mark as `FAILED` or escalate.
  4. Re‑queuing jobs whose worker heartbeat expired (worker death).
- Leader election uses etcd. Followers are hot standbys, ready to take over if leader lease expires.

### 3.5 Worker
- Long‑running process that connects to Kafka consumer group `worker-group-<group_name>`.
- Concurrency: each worker has a configurable number of *slots* (e.g., 16). It fetches a batch of messages, assigns each to a slot, and executes.
- Execution: Usually an isolated Docker container or subprocess. Worker updates job state to `RUNNING` and sets `assigned_worker` in DB.
- On completion, updates state to `SUCCESS` or `FAILED` and commits Kafka offset.
- Heartbeat: worker writes to `worker_heartbeats` table every 10s with its current load (free slots). If a worker stops, its heartbeats expire after 30s, and the leader can reassign past jobs.

### 3.6 Push vs. Pull Model
- **Pull (current)**: Workers poll Kafka, allowing natural backpressure: workers only fetch what they can handle. Scaling is simple: add more workers/partitions.
- **Push**: Scheduler would need to track worker capacity and push jobs to specific workers via RPC. This reduces latency but adds complexity in failure handling and load balancing. We choose pull for simplicity and reliability.

## 4. Scheduling Algorithm & Features

- **Priority Queues**: Each job has a priority (0–9). Kafka alone provides FIFO per partition. To implement priorities across partitions, we use a separate priority queue in the scheduler leader for jobs that haven’t been enqueued to Kafka yet? Wait: if all jobs go to Kafka, then order is per partition. That's incompatible with global priority. Alternative: use a pull‑based mechanism where workers poll scheduler API for next job, and scheduler holds a global priority queue. But that would make scheduler a bottleneck. Better: use Kafka, but use a two‑level approach: scheduler publishes to topic `high-priority` and `low-priority` with separate consumer groups. Workers assign more concurrency to high‑priority topic. Not perfect.

**Revised approach**: Use the **Scheduler Leader as a priority dispatcher** for high‑priority jobs only. For normal jobs, use Kafka with pull. High‑priority jobs go to a separate, low‑latency queue (Redis List sorted by priority) with a dedicated worker pool. For the majority of jobs, Kafka suffices with its throughput.

We'll document this tradeoff: Kafka gives high throughput but no per‑message ordering across partitions (only within) and no global priority. For global priority, a centralized scheduler is necessary. We choose hybrid: critical jobs via scheduler push, normal via Kafka pull.

- **Recurring Jobs**: Cron scheduler (leader) reads entries from `cron_jobs` table. On each tick, it generates a new job instance, resolving the next scheduled time.

- **Deadlines & Timeouts**: Each job has a `deadline` timestamp and `timeout_duration`. Workers check elapsed time and kill execution if timeout exceeded. Leader periodically cancels jobs past deadline.

- **Retries**: On failure, the worker (or scheduler) increments `retry_count`. If under `max_retries`, the job is re‑enqueued with exponential backoff (delay added to message).

## 5. Capacity Planning

### Assumptions
- Average job arrival rate: **1000 jobs/second** (sustained).
- Job execution time: P50=200ms, P99=2 sec, max=10 sec. Assume average 500ms.
- Each worker node has 16 CPU cores, can run 16 concurrent jobs (slots).
- Target system overhead: scheduler + queue should handle peaks up to 5x base rate (5000 jobs/s).

### Worker Capacity
- Throughput per worker core: 1/0.5s = 2 jobs/s per slot. 16 slots give 32 jobs/s per worker.
- To sustain 1000 jobs/s, need `1000 / 32 ≈ 32 workers`. With N+1 redundancy, **35 workers**.
- Peak (5000 jobs/s): scaling workers horizontally to 160 workers (can be auto‑scaled).

### Kafka Throughput
- Kafka can easily handle millions of messages per second with sufficient partitions and brokers.
- For 1000 msg/s, 6 partitions (distributed over 3 brokers) is more than enough.
- Storage: each message ~1KB. 1000*86400 = 86.4M messages/day ≈ 86 GB/day. With 3x replication and 7‑day retention: ~1.8 TB disk. Standard.

### Metadata Store (DB)
- Job state writes: at job start and completion (2 writes per job). At 1000/s, 2000 writes/s.
- PostgreSQL on modern hardware can handle 10k+ writes/s with proper indexing. Use connection pooling (PgBouncer).
- etcd is limited (~5k writes/s), so only for leader election and worker heartbeats (low write rate). Job metadata uses Postgres.

### Scheduler Leader
- It processes cron, dependency checks, and dead‑job recovery. Dependency checks are batched: every 2 seconds scan for jobs in PENDING with satisfied dependencies. Worst‑case query: `SELECT job_id FROM jobs WHERE state='PENDING' AND depends_on = SUCCESS` with index on (state, depends_on). This can be efficient for up to millions of jobs.
- Leader CPU usage is low (orchestration only). Single leader suffices, with failover.

## 6. Tradeoffs

| Tradeoff | Choice | Rationale |
|----------|--------|-----------|
| Queue technology | Kafka | Durability, replay, high throughput, partitioned parallelism. Overhead: higher latency than Redis, but acceptable for batch jobs. |
| Exactly‑once vs at‑least‑once | At‑least‑once with idempotency keys | Much simpler. Workers must make job logic idempotent (e.g., use job id as dedup key). Kafka transactions possible but not needed. |
| Pull vs Push scheduling | Pull (worker consumers) | Better backpressure, decouples scheduler from worker capacity. Scheduler never overloads workers. |
| Global job priority | Hybrid: high‑priority via push, normal via Kafka | Provides low latency for critical jobs without sacrificing throughput scaling of Kafka. Complexity added. |
| Metadata store consistency | RDBMS + etcd | Relational provides strong consistency for job states; etcd for coordination with watches. Tradeoff: two systems to manage. |
| Dependency resolution | Scheduler polling job states | Simple. Could be event‑driven (DB triggers + Kafka), but polling every few seconds is fine for most latency requirements (sub‑second to seconds). |

## 7. Failure Scenarios & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **Worker crashes** | Jobs assigned to that worker may be lost (if not completed). | Worker heartbeat expiry triggers the leader to re‑queue jobs that were in `RUNNING` state but haven't completed within timeout. Workers commit Kafka offsets only after successful completion, so uncommitted messages are redelivered to other consumers. |
| **Scheduler leader fails** | No cron triggering, no dependency resolution, no dead‑job recovery. | Leader lease (etcd) expires after 5–10 seconds. A follower acquires the lock and takes over. Cron state is in DB, so new leader picks up from last tick. |
| **Kafka broker failure** | Some partitions become unavailable if insufficient ISR. | Replication factor 3, min.insync.replicas=2. Producers block if cannot reach min ISR, but jobs are persisted in DB first, so can be retried. Consumer groups rebalance to other partitions. |
| **Metadata DB outage** | Jobs cannot be submitted, workers cannot update state. | DB replicated (primary‑standby, auto‑failover). API buffers submissions and writes once recovered, though this adds complexity. For critical systems, use distributed SQL (CockroachDB). |
| **Network partition** | Split‑brain between scheduler and workers/DB. | Leader uses etcd lease, which requires quorum; if partitioned from etcd, it steps down. Workers isolated from DB may continue running jobs but can’t update state—they halt new job acceptance until connectivity restored. Use backoff. |
| **Duplicate job processing** | Worker commits after business logic but before offset commit due to crash. The message is replayed to another worker. | Design jobs to be idempotent: worker checks DB if job already in `SUCCESS` or uses idempotency key. The overhead is minimal. |

## 8. Mermaid Diagram (above)

The diagram in Section 2 illustrates the high‑level architecture.