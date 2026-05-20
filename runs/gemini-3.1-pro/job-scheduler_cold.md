---

Design a distributed job scheduler.


---

Here is a comprehensive system design for a large-scale distributed job scheduler, akin to systems like AWS EventBridge, Netflix Conductor, or open-source solutions like Quartz and Celery.

---

### 1. System Requirements

**Functional Requirements:**
*   **Job Submission:** Clients can submit one-off (delayed) or recurring (cron-based) jobs.
*   **Job Execution:** The system must execute custom job payloads via worker nodes.
*   **State Management:** Track job states (`SCHEDULED`, `QUEUED`, `RUNNING`, `COMPLETED`, `FAILED`).
*   **Cancellation/Modification:** Clients can cancel or update jobs before they execute.

**Non-Functional Requirements:**
*   **High Availability (HA):** No single point of failure.
*   **High Throughput & Scalability:** Support hundreds of millions of jobs per day.
*   **Low Execution Latency:** Jobs should start executing within seconds of their scheduled time.
*   **Fault Tolerance:** Handle worker crashes without losing jobs (At-Least-Once delivery).
*   **Idempotency:** The system should strive for exactly-once execution, but given network partitions, workers must handle duplicate deliveries gracefully.

---

### 2. Capacity Planning & Math

Let's design for a massive scale: **100 Million jobs executed per day.**

*   **Throughput:**
    *   Average jobs per second (TPS): $100,000,000 \text{ jobs} / 86,400 \text{ seconds} \approx \textbf{1,150 TPS}$.
    *   Peak TPS (assuming 5x peak-to-average ratio): $\approx \textbf{5,750 TPS}$.
*   **Storage (Database):**
    *   Assume average job metadata + payload size = **2 KB**.
    *   Daily storage: $100,000,000 \times 2 \text{ KB} = \textbf{200 GB/day}$.
    *   Annual storage: $\approx \textbf{73 TB}$. (We will need data tiering/archiving for completed jobs).
*   **Memory (Caching/Dispatching):**
    *   To maintain low latency, the scheduler will pull jobs due in the next 5 minutes into memory/Redis.
    *   5 minutes of jobs = $1,150 \text{ TPS} \times 300 \text{ seconds} = 345,000 \text{ jobs}$.
    *   Memory required: $345,000 \times 2 \text{ KB} \approx \textbf{690 MB}$. This easily fits into a single Redis node (though we will cluster for HA).

---

### 3. High-Level Architecture

The architecture is divided into three planes: **API (Ingestion)**, **Control Plane (Scheduling)**, and **Data Plane (Execution)**.

```mermaid
architecture-beta
    group api(API Plane)
    group control(Control Plane)
    group data(Data Plane)
    group storage(Storage Plane)

    service gateway(API Gateway) in api
    service apiserver(Job API Server) in api

    service dispatcher(Dispatcher / Time Wheel) in control
    service reaper(Reaper / Health Checker) in control

    service mq(Message Queue cluster) in data
    service worker1(Worker Node 1) in data
    service worker2(Worker Node N) in data

    service db(Relational DB Sharded) in storage
    service redis(Redis Cluster) in storage

    gateway:R --> apiserver:L
    apiserver:R --> db:L
    
    dispatcher:T --> db:B
    dispatcher:R --> redis:L
    dispatcher:B --> mq:T
    
    reaper:R --> db:L
    reaper:B --> redis:T
    
    mq:B --> worker1:T
    mq:B --> worker2:T
    
    worker1:L --> db:R
    worker1:L --> redis:R
```

#### Core Components:
1.  **Job API Server:** REST/gRPC endpoints. Writes job definitions to the database.
2.  **Relational Database (PostgreSQL / Aurora):** The persistent source of truth.
3.  **Dispatcher (The Scheduler):** Queries the DB for upcoming jobs and pushes them to a fast, time-based queue (Redis).
4.  **Redis (Time Wheel):** Uses Sorted Sets (`ZSET`) to order jobs by their exact Unix timestamp execution time.
5.  **Message Queue (RabbitMQ / AWS SQS):** Decouples the scheduler from the workers, providing load buffering and consumer groups.
6.  **Worker Nodes:** Pull tasks from the queue, execute the business logic, and update the DB.
7.  **Reaper:** A background process that detects stalled jobs or dead workers.

---

### 4. Core Workflows

#### A. Job Submission
1. Client POSTs a job to the API Server with a `run_at` timestamp (or cron expression) and a `payload`.
2. API Server generates a unique `job_id`.
3. API Server inserts the job into the Database with status `SCHEDULED`.

#### B. The Dispatch Flow (Crucial for Low Latency)
Relying purely on DB polling causes massive latency and DB lock contention. We use a **Forward-Fetching Time Wheel** pattern.

1. **Puller (Every 1 minute):** The Dispatcher runs a query:
   `SELECT * FROM jobs WHERE status = 'SCHEDULED' AND run_at <= NOW() + 5 minutes FOR UPDATE SKIP LOCKED;`
   *(Note: `SKIP LOCKED` is a Postgres feature that prevents multiple dispatcher nodes from pulling the same jobs).*
2. The Dispatcher updates these jobs to `QUEUED` in the DB.
3. The Dispatcher pushes them to the **Redis ZSET**, where the `score` is the `run_at` Unix timestamp, and the `value` is the `job_id` + `payload`.
4. **Trigger (Every 1 second):** A fast loop in the Dispatcher checks Redis:
   `ZRANGEBYSCORE time_wheel -inf <current_unix_timestamp>`
5. It pops these due jobs from Redis and publishes them to the **Message Queue**.

#### C. Execution Flow
1. Worker nodes consume from the Message Queue.
2. Worker marks the job as `RUNNING` in the DB.
3. Worker executes the payload.
4. On success, worker updates DB status to `COMPLETED`.
5. *If it's a Cron Job:* The worker calculates the next `run_at` based on the cron expression, and inserts a *new* job row into the DB with status `SCHEDULED`.

---

### 5. Database Schema (PostgreSQL)

```sql
CREATE TABLE jobs (
    job_id UUID PRIMARY KEY,
    tenant_id UUID,
    job_type VARCHAR(50),
    payload JSONB,
    status VARCHAR(20), -- SCHEDULED, QUEUED, RUNNING, COMPLETED, FAILED
    run_at TIMESTAMP INDEX,
    cron_expression VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Crucial index for the Dispatcher to pull upcoming jobs efficiently
CREATE INDEX idx_jobs_status_run_at ON jobs(status, run_at);
```
*At 100M jobs/day, we must partition this table by `run_at` (e.g., daily partitions) to easily drop old data (archiving) and keep index trees shallow.*

---

### 6. Failure Scenarios & Fault Tolerance ("What could fail?")

A distributed scheduler is essentially a massive state machine. Failures result in jobs getting stuck in intermediate states.

#### Failure 1: Dispatcher crashes after pulling from DB, but before writing to Redis.
*   **State:** Job is marked `QUEUED` in DB, but never reaches Redis/MQ. It will never run.
*   **Solution:** The **Reaper Process**. It periodically runs:
    `SELECT job_id FROM jobs WHERE status = 'QUEUED' AND updated_at < NOW() - 5 minutes`.
    It resets these back to `SCHEDULED` so the Dispatcher picks them up again.

#### Failure 2: Dispatcher crashes after reading from Redis, but before publishing to MQ.
*   **State:** Job is lost from the Time Wheel but isn't in the MQ.
*   **Solution:** Use a Lua script in Redis that atomically moves the job from the `ZSET` to a `PROCESSING_LIST`. Only remove it from the `PROCESSING_LIST` once the MQ returns a successful Publish ACK. If the node crashes, another node can read from the orphaned `PROCESSING_LIST`.

#### Failure 3: Worker crashes while executing a job.
*   **State:** Job is permanently stuck in `RUNNING`.
*   **Solution:** **Heartbeats**. When a worker picks up a job, it writes an expiring key to Redis: `SET heartbeat:{job_id} worker_id EX 30`.
    A background thread on the worker refreshes this TTL every 10 seconds. If the worker hard-crashes, the Redis key expires. The Reaper listens to Redis KeySpace Notifications (or scans the DB for `RUNNING` jobs with old `updated_at` timestamps) and transitions the job back to `QUEUED` or `SCHEDULED`, potentially incrementing a `retry_count`.

#### Failure 4: Worker completes the job, but crashes before updating DB to `COMPLETED`.
*   **State:** The Reaper will think the worker crashed (Heartbeat expires), requeue the job, and **it will run twice**.
*   **Solution:** The system guarantees **At-Least-Once** delivery. Exactly-Once in distributed systems is theoretically impossible without strict, shared transactional boundaries.
    *   **Mitigation:** The *Worker payload/business logic must be idempotent*. If the job is "charge credit card", the worker must pass a unique Idempotency Key (the `job_id`) to the Payment Gateway.

---

### 7. Explicit Tradeoffs

1. **RDBMS (PostgreSQL) vs. NoSQL (DynamoDB/Cassandra):**
   *   *Tradeoff:* We chose PostgreSQL. NoSQL offers better write scaling for 100M+ rows/day. However, RDBMS offers `SELECT ... FOR UPDATE SKIP LOCKED`, which is the absolute best way to build concurrent, lock-free queues without external locking mechanisms.
   *   *Mitigation:* To scale Postgres, we partition the table by date and shard by `tenant_id`.

2. **Push vs. Pull for Dispatching:**
   *   *Tradeoff:* The Dispatcher pushes to MQ, and workers pull from MQ. We could have bypassed MQ and had workers poll the DB directly.
   *   *Justification:* Bypassing the MQ would overwhelm the DB with polling queries from thousands of workers. The MQ acts as a vital shock absorber (load leveler) during peak execution times (e.g., millions of jobs scheduled exactly at midnight).

3. **Redis Time-Wheel vs. Native MQ Delayed Messages:**
   *   *Tradeoff:* RabbitMQ (via plugins) and AWS SQS both support delayed messages (e.g., "hide this message for 15 minutes"). Why build a Redis Time-Wheel?
   *   *Justification:* MQ delays are usually limited (SQS limit is 15 minutes). For a scheduler, jobs might be submitted *months* in advance. The DB + Redis approach allows infinite delay horizons while maintaining millisecond precision at execution time.

4. **One Row per Cron Execution vs. One Row per Cron Definition:**
   *   *Tradeoff:* When a cron job is defined, do we pre-generate 1,000 rows for the next 1,000 executions, or just keep one row and update its `run_at` after every execution?
   *   *Justification:* We chose the "Generate Next on Success" approach. Pre-generating rows pollutes the database and makes updating the cron expression a nightmare (requiring mass deletes/updates). Generating the next run dynamically is much more manageable, though it risks missing a cycle if the generation logic fails (mitigated by the Reaper).