---

Design a distributed job scheduler.


---

# Distributed Job Scheduler – End‑to‑End Design  

*Prepared: May 2026*  

---  

## 1. Problem Statement  

Build a **high‑throughput, low‑latency, fault‑tolerant distributed job scheduler** that can accept millions of jobs per day, schedule them onto a pool of workers, track progress, and provide observability and security guarantees.  

Typical use‑cases: data‑pipeline steps, background analytics, periodic cron‑like jobs, and user‑initiated batch processing.  

---  

## 2. Requirements  

| Category | Requirement | Rationale |
|----------|--------------|-----------|
| **Functional** | **Job submission API** (Create, Cancel, Query, List) | External services need a programmatic entry point. |
| | **Priority & fairness** (multiple priority levels, round‑robin per‑user) | Prevent starvation of low‑priority workloads. |
| | **Delayed / recurring jobs** (cron syntax) | Many batch workloads are periodic. |
| | **Resource‑aware dispatch** (CPU, RAM, GPU) | Workers have heterogeneous capacities. |
| | **At‑least‑once execution + idempotency support** | Guarantees work is not lost; callers can make jobs idempotent. |
| | **Result & status callbacks** (webhook / polling) | Downstream systems need to react to completion. |
| **Non‑functional** | **Throughput** ≥ 10 000 jobs /s (burst up to 20 000) | Large‑scale data pipelines. |
| | **Latency** ≤ 200 ms from submit to “scheduled” state (ignoring actual run time) | User‑facing systems need fast feedback. |
| | **Availability** 99.99 % (four‑nine) | Scheduler downtime must be rare. |
| | **Scalability** – horizontal scaling of API, Scheduler, Workers, and storage | Future growth. |
| | **Durability** – job metadata retained for at least 24 h | Allows troubleshooting and replay. |
| | **Observability** – metrics, logs, distributed traces | Operations & debugging. |
| | **Security** – authentication, authorization, isolation of job execution | Multi‑tenant environment. |

---  

## 3. High‑Level Architecture  

```
+-------------------+        +-------------------+        +-------------------+
|   API Gateway /   |  RPC   |   Scheduler Ring  |  PubSub|   Job Queue (Kafka)|
|   Load Balancer   +------->+ (Leader + Followers)+----->+   (topic per priority)|
+-------------------+        +-------------------+        +-------------------+
         |                                 ^                       ^
         |                                 |                       |
         |            Pull/Push            |                       |
         v                                 |                       |
+-------------------+        +-------------------+        +-------------------+
|   Auth / Rate Lim |        |   Scheduler      |        |   Worker Pool      |
|   Service (OAuth) |<------>+   (Consensus)   +<------>+   (K8s Pods)       |
+-------------------+        +-------------------+        +-------------------+
         |                                 |                       |
         |                                 |   Heartbeat / Status   |
         v                                 v                       v
+---------------------------------------------------------------+
|                     State Store (Postgres + etcd)            |
|   • Job metadata (Postgres)   • Leader election & config (etcd)|
+---------------------------------------------------------------+
         |
         v
+-------------------+        +-------------------+        +-------------------+
|   Metrics (Prom)  |  --->  |   Logging (ELK)   |  --->  |   Alertmanager     |
+-------------------+        +-------------------+        +-------------------+
```

*The diagram is reproduced in mermaid syntax later.*

### Core Components  

| Component | Responsibility | Key Technologies (examples) |
|-----------|----------------|-----------------------------|
| **API Gateway** | TLS termination, request routing, rate‑limit, auth token validation | Envoy, Nginx, Cloud‑LB |
| **Job Submission Service** | Validate JSON schema, enforce quotas, write job record to Postgres, push to Kafka | gRPC, Go/Java, JSON‑Schema |
| **Scheduler Ring** | Leader election (Raft via etcd), pull jobs from Kafka, match jobs → workers (resource‑aware), commit offsets | etcd, Raft, Kotlin/Go |
| **Job Queue** | Durable ordered log, per‑priority partitions, high‑throughput ingestion | Apache Kafka (3‑replica, min.insync.replicas=2) |
| **Worker Pool** | Pull jobs, spin up isolation container (Docker/CRI‑O), enforce cgroup limits, report status & result | K8s Deployments, containerd, side‑car heartbeat |
| **State Store** | Persistent job metadata, status history, auditing, configuration | PostgreSQL (partitioned tables), etcd for transient config |
| **Observability Stack** | Export Prometheus metrics, structured logs to Elasticsearch, trace spans (OpenTelemetry) | Prometheus, Grafana, Loki, Jaeger |
| **Auth Service** | Issue/validate JWT, map to RBAC policies, per‑user throttling | OIDC provider (Keycloak), Redis cache for tokens |

---  

## 4. Detailed Data Flow  

1. **Submit**  
   - Client → **API GW** (TLS) → **Job Service** (gRPC).  
   - Service validates payload, assigns a **UUID** (`job_id`), stores a row in `jobs` table (status = `PENDING`).  
   - Payload (≤ 10 KB) is serialized and produced to Kafka topic `jobs.high`, `jobs.normal`, or `jobs.low` based on priority.  

2. **Schedule** (leader only)  
   - Scheduler continuously **polls** the highest‑priority partitions it owns (Kafka consumer group `scheduler`).  
   - For each record, it **matches** job resource request (`cpu:2, mem:1Gi`) against a **Worker Capability Registry** (cached in etcd, refreshed via heartbeats).  
   - Once a suitable worker (`worker_id`) is found, the scheduler **writes a dispatch record** to a **dispatch Kafka topic** (`dispatch.high`).  
   - The original job record offset is **committed only after dispatch write succeeds**, guaranteeing at‑least‑once semantics.

3. **Dispatch → Worker**  
   - Workers run a **pull consumer** on the `dispatch.*` topics (partition per worker for ordering).  
   - Upon receipt, worker **acknowledges** (commits offset) **iff** it can start the job (e.g., container image pulled).  
   - Worker writes **status=RUNNING** to Postgres and starts the isolated container.  

4. **Run & Complete**  
   - Container runs; on exit, worker captures **exit code, stdout/stderr**, optional result JSON.  
   - Worker writes **status=SUCCEEDED/FAILED**, `finished_at`, `attempts`, `result_blob` back to Postgres.  
   - Worker publishes a **completion event** on Kafka topic `job-complete`.  

5. **Client Notification**  
   - Optional webhook UI: a **notification service** consumes `job-complete` and POSTs to the URL supplied at submission.  
   - Alternatively, clients poll **GET /jobs/{id}**.

6. **Retry / Backoff**  
   - Scheduler watches for jobs with status `FAILED` and `attempts < max_attempts`.  
   - It schedules a **delayed re‑enqueue** (exponential backoff, jitter) via a **Kafka delayed‑message queue** (or a dedicated “timer” table).  

---  

## 5. Capacity Planning & Math  

Assumptions (adjustable per‑deployment).  

| Parameter | Value | Reason |
|-----------|-------|--------|
| **Average job payload** | 10 KB | JSON description, container image tag, maybe small input data |
| **Average job result size** | 1 KB | exit code & tiny stdout |
| **Job execution time (CPU)** | 100 ms CPU (0.1 core) | Typical lightweight background task |
| **Concurrent jobs per worker** | 100 (container + cgroup) | Modern node with 64 vCPU can multiplex 100 lightweight containers |
| **Worker node spec** | 64 vCPU, 128 GiB RAM, 10 Gbps NIC | Typical B2‑large VM |
| **Target throughput** | 10 000 jobs/s (burst 20 000) | Business requirement |

### 5.1 Compute  

- **CPU required**: `10 000 jobs/s × 0.1 core = 1 000 cores`.  
  → With 64‑core workers, need **≈ 16 workers** to sustain steady‑state (but we over‑provision for spikes, so round to **20 workers**).  

- **RAM for jobs**: assume each running job holds 10 MiB of temporary data (e.g., in‑memory buffers).  
  `100 jobs × 10 MiB = 1 GiB` per worker; plus OS & container runtime ≈ 2 GiB.  
  → 20 workers → **≈ 40 GiB** RAM.  

- **Network I/O per worker**: Pull 10 KB payload + push 1 KB result = 11 KB per job.  
  `100 jobs/s × 11 KB = 1.1 MiB/s` per worker.  
  20 workers → **≈ 22 MiB/s** (~0.2 Gbps), well below a 10 Gbps NIC.  

### 5.2 Kafka  

- **Ingress rate**: `10 000 jobs/s × 10 KB = 100 MiB/s`.  
- **Dispatch & completion streams** add another ≈ 11 MiB/s each.  
  → Total ≈ 133 MiB/s ≈ **1.07 Gbps**.  

Kafka cluster sizing (3‑node):  
- Each broker handles ≈ 0.36 Gbps i/o → well within 10 Gbps NICs.  
- Disk: assume **log retention 24 h** → `133 MiB/s × 86400 s = 11.5 TiB`.  
  → Use 3 × 4 TiB SSD per broker, configured with replication factor 3 → raw on‑disk ≈ 34.5 TiB (still feasible with modern NVMe).  

### 5.3 PostgreSQL  

- Row size ≈ 1 KB (metadata) + optional `result_blob` (1 KB).  
- **Write rate**: 10 000 inserts/s + 10 000 updates/s ≈ 20 000 write ops/s.  
- With **partitioned tables** by day and **indexes** on `job_id`, `status`, `submitted_at`, a single primary‑replica (e.g., 16‑core, 64 GiB, NVMe) can handle ~30 k‑50 k write ops/s comfortably.  
- **Retention**: 24 h → ≈ 864 MiB of data; << 100 GiB, trivial.  

### 5.4 etcd  

- Stores **worker capability** (≈ 200 bytes per worker) and **scheduler shared config**.  
- With 100 workers, total ≈ 20 KB.  
- Write load: heartbeat every 5 s → 20 writes/s, negligible.  

---  

## 6. Scalability Strategies  

| Dimension | Scaling Method | Detail |
|-----------|----------------|--------|
| **API** | Horizontal pod autoscaling (HPA) based on request latency & CPU | Stateless; can be fronted by a CDN/LB. |
| **Scheduler** | **Active‑passive** leader election; multiple follower nodes pulling same partitions (read‑only) to pre‑fetch & cache worker capabilities – only leader writes dispatch. Scale followers for read pressure. |
| **Kafka** | Add partitions (e.g., 96 partitions per priority) → more parallel consumers (workers). | Ensure ordering per‑job is not required (only per‑priority). |
| **Workers** | HPA on Kubernetes, metric = `queued_jobs_per_worker` or `CPU utilisation`. | New nodes spin up in seconds; cluster auto‑provisioning via cluster‑autoscaler. |
| **Postgres** | Scale read replicas for API status queries; primary handles writes. Use logical replication for reporting dashboards. |
| **Observability** | Scrape metrics at 30‑second intervals; use sharding for high‑cardinality labels (e.g., tenant‑id). |
| **Multi‑region** | Deploy independent clusters per region with a **global load balancer**; a **replication pipe** (Kafka MirrorMaker) propagates jobs across regions if needed for disaster recovery or locality. |  

---  

## 7. Trade‑offs  

| Decision | Pro | Con / Risks |
|----------|-----|--------------|
| **Leader‑only dispatch** (single scheduler leader) | Simple global view, avoids duplicate dispatch, easy ordering per priority. | Scheduler leader is a hot spot; need fast failover (≤ 1 s) to avoid stalls. |
| **Push model (scheduler → worker)** vs **Pull model** | Push gives lower latency, but requires workers to be reachable (NAT/firewall issues). | Pull (worker‑initiated) works behind NAT, simpler scaling, but adds a small poll latency (≤ 100 ms). |
| **Kafka vs Redis Streams** | Kafka provides durability, replay, and massive throughput; retains data for 24 h+. | Higher latency (few ms) vs Redis (sub‑ms). Redis is memory‑bound, less safe for loss. |
| **Container isolation (Docker)** vs **process isolation (chroot + cgroups)** | Containers support arbitrary user code, images, reproducibility. | Higher startup overhead (≈ 200 ms) vs lightweight process (≈ 50 ms). |
| **Strong consistency on job status** (Postgres) vs **Eventual consistency** (Cassandra) | Easy to query current state, ACID guarantees. | Write scalability limited; but given our 20 k writes/s, Postgres remains comfortable. |
| **At‑least‑once execution** vs **Exactly‑once** | Simpler implementation; jobs can be idempotent. | Requires user‑level idempotency logic; duplicate runs possible during failure recovery. |
| **Single global priority queue** vs **Separate queues per tenant** | Simpler scheduler logic. | Potential starvation of low‑traffic tenants; per‑tenant queues add fairness but increase management overhead. |

---  

## 8. Failure Scenarios & Mitigations  

| Failure Mode | Impact | Detection | Recovery / Mitigation |
|--------------|--------|-----------|-----------------------|
| **Scheduler leader crash** | No new dispatches until new leader elected. | Heartbeat loss in etcd; election timeout. | Raft automatically elects new leader (< 1 s). In‑flight jobs stay in `RUNNING`; workers keep executing. |
| **Worker node loss** (power failure, network partition) | Running jobs become orphaned. | Missed heartbeats, probe timeout (e.g., 30 s). | Scheduler marks jobs `FAILED` after a configurable grace period, increments `attempts`, and re‑queues (backoff). |
| **Kafka broker outage** (single node) | Temporary ingestion slowdown. | Broker metrics, cluster alerts. | With 3‑replica config, remaining brokers continue; producer retries with exponential backoff. If >1 broker down, inbound throttling occurs; API can return `429 Too Many Requests`. |
| **Postgres primary outage** | No metadata writes → job submission blocked. | Health check failure, replication lag. | Failover to replica using Patroni; DNS / service endpoint switches; brief (~10 s) unavailability. |
| **Network saturation** (e.g., burst > 200 k jobs/s) | Increased latency, possible request timeouts. | Queue depth spikes, API latency metrics > 500 ms. | Rate‑limit at API gateway; back‑pressure to producers; auto‑scale workers if headroom exists. |
| **Duplicate job execution** | Business logic violations if job not idempotent. | Detect via `job_id` + `attempt` in DB (unique index). | Scheduler deduplicates by checking status before dispatch; workers verify they have not already processed the same `job_id` (idempotency token). |
| **Time‑skew** (worker clocks far from scheduler) | Incorrect timeout handling, premature retries. | Compare worker‑reported timestamps with scheduler’s NTP. | Use logical timestamps from etcd (monotonic counter) for scheduling; ignore local clocks. |
| **Container image pull failure** | Job never starts. | Worker logs error; status `FAILED` with reason. | Automatic retry with back‑off; fallback to cached image registry; alert on persistent failures. |
| **Security breach** (malicious job) | Host compromise. | Runtime security monitors (Falco, eBPF) alert on syscalls. | Workers run in dedicated VMs or sandbox (gVisor/Firecracker). Use Kubernetes `PodSecurityPolicy` (or replacement) to restrict capabilities. |

---  

## 9. Security & Isolation  

| Layer | Controls |
|-------|----------|
| **Authentication** | JWT issued by OIDC provider; validated at API gateway. |
| **Authorization** | RBAC table in Postgres (`user → allowed tenant(s) → max concurrent jobs`). |
| **Network** | API only reachable via TLS 1.3; workers behind VPC, communicate over mutual‑TLS to Kafka & Postgres. |
| **Execution sandbox** | Each job runs in its own container with **read‑only rootfs**, limited **cgroup** resources, **seccomp** profile, **no privileged escalation**. |
| **Secret injection** | Use Vault to mount short‑lived tokens into container at start; tokens revoked after job ends. |
| **Audit logging** | Immutable log entries for every job submission, status change, and dispatch stored in WORM‑enabled S3 bucket. |
| **Compliance** | Data‑in‑flight encryption, at‑rest encryption of PostgreSQL (Transparent Data Encryption) and Kafka (SSL + SASL). |

---  

## 10. Observability  

| Metric (Prometheus) | Description |
|---------------------|-------------|
| `job_submissions_total{priority}` | Counter of submitted jobs. |
| `job_schedule_latency_seconds{priority}` | Histogram of time from `PENDING` → `SCHEDULED`. |
| `worker_queue_length{worker_id}` | Number of jobs waiting on a worker. |
| `scheduler_leader{}` | 1 if this instance is leader, 0 otherwise. |
| `kafka_consumer_lag{topic,partition}` | Pending records per partition. |
| `postgres_active_connections` | DB load. |
| `api_request_duration_seconds{code,method}` | API latency. |

*Tracing* – OpenTelemetry spans: `SubmitJob → Enqueue → Schedule → Dispatch → Run → Complete`.  

*Alerting* – Simple thresholds (e.g., `job_schedule_latency_seconds{p95} > 0.5s`), plus SLO burn‑rate alerts for 99.99 % availability.

---  

## 11. Operational Considerations  

1. **CI/CD** – Build container images for scheduler & worker services, push to private registry, Helm chart for K8s deployment.  
2. **Chaos testing** – Periodically kill a scheduler leader, drop a Kafka broker, block network to a worker, verify automatic recovery.  
3. **Capacity testing** – Use a load‑generator (e.g., Locust) to push 20 k jobs/s; monitor queue depth, latency, CPU.  
4. **Backup & DR** – Daily logical dump of Postgres to S3; Kafka MirrorMaker to a secondary region for up to 48 h replay.  
5. **Versioned job schemas** – Store schema version in job metadata; workers use side‑car to validate and migrate payload.  

---  

## 12. Implementation Sketch (Technology Choices)  

| Layer | Sample Open‑Source Stack |
|-------|--------------------------|
| API & Auth | **Envoy** + **Keycloak** + **gRPC‑Go** |
| Scheduler | **Go** service using **etcd client** for Raft, **sarama** for Kafka |
| Kafka | **Apache Kafka 3.x** (Confluent or self‑hosted) |
| Workers | **Kubernetes Deployments** (Pod per worker), container runtime **containerd**, job runner **Go** + **Docker SDK** |
| DB | **PostgreSQL 15** (partitioned by day) + **pg_partman** |
| Config Store | **etcd 3.5** |
| Observability | **Prometheus**, **Grafana**, **Jaeger**, **ELK** |
| Secrets | **HashiCorp Vault** + **Kubernetes CSI** |

*All components are stateless except the Postgres/Kafka data stores, allowing horizontally scaling the compute layer.*

---  

## 13. Mermaid Diagram  

```mermaid
graph TD
    %% External clients
    C[Client (REST/gRPC)] -->|TLS| LB[API Gateway / Load‑Balancer]
    LB -->|Auth & Rate‑limit| Auth[Auth Service (Keycloak/OIDC)]
    Auth -->|Valid JWT| Sub[Job Submission Service]
    
    %% Persistence
    Sub -->|Write metadata| PG[(PostgreSQL)]
    Sub -->|Publish payload| K1[Kafka Topic: jobs.high/normal/low]
    
    %% Scheduler ring
    subgraph SchedulerCluster[Scheduler Ring (etcd Raft)]
        S1[Scheduler Node 1]
        S2[Scheduler Node 2]
        S3[Scheduler Node 3]
        S1 -.->|Leader election| etcd[etcd Cluster]
        S2 -.-> etcd
        S3 -.-> etcd
    end
    K1 -->|Consumer group "scheduler"| S1
    S1 -->|Dispatch| K2[Kafka Topic: dispatch.high/normal/low]
    S1 -->|Write dispatch record| PG
    
    %% Workers
    subgraph Workers[Worker Pool (K8s Pods)]
        W1[Worker 1]
        W2[Worker 2]
        W3[Worker 3]
        %% many more ...
    end
    K2 -->|Pull per‑worker partition| W1
    K2 -->|Pull per‑worker partition| W2
    K2 -->|Pull per‑worker partition| W3
    
    %% Completion
    W1 -->|Write status/result| PG
    W1 -->|Publish| K3[Kafka Topic: job-complete]
    W2 -->|Write status/result| PG
    W2 -->|Publish| K3
    W3 -->|Write status/result| PG
    W3 -->|Publish| K3
    
    %% Notification
    K3 -->|Consume| Notify[Webhook/Callback Service]
    Notify -->|POST| C
    
    %% Observability
    PG -->|Metrics| Prom[Prometheus]
    S1 -->|Metrics| Prom
    W1 -->|Metrics| Prom
    K1 -->|Metrics| Prom
    etcd -->|Metrics| Prom
    subgraph Obs[Observability Stack]
        Prom
        Graf[Grafana Dashboard]
        Loki[ELK Logging]
        Jaeger[Jaeger Tracing]
    end
    Prom --> Graf
    PG --> Loki
    S1 --> Loki
    W1 --> Loki
    K1 --> Loki
    Sub --> Loki
    Notify --> Loki
```  

---  

## 14. Summary  

* The design delivers **≥ 10 k jobs/s** with **≤ 200 ms scheduling latency** while guaranteeing **four‑nine availability** through leader election, replicated Kafka, and resilient PostgreSQL.  
* Capacity calculations show that **≈ 20 worker nodes** (64 vCPU each) comfortably meet the compute demand, while a **3‑node Kafka cluster** and a **single primary PostgreSQL with read replicas** handle the I/O load.  
* The **pull‑based worker model** sidesteps NAT/firewall issues, and **Kafka** provides durable replay for at‑least‑once semantics.  
* Trade‑offs (central leader vs distributed dispatch, container vs process isolation) have been evaluated with explicit mitigations.  
* A comprehensive failure‑mode matrix, security hardening plan, and observability suite round out the design, enabling safe operation at scale.  

Implementations can start with the technology stack listed above, iterate through Chaos‑ and load‑testing, and then expand to multi‑region deployment for geo‑redundancy if required.