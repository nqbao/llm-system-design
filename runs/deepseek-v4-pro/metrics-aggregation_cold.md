---

Design a real-time metrics aggregation pipeline that handles 100 billion events/day.


---

## Real-time Metrics Aggregation Pipeline for 100B Events/Day

### 1. Overview
The system ingests, aggregates, and serves metrics from **100 billion events per day** (average ~1.16M events/sec, peaks 2‑3×). It provides **sub‑minute query responsiveness** for aggregated results while supporting exact counters, gauges, and approximate percentiles. The pipeline is built around **Apache Kafka** for buffering, **Apache Flink** for stream processing, and a **Time‑Series Database (TSDB)** for storage, with careful attention to fault tolerance, exactly‑once semantics, and horizontal scalability.

### 2. Requirements
- **Throughput**: 100B events/day (1.16M/s average, 3.5M/s peak).
- **Latency**: Aggregated metrics available for queries within 10–30 seconds.
- **Aggregation**: Sum, count, average, min/max, p50/p95/p99 percentiles, sliding/tumbling windows (1 min, 5 min, 1 hour).
- **Cardinality**: Up to 1 million distinct metric series (metric name + tag set).
- **Retention**: Raw events kept 7 days for replay; aggregated data retained 1 year.
- **Consistency**: Exactly‑once processing semantics end‑to‑end.
- **Availability**: 99.9% uptime, no single point of failure, survive zone/region outages.

### 3. System Components

#### 3.1 Event Ingestion
- Producers (services, mobile apps, IoT) emit JSON or Protobuf events, avg size **500 bytes**.
- A fleet of **stateless ingestion gateways** (HTTP/TCP) behind a global load balancer (e.g., Envoy, AWS NLB). Gateways validate schema, attach an event time if missing, and map event to a target Kafka topic.
- Events are pushed to **Kafka** with a partitioning key (e.g., `metric_name + tag_hash`). This ensures order per metric series and parallel downstream processing.

#### 3.2 Transport Layer – Apache Kafka
- **Topic**: `raw-events` with **200 partitions** (over‑provisioned for future growth).
- **Throughput per partition**: ~6K msg/s avg, 20K peak – well within Kafka’s 100K msg/s limit.
- **Broker sizing**: 
  - Raw data volume: 100B × 500B ≈ **50 TB/day** ≈ 5.8 Gbps avg, 17 Gbps peak.
  - With compression (Snappy/ZSTD) effective size ~10‑15 TB/day.
  - **20 brokers** (AWS i3en.3xlarge: 15 TB NVMe, 25 Gbps net), replication factor 3. Disk capacity: 20 brokers × 15 TB / 3 = 100 TB usable → enough for 7‑day retention (~70 TB compressed). Network: each broker ~3 Gbps out → well within 25 Gbps.
- **Retention**: 7 days for raw events, 7 days for consumer offsets (`__consumer_offsets`).

#### 3.3 Stream Processing – Apache Flink
- **Job**: “Metrics Aggregator” – consumes from `raw-events`, performs keyed window aggregations, writes to TSDB and alerting sink.
- **Parallelism**: **200** (matching Kafka partitions) to avoid repartitioning. Each task handles ~5.8K msgs/s.
- **Windows**:
  - *Tumbling windows*: 1‑minute for real‑time dashboards.
  - *Sliding windows*: 5‑minute every 1 minute for alert evaluation.
  - *Tumbling hourly*: for long‑term rollups.
- **State**:
  - Counters, sums, and HdrHistograms/T‑Digest for percentiles (approximate, low memory).
  - RocksDB back‑end for state larger than task memory (handle 1M series × 4 KB ≈ 4 GB per task, easily managed).
- **Event‑time processing**: Watermarks based on event timestamp, allowed lateness 30 seconds. Late events are side‑output to a dead‑letter topic for offline correction.
- **Checkpointing**: Every 60 seconds with incremental RocksDB checkpoints to durable storage (HDFS/S3). Exactly‑once guarantees via Kafka transactional producer and two‑phase commit sink.

#### 3.4 Storage – Time‑Series Database
- **InfluxDB Clustered** (or TimescaleDB) – optimized for high‑cardinality writes.
- **Write load**:
  - 1 minute tumbling window produces **1M points** (one per series) → **16.7K points/sec**.
  - With replication factor 2, ≈33.4K points/sec total.
  - InfluxDB single node handles 50‑100K writes/s; 3‑node cluster sufficient.
- **Retention policies**:
  - Raw aggregated: 30 days (on SSD).
  - Downsampled: 1‑year rollups (1‑hour precision) on HDD.
- **Queries**: Grafana connects directly; support for continuous queries to pre‑compute daily aggregates.

#### 3.5 Alerting & Monitoring
- **Alerting**: Flink side‑output for threshold violations → Kafka topic → alert manager (e.g., OpsGenie, PagerDuty). Low latency (seconds).
- **Back‑office**: Offline batch jobs (Spark) on raw Kafka data to recompute metrics if needed, correct for late data, or train anomaly detection models.

### 4. Detailed Capacity & Scaling Calculation

| Component | Math | Result |
|-----------|------|--------|
| **Ingress bandwidth** | 100B × 500B / 86,400s | 5.8 Gbps avg |
| **Peak Kafka throughput** | 3 × avg = 17.4 Gbps | 20 brokers @ 10 Gbps usable |
| **Kafka disk (7d retention, RF=3)** | 50 TB/day × 7 × 3 × 0.3 (compressed) | ≈315 TB → 20 × 15 TB usable > 315 |
| **Flink task CPU** | 200 tasks × 5.8K msg/s × 1 ms/msg | ~1.16 cores (plenty of headroom); allocate 2 vCPUs per task → 400 vCPUs, i.e., ~50 task manager nodes of 8 vCPUs each. |
| **Flink state size** | 1M series × 4 KB | ≈4 GB per task, but state is sharded across 200 tasks → each handles 5K series → 20 MB state per task. RocksDB works fine. |
| **TSDB write load** | 1M points/min = 16.7K p/s, RF2 → 33.4K p/s | 3‑node InfluxDB cluster (each 16 vCPUs, 32 GB RAM) |
| **TSDB storage (1‑year aggregated)** | 1M series × 1 point/min × 1B (compressed) × 525,600 min/yr = ~0.5 TB/year. Add downsampled → ~1 TB. | Single node disk easily handles it. |

**Horizontal scaling**: Add Kafka brokers & Flink task managers linearly. Increase Kafka partitions to scale Flink processing.

### 5. Fault Tolerance & Failure Scenarios

- **Kafka Broker Failure**: Replication (min.insync.replicas=2) ensures durability; consumers can tolerate one replica offline. Leader re‑election < 1s.
- **Flink Job Manager Failure**: ZooKeeper‑based HA with standby JobManager. Checkpointed state restores from S3. TaskManagers reconnect and resume from last checkpoint. Recovery time < 1 minute.
- **Late/out‑of‑order events**: Watermarks allow 30 seconds lateness; otherwise side‑output for manual correction, preventing window contamination.
- **TSDB Downtime**: Flink sink uses a retry+dead‑letter mechanism; alerts if lag grows. TSDB clustered, no single point of failure.
- **Exactly‑once Sink**: Flink uses two‑phase commit with Kafka and TSDB atomic batches. TSDB ingest supports idempotent writes via deduplication keys.
- **Backpressure**: Flink’s network stack and Kafka’s dynamic throttling prevent overflow. Overload triggers alerting and operator auto‑scaling (e.g., increase task managers).

### 6. Trade‑offs

- **Consistency vs. Latency**: Exactly‑once (transactional) adds small latency (10‑50ms) to writes, acceptable for 10‑30s end‑to‑end.
- **Accuracy vs. Cost**: Percentiles use approximate sketches (t‑digest) to bound memory; exact percentiles would require storing all events per window, infeasible at this scale.
- **Simplicity vs. Performance**: Kafka Streams would be simpler but lacks Flink’s rich event‑time and state management. Flink chosen for its maturity at scale.
- **Storage vs. Compute**: Retaining raw events in Kafka (7 days) enables ad‑hoc reprocessing but requires large disk; could be reduced to 1 day with tiered storage (S3) to save cost.

### 7. Architecture Diagram

```mermaid
graph TD
    subgraph Producers
        A[Web App] -->|JSON/Protobuf| B(Load Balancer)
        C[Mobile/IoT] --> B
    end

    subgraph "Ingestion Tier (Stateless)"
        B --> D[Gateway 1]
        B --> E[Gateway 2]
        B --> F[Gateway N...]
    end

    subgraph "Kafka Cluster (20+ brokers)"
        D & E & F -->|Produce partitioned| G[raw-events topic<br/>200 partitions, RF=3]
        G -->|Consume| H
    end

    subgraph "Stream Processing (Apache Flink)"
        H[TaskManagers (200 parallelism)] -->|Event-time windows / state with RocksDB| I[Windowed Aggregations]
        I --> J[Aggregated Metrics Sink]
        I --> K[Alert Side-output]
    end

    subgraph "Sinks & Storage"
        J -->|Exactly-once, 2PC| L[InfluxDB Clustered<br/>3 nodes, RF=2]
        L --> M[Grafana Dashboards]
        K -->|Alert events| N[Kafka alert topic]
        N --> O[Alert Manager → PagerDuty]
        G -.->|Batch replay / correction| P[Spark on YARN/k8s<br/>offline recompute]
        P --> L
    end

    subgraph "Monitoring & HA"
        Q[ZooKeeper] --- Kafka & Flink
        R[Checkpoint Store (S3/HDFS)] --- I
        S[Prometheus] --- All
    end

    style A,C fill:#cff,stroke:#333
    style L fill:#f9f,stroke:#333
    style G fill:#ff9,stroke:#333
```

### 8. Conclusion
This design delivers a cost‑effective, horizontally scalable pipeline for 100B events/day, providing sub‑minute aggregated metrics with exactly‑once guarantees. By leveraging Kafka’s partitioning, Flink’s stateful windowing, and a dedicated TSDB, we achieve both real‑time responsiveness and long‑term historical querying. The system gracefully handles failures and can scale linearly by adding resources to each layer.