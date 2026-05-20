---

Design a real-time metrics aggregation pipeline that handles 100 billion events/day.


---

## 1. Requirements & Scope

| Item | Value |
|------|-------|
| **Throughput** | 100 × 10⁹ events / day ≈ **1 160 000 events/s** |
| **Event size** | Average **200 bytes** (JSON/Avro) |
| **Raw event retention** | **7 days** (for replay & debugging) |
| **Aggregated data resolution** | **10 s tumbling windows** (configurable) |
| **Latency** | **≤ 5 s** from ingestion → aggregated output |
| **Exactly‑once** | Where possible (particularly for counters) |
| **High availability** | ≥ 99.99 % for ingestion & aggregation |
| **Observability** | All components monitored & metrics exposed |
| **Security** | TLS for transport, IAM/role‑based access to storage, encryption at rest |
| **Cost model** | On‑prem or cloud; design preserved for either |

_The pipeline must support 100 B events per day – roughly 1.16 M events every second – with minimal loss and near‑real‑time aggregation._

---

## 2. High‑level Architecture

```
+---------------------------+      +---------------------------+
|         Producers         |----->|         Load Balancer     |
|   (App agents, SDKs, API) |      +---------------------------+
+------------+--------------+                     |
             |                                   |
             | (Batch + compression + TLS)          |
             v                                   v
+------------------------------+   Kafka Connect     +--------------------------+
|       Kafka Cluster          | <----------------- |  S3 (Parquet, GZIP)      |
|  (topic: raw_events)         |  Replay raw data   +--------------------------+
+------------------------------+                      |
             |  (source connector)                  |
             v                                      |
+------------------------------+   Streaming job      +---------------------------+
|  Flink Cluster (Stream UI)   |——→   [Window + Aggregate]——→  | Druid / ClickHouse |
|  State: RocksDB (key≈sum/count) |   Aggregated 10‑s windows |  (real‑time)  |
+------------------------------+                                     +---------------------------+
             |
             v
+------------------------------+
|  Query Layer (Presto/Druid)  |
+------------------------------+
             |
             v
+------------------------------+
|   Dashboards / UI / API      |
+------------------------------+
```

*All communication over TLS/SSL. Each component is distributed and horizontally scalable.*

---

## 3. In‑depth Component Design

| Component | Responsibility | Design Choices | Capacity Notes |
|-----------|----------------|----------------|----------------|
| **Producers** | Emit raw metric events to ingestion layer. | gRPC/HTTP → ingestion endpoint. Batch up to 1 k events, compress (Snappy) before sending. Use idempotent serializer (Avro or Protobuf). | 100 % of YAML throughput is produced by 1500+ microservices ⇒ Use Kubernetes services + Horizontal Pod Autoscaler. |
| **Ingestion Layer / Load Balancer** | Distribute traffic to a pool of ingestion servers. | NGINX+TLS termination; distribute over `k` ingestion pods. 1000s of concurrent connections => keep `3×` headroom. | Each ingestion server forwards to Kafka producer with `acks=all`, retry=5. |
| **Kafka Cluster** | Durable buffer, decouples producers and stream processors. | 12 brokers (3‑AZ pattern). <br> Replication Factor = 3. <br> 400 partitions (≈ 8 partitions/ broker). <br> Topic retention = 7 days (raw). <br> `min.insync.replicas=2`. | Throughput per partition ≈ 2 MB/s (200 B × 10 k).<br> 400 partitions × 2 MB/s = 800 MB/s > 232 MB/s. <br> Disk: 400 × (200 B × 1 160 k × 86400 s)/3 ≈ 75 TB (≈ 6.25 TB/ broker). <br> Each broker ~12 TB SSD for redundancy. |
| **Kafka Connect** | Move raw events to S3 for 7‑day retention and re‑play. | Source connector “kafka‑to‑s3” (S3‑Sink). <br> 64 kB compression (gzip). <br> Store as Parquet for analytics. | Throughput 232 MB/s → ~6.6 TB/day. |
| **Stream Processor (Apache Flink)** | Stateful aggregation: tumbling 10 s windows; counters, sums; optional late‑data handling. | <br> **Parallelism**: 512 operators → 512 slots (cumulatively 512 cores). <br> **State Backend**: RocksDB; TTL 1 hour.<br> **Checkpoints**: 30 s interval, checkpoint store S3, 200 GB/ day.<br> **Exactly‑once to Kafka**: use Flink’s `KafkaSink` with idempotent commit. | **CPU**: 10 k events/s per core (200 µs per event).<br> 1.16 M / 10 k = **116 cores** needed. <br> Allocate 512 cores for headroom & parallel user jobs.<br> **Memory**: RocksDB ~ 4 GB per TaskManager slot + 1 GB JVM. |
| **Aggregated Store (Druid)** | Real‑time micro‑service with CRUD & analytics. <br> Persist 10‑s aggregated rows. | Druid real‑time ingestion <br> Time granularity: 10 s <br> Partition by `metric+tags` <br> Compaction incremental.<br> Off‑heap storage = 70% of cluster memory. | **Estimated rows**: 100 M distinct key combinations. <br> 10‑s windows → 8640 windows per day. <br> Each row avg 150 B → 1.3 TB/day raw. <br> Compression ~5:1 → 260 GB. <br> 2‑AZ cluster, 5 nodes, 4 Tb total. |
| **Fallback Store (ClickHouse)** | If instant SQL queries needed. <br> Uses MergeTree on 10‑s granularity. | Similar row estimates. | 200 GB/day size.  |
| **Query Layer (Presto/Druid)** | APIs for ad‑hoc/BI queries. <br> Executes against ClickHouse (SQL) or Druid (native). | Presto+Trino on 8‑node cluster. <br> Query latency < 2 s for aggregated data. | Low CPU per node; 2 GB RAM minimal. |
| **Dashboard / UI** | Grafana or custom React app → Query API. | Any open‑source Prometheus or Druid dashboards. | 50 concurrent users → trivial. |

### 3.1  Exactly‑Once & Idempotency

- **Kafka Producers**: `enable.idempotence=true` (Kafka guarantees ≤1 duplicate).  
- **Flink**: checkpoints held in S3; rollback on failure.  
- **Druid**: dedup + “merge last commit” for overlapping segments.  

### 3.2  Backpressure & Flow‑Concealment

- **Kafka**: `fetch.min.bytes` and `fetch.max.wait.ms` tuned for bursty traffic.  
- **Flink**: `parallelism` of source operator set to match Kafka partition count.  
- **Connector**: `exactly_once` and `max.parallelism`, use back‑pressure metrics.  

### 3.3  Retention & Archival

| Store | Retention | Storage | Comments |
|-------|-----------|---------|----------|
| Raw Kafka | 7 days | SSD (75 TB) | Flex re‑play, low-latency |
| S3 (Parquet) | 90 days | Glacier/Tiered | Cost-effective |
| Druid / ClickHouse | 30 days | SSD/ HDD | Fast queries |
| Aggregate snapshots | 365 days | Snowflake / BigQuery | All‑time analysis |

---

## 4.  Capacity Calculations

| Metric | Value |
|--------|-------|
| **Throughput** | 1 160 000 events/s |
| **Raw byte rate** | 232 MB/s |
| **Raw daily storage** | 20 TB (uncompressed) → 6.6 TB (gzip) |
| **Kafka storage** | 20 TB × 3 (replication) = 60 TB/day → 420 TB for 7 days |
| **Flink state** | 10 m distinct keys × 200 B = 2 GB;  
  RocksDB overhead 4× ⇒ 8 GB;  
  1 TaskManager = 16 cores → 128 GB RAM. |
| **Druid ingestion** | 100 M rows × 150 B = 15 GB; GZIP 5:1 → 3 GB;  
  5 nodes → 0.6 TB |
| **Checkpoints** | 512 TB/ day for all state snapshots (per 30 s). |

### Horizontal Scaling Plan

| Layer | Burden | Scale Out Unit | Scaling Units |
|-------|--------|---------------|---------------|
| Kafka | I/O, storage | Broker + 1 TB SSD | 8–12 brokers |
| Flink | CPU + state | TaskManager 32‑core | 15–20 TaskManagers |
| Druid | Ingestion slots, storage | Historical+MiddleManager | 6‑10 nodes |
| Query | SQL, aggregation | Presto node | 8 nodes |

---

## 5.  Failure Modes & Mitigation

| Failure | Likely Root Causes | Mitigation |
|---------|--------------------|------------|
| **Kafka broker failure** | PMCs, disk, network | Replication 3, ZK/KRaft, Kafka’s `broker.remote.exception` |
| **Producer outage** | Service crash, network | Retries, batch ack, dead‑letter queue per topic |
| **Flink job loss** | Crash, memory overcommit | Checkpoints stored in object store, restart strategy `exactly_once` |
| **State corruption** | Checkpoint failure | Retried through S3 checkpoint store; VVV, TTL |
| **Disk full on broker** | Unbounded retention | Expire older topics, increase S3 fallback |
| **S3 durability loss** | Access IAM misconfig | Encryption, multi‑AZ; IAM role, `aws:SecureTransport` |
| **Backpressure causing burst** | Traffic spike | Rate‑limit producers, auto‑scale |
| **Network partition** | AZ outage | Multi‑AZ deployment, Route53; LB health checks |

**Hot‑SWAP Recovery**

- Kafka: Delete the failed broker and replace; use `kafka-reassign-partitions`.  
- Flink: Kill job, resume from latest checkpoint.  
- Druid: Delete target segments; ingestion re‑starts.  

**Disaster Recovery**

- Backups daily of Kafka config, Flink checkpoints, Druid segments to S3.  
- S3 cross‑region replication for raw events.  
- Automated AWS CloudFormation or Terraform scripts.

---

## 6.  Security & Compliance

| Layer | Controls |
|-------|----------|
| **Transport** | TLS 1.3 everywhere (Kafka client; Flink; Druid). |
| **Auth** | OAuth2 for Producers API; IAM roles for IAM sequencing and Cred Store. |
| **Encryption** | Kafka at‑rest (EBS‑encrypt), S3 server‑side encryption, Druid segments encrypted. |
| **Audit** | Kafka ACLs, Flink job logs, Druid logs to CloudTrail. |
| **Retention** | GDPR “right to delete”, at 7 days raw is challenged; external audit logs kept. |
| **Vulnerability** | Regular scanning of OS images, dependency audit, use of hardened base images. |

---

## 7.  Observability

- **Metrics**: Prometheus with exporters for Kafka (`kafka_exporter`), Flink (`flink-exporter`), Druid (`druid.zip`), S3 usage (`s3_exporter`).  
- **Logs**: ELK/Kibana stack or cloud‑native (ECS).  
- **Tracing**: OpenTelemetry across producers → Kafka → Flink → Druid.  
- **Alerts**: Jitter alerts: Kafka lag > 10 m, Flink backpressure > 100 ms, Druid segment error > 5 %.

---

## 8.  Cost Baseline (AWS for 30‑day month)

| Resource | Qty | Cost/month |
|----------|-----|-----------|
| **EKS** (K8s) | 12 nodes (c5.4xlarge) | $9 k |
| **Kafka** (MSK) | 12 brokers (c5.6xlarge + 4 TB NVMe SSD) | $15 k |
| **Flink** (g5dn.4xlarge) | 20 nodes | $20 k |
| **Druid** (m5.8xlarge) | 8 nodes | $25 k |
| **S3** (Glacier) | 420 TB *10 % / day | $8 k |
| **Network** | VPC peering, IGW | $1 k |
| **Monitoring** | CloudWatch, Prometheus | $2 k |
| **Support** | Enterprise | $2 k |
| **Total** |  | **$81 k** |

_Tightening cluster sizing (more nodes, fewer nodes) will trade latency for cost._

---

## 9.  Deployment Road‑map

| Phase | Deliverables | Time |
|-------|--------------|------|
| P0 | Producer SDK, ingestion API, Kafka cluster, baseline testing | 4‑6 wks |
| P1 | Flink job (window + counter), Druid ingestion, S3 archive | 6‑8 wks |
| P2 | Query layer (Presto), dashboards | 4‑6 wks |
| P3 | Security hardening, IAM roles, encryption | 2‑4 wks |
| P4 | Auto‑scaling, monitoring, alerting | 3‑5 wks |
| P5 | Disaster‑recovery drills, cost optimization | 4‑6 wks |

---

## 10.  Mermaid Diagram

```mermaid
graph TD
  subgraph Producers
    P1[Microservice A] -->|Batch+Compress| L1[Ingress LB]
    P2[Microservice B] -->L1
  end

  L1 -->|TLS+HTTP| I[Ingestion Pods]
  I -->|Kafka Producer| K[KAFKA: `raw_events`]
  K -->|Kafka Connect| S3[RAW_EVENTS_S3]
  K -->|Source Connector| F[Flink Stream]
  F -->|State (RocksDB) | R(RocksDB)
  F -->|Sink (Druid)| D[DRUID_STORE]
  D -->|Query API| Q[Query Layer]
  Q -->|Grafana/BI| UI[Dashboard/UI]

  subgraph Monitoring
    F -->|Prometheus| M1[Prometheus]
    K -->M1
    D -->M1
    UI -->M1
  end
```

---

## 11.  Key Trade‑offs

| Decision | Pros | Cons | When to pick |
|----------|------|------|--------------|
| **Kafka vs Pulsar** | Kafka mature, many producers; Pulsar has per‑segment storage, easier to finality. | Pulsar lower throughput each partition. | Use Kafka for proven throughput. |
| **Flink vs Spark Structured Streaming** | Low‑latency (sub‑second); Spark > faster batch. | Flink more complex to manage; larger memory per task. | Real‑time 5‑s requirement → Flink. |
| **Druid vs ClickHouse** | Druid maxes out query latency and roll‑up; ClickHouse simpler, cheaper. | Druid more operational overhead. | Druid for 10‑s online, ClickHouse as fallback. |
| **S3 vs HDFS** | S3 cost‑effective, global access. | Latency for small reads. | Raw retention 7 days → S3. |
| **Retain 1 day on Kafka vs 7 days** | Lower storage cost, higher throughput. | Can't re‑process older data. | Use 1 day with fallback Connect.|
| **Compression** | Reduces storage & network. | CPU overhead on producers. | Use Snappy or LZ4 on producers. |

---

## 12.  Summary

- The pipeline ingests **100 B events/day** (~1.16 M evt/s) using a **Kafka‑based buffer** (12 brokers, 400 partitions, 3‑fold replication).  
- **Flink** consumes, windows (10 s tumbling), aggregates counters & sums, materializes to **Druid** (real‑time) for low‑latency queries, with a fallback **ClickHouse** for SQL workloads.  
- Raw events are archived to **S3** via Kafka Connect in Parquet‑gzip for 90 days.  
- Exactly‑once semantics are achieved via Kafka idempotence, Flink checkpointing, and Druid dedup.  
- Scaling: 512 CPU cores across 20 Flink TaskManagers, 12 Kafka brokers, 5–10 Druid nodes.  
- Cost: ~US$80k/month on AWS; can be tuned to lower budgets or higher performance.  
- Observability: Prometheus metrics, logs, tracing, alerting.  
- Security: TLS, IAM, encryption, audit logs.  
- Failure handling: replication, checkpoints, reprocessing, backpressure.

With this design, the system can sustain the required volume, provide real‑time aggregated metrics with sub‑minute latency, and be resilient to failures at scale.