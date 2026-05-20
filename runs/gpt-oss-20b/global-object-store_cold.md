---

Design a global object storage system like S3.


---

## 1.  Overview & Design Goals  

| Goal | Why it matters |
|------|----------------|
| **Durability ≥ 99.999999999 % (eleven‑nines)** | Guarantees data is rarely lost even in large‑scale failures. |
| **Low‑latency, high‑throughput API** | Users expect sub‑second responses for reads/writes, especially for hot data. |
| **Global, multi‑region availability** | Reduce e‑latency, comply with data‑residency laws, support disaster recovery. |
| **Elastic scalability** | Accommodate petabyte‑scale growth and sudden traffic spikes without redesign. |
| **Cost‑effective** | Keep storage and network costs within bounds while satisfying durability. |
| **Strong security & compliance** | Encryption, IAM, audit logging, GDPR, HIPAA, etc. |
| **Transparent lifecycle & tiering** | Move data from hot (SSD) to cold (HDD/Tape) automatically. |

The system is a **S3‑like object store** that exposes the same CRUD APIs (PUT, GET, DELETE, LIST, HEAD, etc.) plus a few S3‑specific features such as *bucket policies*, *access‑control lists (ACLs)*, *multipart uploads*, *object tagging*, *versioning* and *life‑cycle policies*.

---

## 2.  High‑level Architecture

Below is a **component diagram** showing the major parts of the system.  

```mermaid
flowchart TD
    subgraph Front‑End
        LB[Load Balancer]
        API[Stateless API Node]
        Auth[Auth/Signature Verifier]
    end

    subgraph Metadata
        MetaDB[Distributed KV Store (Cassandra/Scylla)]
        MetaIndex[Secondary Index (ElasticSearch/Redis)]
    end

    subgraph Storage
        DataNodes{{Storage Nodes}}
        Replicator[Replication & Erasure‑Coding Service]
    end

    subgraph Caching
        EdgeCache[Edge CDN (CloudFront)]
        MemCache[In‑memory Cache (Redis)]
    end

    subgraph Lifecycle
        LifeCycle[Lifecycle Manager]
        TierStore[Cold Tier (HDD/Tape)]
    end

    subgraph Monitoring
        Metrics[Prometheus/Grafana]
        Logs[ELK Stack]
        Alerts[Alertmanager]
    end

    LB --> API
    API --> Auth
    API --> MetaDB
    API --> MetaIndex
    API --> DataNodes
    API --> EdgeCache
    API --> MemCache
    DataNodes --> Replicator
    Replicator --> DataNodes
    Replicator --> TierStore
    LifeCycle --> MetaDB
    LifeCycle --> TierStore
    Monitoring --> Metrics
    Monitoring --> Logs
    Monitoring --> Alerts
```

**Explanation of components**

| Component | Responsibility |
|-----------|----------------|
| **Load Balancer** | Global entry point, distributes requests to API nodes, supports TLS termination and HTTP/2. |
| **API Node** | Stateless, implements S3 REST API, performs request parsing, routing, rate‑limiting, and metrics emission. |
| **Auth** | Verifies AWS‑style signatures, IAM roles, and bucket policies; produces a short‑lived access token. |
| **Metadata Store** | Holds object metadata (size, ETag, ACL, tags, lifecycle state, versioning info). Implemented with a strongly consistent, partitioned KV store (Cassandra, Scylla, or DynamoDB). |
| **Secondary Index** | Enables fast `LIST` and tag‑query operations (Elasticsearch or Redis). |
| **Storage Nodes** | Low‑latency storage tier; each node runs a file‑system‑like layer (e.g., Ceph‑RGW or custom). Objects are striped across nodes via consistent hashing of the object key. |
| **Replication & Erasure‑Coding Service** | Ensures durability: asynchronously replicates writes to other AZs and applies erasure coding for archival. |
| **Edge CDN** | Global edge cache (similar to CloudFront) that serves cached GET responses. |
| **In‑memory Cache** | For hot objects that are too large for CDN caching or to reduce back‑end load. |
| **Lifecycle Manager** | Evaluates lifecycle rules, moves objects to cold tier (HDD or tape) or deletes them. |
| **Cold Tier** | Persistent, low‑cost storage (HDD, LTO tape, object‑store with very low read latency). |
| **Monitoring / Logging** | Observability stack to keep SLAs, detect failures, and audit access. |

---

## 3.  Data Model & API

### 3.1  Object Namespace

```
/{bucket}/{key}
```

* `bucket` – user‑owned namespace, unique globally or per‑region.
* `key` – arbitrary string, can include `/` (used for “folders” in UI).
* `versionId` – optional, for versioned buckets.

### 3.2  Metadata

| Field | Type | Size | Notes |
|-------|------|------|-------|
| `size` | int64 | 8 B | Bytes |
| `ETag` | string | 32 B | MD5 hash (or SHA‑256) |
| `last-modified` | timestamp | 8 B |
| `acl` | bitmask | 4 B | Grants |
| `tags` | map<string,string> | variable | Optional |
| `version-id` | string | 16 B | If versioning is enabled |
| `storage-class` | enum | 1 B | `STANDARD`, `INFREQUENT_ACCESS`, `GLACIER` |
| `checksum` | SHA‑256 | 32 B | For integrity |

All metadata lives in the *Metadata Store* (KV). Keys are constructed as:

```
META:{bucket}:{key}[:{version-id}]
```

### 3.3  Object Storage

Objects are **chunked** into **segments** (default 4 MB). Each segment is stored as a *blob* on a storage node. The storage layer maintains a *segment map* – a list of segment IDs and the nodes where they reside.

The **object layout** (simplified):

```
OBJECT:{bucket}:{key}[:{version-id}] --> [segment-1, segment-2, ...]
```

The *segment map* is stored in the metadata store as part of the object’s entry.

### 3.4  API Operations

| Operation | HTTP Verb | Path | Notes |
|-----------|-----------|------|-------|
| PUT Object | `PUT /{bucket}/{key}` | `x-amz-tagging`, `x-amz-version-id` | Multipart support with `Content-Range`. |
| GET Object | `GET /{bucket}/{key}` | `Range` header | Streaming support. |
| DELETE Object | `DELETE /{bucket}/{key}` |  | Creates a delete marker in versioned buckets. |
| HEAD Object | `HEAD /{bucket}/{key}` |  | Returns metadata. |
| LIST Objects | `GET /{bucket}/?prefix=...` |  | Uses `MetaIndex`. |
| COPY Object | `PUT /{bucket}/{key}?copy-source=...` |  | Performs metadata copy, creates new segment map. |
| Versioning | `PUT /{bucket}?versioningConfiguration=` |  | Enables/disables. |
| Lifecycle | `PUT /{bucket}?lifecycleConfiguration=` |  | Rules are stored in `MetaDB`. |
| ACL | `PUT /{bucket}/{key}?acl` |  | JSON body. |
| Policy | `PUT /{bucket}?policy` |  | Bucket policy JSON. |

All requests are authenticated by an **AWS Signature V4**‑like scheme; the `Auth` component verifies the signature and extracts the IAM policy to check permissions.

---

## 4.  Storage & Durability Strategy

### 4.1  Replication & Erasure Coding

| Tier | Storage Medium | Replication | Erasure‑Coding | Overhead | Notes |
|------|-----------------|-------------|----------------|----------|-------|
| **Hot** | SSD | 3 replicas per AZ | 4+2 (6‑chunk) | 1.5× | Low latency, high throughput. |
| **Warm** | HDD | 3 replicas | 4+2 | 1.5× | 10× cheaper than SSD. |
| **Cold** | LTO‑8 Tape | 2 replicas | 4+2 | 1.5× | 100× cheaper, 10‑day retrieval latency. |
| **Archive** | Object‑store (e.g., Amazon Glacier) | 3 replicas | 4+2 | 1.5× | 90‑day retrieval. |

**Why 4+2?**  
With *4 data chunks* and *2 parity chunks* we can tolerate any **2** node failures out of 6. The storage overhead is 150 % (6/4). This balances durability with storage cost.

### 4.2  Redundancy across Availability Zones (AZs)

* Each **replica** must reside in a **different AZ** to protect against a single AZ failure.  
* In a 3‑AZ region we keep 3 replicas; each is stored on a different node set in the distinct AZ.  
* The **erasure‑coding service** splits the object into 6 chunks; any 4 are enough to reconstruct. These chunks are distributed across the 3 AZs, with 2 parity chunks typically placed in a 4th “spare” AZ (or on separate nodes within the same AZ but different racks).

### 4.3  Global Replication

* **Cross‑region replication (CRR)** is *asynchronous*.  
* A *change‑log* (Kafka or Kinesis‑like) per region feeds a *replication worker* that pushes updates to the destination region(s).  
* For *low‑latency read* in a remote region, we use the CDN; the CDN fetches the object from the nearest region, caches it, and serves it to the client.  
* For *strong consistency* (rarely needed), we provide a *synchronous replication* path with a small SLA (e.g., < 200 ms).

### 4.4  Data Consistency

* **Eventual consistency** is the default for GET after PUT.  
* Optionally, clients can request *strong consistency* by adding `x-amz-expected-etag` header; the API verifies that the current ETag matches the provided value.  
* Versioning ensures that a DELETE never removes data permanently; it just adds a *delete marker*.

---

## 5.  Capacity Planning (Example)

Assumptions (per region):

| Parameter | Value | Explanation |
|-----------|-------|-------------|
| Number of customers | 1 M | 1,000,000 users |
| Average bucket size | 1 TB | Typical workload |
| Number of buckets per customer | 1 | Simplifying assumption |
| Number of objects per bucket | 1 M | Average |
| Avg. object size | 1 GB | Heavy‑image/ video workloads |
| Replication factor (AZ) | 3 | 3 replicas |
| Erasure coding overhead | 1.5× (4+2) | 2 parity chunks |
| Number of AZs | 3 | Standard region |
| Number of regions | 5 | Global distribution |

### 5.1  Raw data per region

```
RawDataRegion = customers × avg_bucket_size
               = 1 M × 1 TB
               = 1 PB
```

### 5.2  Storage required for replication

```
StorageReplicated = RawDataRegion × ReplicationFactor
                  = 1 PB × 3
                  = 3 PB
```

### 5.3  Storage required with erasure coding

```
PhysicalCapacity = StorageReplicated × ErasureCodingOverhead
                  = 3 PB × 1.5
                  = 4.5 PB
```

### 5.4  Total across all regions

```
TotalPhysical = PhysicalCapacity × Regions
              = 4.5 PB × 5
              = 22.5 PB
```

### 5.5  Number of storage nodes

Assuming each node has 8 TB usable (8×1 TB SSDs, 25 % overhead for filesystem)

```
NodesPerRegion = PhysicalCapacity / UsablePerNode
                = 4.5 PB / 8 TB
                = 562.5 ≈ 563 nodes
```

Across 5 regions: ~2 800 nodes.

### 5.6  Cost estimation

* **SSD storage cost** ≈ $0.02 / GB‑month (US‑East‑1).  
* **HDD storage** ≈ $0.01 / GB‑month.  
* **Tape** ≈ $0.003 / GB‑month.

For hot tier (4 PB) at $0.02 / GB:

```
HotCostPerMonth = 4 PB × 1 000 GB/PB × $0.02
                 = $80 M
```

For warm tier (1 PB) at $0.01 / GB:

```
WarmCostPerMonth = $40 M
```

Total monthly storage cost ≈ $120 M (excluding network, replication, monitoring).

> **Trade‑off**: Switching a hot tier to HDD would cut cost by 50 % but increase IOPS and latency. Use SSD only for the top‑10 % of objects (hot tier).

---

## 6.  Performance Optimizations

| Technique | Impact | Trade‑offs |
|-----------|--------|------------|
| **Multipart upload** | Parallel upload, resumable | Requires client to manage state |
| **Chunk caching** | Faster reads for hot objects | Extra memory cost |
| **Edge CDN** | Reduces origin load, latency | Cache invalidation complexity |
| **Parallel write/repair** | Faster consistency after failure | Increased network traffic |
| **Data locality** | Lower network hops | Harder to balance load if data skew |

---

## 7.  Security & Compliance

| Feature | Implementation |
|---------|----------------|
| **Transport encryption** | TLS 1.2+ everywhere (API LB, CDN, data nodes). |
| **Encryption at rest** | Server‑side encryption (SSE‑S3) via AES‑256 on the storage node. Optionally client‑side encryption (SSE‑KMS). |
| **IAM** | Role‑based access, bucket policies, ACLs. |
| **Audit logging** | All API calls logged to a central Kinesis stream → ELK for forensic analysis. |
| **Data residency** | Users can specify region on bucket creation; data never leaves that region unless explicitly replicated. |
| **Compliance** | Data retention, encryption, GDPR “right to be forgotten” via object lifecycle and delete markers. |

---

## 8.  Failure Modes & Mitigation

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **Node crash** | Data on node temporarily unavailable | 1‑second heartbeat; replication ensures data is still available elsewhere; read from other replicas; asynchronous repair brings node back. |
| **Rack / network** | Loss of multiple nodes | Multi‑AZ replication ensures availability; cross‑region replication provides DR. |
| **AZ outage** | 1/3 of replicas lost | Replication factor of 3 across AZs; data remains available from other AZs. |
| **Region outage** | 20 % of data lost (if not replicated) | Cross‑region replication (CRR) ensures data in at least one other region. |
| **Storage corruption** | Data integrity loss | Erasure coding + checksums; periodic scrub to detect and repair corruption. |
| **Auth server outage** | Clients cannot authenticate | API nodes can cache short‑lived tokens; fail‑over cluster of Auth. |
| **Metadata store outage** | API cannot fetch metadata | Metadata store is replicated (3‑node cluster) with quorum; writes are queued. |
| **Network partition** | Split brain | Quorum-based metadata store; writes only succeed if majority of nodes agree. |

### 8.1  Recovery Workflow

1. **Detection** – heartbeat loss triggers alert.  
2. **Isolation** – the failed node is marked *dead* in the cluster; traffic routed away.  
3. **Reconstruction** – replication worker reads the missing segments from surviving replicas, applies erasure coding, and writes to a new node.  
4. **Re‑integration** – node is re‑joined once healthy; the system rebalances data (optional).  

---

## 9.  Monitoring, Alerting & SLAs

| Metric | Threshold | Alert |
|--------|-----------|-------|
| **Request latency** | 95 % percentile < 200 ms | `HighLatency` |
| **Error rate** | > 0.1 % | `HighErrorRate` |
| **Replication lag** | > 10 min | `ReplicationLag` |
| **Disk usage** | > 90 % | `LowDiskSpace` |
| **Cache hit ratio** | < 70 % | `LowCacheHit` |

SLAs:

| Service | Availability | Latency |
|---------|--------------|---------|
| **PUT/GET** | 99.999 % (5‑nines) | < 200 ms (99 % of requests) |
| **List** | 99.999 % | < 400 ms |
| **Replication** | 99.999 % | < 10 min (async) |

All metrics are pushed to **Prometheus**; alerts go to **Alertmanager** and are surfaced on Grafana dashboards.

---

## 10.  Trade‑off Summary

| Decision | Benefit | Cost |
|----------|---------|------|
| **Erasure Coding (4+2)** | 50 % storage overhead vs replication; higher durability | Slightly higher CPU for encoding/decoding |
| **Replication factor 3** | Protects against AZ failures | 2× storage overhead |
| **SSD hot tier** | Low latency, high throughput | 2–3× cost per TB |
| **HDD warm tier** | Cheaper storage | 10× higher latency |
| **Asynchronous CRR** | Lower network cost, lower write latency | Eventual consistency |
| **Strong consistency option** | Predictable reads | Write latency increases, more coordination |
| **Cache CDN** | Offloads origin | Additional cost for CDN edges |

---

## 11.  Summary

The proposed design is a **globally distributed, highly durable, and cost‑effective object storage system** that meets the same functional contracts as AWS S3 while providing granular control over durability, consistency, and lifecycle. The key enablers are:

* **Distributed, replicated metadata** with fast secondary indexing for listings.
* **Chunked, erasure‑coded data** with per‑AZ replication.
* **Global edge CDN** to serve read traffic with minimal latency.
* **Robust failure detection & self‑repair** to maintain availability.
* **Fine‑grained security** through IAM, bucket policies, and encryption.

By carefully balancing **storage overhead**, **latency**, and **cost**, the system can scale to petabytes per region, billions of objects, and millions of concurrent clients, while staying within a predictable budget and providing the durability and availability guarantees required for mission‑critical workloads.