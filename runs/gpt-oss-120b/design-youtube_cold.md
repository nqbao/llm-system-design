---

Design a video sharing platform like YouTube.


---

## 1. Overview
We design **“VidShare”** – a globally‑distributed, highly‑available video‑sharing platform that supports:

* **Upload** of user‑generated videos (any format, up to many GB)
* **Transcoding** into adaptive bitrate formats (HLS/DASH)
* **Streaming** to billions of viewers with < 2 s startup latency
* **Search**, **recommendations**, **comments**, **likes**, **subscriptions**, and **notifications**
* **Analytics** (view‑count, watch‑time, ad‑impressions) at petabyte scale

The design is intentionally modular so that each functional block can be scaled independently.

---

## 2. Functional Requirements

| # | Requirement | Description |
|---|-------------|-------------|
| 1 | **Video upload** | Chunked/resumable upload, client‑side validation, virus‑scan, metadata extraction |
| 2 | **Video processing** | Transcode to multiple resolutions, generate thumbnails, create HLS/DASH manifests |
| 3 | **Video storage** | Durable, low‑cost object store (e.g., S3‑compatible) |
| 4 | **Streaming** | Adaptive‑bitrate HTTP streaming through CDN |
| 5 | **User accounts** | Sign‑up, login (OAuth2), channel management |
| 6 | **Social features** | Likes, dislikes, comments, replies, channel subscriptions |
| 7 | **Search** | Full‑text + tag based search (elastic‑style) |
| 8 | **Recommendation** | Home‑feed, “Up‑next”, “Related videos” (collaborative + content‑based) |
| 9 | **Notifications** | Push/email for new videos, replies, likes |
|10 | **Analytics** | View‑counts, watch‑time, audience retention, ad‑metrics |
|11 | **Content moderation** | Automated (ML) & human review pipelines |

---

## 3. Non‑functional Requirements

| Property | Target | Rationale |
|----------|--------|-----------|
| **Scalability** | > 100 M DAU, peak **2 M RPS** on API, **10 Tbps** outbound video traffic | Must handle viral spikes & live events |
| **Availability** | 99.99 % (four‑9s) for API, 99.999 % for video storage (11‑9s) | Users expect videos to “just work” |
| **Latency** | < 200 ms for API responses, < 2 s video start‑up | Good UX |
| **Consistency** | **Eventual** for view‑counts / recommendation cache; **Strong** for user data & upload metadata |
| **Durability** | 11 9’s for video objects | Prevent data loss |
| **Security & Privacy** | OAuth2, JWT, rate‑limiting, encryption‑at‑rest & in‑flight, GDPR compliance | Protect user data |
| **Operational** | Health monitoring, auto‑scaling, graceful degradation | Maintain service under failures |

---

## 4. High‑Level Architecture

```mermaid
flowchart LR
    subgraph Internet
        C[Clients: Web / iOS / Android]
    end

    subgraph Edge[CDN Edge]
        CDN[CDN (Cache + TLS termination)]
    end

    subgraph Frontend
        DNS[DNS (Geo‑routing)]
        LB[Global Load Balancer]
        API[API Gateway / Auth Proxy]
    end

    subgraph Services
        Auth[Auth Service]
        User[User/Profile Service]
        VidMeta[Video‑Metadata Service]
        Upload[Upload Service]
        Proc[Transcoding Service]
        Store[Object Store (S3‑compatible)]
        Search[Search Service (ES)]
        Rec[Recommendation Service]
        Comment[Comment & Like Service]
        Sub[Subscription Service]
        Notif[Notification Service]
        Analytics[Analytics / Data‑Warehouse]
        Queue[Message Queue (Kafka)]
        Cache[Cache (Redis / Memcached)]
    end

    subgraph Workers
        Transcoder[Transcoder Workers]
        Moderation[Content‑Moderation Workers]
    end

    %% Data flow
    C -->|HTTPS API| DNS --> LB --> API
    API --> Auth
    API --> User
    API --> VidMeta
    API --> Upload
    API --> Search
    API --> Rec
    API --> Comment
    API --> Sub
    API --> Notif

    Upload --> Store
    Upload -->|msg| Queue
    Queue --> Proc
    Proc --> Store
    Proc -->|msg| Queue
    Queue --> Moderation

    VidMeta --> Store
    VidMeta --> Cache
    Search --> Store
    Rec --> Cache
    Comment --> Store
    Sub --> Store
    Notif --> Store
    Analytics --> Store

    C -->|GET video URL| CDN
    CDN -->|Cache miss| Store
```

*Clients* always hit the **API Gateway** for meta‑operations (login, upload, comments, search).  
Video playback never goes through the API – it streams directly from the **CDN** which pulls from the **Object Store** on a cache miss.

---

## 5. Component Deep‑Dive

### 5.1. Upload Service
* **Chunked/Resumable upload** (RFC 7538‑style) → S3 multipart upload.
* **Pre‑flight validation**: filetype, size, basic virus‑scan (ClamAV).
* **Metadata extraction**: duration, fps, audio codec → stored in **Video‑Metadata DB**.
* **Post‑Upload**: push a message `video_uploaded` onto **Kafka**.

### 5.2. Transcoding Pipeline
```
video_uploaded → Kafka topic → Transcoder Workers (containers) → 
   • Generate HLS/DASH manifests
   • Encode to 144p, 360p, 720p, 1080p (optional 4K)
   • Extract thumbnails (3 per video)
   • Store outputs back to Object Store
   → Emit `video_ready` event
```
*Workers* are stateless, autoscale based on queue depth.  
GPU‑enabled nodes for HEVC/AV1 encoding if needed.

### 5.3. Object Store
* **Durability**: 11 9’s (e.g., multi‑AZ replication, erasure coding).  
* **Naming**: `videos/{video_id}/{resolution}/{segment_number}.ts`.  
* **Lifecycle**: move cold videos (> 2 yr) to cheaper “ glacier‑class” storage.

### 5.4. CDN (Cache Layer)
* **Edge locations** near end‑users (AWS CloudFront, Akamai, or self‑built).  
* **Cache‑key** = `{video_id}-{manifest_version}`.  
* **Stale‑while‑revalidate** to tolerate short transcoding delays.  
* **Adaptive‑bitrate**: client requests `m3u8` manifest → CDN serves .ts segments.

### 5.5. Video‑Metadata Service
* Stores **canonical metadata**: title, description, tags, upload timestamp, owner, list of renditions, view‑count, like‑count.
* **Schema** (`video_id PK`, `owner_id FK`, `title`, `description`, `tags[]`, `duration_ms`, `renditions[]`, `view_cnt`, `like_cnt`, `dislike_cnt`, `status`).
* **Implementation**: 
  * **Primary** – distributed **NewSQL** (e.g., CockroachDB) for strong consistency on writes.
  * **Secondary** – **read‑replicas** + **Redis cache** for low‑latency reads.

### 5.6. View‑Count & Like‑Count Updates
* **Sharded counters** in Redis (or DynamoDB Atomic Counters).  
* Periodic **batch flush** to the metadata DB (every few seconds) → **eventual consistency** is acceptable.

### 5.7. Search Service
* **Index** built from video metadata, tags, transcript (optional speech‑to‑text).  
* **Technology**: Elasticsearch or OpenSearch cluster; sharded by `video_id` hash.  
* **Realtime updates**: on `video_ready` and metadata edits push updates to the index.

### 5.8. Recommendation Service
* **Hybrid approach**:
  * **Batch** collaborative‑filtering (Spark/Dataproc) runs nightly → stores user‑item matrix in a KV store.
  * **Online** “up‑next” using **Memcached** of the last‑watched video’s similar‑video list (pre‑computed via **FAISS** nearest‑neighbor on video embeddings).
* Results cached per‑user for ~5 min to reduce DB pressure.

### 5.9. Comment & Like Service
* **Data model**: `comment_id PK`, `video_id`, `user_id`, `parent_id` (for replies), `text`, `timestamp`.
* **Storage**: **Cassandra** (wide‑row) that can handle high write throughput and hot reads on recent comments.
* **Read API**: Pull latest N comments, then lazy‑load older pages.

### 5.10. Subscription Service
* **Follow‑graph** stored in a **graph‑DB** (e.g., Neo4j) or as **materialized lists** in **Redis** (sorted‑set per channel).  
* **Home feed generation**: Pull the newest videos from subscribed channels (fan‑out‑on‑read) → merge‑sorted by publish time.

### 5.11. Notification Service
* **Push** via Firebase Cloud Messaging / Apple Push Notification Service.  
* **Email** via a transactional email provider (SES, SendGrid).  
* **Back‑pressure**: Event‐driven (Kafka) → worker pool → idempotent delivery.

### 5.12. Analytics & Data Warehouse
* **Real‑time** metrics (views, watch‑time) written to **Kafka → ClickHouse** for OLAP queries.  
* **Batch** pipelines (Airflow) ingest logs → **BigQuery / Snowflake** for deeper insights & ad‑targeting.

### 5.13. Messaging & Event Bus
* **Kafka** (or Pulsar) with multiple topics: `video_uploaded`, `video_ready`, `comment_created`, `like_added`, `user_follow`, etc.  
* Partition key = `video_id` (or `user_id`) to guarantee ordering per entity.

### 5.14. Caching
| Layer | Technology | What’s cached |
|-------|------------|---------------|
| **API responses** | CloudFront + Lambda@Edge (for static pages) |
| **Metadata** | Redis (TTL 5 min) – video info, channel info |
| **Counters** | Redis sharded counters (view, like) |
| **Recommendation lists** | Memcached (per‑user 5‑min) |
| **Search results** | Elasticsearch query cache |

---

## 6. Capacity Planning (Numbers in **real** units)

### 6.1. Traffic Assumptions
| Metric | Estimate |
|--------|----------|
| **Monthly Active Users (MAU)** | 200 M |
| **Daily Active Users (DAU)** | 70 M |
| **Avg. sessions / DAU per day** | 1.8 |
| **Avg. watch time per session** | 12 min |
| **Avg. video bitrate** (effective) | 2.5 Mbps (≈ 0.312 MiB/s) |
| **Upload share** (users uploading per day) | 1 % |
| **Avg. video size** | 500 MiB (≈ 30 min @ 2 Mbps) |

### 6.2. Video **Streaming** Bandwidth
*Total daily watch minutes*  
`70 M DAU × 1.8 sessions × 12 min = 1.512 B minutes ≈ 25.2 M hours`

*Data per hour*  
`0.312 MiB/s × 3600 s = 1125 MiB ≈ 1.1 GiB`

*Total daily egress*  
`25.2 M hrs × 1.1 GiB ≈ 27.7 PB`

**Peak (15 min) traffic** – assume 5 % of daily watchers at peak simultaneously:  
`1.512 B × 0.05 ≈ 75.6 M concurrent streams`  
Bandwidth needed: `75.6 M × 0.312 MiB/s ≈ 23.6 Tbps`.

> **Result:** CDN must provision ≈ 25 Tbps outbound capacity with aggressive edge caching.

### 6.3. Video **Upload** Capacity
*Daily upload volume*  
`200 M users × 1 % upload × 500 MiB = 1 M × 500 MiB = 500 TB per day`

Peak upload rate (assuming 30 min upload window) → `500 TB / 1800 s ≈ 278 GB/s` → ~ **2.2 Tbps** inbound to upload front‑ends.

### 6.4. **Object Store** Size (1‑year horizon)
*Total video hours per year*: `25.2 M hrs/day × 365 ≈ 9.2 B hrs`  
*Data*: `9.2 B hrs × 1.1 GiB/hr ≈ 10.1 EB` (exabytes).  
Real‑world YouTube stores **> 1 EB**, so the design must handle **exabyte‑scale** (erasure‑coded, multi‑region).

### 6.5. **API** Load
*Read‑heavy operations* (play page, search, comments):
- Assume **2 M RPS** total.
- Breakdown: 30 % video‑metadata, 20 % comments, 15 % search, 15 % recommendation, 20 % other (auth, subscriptions).

*Write operations* (upload, like, comment):
- **500 K QPS** peaks (likes/comments) + **100 K QPS** for upload metadata.

### 6.6. **Database** Scaling
**Video‑Metadata DB**
- **Read QPS**: ~600 K/s → 3 × read‑replica pool (each ~200 K QPS) → use **CockroachDB** with 5‑way replication.
- **Write QPS**: ~100 K/s (new videos, updates) → sharded by `video_id` hash across 10 nodes.

**Comment DB (Cassandra)**
- Writes: ~500 K/s (comments/likes) → 20‑node cluster (≈ 25 K writes/node) with `RF=3`.

**Search Cluster**
- Index size: ~ (metadata ≈ 2 KB per video) × 200 M ≈ **400 GB** → comfortably fits in a **10‑node** ES cluster with 3‑shard replication.

**Cache**
- **Redis** 100 GB memory footprint → 20‑node cluster (5 GB per node) for counters & metadata hot‑cache.

---

## 7. Data Model Sketch

```sql
-- Video metadata (CockroachDB)
CREATE TABLE videos (
    video_id UUID PRIMARY KEY,
    owner_id UUID NOT NULL,
    title TEXT,
    description TEXT,
    tags TEXT[],
    duration_ms BIGINT,
    upload_ts TIMESTAMP,
    status ENUM('processing','ready','failed'),
    renditions JSONB,      -- [{resolution:"720p", url:"..."}]
    view_cnt BIGINT DEFAULT 0,
    like_cnt BIGINT DEFAULT 0,
    dislike_cnt BIGINT DEFAULT 0
);

-- Users
CREATE TABLE users (
    user_id UUID PRIMARY KEY,
    email TEXT UNIQUE,
    hashed_pwd TEXT,
    display_name TEXT,
    created_ts TIMESTAMP
);

-- Comments (Cassandra)
CREATE TABLE comments (
    video_id UUID,
    comment_id TIMEUUID,
    user_id UUID,
    parent_id TIMEUUID,
    text TEXT,
    created_ts TIMESTAMP,
    PRIMARY KEY (video_id, comment_id)
) WITH CLUSTERING ORDER BY (comment_id DESC);
```

Counters are maintained in **Redis**; periodic CRON jobs sync them back to the relational store.

---

## 8. Scaling Strategies per Component

| Component | Scaling Technique | Reason |
|-----------|-------------------|--------|
| **API Gateways / Load Balancers** | Horizontal auto‑scale + DNS‑based geo‑routing | Stateless; can add nodes instantly |
| **Upload Service** | Stateless pods behind LB; **S3 multipart** splits load to storage | Handles bursty upload traffic |
| **Transcoding Workers** | Container‑orchestrated (K8s) with **GPU node pool**; autoscale on **Kafka lag** | Compute‑intensive, can be over‑provisioned during events |
| **Object Store** | **Multi‑AZ replication**, **erasure coding**, **tiered storage** (hot → cold) | Provides durability & cost efficiency |
| **CDN** | Global edge network, **origin‑pull** from object store, **cache‑fill** on demand | Offloads most traffic from origin |
| **Metadata DB** | **Sharding** by `video_id` hash, **read replicas**, **leader‑follower** for writes | Supports high read‑QPS, fast failover |
| **Comment DB (Cassandra)** | **Ring‑based scaling**, add nodes → linear write capacity | Handles write‑heavy comment flow |
| **Search** | **Horizontal ES shards**, **index‑only nodes**, **refresh interval tuning** | Keeps search latency < 100 ms |
| **Recommendation** | **Batch job → KV store** (Redis) + **online fallback** | Low‑latency home feed with periodic re‑ranking |
| **Cache** | **Consistent hashing**, **LRU eviction**, **warm‑up scripts** | Keeps hot objects in‑memory; avoids stampedes |
| **Message Bus** | **Kafka partitioning** by key, **replication factor 3**, **log compaction** for idempotent events | Guarantees delivery ordering per video/user |
| **Analytics** | **ClickHouse** column store for real‑time dashboards, **Dataflow** for batch | Scales to petabyte query workloads |

---

## 9. Key Trade‑offs

| Trade‑off | Option A | Option B | Chosen Approach & Why |
|-----------|----------|----------|-----------------------|
| **Metadata storage** | Pure **SQL** (strong ACID) | **NoSQL** (high write throughput) | Use **NewSQL** (CockroachDB) – gives ACID for critical ops (ownership, privacy) while scaling horizontally. |
| **View‑count consistency** | Immediate strong consistency (single DB write) | Sharded eventual counters (Redis) | Eventual consistency is acceptable; guarantees < 1 s latency and avoids DB hotspot. |
| **Recommendation generation** | Fully **real‑time** (online ML) | **Hybrid** (batch + cache) | Hybrid solution reduces compute cost, still delivers fresh recommendations (cache refreshed every 5 min). |
| **Transcoding** | In‑house GPU farm | Outsource to cloud media service (e.g., AWS Elastic Transcoder) | In‑house gives cost control and can scale with spot instances; chosen for cost & flexibility. |
| **CDN vs Origin‑only** | Serve from origin servers (e.g., EC2) | Use CDN edge caches | CDN drastically reduces latency and bandwidth cost; required for global audience. |
| **Comment storage** | Relational (PostgreSQL) | Wide‑row NoSQL (Cassandra) | Cassandra handles write spikes from popular videos without locking; chosen for scalability. |
| **Search indexing latency** | Near‑real‑time (seconds) | Batch (minutes) | Near‑real‑time index (using ES’s `refresh_interval=1s`) offers fresh search; still safe because edits are infrequent. |

---

## 10. Failure Scenarios & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **CDN edge outage** (single POP) | Increased latency for users in that region | **Anycast DNS** → reroute to nearest healthy POP; fallback to origin (object store) via **origin‑pull**; also **multi‑CDN** (e.g., CloudFront + Akamai) |
| **Object‑store AZ loss** | Lost video files (durability breach) | **Cross‑region replication** + **erasure coding**; **regular S3 inventory & checksum verification** |
| **Transcoder node crash** | Video stays in “processing” state | **Kafka consumer group** – other workers will pick up un‑acked messages; **dead‑letter queue** for files that repeatedly fail |
| **Database primary partition** | Write stall for metadata (new uploads) | **Multi‑master (CockroachDB)** automatically elects new leader; **read‑only mode** for serving existing videos |
| **Cache stampede** (popular video cache miss) | Sudden load on origin, latency spike | **Cache‑aside with request coalescing** (leaky‑bucket), **stale‑while‑revalidate**, **probabilistic early expiration** |
| **Message bus lag** (Kafka backlog) | Delayed downstream processing (likes, views) | **Autoscale consumer groups**, **disk‑backed brokers**, **alert on consumer lag > 5 min** |
| **DDoS on API** | Exhaustion of load balancer / API servers | **IP rate limiting**, **WAF**, **scrubbing service** (Cloudflare Spectrum), **global throttling** per user token |
| **Partial network partition** (split‑brain) | Duplicate view‑count increments, inconsistent recommendation data | Use **idempotent events** (message IDs), **CRDT counters**, **conflict‑free merge** on reconnection |
| **Security breach** (stolen JWT) | Unauthorized actions (upload, delete) | **Short‑lived access tokens** (5‑min), **refresh token rotation**, **monitor abnormal usage patterns**, **MFA for privileged actions** |

Graceful degradation paths:
* If **recommendation** service is down → fallback to “Trending” static list.
* If **search** is unavailable → show “Explore categories” page.
* If **comments** DB is unreachable → disable posting, show cached comment count.

---

## 11. Security & Compliance

| Concern | Controls |
|---------|----------|
| **Authentication** | OAuth2 + OpenID Connect, JWT signed with RSA‑2048, token revocation list (Redis) |
| **Authorization** | ACL per video (public / private / unlisted), IAM for internal services |
| **Data at rest** | Server‑side encryption (SSE‑S3) + customer‑managed keys (AWS KMS) |
| **Data in flight** | TLS‑1.3 everywhere (API, internal gRPC, CDN‑origin fetch) |
| **Content moderation** | Automated NSFW, hate‑speech detectors, human review queue; videos flagged as **restricted** are not served to under‑18 accounts |
| **Privacy (GDPR/CCPA)** | User data stored in regional buckets, right‑to‑be‑forgotten workflow (metadata deletion + S3 Object Lifecycle to expire within 30 days) |
| **Audit** | Immutable CloudTrail logs for all admin actions, signed logs stored in WORM bucket |
| **Rate limiting** | Leaky‑bucket per IP & token (e.g., 200 req/s burst, 50 req/s sustained) |

---

## 12. Monitoring, Alerting & Operations

* **Metrics (Prometheus)** – request latency, error rates, cache hit ratio, transcoder lag, Kafka consumer lag, CPU/mem per node.
* **Dashboards (Grafana)** – real‑time view of traffic spikes, CDN egress, storage growth.
* **Tracing (OpenTelemetry)** – end‑to‑end request flow from API → metadata DB → CDN → client.
* **Alerting (PagerDuty)** – thresholds: 5‑xx error rate > 1 %, CPU > 80 % for > 5 min, queue lag > 10 min.
* **Chaos Engineering** – periodic pod/instance termination, network latency injection to verify auto‑recovery.
* **Capacity alerts** – storage usage > 80 % of provisioned tier, CDN egress cost spikes.

---

## 13. Future Extensions

| Feature | Additional Components |
|---------|-----------------------|
| **Live Streaming** | Ingest servers (RTMP/LL‑HLS), real‑time transcoding, low‑latency CDN, DVR storage |
| **Short‑Form Reels (TikTok‑style)** | Separate “short‑video” service, user‑generated playlists, real‑time recommendation with graph embeddings |
| **VR/360° video** | Higher bitrate (up to 25 Mbps), segment‑based stitching, spatial audio processing |
| **Monetization (Ads, Memberships)** | Ad‑server, billing microservice, per‑video revenue reporting |
| **Multi‑language transcripts & subtitles** | Speech‑to‑text pipelines, translation service, subtitle selection API |
| **Community Moderation Tools** | Reputation system, community flagging, automated takedown API |

---

## 14. TL;DR Summary

* **Stateless API + CDN** delivers low‑latency video playback.  
* **Object store + erasure coding** gives exabyte‑scale durability.  
* **Kafka‑driven transcoding pipeline** decouples upload from processing.  
* **NewSQL for metadata**, **Cassandra for comments**, **Elasticsearch for search**, **Redis for hot caches** – each chosen for its optimal read/write pattern.  
* **Capacity**: ~25 Tbps CDN egress, ~2 Tbps upload ingress, petabytes of storage, 2 M RPS API; auto‑scaled per‑component.  
* **Trade‑offs** favor eventual consistency for counters, hybrid recommendations, and in‑house transcoding for cost control.  
* **Failure handling** uses multi‑region replication, graceful degradation, and robust monitoring.  

This architecture can start at “tens of millions” of users and scale to “billions” with minimal refactoring, providing the core experience expected from a modern YouTube‑like platform.