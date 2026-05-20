---

Design a microblogging platform like Twitter.


---

Below is a **complete, self‑contained design** for a Twitter‑like micro‑blogging service (we’ll call it **TwitX**).  
The design covers functional & non‑functional requirements, traffic estimates, data modelling, component‑level design, trade‑offs, failure handling and operational concerns, and it includes two Mermaid diagrams (overall architecture and two critical data‑flows).

---

## 1. Problem Statement & Core Requirements

| Category | Requirement |
|----------|--------------|
| **Functional** | • Post a tweet (≤ 280 char) with optional media (image/video). <br>• Follow / unfollow users. <br>• Home timeline (tweets from people you follow) with pagination. <br>• User timeline (all tweets you authored). <br>• Like, retweet, reply. <br>• Search by keyword or hashtag. <br>• Trending hashtags (real‑time). |
| **Scale** | • 100 M DAU (daily active users) on day‑0; target 500 M DAU in 3 years. <br>• Avg. 5 tweets per user per day → ~500 M tweets / day. <br>• Peak write traffic ≈ **15 k tweets / sec** (burst factor ≈ 2.5). <br>• Peak read traffic ≈ **150 k timeline‑fetch requests / sec** (5 per user × 100 M DAU). |
| **Latency** | • Post‑tweet response ≤ 200 ms. <br>• Home‑timeline fetch ≤ 100 ms (first page). |
| **Durability** | • “Never lose a tweet”. 30‑day disaster‑recovery RPO ≤ 5 min, RTO ≤ 2 h. |
| **Availability** | • 99.9 % overall, 99.99 % for read‑only APIs (timeline, search). |
| **Security & Privacy** | • OAuth2 + JWT, rate‑limiting, GDPR “right to be forgotten”. |
| **Operational** | • Metrics, tracing, log aggregation, blue‑green deployments, canary releases. |

---

## 2. High‑Level Architecture Overview

```mermaid
%%{init: {'theme':'neutral','flowchart':{'curve':'linear'}}}%%
graph TD
    subgraph Clients
        A[Web UI] 
        B[Mobile App] 
        C[Third‑party API client]
    end

    subgraph Edge
        D[CDN (media)] 
        E[Global Load Balancer] 
        F[API Gateway / Edge Router]
    end

    subgraph Auth
        G[Auth Service (OAuth2 + JWT)] 
    end

    subgraph Core[Core Micro‑services]
        H[User Service] 
        I[Tweet Service] 
        J[Timeline Service] 
        K[Media Service] 
        L[Search Service] 
        M[Notification Service] 
        N[Analytics / Trending Service] 
        O[Rate‑Limiter Service]
    end

    subgraph Data
        P[MySQL Shards (User, Tweet)] 
        Q[Cassandra Cluster (Home‑Timeline)] 
        R[Redis Cluster (Cache)] 
        S[ElasticSearch Cluster (Search)] 
        T[Object Store (S3/GS) + CDN] 
        U[Kafka Cluster (Event Bus)] 
        V[Graph Store (e.g., Neo4j or adjacency in Cassandra)] 
    end

    %% Data flows
    A -->|HTTPS| E
    B -->|HTTPS| E
    C -->|HTTPS| E
    E --> F
    F --> G
    G -->|validated JWT| H
    G -->|validated JWT| I
    G -->|validated JWT| J
    G -->|validated JWT| K
    G -->|validated JWT| L
    G -->|validated JWT| M
    G -->|validated JWT| N

    %% User actions
    H --> P
    I --> P
    I --> U
    K --> T
    K --> U
    J --> Q
    J --> R
    L --> S
    M --> U
    N --> U
    O -->|Rate limit| F

    %% Async pipelines
    U -->|TweetCreated| I
    U -->|TweetCreated| J
    U -->|TweetCreated| L
    U -->|TweetCreated| N
    U -->|MediaUploaded| K

    %% Cache
    R -->|Cache Miss| P
    R -->|Cache Miss| Q

    %% Back‑ups
    P -->|Replicas| P
    Q -->|Replicas| Q
    S -->|Replicas| S
    R -->|Replicas| R
```

**Key points**

* All services are **stateless** (except DB nodes) and can be autoscaled behind the load balancer.
* **Kafka** is the backbone for eventual‑consistent pipelines (fan‑out, indexing, analytics).
* **Redis** is used for hot‑data caching: tweet objects, per‑user home‑timeline IDs, trending lists.
* **Cassandra** (or ScyllaDB) stores *home timelines* as write‑optimized, partition‑key‑by‑user‑id.
* **MySQL** (or Aurora) stores the canonical tweet & user records (strong consistency when needed).
* **ElasticSearch** powers full‑text & hashtag search.
* **Object Store + CDN** hosts media (images, videos, GIFs).
* **Graph Store** (or a dense adjacency list in Cassandra) captures the follow‑graph.

---

## 3. Detailed Component Design

### 3.1 API Gateway / Edge Router
* Terminates TLS, does request routing, integrates **rate‑limiter** (token bucket per API key/IP) and **WAF**.
* Stateless; horizontally scalable behind the Global Load Balancer.
* Low‑latency path for static assets (media) → CDN.

### 3.2 Auth Service
* Implements **OAuth 2.0 Authorization Code** flow + **JWT** access tokens (signed with RSA‑256).
* Stores client secrets and user‑password hashes (bcrypt) in MySQL (or Cognito/Keycloak).
* Token introspection performed by downstream services via a **shared public key** (no RPC).

### 3.3 User Service
* CRUD for profiles, follow/unfollow.
* Follow‑graph stored as **edge list** in a *graph store* (or a **wide row** table: `followers:user_id -> [follower_id]` and `following:user_id -> [followee_id]`).
* Horizontal sharding by `user_id` (hash‑based). Replication factor = 3.
* Write path:
  1. Verify JWT.
  2. Write to MySQL (`users` table) + **Cassandra adjacency table** for fast look‑ups.
  3. Emit `FollowCreated`/`FollowDeleted` events to Kafka for downstream cache warm‑up (e.g., populate follower counts).

### 3.4 Tweet Service
* **POST /tweet** – validates length, media IDs, user caps.
* Writes a new row to the **MySQL `tweets`** table (primary key = `tweet_id` – auto‑increment or Snowflake ID). Row contains:
  * `tweet_id` (64‑bit)
  * `user_id`
  * `text` (UTF‑8, ≤ 280 char)
  * `created_at`
  * `reply_to_id` (nullable)
  * `media_ids` (array)
  * `metrics` (likes, retweets – eventually consistent)
* Immediately **produces a `TweetCreated` event** to Kafka (partitioned by `user_id` for ordering).
* **Fan‑out on write** (see Section 3.8) consumes the event.

### 3.5 Media Service
* **POST /media** – multipart upload → validates size/type, assigns a UUID `media_id`.
* Stores raw bytes in **Object Store (S3/GS)** under a path `media/{media_id}`.
* Returns signed URL (short‑lived) to the client.
* Emits `MediaUploaded` event to Kafka for downstream tasks (size‑based transcoding, thumbnail generation).

### 3.6 Timeline Service
Two logical timelines:

| Timeline | Storage | Generation |
|----------|---------|------------|
| **User Timeline** (tweets authored by a single user) | MySQL `tweets` table (range query by `user_id`, `created_at`) | Pull‑on‑read (simple primary‑key scan). |
| **Home Timeline** (tweets of all followees) | **Cassandra `home_timeline`** table: `partition_key = user_id`, clustering column = `tweet_ts DESC`, column = `tweet_id` | **Hybrid fan‑out** (write‑push for “ordinary” users, read‑pull for high‑fan‑out accounts). |

#### 3.6.1 Fan‑out on Write (standard case)

1. `TweetCreated` event → **Fan‑out Service** reads follower list from Graph Store (or pre‑computed in memory cache).
2. For each follower **≤ 10 k** followers, create a **Cassandra mutation**: `INSERT INTO home_timeline (user_id, tweet_ts, tweet_id) VALUES (…)`.
3. Mutations are **batched** per follower (e.g., up to 100 tweet IDs per batch) and sent via **Kafka → Workers → Cassandra driver**.
4. **Back‑pressure** is applied using the Kafka consumer’s `max.poll.records` and local queue depth.

#### 3.6.2 Fan‑out on Read (high‑fan‑out accounts)

* Accounts with > 10 k followers (celebrities) are **flagged** in the User Service.
* Their tweets are **not** pushed to each follower’s home timeline; instead,
  * At read time, the Timeline Service merges:
    * The follower’s **Cassandra home timeline** (push‑based portion).
    * The **latest N tweets** of each high‑fan‑out followee directly fetched from MySQL (or a materialized view `high_fanout_tweets`).
* Merge is performed in memory (or via a **Redis sorted‑set** per request) and trimmed to the page size (e.g., 20 tweets).

#### 3.6.3 Caching

* **Redis LRU cache** holds *the most recent 200 tweet IDs* per user (key `home:{user_id}` → list of tweet IDs). Updated by the Fan‑out Service.
* **Tweet objects** themselves are cached (`tweet:{tweet_id}` → JSON blob). TTL = 24 h (or unlimited if frequently accessed).
* On timeline request:
  1. Try Redis `home:{user_id}` → list of tweet IDs.
  2. If cache miss -> query Cassandra, populate Redis.
  3. Batch‑fetch tweet objects from Redis; on miss fetch from MySQL → write‑through to Redis.

### 3.7 Notification Service
* Consumes `TweetCreated`, `Like`, `Retweet`, `Reply` events.
* Pushes real‑time notifications via:
  * **WebSocket/PubSub** for active sessions.
  * **APNs/FCM** for mobile push.
  * **Email** for digest (daily/weekly).
* Stores **notification inbox** per user in a **Cassandra table** (`notifications(user_id, ts, notif_id, payload)`) with TTL (30 days) for quick retrieval.

### 3.8 Search Service
* **Kafka → Indexer Workers** read `TweetCreated` and **index** into Elasticsearch.
* **Shards**: 12‑shard index per day (time‑based rollover). Keep the last 30 days as *hot* indices; older indices are **frozen** or **moved to cold storage** (e.g., S3 with searchable snapshot).
* **Query latency** target ≤ 100 ms for typical keyword/hashtag search.

### 3.9 Trending / Analytics Service
* Real‑time **Kafka Streams** (or Flink) aggregates **hashtag counts** per sliding window (1 min, 5 min, 1 h).
* Top‑N per window (e.g., top 10) stored in **Redis Sorted Set** `trending:{window}`.
* A periodic **batch job** (Spark) recomputes **daily/weekly** trending topics for historical pages.

### 3.10 Rate Limiter Service
* Token‑bucket per API key + per‑user policies (e.g., 300 tweets/day, 1000 follows/day).
* Implemented with **Redis Lua scripts** for atomicity.
* Violations result in **429 Too Many Requests** responses from API Gateway.

---

## 4. Data Modeling & Storage Choices

| Entity | Primary Store | Reason |
|--------|---------------|--------|
| **Users** | MySQL (`users` table) – PK `user_id`. | Strong consistency for profile updates, login, password. |
| **Tweets** | MySQL (`tweets`). | ACID for write‑once, read‑many pattern; can be sharded by `tweet_id`. |
| **Follower Graph** | **Cassandra adjacency rows** (`followers_by_user`, `following_by_user`) **or Neo4j**. | Extremely high read‑throughput for “who follows X” (needs O(1) fetch). |
| **Home Timelines** | Cassandra (`home_timeline`). | Write‑optimized, linear scalability, tunable consistency. |
| **Tweet Cache** | Redis (hash `tweet:{id}`). | Sub‑ms reads for hot tweets. |
| **Media Blobs** | Object Store (S3/GS) + CDN. | Cheap, durable, parallel streaming. |
| **Search Index** | Elasticsearch. | Full‑text, inverted index, ranking. |
| **Notifications** | Cassandra (`notifications`). | Append‑only, high write throughput, TTL. |
| **Trending Hashtag Counters** | Redis Sorted Sets (`trending:{window}`). | Real‑time top‑K retrieval. |
| **Event Bus** | Kafka (3‑replica, 12‑partition per service). | Decoupling, replayability. |

### 4.1 Sharding & Partitioning

* **MySQL**: Horizontal sharding by `user_id` for `tweets` and `users`. Use a **lookup service** (hash mod N) to route queries. Replication factor = 3 (1 primary + 2 secondaries).  
* **Cassandra**: Partition key = `user_id`. Clustering column = `tweet_ts` (DESC). Replication factor = 3 across at least 3 AZs.  
* **Redis**: Clustered with 2‑digit hash slots (16384 slots) evenly distributed across 20 nodes.  
* **Elasticsearch**: 12 primary shards per index, 1 replica (total 24 shards) → 6 nodes (4 shards/node) with 64 GB RAM each → ~384 GB heap (under 50 % of RAM).  

### 4.2 Capacity Planning (Numbers for Day‑0)

| Metric | Value (Day‑0) | Calculation |
|--------|---------------|-------------|
| **Avg tweets / day** | 500 M | 100 M users × 5 |
| **Tweet size (raw)** | ~300 B | text (≈ 200 B) + metadata |
| **Daily tweet storage** | 150 GB | 500 M × 300 B |
| **Compressed (InnoDB)** | ~60 GB | 2.5× compression |
| **Yearly tweet storage** | ~22 TB | 60 GB × 365 |
| **Peak tweet writes** | 15 k TPS (burst) | 500 M / 86400 ≈ 5.8 k × 2.5 |
| **Home‑timeline writes** (push) | 1.2 M TPS | 15 k × avg followers 200 |
| **Cassandra write throughput** | 1.2 M TPS | As above |
| **Cassandra node capability** | 50 k TPS (writes) | Typical SSD+‑optimized node |
| **Cassandra nodes needed** | 25 nodes (push) | 1.2 M / 50 k ≈ 24 |
| **Redis cache size (tweets)** | 250 GB | 20 M hot tweets × ~12 KB (tweet + metadata) |
| **Redis nodes** | 10 × 32 GB** (max‑usable ~25 GB) | 250 GB / 25 GB |
| **Media volume** | 30 TB / day | 500 M tweets × 20 % media × 300 KB |
| **Object store (S3)** | 30 TB daily, 900 TB monthly | Lifecycle to transition to IA after 30 days |
| **Search index (hot)** | 30 TB (30 days) | 1 KB per tweet × 500 M × 30d |
| **Search nodes** | 30 nodes (8 TB RAM total) | 1 TB per node (SSD) + 32 GB heap |
| **Kafka throughput** | 100 GB / day | Events ~200 B per tweet × 500 M + media events |
| **Kafka brokers** | 12 nodes (3 TB total storage) | 100 GB / 12 ≈ 8 GB, replication factor 3 |

> **Note:** All numbers include a **20 % safety buffer**. The cluster can be horizontally expanded as traffic grows.

---

## 5. Consistency, Availability & Trade‑offs

| Feature | Consistency Model | Trade‑off |
|---------|-------------------|-----------|
| **Tweet write** | **Strong** (MySQL primary) | Guarantees no duplicate tweet IDs; latency ≈ 50 ms. |
| **Home timeline** | **Eventual** (Cassandra, fan‑out) | Small delay (≤ 2 s) before tweet appears; dramatically reduces read latency. |
| **Follow/Unfollow** | **Strong** for own edges, **eventual** for followers of others (cached in Redis). | Guarantees user cannot follow themselves; stale counts acceptable for a few seconds. |
| **Like/Retweet counts** | **Eventually consistent** (incremental counters in Cassandra, merged async) | Real‑time counters may drift by < 5 % temporarily. |
| **Search** | **Near‑real‑time** (indexing latency 1‑2 s) | Acceptable for search; not used for timeline ordering. |
| **Trending hashtags** | **Exactly‑once** processing via Kafka Streams + state stores | Guarantees correct ranking; higher compute cost but feasible. |

### Fan‑out Strategies

| Strategy | Write Load | Read Latency | Storage Overhead | Complexity |
|----------|------------|--------------|------------------|------------|
| **Fan‑out on Write (push)** | **High** (writes × followers) | **Low** (home timeline ready) | **High** (duplicate tweet IDs) | Simple read path |
| **Fan‑out on Read (pull)** | Low | Higher (merge on‑the‑fly) | Low | More compute per read |
| **Hybrid** (push for normal users, pull for high‑fan‑out) | Moderate | Low for most, moderate for celebrity feeds | Moderate | Best of both worlds; added logic for “high‑fan‑out flag”. |

### Storage Choice Trade‑offs

| Store | Strong Consistency | Write Scalability | Read Scalability | Cost |
|-------|--------------------|-------------------|------------------|------|
| MySQL (InnoDB) | ✔ | Moderate (sharding needed) | High (read replicas) | Higher per‑GB |
| Cassandra | ✖ (Eventual) | ✔ (linear) | ✔ (fast partition reads) | Commodity hardware |
| Redis | ✖ (volatile) | ✔ (in‑memory) | ✔ (sub‑ms) | RAM‑intensive |
| Elasticsearch | ✖ (eventual) | ✔ (bulk index) | ✔ (search) | Disk + memory |
| Object Store | – | ✔ (parallel) | ✔ (CDN) | Cheap per‑GB |

---

## 6. Failure Scenarios & Mitigations

| Failure | Impact | Detection → Mitigation |
|---------|--------|------------------------|
| **Cassandra node loss** | Lost partitions → some home timelines unreadable. | **Failure detector** (GOSSIP) → rebuild replicas automatically. Reads fallback to **read‑repair**. |
| **MySQL primary crash** | New tweets blocked. | **Automatic failover** via MySQL Group Replication / Aurora; promotion of secondary. |
| **Kafka broker leader loss** | Event pipeline stalls → delayed timeline fan‑out, search indexing, notifications. | **Controller elects new leader**; producer/consumer retries with exponential back‑off. |
| **Redis cache eviction** | Increased latency (cache miss) → hits DB/Cassandra. | Not a hard failure; caches are warm‑up automatically; monitor **cache hit rate** (> 85 %). |
| **Media CDN outage** | Users see broken images/videos. | **Multi‑CDN** redundancy; fallback to signed S3 URL if CDN 5xx. |
| **Hot partition in Cassandra** (e.g., viral tweet from a celebrity with millions of followers) | Write amplification, latency spikes. | **Hybrid strategy**: high‑fan‑out accounts are flagged; later tweets bypass fan‑out write path, using read‑pull. Also **rate‑limit fan‑out workers** and **circuit‑break** to fallback to read‑pull. |
| **Network partition between data‑centers** | Split‑brain, divergent timelines. | Deploy **multi‑region active‑active** with **read‑only replicas** during outage; eventual reconciliation via **global Kafka topic** and **idempotent writes**. |
| **DDoS attack on API gateway** | Service degradation for legitimate users. | **Rate limiting**, **IP reputation**, **WAF rules**, **scrubbing service** (e.g., Cloudflare). |
| **Data corruption in Object Store** | Media loss. | **Versioned objects**, **MD5 checksum validation** on upload, **cross‑region replication**, **periodic integrity scan**. |
| **Bug causing duplicate tweet IDs** | Inconsistent timeline ordering. | **Snowflake‑style ID generator** guarantees uniqueness (timestamp+machine+sequence). |
| **Latency spikes in timeline service** | Missed SLA (> 100 ms). | **Circuit breaker** → fallback to read‑pull; **request queuing**, **auto‑scale** timeline pods based on QPS metrics. |

---

## 7. Security & Privacy

| Concern | Mitigation |
|---------|------------|
| **Authentication** | OAuth 2.0, JWT signed with RSA‑256, short‑lived access tokens (15 min) + refresh tokens (7 days). |
| **Authorization** | Scope‑based access (read/write). Server checks `user_id` from JWT for each operation. |
| **Transport security** | TLS 1.3 everywhere (edge → services). |
| **Data at rest** | MySQL tables encrypted (InnoDB tablespace encryption). Cassandra encryption‑at‑rest via Transparent Data Encryption (TDE). S3 bucket with SSE‑S3 or SSE‑KMS. |
| **Rate limiting** | Redis token‑bucket per API key + per‑user, prevents abuse. |
| **Input validation** | Unicode sanitisation, emoji handling, SQL‑injection safe parameterised queries. |
| **Content moderation** | Background job scans text & media (ML models) → flag/removal workflow. |
| **GDPR / “Right to be Forgotten”** | Soft‑delete flag in MySQL + background job scrubs tweet, media, indexes, and removes from caches within 30 days. |
| **Audit logging** | All write actions logged to an immutable audit store (e.g., CloudWatch Logs Insight) with user‑id, timestamp, IP. |

---

## 8. Operations, Monitoring & Alerting

| Area | Tooling |
|------|---------|
| **Metrics** | Prometheus (service‑level metrics: QPS, latency, error rates). |
| **Tracing** | OpenTelemetry (distributed traces across API‑gateway → services). |
| **Logging** | Fluentd → Elasticsearch + Kibana (central searchable logs). |
| **Alerting** | Alertmanager → PagerDuty (SLA breach, high latency, cache miss < 70 %). |
| **Capacity planning** | Autoscaling policies (Kubernetes HPA) based on CPU, request latency, queue depth. |
| **Backup & DR** | MySQL logical dumps + binary logs → S3, point‑in‑time restore. <br> Cassandra snapshots → S3 with cross‑region replication. |
| **CI/CD** | GitHub Actions → Blue/Green deployments via Kubernetes Deployments + Istio traffic‑splitting. |
| **Chaos testing** | Chaos Monkey style pod/instance termination to verify resilience. |
| **Feature toggles** | LaunchDarkly / internal config service for canary rollout of new features (e.g., new retweet model). |

---

## 9. Future Scalability & Extensions

| Feature | Scaling Path |
|---------|--------------|
| **Live video streams** | Add dedicated **Media‑Live Service** with RTMP/LL‑HLS ingest, store segments in object store, use **CDN edge workers**; separate timeline “live‑event” object. |
| **Threaded conversations** | Extend tweet schema with `conversation_id`; add a **Thread Service** that aggregates replies using materialized view. |
| **Personalized ranking** | Feed a **Machine‑Learning ranking model** (BERT‑based) as a micro‑service; pass candidate tweet IDs from Timeline Service → rank → return top N. |
| **Graph‑search (“who follows me”)** | Use a dedicated **graph database** (Neo4j) for mutual‑follow queries, or store pre‑computed “friend‑of‑friend” graphs in a key‑value store. |
| **Multi‑region active‑active** | Deploy duplicated clusters per region, use **global traffic manager** with latency‑based routing. Replicate events via **Kafka MirrorMaker**. |
| **Server‑less tweet processing** | Migrate fan‑out workers to **AWS Lambda / GCP Cloud Functions** for automatic scaling and cost efficiency on low‑traffic periods. |
| **End‑to‑end encryption** | Allow users to encrypt tweet bodies client‑side; store encrypted blobs only; service never sees plaintext (future privacy‑first version). |

---

## 10. Summary of Core Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Hybrid fan‑out (push + pull)** | Keeps read latency low for the majority while avoiding write explosion for celebrity accounts. |
| **Cassandra for home timelines** | Write‑optimized, linear scalability, tunable consistency – perfect for massive fan‑out writes. |
| **Redis as hot‑cache** | Sub‑millisecond access for the most recent tweets and per‑user timeline IDs, dramatically reducing DB load. |
| **Kafka as the central event bus** | Decouples services, enables replay, supports exactly‑once semantics for critical pipelines (trending). |
| **ElasticSearch for search** | Proven solution for full‑text, filters, and ranking; allows independent scaling of search workload. |
| **Object Store + CDN for media** | Offloads heavy bandwidth to edge, cheap, highly durable storage. |
| **MySQL sharded for source‑of‑truth** | Guarantees strong consistency for tweet creation and user profile updates. |
| **Rate limiting at edge** | Protects the entire platform from abuse without requiring per‑service enforcement. |
| **Observability stack (Prometheus + OpenTelemetry + ELK)** | Gives end‑to‑end visibility, critical for meeting sub‑100 ms SLA. |

With the above architecture, **TwitX** can comfortably handle the projected day‑0 load (≈ 15 k tweet writes/s, 150 k timeline reads/s) while preserving latency SLAs, providing strong durability guarantees, and maintaining a clear path to scale into hundreds of millions of daily active users.

--- 

**End of design.** If you’d like deeper dives into any individual component (e.g., the fan‑out worker implementation, Snowflake ID generation, or the exact Cassandra data model), just let me know!