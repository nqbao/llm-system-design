---

Design a microblogging platform like Twitter.


---

## 1. Overview & Goal

**Goal:**  
Design a highly‑available, horizontally‑scalable microblogging platform (think “Twitter”) that supports:

| Feature | Minimum Scale |
|--------|---------------|
| Registered users | 500 M (active at any time) |
| Tweets per day | 1 B |
| Avg. followers per user | 300 |
| Average tweet size | 280 bytes payload + 30 bytes metadata ≈ 320 bytes |
| Peak read‑write traffic | 250 k RPS |

> **Why 500 M users?**  
> This matches the user base of a “real” Twitter‑sized service that still lives in the cloud. Every number below is derived from this baseline.

---

## 2. Functional Requirements

| # | Feature | Description |
|---|---------|-------------|
| 1 | **User Management** | Sign‑up, login, MFA, GDPR data wipes |
| 2 | **Tweeting** | Create, edit (only within 10 min), delete |
| 3 | **Followers** | Follow / unfollow |
| 4 | **Timeline** | Feed of recent tweets from followees |
| 5 | **Likes / Retweets** | Like, retweet, quote‑tweet |
| 6 | **Hashtags & Mentions** | Tag parsing & search |
| 7 | **Media** | Image / GIF / video uploads, thumbnails |
| 8 | **Search** | Keyword, hashtag, user, timeline search |
| 9 | **Realtime** | Push notifications, follower alerts, trending topics |
|10 | **Analytics** | User engagement, tweet popularity |
|11 | **Rate‑limiting** | Per‑IP & per‑user request limits |

---

## 3. Non‑Functional Requirements

| # | NFR | Target |
|---|-----|--------|
| 1 | **Availability** | 99.99 % for public endpoints |
| 2 | **Latency** | ≤ 150 ms for 95 % of read requests (timeline, tweet) |
| 3 | **Scalability** | Linear scale‑out across thousands of VM / container instances |
| 4 | **Consistency** | Strong on account data, eventual on timeline |
| 5 | **Durability** | ≥ 1 × R to prevent data loss |
| 6 | **Monitoring** | Prometheus + Grafana, tracing with OpenTelemetry |
| 7 | **Security** | TLS everywhere, OAuth 2.0 for client apps |
| 8 | **Compliance** | GDPR & CCPA compliance, data minimization |

---

## 4. High‑Level Architecture Diagram

```mermaid
graph TD
    subgraph Clients
        PC[Web]
        iOS[Mobile iOS]
        Android[Mobile Android]
        TwiClient[Third‑party Client]
    end

    subgraph Edge
        CDN[Edge CDN]
        LB1[Global L4/7 LB (AWS ELB/ GCP LB)]
    end

    subgraph API
        APIGW[API Gateway (API‑Management)]
        AuthS[Auth Service (OAuth2)]
        UserS[User Service]
        TWS[Tweet Service]
        FLS[Follow Service]
        TLV[Timeline Service]
        MP[Media Service]
        SRV[Search Service]
        NOT[Notification Service]
        ANAL[Analytics Jobs]
    end

    subgraph Storage
        RDS[PostgreSQL (Schema: user, auth, follow)]
        KV[Redis (Session + Cache)]
        CASS[Apache Cassandra / ScyllaDB (Tweets + Adjacency)]
        ES[Elasticsearch (Search Indexes)]
        S3[Object Store (Media)]
        KAFKA[Kafka (Message Bus)]
        REDISSET[Redis Sorted sets (Follower Lists)]
        HBASE[HBase or BigTable (Read‑side feeds)]
    end

    subgraph Ops
        MON[Monitoring + Logging]
        CI[CI/CD Pipelines]
        BACKUP[Backup & DR]
    end

    PC --> CDN
    iOS --> CDN
    Android --> CDN
    TwiClient --> CDN
    CDN --> LB1
    LB1 --> APIGW
    APIGW --> AuthS
    AuthS --> UserS
    AuthS --> TWS
    AuthS --> FLS
    AuthS --> TLV
    AuthS --> MP
    AuthS --> SRV
    AuthS --> NOT
    AuthS --> ANAL

    UserS -->|Read/Write| RDS
    FLS -->|Read| RDS
    FLS -->|Event| KAFKA
    TWS -->|Write| CASS
    TWS -->|Publish| KAFKA
    TLV -->|Read| HBASE
    TLV -->|Update| KAFKA
    TLV -->|Cache| KV
    SRV -->|Search| ES

    MP -->|Public media| S3
    MP -->|Cached| CDN
    NOT -->|Events| KAFKA
    ANAL -->|Jobs| KAFKA
    KAFKA -->|Consume| HBASE
    KAFKA -->|Consume| ES
    KAFKA -->|Consume| REDISSET
    REDISSET -->|Cached followers| KV

    MON --> APIGW
    MON --> AuthS
    MON --> UserS
    MON --> TWS
    MON --> FLS
    MON --> TLV
    MON --> MP
    MON --> SRV
    MON --> NOT
    MON --> ANAL
    MON --> RDS
    MON --> CASS
    MON --> ES
    MON --> KAFKA
    MON --> Redis
    MON --> S3
```

*Explanation*  
The API Gateway fronts all traffic, authenticates via the **Auth Service**, then routes to the appropriate microservice. All writes flow through **Kafka** to a **Write‑Side** data store (Cassandra for tweets, Postgres for relational data). Read‑heavy services (timeline, search) hit sharded **Read‑Side** replicas (HBase/BigTable, Elasticsearch). Media goes to S3 and is served via a CDN.

---

## 5. Component Deep‑Dive

| Component | Responsibility | Technology Choices | Sharding / Replication | Key Metrics |
|-----------|----------------|--------------------|------------------------|-------------|
| **API Gateway / Load Balancer** | Global routing, TLS termination, rate‑limit | AWS Global Accelerator + ALB | 3 AZs, 15+ instances | 250 k RPS |
| **Auth Service** | OAuth2, JWT issuance, single‑sign‑on | Golang/Node+Authlib, Redis for session | 8 nodes (stateless), 2× replication | < 50 ms |
| **User Service** | CRUD user profile, privacy prefs | PostgreSQL + PostGIS for location | 12 DB nodes (RAID‑10) | 50 k RPS |
| **Follow Service** | Follow/unfollow, adjacency graph | Redis Sorted Sets + a write‑side graph table | 32 nodes, sharded on user ID | 200 k writes/second |
| **Tweet Service** | Create/Edit/Delete tweet, media refs | Cassandra (UTF‑8) | 24 nodes, 3× replication | 200 k writes/second |
| **Timeline Service** | Read‑side feed construction | HBase / BigTable | Partitioned on user ID, 8 TB raw | 250 k reads/second |
| **Media Service** | Upload, transcoding, thumbnails | S3 + FFmpeg + CloudFront | 8 S3 buckets (region‑aligned) | 50 MBps |
| **Search Service** | Full‑text search | Elasticsearch 7.x (distributed) | 10 shards + 5 replicas | 30 k search RPS |
| **Notification Service** | Push/Broadcast | Kafka + WebSocket (socket.io) + APNS/FCM | 8 nodes | 100 k events/sec |
| **Analytics** | Batch jobs, real‑time trend analysis | Spark + Flink + KSQL | 12 worker nodes | 500 k messages/day |
| **Caching** | Hot tweets, follower lists, session | RedisCluster | 12 nodes, 256 GB | 95 % hit rate |
| **CDN** | Static content, media, thumbnails | CloudFront | Edge‑locations globally | 90 % cache hit |

---

## 6. Capacity & Performance Calculations

### 6.1 User Base & Traffic

| Metric | Value | Notes |
|--------|-------|-------|
| Avg. users | 500 M | 20 % active per day = 100 M active |
| Avg. tweets / user / day | 2 | 1 B tweets / day |
| Tweets / second | ~11 k | 1 B / 86 400 |
| Read ops per tweet | 4 (timeline, search, like, retweet) | 4 B read ops per day |
| Avg. read ops / second | ~46 k | 4 B / 86 400 |
| Peak read ops | 150 k (flash sales, trending moments) | 3× avg |
| Write ops | 1 B tweet writes / day | 11 k /writes/second |
| Peak writes | 20 k /writes/second | 2× avg |
| DB writes per second to Cassandra | 20 k | 3× replication → 60 k writes |
| Bandwidth for tweet data | 320 bytes * 1 B = 320 GB/day | 12 TB/year |

### 6.2 Data Storage

| Store | Size | Calculation |
|-------|------|-------------|
| **Posts** | 320 B * 1 B = 320 GB/day → 1.2 TB/month → 12 TB/year | Data is retained for 3 years → 36 TB |
| **Followers graph** | 150 B edges, avg 8 bytes per edge = 1.2 PB? | Stored as adjacency *set* per user: < 3 KB per user → 1.5 TB |
| **User profiles** | 200 B * 500 M = 100 GB | With indexes ≈ 200 GB |
| **Media** | Avg 2 MB per tweet (image) * 1 B = 2 PB/day | 3 PB/month → 30 PB/year (compressed) |
| **Search indexes** | 2× size of posts (tokenization) | 60 TB/year |
| **Cache (Redis)** | 10 % hot tweets (100 M tweets) * 320 B = 32 GB | + follower lists (1.5 TB) → 1.6 TB |

### 6.3 Compute & Cluster Size

| Service | Nodes (stateless/DB) | CPU/GPU | RAM per node | Total |
|---------|----------------------|---------|--------------|-------|
| Auth + API Gateway | 20 | 4 vCPU | 8 GB | 20 |
| User Service (Postgres) | 12 | 16 vCPU | 32 GB | 12 |
| Tweet Service (Cassandra) | 24 | 16 vCPU | 64 GB | 24 |
| Timeline (HBase) | 16 | 32 vCPU | 128 GB | 16 |
| Follow (Redis) | 8 | 8 vCPU | 32 GB | 8 |
| Search (Elasticsearch) | 10 | 32 vCPU | 256 GB | 10 |
| Media & CDN | 8 | 4 vCPU | 8 GB | 8 |
| Notify | 8 | 8 vCPU | 32 GB | 8 |
| Analytics | 12 | 32 vCPU | 256 GB | 12 |
| **Total compute** | **104** node equivalents | | |

All clusters are deployed across 3 availability zones to satisfy HA and reduce RPO.

### 6.4 Latency Boundaries

| Operation | Avg. latency | 95th percentile |
|-----------|--------------|-----------------|
| Tweet POST | 40 ms | 80 ms |
| Timeline GET | 70 ms | 140 ms |
| Like/Retweet | 30 ms | 60 ms |
| Search | 120 ms | 250 ms |
| Media upload | 1–3 s (async) | – |

These numbers are derived from *real‑world* Twitter metrics (~25 ms for tweet POST, 80 ms for timeline), scaling for 3× traffic.

---

## 7. Trade‑Offs & Rationale

| Decision | Reason | Alternatives |
|----------|--------|---------------|
| **Push‑on‑write vs Pull‑based timeline** | Push‑on‑write creates fan‑out for massive followees, increasing write cost. Pull‑based reduces load but adds read latency if we must compute on the fly. | *Hybrid*: Cached windows of most recent tweets per user; fallback to K‑stream join when cache misses. |
| **Cassandra for tweets** | Linear scalability, strong consistency per node, wide‑column model fits. | PostgreSQL (sharded), DynamoDB (managed). |
| **Redis for follower lists** | O(1) lookup for follow checks and fan‑out list retrieval. | HBase (overkill), MySQL join (slow). |
| **Elasticsearch for search** | Near real‑time index, full‑text queries, aggregations. | Apache Solr (similar), PostgreSQL LOB (not performant). |
| **Kafka as bus** | Durable, partitioned, many consumers, well‑tested. | RabbitMQ (good for small jobs), GCP Pub/Sub. |
| **CDN for media** | Off‑loads traffic, edge locality. | Direct S3 distribution (no caching). |
| **OpenTelemetry** | Observable system for micro‑services. | Zipkin alone (old), Datadog APM (proprietary). |
| **Consistent hashing** | Balances sharding load evenly, minimal data movement on scaling. | Range partitioning (requires manual re‑balancing). |

---

## 8. Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **Network Partition (AZ failure)** | Some users lose service | Multi‑AZ replicas, write to multiple DB nodes, read‑replica redirect |
| **Kafka Outage** | Ingestion pipeline stalls, timeline updates lost | Dual‑cluster quorum, backup consumers to HBase directly |
| **Cassandra Node Loss** | Write unavailability for impacted partitions | 3× replication, hinted handoff, anti‑entropy repair |
| **Redis Cache Crash** | Cache miss → DB load spike | Auto‑restart, local fallback, persistence (RDB + AOF) |
| **Search Index Corruption** | Search becomes inaccurate | Snapshots + nightly full re‑index; separate write/read indices |
| **Media Upload Failure** | Broken media links | Client retries, idempotent uploads (hash‑based) |
| **Rate‑Limit Bypass** | DDoS / flash crowds | Adaptive rate‑limit per IP + per user, CAPTCHA |
| **Data Loss** | GDPR deletion requests & backups | 3× replication, WORM backups to cold storage |


**Observability**: Every micro‑service publishes metrics (via Prometheus) and logs (ELK stack). Alerting rules notify ops on latency thresholds, error rates, and missing replication. Chaos‑engineering (Gremlin, Chaos Mesh) runs nightly to validate failover paths.

---

## 9. Operational Considerations

| Activity | Tool | Notes |
|----------|------|-------|
| **Continuous Integration / Delivery** | GitHub Actions + Terragrunt | Infrastructure as code, immutable deployments |
| **Database Backups** | Snapshots + incremental WAL to S3 | 48 h RTO, 30 min RPO |
| **Disaster Recovery** | Secondary region (failover) | Weekly cross‑region sync |
| **Security Testing** | OWASP ZAP, Snyk | Weekly scans, license compliance |
| **Capacity Planning** | Grafana dashboards + forecasting | Auto‑scaling groups with CloudWatch alarms |
| **Cost Optimization** | Spot + Reserved Instances | 30 % reduction vs. On‑Demand |

---

## 10. Future‑Proofing

1. **Video & Live Streaming** – Replace media service with Kinesis Video Streams + transcoding micro‑service; store segments in cloud storage.
2. **Spaces‑like Audio** – Add WebRTC signaling server, STUN/TURN, integrate with notification service.
3. **AI‑powered Features** – Integrate GPT & moderation models as external services; process tweets via message queue.
4. **GraphQL API** – Provide a single endpoint for rich client queries.
5. **Multi‑tenant** – For a SaaS‑like spin‑off, partition data by namespace within Cassandra.

---

## 11. Sample Micro‑Service Flow: “Tweet Post”

```
Client → CDN → LB → API GW → Auth → Tweet Service
   └── Kafka (tweet create event) ──────┐
          │                              │
          ▼                              ▼
      Cassandra (write)              Timeline Service
          │ (replica)                  │ (consume, update)
          ▼                              ▼
   [Persisted tweet]              Update follower feeds
```

- **Step 1 – Auth**: API Gateway verifies JWT; if missing, returns 401.
- **Step 2 – Validation**: Tweet Service validates payload, user rate‑limit.
- **Step 3 – Persist**: Write to Cassandra; the write is acknowledged immediately (once all 3 replicas ack) → < 40 ms.
- **Step 4 – Event**: Publish `tweet_created` to Kafka partition keyed by user_id.
- **Step 5 – Consumer**: Timeline Service consumes message, retrieves list of follower IDs from Redis, pushes new tweet IDs into each follower’s “feed” table (HBase). The write uses pipelining to keep throughput above 20 k rps.
- **Step 6 – Cache**: Update hot‑tweet cache in Redis and invalidate follower feed cache entries.

---

# Summary

This design balances **low latency** for read‑heavy endpoints, **scalable write capacity** for millions of users, and **strong consistency** for user and tweet data. By combining **stateless services**, **partitioned time‑series stores**, and a **robust message bus**, the system achieves the following:

- **≥ 99.99 % uptime** across all tiers.  
- **250 k RPS** peak support with linear scale‑out.  
- **Sub‑150 ms user‑perceived latency** for core operations.  
- **High resilience** with multi‑AZ, replication, and automated recovery.

Implementing this architecture in a cloud‑native environment (Kubernetes + managed services) would allow rapid iteration, A/B testing on new features, and graceful scaling toward the scale of the world’s largest social platforms.