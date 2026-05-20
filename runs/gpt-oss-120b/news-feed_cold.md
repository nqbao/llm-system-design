---

Design a news feed system like Twitter or Facebook.


---

# News‑Feed System Design (Twitter / Facebook‑style)

Below is a **complete, self‑contained design** for a modern, high‑throughput news‑feed service.  
All major decisions are justified with **capacity calculations**, explicit **trade‑offs**, and a discussion of **failure modes**.  
A full component diagram is included using **Mermaid**.

---

## 1. Goals & Requirements

| Category | Requirement |
|----------|-------------|
| **Functional** | • Users can **post** short messages (text + media). <br>• Users can **follow / unfollow** other users. <br>• Feed is **chronologically + algorithmically** ordered. <br>• Support **likes, replies, retweets/shares**. <br>• Real‑time **push notifications** for mentions/likes. |
| **Non‑functional** | • **Low latency**: 95‑th percentile feed read ≤ 200 ms, post propagation ≤ 1 s. <br>• **High availability**: 99.99 % uptime per service, graceful degradation. <br>• **Scalable** to > 1 billion daily active users (DAU). <br>• **Durable** storage (no data loss < 1 hour). <br>• **Consistent** for user‑level actions (e.g., a user must see their own post immediately). <br>• **Security & privacy**: OAuth2, TLS, granular ACLs. |
| **Operational** | • Fine‑grained **metrics & alerting**. <br>• **A/B testing** of ranking algorithms. <br>• **Rate limiting** & abuse detection. |

---

## 2. High‑Level Architecture

```mermaid
graph TD
    %% External
    A[Client (Web / Mobile)] -->|HTTPS| LB[API Gateway / Load‑Balancer]

    %% API layer
    LB -->|REST/GraphQL| Auth[Auth Service (OAuth2)]
    LB -->|REST| Router[Request Router]

    %% Core services
    Router -->|POST /tweet| PostSVC[Post Service]
    Router -->|GET /timeline| FeedSVC[Feed Service]
    Router -->|POST /follow| FollowSVC[Follow Service]
    Router -->|POST /like| InteractionSVC[Interaction Service]

    %% Async pipelines
    PostSVC -->|Kafka "post‑events"| FanoutSVC[Fan‑out Service]
    FanoutSVC -->|Write| TimelineDB[Timeline Store (Cassandra/DynamoDB)]
    FanoutSVC -->|Write| UserCache[Redis Cache (per‑user feed)]

    InteractionSVC -->|Kafka "engagement‑events"| RankSVC[Ranking Service]
    RankSVC -->|Write| RankDB[Ranking Model Store (Elasticsearch/Redis)]

    %% Media
    PostSVC -->|Upload| MediaSVC[Media Service (S3+CDN)]

    %% Search / Graph
    Router -->|GET /search| SearchSVC[Search Service (Elasticsearch)]

    %% Notification
    InteractionSVC -->|Kafka "notif‑events"| NotifSVC[Notification Service]

    %% Monitoring
    subgraph Monitoring
        Prom[Prometheus] --> Alert[Alertmanager]
        Logs[ELK Stack] --> Graf[Grafana]
    end

    %% Data replication
    TimelineDB -->|replicate| TimelineRO[Read Replicas]
    RankDB -->|replicate| RankRO[Read Replicas]
```

*The diagram shows synchronous request flow (solid arrows) and asynchronous pipelines (Kafka topics).*

---

## 3. Detailed Component Design

| Component | Primary Responsibilities | Tech Choices | Scaling Strategy |
|-----------|--------------------------|--------------|-------------------|
| **API Gateway / LB** | TLS termination, request routing, DDoS protection | Envoy / HAProxy, Cloud‑LB (AWS ALB) | Horizontal autoscaling, geo‑routing |
| **Auth Service** | Token issuance, validation, OAuth2, SSO | OAuth2 server (Keycloak) + JWT | Stateless, replicated |
| **User Service** | Profile CRUD, user settings, privacy flags | MySQL (strong consistency) + Vitess sharding | Write‑heavy: primary‑replica, read replicas |
| **Follow Service** | Manage follow/unfollow, fan‑out on follow | Cassandra (wide rows) | Partition by follower‑id hash |
| **Post Service** | Validate, persist post meta, store media ref | Write‑through to Cassandra + S3 for media | Partition by author‑id hash |
| **Media Service** | Store binaries, generate thumbnails, CDN | Object storage (S3) + CloudFront/Akamai CDN | Unlimited scalability |
| **Fan‑out Service** | **Push** new post to followers’ timelines (fan‑out‑on‑write) | Kafka consumer → batch write to Timeline DB (Cassandra) | Multi‑partition batch, rate‑limited, back‑pressure |
| **Timeline Store** | Per‑user feed entries (post‑id, timestamp, rank‑score) | Cassandra / DynamoDB (log‑structured, high write throughput) | Partition key = user‑id hash, auto‑sharding |
| **User Cache (Redis)** | Hot‑user feed cache (e.g., last 200 posts) | Redis Cluster (LRU eviction) | Replicated shards, write‑through from Timeline DB |
| **Feed Service** | **Read** path: assemble timeline, apply ranking, paging | Reads from Redis → fallback to Timeline DB → ranking service | Cache‑first, read‑replicas, pagination token |
| **Interaction Service** | Likes, replies, retweets, comment threads | Write to Cassandra, enqueue events | Stateless workers, idempotent writes |
| **Ranking Service** | Machine‑learning based scoring, personalization | Offline batch jobs (Spark/Flink) + online feature store (Redis) | Model versioning, A/B testing |
| **Search Service** | Full‑text search, hashtag lookup | Elasticsearch | Index replication & scaling |
| **Notification Service** | Push notifications, email, in‑app alerts | Kafka → worker pool → APNs/FCM | Rate‑limited per user |
| **Monitoring** | Metrics, logs, tracing | Prometheus, Grafana, OpenTelemetry | High‑resolution alerts |

---

## 4. Data Model (simplified)

| Table / entity | Primary Key | Important Columns |
|----------------|-------------|-------------------|
| **users** | `user_id (UUID)` | `handle`, `email`, `created_at`, `profile_json` |
| **posts** | `post_id (UUID)` | `author_id`, `content_text`, `media_key`, `created_at`, `reply_to_id` |
| **followers** | `user_id (partition)`, `follower_id (clustering)` | `followed_at` |
| **timeline** | `user_id (partition)`, `post_ts (clustering desc)` | `post_id`, `author_id`, `rank_score` |
| **interactions** | `(post_id, user_id)` composite | `type (LIKE/RETWEET/REPLY)`, `created_at` |
| **rank_features** | `user_id` | `feature_vector JSON` |

*All tables are **log‑structured** (append‑only). Deletions are tombstoned (eventual GC).*

---

## 5. Feed Generation Strategies

| Strategy | Description | Pros | Cons | When to use |
|----------|-------------|------|------|-------------|
| **Fan‑out on Write (Push)** | When a post is created, the system writes a copy of the post id into every follower’s timeline. | • Immediate consistency for followers. <br>• Low read latency (single DB read). | • Write amplification for users with millions of followers (celebrity). <br>• Hot‑shard risk. | • Most users (≤10k followers). |
| **Fan‑out on Read (Pull)** | Store post only once; when a user reads their feed, the service fetches recent posts from the authors they follow and merges on‑the‑fly. | • No write amplification. <br>• Naturally handles massive follow‑counts. | • Higher read latency (multiple DB hits). <br>• Requires powerful aggregation. | • High‑profile users (≥100k followers) and **cold** users (few reads). |
| **Hybrid (Hierarchical Fan‑out)** | Combine both: push for “regular” followers, pull for a “tail” of followers (e.g., beyond 10k). Use a **pre‑computed “fan‑out feed”** plus **on‑the‑fly lookup** for the rest. | • Balances write vs read cost. <br>• Keeps hot‑user latency low. | • More complex implementation, need to track follower buckets. | • Production systems at Twitter‑scale. |
| **Ranking Layer** | After fetching raw timeline entries, apply a **score = f(recency, engagement, personalization)**. This can be done at read‑time (online) or pre‑computed (offline). | • Allows algorithmic feed. | • Online scoring adds CPU; offline needs periodic recompute. | • Any system that wants “home‑timeline” beyond chronological order. |

**Chosen approach** for this design: **Hybrid push + pull** with a **ranking service**.

- **Push** for users with ≤ 10 k followers (≈ 95 % of accounts).  
- **Pull** for the remaining “celebrity” bucket (≥ 10 k followers).  
- **Cache** the most recent 200 posts per user in Redis; fallback to DB + pull if cache miss.

---

## 6. Capacity Planning & Math

Assumptions (worst‑case design size):

| Metric | Value |
|--------|-------|
| **Daily Active Users (DAU)** | 1 billion |
| **Average posts per active user per day** | 4 |
| **Average followers per user** | 200 (skewed distribution: 90 % ≤ 10 k, 0.1 % ≥ 1 M) |
| **Peak factor (traffic bursts)** | 2× average |
| **Read‑to‑write ratio** | 100 : 1 (users read far more than write) |
| **Retention** | 30 days of timeline stored |

### 6.1 Write Load (Posts)

- **Total posts per day** = 1 B × 4 = 4 B posts  
- **Average posts per second** = 4 B / 86 400 ≈ 46 300 pps  
- **Peak (×2)** ≈ 93 k pps.  

**Write capacity needed**:

| Service | Rate (ops/s) | Scaling |
|--------|--------------|---------|
| Post Service (validate + store) | 93 k | 30× 3‑node write pods (≈ 3 k ops each) |
| Fan‑out Service (push) | For push users (≈ 95 % of posts) → 0.95 × 93 k ≈ 88 k writes per sec to Timeline DB (each write may fan‑out to **N** followers). <br>Average fan‑out = 200 → **~17 M row writes/s**. | Use **Kafka → batch workers** (e.g., 500 workers each handling 30 k rows/s). |
| Pull‑only (celebrity) writes: negligible (store once). |
| Interaction writes (likes, retweets) | Assume 20 % of posts generate 10 engagements avg → 4 B × 0.2 × 10 = 8 B events/day → 93 k pps. | Same pipeline as interactions → Kafka → Interaction Service. |

### 6.2 Read Load (Timeline)

- **Average reads per user per day** = 120 (scroll 20 times × 6 posts each)  
- **Total read ops/day** = 1 B × 120 = 120 B reads  
- **Average reads/s** = 120 B / 86 400 ≈ 1.39 M rps  
- **Peak (×2)** ≈ 2.8 M rps.

**Read path**:

1. **Cache hit** (Redis) for hot users – we target **80 %** cache hit → 2.24 M rps served directly from Redis.  
   - Redis cluster must sustain **~2.3 M GETs/s**. With 10 shards, each shard ≈ 230 k GET/s, well within modern Redis‑Cluster capability (≈ 1 M GET/s per node).  
2. **Fallback to Timeline DB** for cold users (20 % → 0.56 M rps).  
   - Use **read‑replicas** (Cassandra) in a fan‑out fashion; each replica can handle ≈ 100 k rps, so 6 replicas suffice.

### 6.3 Storage Requirements

| Entity | Size per item | Daily volume | 30‑day total |
|--------|---------------|--------------|--------------|
| Post metadata (JSON) | 300 B | 4 B × 300 = 1.2 GB | 36 GB |
| Media (average 100 KB) | 100 KB | 4 B × 100 KB = 400 GB | 12 TB |
| Timeline rows (post‑id + ts + score) | 40 B | 4 B × 200 followers avg → 800 B rows → 800 B × 40 B ≈ 32 GB | 960 GB |
| Interactions (likes, retweets) | 30 B | 8 B × 30 B ≈ 240 GB | 7.2 TB |

**Total storage ≈ 20 TB** for 30 days – comfortably hosted on **cloud object storage + SSD‑based DB nodes**.

### 6.4 Network Bandwidth

- **Ingress (posts + media uploads):** Assume 80 % of posts have media → 3.2 B × 100 KB ≈ 320 TB/day ≈ 3.7 Gbps average (peak ≈ 7 Gbps).  
- **Egress (timeline reads):** 1.39 M rps × average payload 2 KB (post id + author meta) ≈ 2.8 GB/s ≈ 22 Gbps (peak ≈ 44 Gbps).  

Solution: **10 Gbps + 40 Gbps** NICs on read‑heavy nodes; **CDN** for media reduces egress from origin.

---

## 7. Scaling Techniques & Trade‑offs

| Technique | How it works | Benefit | Caveat |
|-----------|--------------|---------|--------|
| **Horizontal sharding by user‑id hash** | Each service stores data in partitions based on a consistent hash of `user_id`. | Unlimited scalability, uniform load distribution. | Hot‑user hot‑spot if hash function not random enough → monitor skew. |
| **Time‑based partitioning for timeline (e.g., `user_id|date`)** | Allows TTL & easier GC of old rows. | Faster range scans for recent feeds. | Slightly more complex query logic. |
| **Write‑through cache** (Redis) | On post creation, also push entry into cache for author’s followers. | Keeps hot‑feed fresh without DB round‑trip. | Cache invalidation required on delete/privilege change. |
| **Back‑pressure via Kafka** | Producers block when queues fill; workers scale out automatically. | No overload of downstream DB. | Must size partitions correctly to avoid hot partitions. |
| **Circuit Breaker & Bulkhead** | Each service tracks latency; trips breaker if downstream unhealthy. | Prevent cascading failures. | Adds complexity in client SDKs. |
| **Graceful degradation (pull‑fallback)** | If push pipeline fails, read service falls back to pull model for affected users. | Users still see a feed (older posts). | Slightly higher latency for those users. |
| **Autoscaling (CPU/queue length)** | Cloud autoscaling groups expand based on metrics (e.g., Kafka lag). | Cost‑effective under variable load. | Scale‑up latency (seconds‑minutes) → keep warm pools. |
| **Read‑replicas + quorum reads** | Timeline DB replicates data; reads can be served by any replica. | Low latency, high read throughput. | Consistency: may be slightly stale (eventual). |
| **Cold‑storage tier** | Move posts older than 30 days to cheaper blob store, keep only IDs. | Reduces primary DB size. | Must fetch from cold storage if user scrolls deep. |

**Decision Matrix** (Push vs Pull vs Hybrid)

| Metric | Push‑only | Pull‑only | Hybrid (chosen) |
|--------|----------|-----------|-----------------|
| Write amplification | **High** (↑ × followers) | Low | Medium (≈ 95 % push, 5 % pull) |
| Read latency | Low (single read) | Higher (multiple reads) | Low for most, moderate for celebrities |
| Complexity | Simple | Simple | **Higher** (needs routing logic) |
| Cost (writes vs reads) | Write‑heavy (more DB IOPS) | Read‑heavy | Balanced |
| Failover | If fan‑out down → no new posts for followers | Works regardless | Hybrid fallback: pull for affected users |

---

## 8. Consistency & Ordering Guarantees

| Operation | Desired Consistency | Mechanism |
|-----------|---------------------|-----------|
| **User sees own post** | **Strong** (immediate) | Write to Post DB + synchronous write to author’s own timeline cache (blocking call). |
| **Follower sees new post** | **Read‑after‑write** within ≤ 1 s for push users. | Fan‑out workers commit before acknowledging; use **Kafka offset** as commit point. |
| **Engagement counts (likes, retweets)** | **Eventual** (may lag a few seconds) | Increment counters via **Redis atomic INCR** with periodic flush to DB. |
| **Timeline ordering** | **Total order per user** (by post timestamp + tie‑breaker) | Use **snowflake‑style IDs** (timestamp + seq) → deterministic ordering. |
| **Cross‑device deduplication** | **Idempotent** writes | Client includes `request_id` UUID; service stores seed hash to ignore duplicates. |
| **Delete / moderation** | **Strong** for the post itself; **eventual** for downstream copies. | Mark row as tombstone; background compaction removes from timeline DB. |

---

## 9. Fault‑Tolerance & Resilience

| Failure Mode | Impact | Mitigation |
|--------------|--------|------------|
| **Kafka broker failure** | Event loss, fan‑out stall | Replicated partitions (RF = 3), automatic leader election, producer retries. |
| **Timeline DB node outage** | Reads from affected shard fail | Read‑replicas + consistent hashing, automatic fail‑over, data re‑balancing. |
| **Redis cache loss** | Cold reads (higher latency) | Persist to disk (`appendonly`), multi‑AZ replication, warm‑up from DB on restart. |
| **Network partition between micro‑services** | Service timeouts | Circuit‑breaker + fallback to stale cache (read‑only). |
| **Hot‑shard overload (celebrity fan‑out)** | Queue build‑up, latency spikes | **Hybrid** pushes; use **batch fan‑out** with throttling; route overflow to **pull** path. |
| **Media CDN outage** | Broken image/video links | Multi‑CDN strategy + signed URLs fallback to origin. |
| **Security breach (token theft)** | Unauthorized actions | Short‑lived JWT (≤ 15 min) + refresh tokens, token revocation list in Redis. |
| **Data center loss** | Global outage | Multi‑region deployment; DNS‑based traffic steering; eventual consistency across regions (active‑active). |
| **Software bug causing cascade delete** | Data loss | **Feature flag** gating, canary roll‑out, audit logs + manual recovery scripts. |

**Graceful degradation flow**: If the fan‑out pipeline is unhealthy, the Feed Service automatically **switches to pull‑only** for affected users (reads from Post DB + follows table). This ensures the user still receives a feed albeit with higher latency.

---

## 10. Security & Privacy

| Concern | Solution |
|---------|----------|
| **Authentication** | OAuth 2.0/OpenID Connect; JWT signed with RSA‑256, rotated keys via JWKS. |
| **Authorization** | ACL checks in each service (`user_id` vs `target_id`). Private accounts stored flag; feed service filters. |
| **Transport security** | TLS 1.3 everywhere (edge → internal). |
| **Data at rest** | Server‑side encryption (SSE‑S3, encrypted Cassandra tables). |
| **GDPR / Data deletion** | Logical delete flag + asynchronous purge job; user can request complete removal. |
| **Rate limiting** | Token bucket per IP + per‑user; abuse detection via anomaly scores (Kafka stream). |
| **Audit** | Immutable write‑once logs (AWS CloudTrail / Kafka log), retained 90 days. |
| **DDoS protection** | Cloud‑front/WAF; API‑gateway throttling; IP reputation. |

---

## 11. Monitoring, Observability & Operations

| Aspect | Tooling |
|--------|--------|
| **Metrics** | Prometheus (exporters from each service). Key metrics: request latency (p95), error rate, cache hit ratio, Kafka lag, fan‑out queue depth. |
| **Tracing** | OpenTelemetry (Jaeger) – end‑to‑end request ID propagated across services. |
| **Logging** | Elastic Stack (Filebeat → Logstash → Elasticsearch). Structured JSON logs, log enrichment with request IDs. |
| **Alerting** | Alertmanager – thresholds (e.g., cache hit < 70 %, fan‑out lag > 30 s, 5‑xx error rate > 0.5 %). |
| **Capacity planning** | Periodic load‑testing with **k6** / **Gatling**, auto‑scale policies tuned based on observed headroom. |
| **Chaos Engineering** | **Chaos Monkey** for instance termination, network latency injection, verifying fallback paths. |
| **Deployment** | Blue‑Green / Canary via **ArgoCD**; feature flags via **LaunchDarkly**. |
| **Backup & DR** | Daily snapshots of Cassandra + off‑site copies; point‑in‑time restore for user data; media bucket versioning. |

---

## 12. Trade‑offs & Alternatives

| Decision | Reasoning | Alternatives |
|----------|------------|--------------|
| **Hybrid push/pull** | Balances write amplification vs read latency; proven at Twitter scale. | Pure push (simpler but expensive for celebrities). <br>Pure pull (simpler but higher read latency). |
| **Cassandra for timeline** | Write‑optimized, linear scalability, natural for wide rows (followers). | DynamoDB (managed), HBase, or **ScyllaDB** (Cassandra‑compatible but higher performance). |
| **Redis cache** | Sub‑millisecond reads for hot feeds; cheap compared to DB reads. | Memcached (stateless) – but lacks persistence for recovery. |
| **Kafka for event streams** | Exactly‑once semantics with idempotent consumers; high throughput. | Pulsar (multi‑tenant, built‑in tiered storage) – could replace Kafka for long‑term retention. |
| **SQL (MySQL) for user profiles** | Strong consistency for critical user data, relational constraints. | NoSQL (Couchbase) – higher write throughput but weaker transaction guarantees. |
| **Batch ranking (offline)** | Allows complex ML models without impact on request latency. | Real‑time scoring (online) – lower latency but heavier CPU per request. |
| **Single region vs multi‑region active‑active** | Multi‑region provides disaster recovery, lower latency globally. | Start with single region & add regions later via data‑replication. |

---

## 13. Potential Failure Scenarios & Detailed Mitigations

| Scenario | Symptom | Detection | Response |
|----------|---------|-----------|----------|
| **Fan‑out worker deadlock** | Kafka lag grows, new posts not visible to followers. | Lag metric > 5 min; alerts on consumer pause. | Auto‑restart workers; if persistent, switch affected users to pull‑only for a defined window. |
| **Redis cache loss** | Feed latency spikes from 30 ms → 200 ms; cache hit rate drops. | Cache hit metric < 70 %; increased DB load. | Spin up new Redis nodes from snapshot; warm up hot keys from DB using **lazy population**. |
| **Network partition between region A and core DB** | Writes succeed locally but not replicated; stale reads in other regions. | Inconsistent replica lag, erratic version vectors. | Activate **read‑only mode** for region; route writes to healthy region via API‑gateway; inform users with “offline” banner. |
| **Massive surge (e.g., celebrity tweet)** | Fan‑out queue saturates, latency > 30 s, errors 5xx. | Queue depth → threshold; error rate spikes. | **Rate‑limit** the specific post’s fan‑out (throttle to 100 k writes/s); fallback to pull for all followers (they will request directly from Post DB). |
| **Media CDN cache‑poison attack** | User sees corrupted images, large bandwidth to origin. | Origin 502 errors, abnormal error logs. | WAF rule to block malformed URLs; purge CDN edge caches for the targeted path. |
| **Compromised JWT** | Unauthorized actions (e.g., posting as another user). | Spike in unusual IPs, anomaly detection. | Immediate revocation list push to Auth service; force token refresh; audit log review. |

---

## 14. Summary

The design presents a **scalable, low‑latency news‑feed system** capable of handling **billion‑scale DAU** while delivering a **personalized timeline**. Key take‑aways:

1. **Hybrid fan‑out** (push for the vast majority, pull for high‑fan‑out users) dramatically reduces write amplification while preserving sub‑second feed freshness.
2. **Cassandra‑style timeline storage** combined with a **Redis hot‑feed cache** satisfies the read‑heavy workload with cheap, linear scaling.
3. **Kafka‑driven asynchronous pipelines** decouple user‑facing requests from heavy background processing (fan‑out, ranking, notifications), enabling graceful degradation.
4. **Capacity calculations** show that with modest node counts (≈ 50 write pods, 500 fan‑out workers, 10 Redis shards) the system can service **≈ 100 k writes/sec** and **≈ 3 M reads/sec**, well within limits of commodity cloud resources.
5. **Fault‑tolerance** is addressed at every layer: replicated data stores, circuit breakers, fallback pull mode, and multi‑region active‑active deployment.  
6. **Observability** (metrics, tracing, logs) and **chaos engineering** ensure rapid detection and remediation of anomalies.

With incremental development—starting with a single region, simple push‑only fan‑out, and expanding to the hybrid model as user‑growth drives fan‑out load—this architecture can evolve smoothly from MVP to global production scale.