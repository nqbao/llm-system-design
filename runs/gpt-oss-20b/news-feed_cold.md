---

Design a news feed system like Twitter or Facebook.


---

# 1.  System Overview  

| Aspect | Description |
|--------|-------------|
| **Goal** | Deliver a real‑time “news‑feed” (timeline) to millions of active users, similar to Twitter or Facebook. |
| **Scale** | 1 billion monthly active users (MAU), 1 billion tweets / day (≈ 11 k writes / s). Peak bursts up to 1 million writes / s. |
| **Latency** | Feed read ≤ 80 ms (99 %ile). Tweet create ≤ 300 ms. |
| **Consistency** | Eventual consistency for follower graph; strong ordering for a user’s own tweets. |
| **Reliability** | ≥ 99.99 % uptime; automatic fail‑over; graceful degradation on partial failures. |

> **TL;DR** – The system ingests tweets, persists them in a distributed columnar store, continuously pushes them into follower timelines (or keeps a “pull” cache), ranks them per user, and serves the feed via a highly‑caching CDN‑backed API layer.

---

## 2.  High‑level Architecture

```mermaid
flowchart TD
    subgraph CLIENTS
        A[Mobile/Web] --> B[API Gateway]
    end

    subgraph AUTH
        B --> C[Auth Service]
    end

    subgraph POSTING
        C --> D[Post Service] -->|writes to| E[Kafka: posts]
    end

    subgraph STORING
        E --> F[Post‑Persist Service] --> G[Cassandra: posts]
    end

    subgraph FOLLOW
        C --> H[Follow Service] --> I[Redis: follower sets]
    end

    subgraph PREDICTION
        E --> J[Feed‑Processor (Kafka Streams)] --> K[Ranking Engine]
    end

    subgraph CACHE
        K --> L[R2: user‑timeline cache]   %% Tier‑2 (in‑memory per user)
        L --> M[R1: global hot‑tweet cache]
    end

    subgraph FEED REQ
        B --> N[Feed API] --> R1
        R1 -->|miss| G
    end
```

| Layer | Purpose | Key Tech |
|-------|---------|----------|
| `API Gateway` | Entry point, throttling, routing | Kong, Envoy |
| `Auth Service` | JWT validation, rate‑limit | Auth0, custom |
| `Post Service` | Accept tweet, enrich metadata | HTTP, gRPC |
| `Kafka` | Durable, ordered ingestion | 3‑node cluster |
| `Post‑Persist Service` | Persist to column store | Cassandra / Bigtable |
| `Follow Service` | Maintain follower graph | Redis (set), Dynamo / Bigtable |
| `Feed‑Processor` | Real‑time timeline builder | Kafka Streams / Flink |
| `Ranking Engine` | Score & order tweets | ML‑service (Python) |
| `Cache` | Hot‑feed & per user | Redis L1, Redis L2, CDN |
| `Feed API` | Serve client feed | gRPC/HTTP, CDN |

---

## 3.  Data Model & Storage Design

| Entity | Schema (`Post`) | Size | Notes |
|--------|-----------------|------|-------|
| `Post` | ```{id, user_id, timestamp, text, media_ids, meta}``` | ~1 KB | Column‑family `CFPosts`. Partition key = `post_id`; clustering on `timestamp`. |
| `UserMeta` | `{user_id, name, avatar, settings}` | 200 B | Relational DB (PostgreSQL) or DynamoDB. |
| `FollowerSet` | `user_id -> set<followee_id>` | 5 KB per user (10 k follows * 8 B) | Redis sets with `SADD/SREM`. |

### 3.1  Partitioning & Replication

| Table | Partitioning | Replication | Consistency |
|-------|--------------|-------------|-------------|
| `CFPosts` | **Time‑Based** – `hash(post_id)` → 128 shards; also store `user_id` as clustering column | 3‑node RF (Quorum) | Quorum reads (strong ordering for same user). |
| `FollowerSets` | Partition by `user_id` | 3‑node | Eventual (reads can hit stale set during a follow/unfollow). |
| `UserMeta` | Hash on `user_id` | 3‑node | Strong. |

### 3.2  Capacity Math

1. **Tweet write throughput**  
   *Average 1 B tweets / day* →  
   \[
   \frac{1 \times 10^9}{86\,400} ≈ 11\,574 \text{ writes/s}
   \]  
   Peak ≈ 1 M writes/s → **Scale Kafka to 3‑node + 10‑topic partitions** (≈ 400 k writes/s each).

2. **Storage**  
   *1 KB per tweet* → 1 TB per day → 365 TB per year → 3.65 PB for 10 years.  
   *Follower sets* – 10 k follows × 5 B = 5 GB per user.  
   1 B users → 5 PB of follower data.  
   **Total raw ≈ 9 PB**.  
   With RAID 6 / erasure coding (30 % overhead) → 12 PB.

3. **Cache size**  
   *Hot tweets per second* = 10 000 × 10 % = 1 000 tweets → 1 MB.  
   *User timeline (top 200)* = 200 B × 1 B users → 200 GB (Redis L2).  
   **L1 CDN** holds 1 % of feeds → 2 GB.

### 3.3  Query Patterns

| Query | Implementation |
|-------|----------------|
| `GET /feed/:user_id` | Redis L1 → Redis L2 → Cassandra (fallback). |
| `GET /post/:post_id` | Direct read from Cassandra (`getById`). |
| `GET /followers/:user_id` | Redis set membership. |
| `GET /following/:user_id` | Redis set membership. |

All reads should be **cache‑first**; if miss, fall back to database.  

---

## 4.  Ingestion & Timeline Generation

### 4.1  Push (Pre‑computed) vs. Pull

| Strategy | Pros | Cons |
|----------|------|------|
| **Push** (Fan‑out on post) | Instant visibility. No per‑request computation. | Linear fan‑out cost: 10 k followers → 10 k writes. |
| **Pull** (Read‑time merge of recent tweets) | No fan‑out; cheap on low follower count. | Heavy per‑request compute; slower. |

**Our Design** – *Hybrid*:

1. **Push** for *high‑engagement* posts or *public* users with > 1 M followers.  
2. **Pull** for normal users; use caching of a “recent‑posts” window (30 minutes).  

> **Why hybrid?** Twitter’s “reverse index” + user‑specific “timeline” table; Facebook uses *pre‑computed* “News Feed” for each user but only stores top N. We combine the advantages: push ensures freshness for large audiences, while pull keeps per‑user write cost low.

### 4.2  Timeline Pipeline

```text
Post created → Kafka -> Post Persist Service -> Post table

If followers > 1M:
    Kafka topic “push_timeline”
    Kafka Streams aggregates: foreach post, for each follower
        → Push to user’s Redis L2 key “timeline:{user_id}”

Else:
    Kafka Streams writes tweet_id to “recent:{user_id}” set (LRU size 10k)
```

*Ranking Engine* runs every 10 s, consuming `push_timeline` events:

```pseudo
score = popularity_score(tweets) + recency(tweets) + user_pref_score
insert_into_sorted_set("timeline:{user_id}", score, tweet_id)
```

The **sorted set** allows quick fetch of top‑N.

---

## 5.  Caching & Delivery

| Tier | Cache | Data stored | Eviction | Access latency |
|------|-------|-------------|----------|----------------|
| **L1 (CDN edge)** | Anycast, HTTP/2 | Full user feed (JSON) (≈ 100 KB) | TTL 5 s | 5–10 ms |
| **L2 (Redis per user)** | 8‑core node, 64 GB | Sorted set per user (top‑200 IDs) | LRU | 1–3 ms |
| **L3 (Redis global)** | 16‑core, 128 GB | Hot tweet IDs (most popular 10k) | LFU | 0.5–2 ms |

- **Feed API** reads L1 → hit: return.  
- If miss → read L2 → compose JSON by fetching tweet metadata from Cassandra (batch).  
- If L2 miss → compute from pull set, write to L2, then return.

**Cache warming**: On follower follow, push recent tweets of the followee into follower’s cache.

---

## 6.  Consistency & Ordering Guarantees

| Requirement | Solution |
|-------------|----------|
| **Tweet ordering per user** | Kafka partition key = `user_id`. Kafka guarantees order. |
| **Follow graph consistency** | Eventual: `SADD/SREM` in Redis. Reads can be stale for < 1 s. |
| **Feed freshness** | TTL on L1 = 5 s. L2 updates on new post. |
| **Idempotence** | Post service assigns unique `post_id` (UUID v4). Kafka idempotent producers. |
| **At‑least‑once** | Kafka + retries + DLQ (dead‑letter). Application dedupes via `post_id` check. |

> In practice, **strong ordering** of a user’s own tweets is mandatory; follower ordering is eventually consistent. We can achieve stronger consistency for critical paths (e.g., follower set reads) by using synchronous quorum (`SADD` with `WATCH`), at the cost of latency.

---

## 7.  Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **Kafka node crash** | Ingestion stall | 3‑node cluster + ISR > 2. Automatic fail‑over. |
| **Follower Cache miss** | Feed latency increase | Fallback to DB, increment cache miss counter. |
| **Post Persist timeout** | Duplicate posts | Retry with back‑off, dedupe via `post_id`. |
| **Follower graph partition** | Stale follow data | Periodic compaction via background job. |
| **Redis outage** | Feed cache blow‑through | Tier‑2 fallback to DB; tier‑1 CDN still works on hot feed. |
| **API rate‑limit hit** | User experience degrade | Circuit breaker; serve static older feeds from cache. |
| **Data loss** | User data loss | 3‑node RF + compaction. Off‑site snapshots weekly. |
| **Network partition** | Split‑brain for follower reads | Use read‑repair on followers, eventually converge. |
| **Denial‑of‑Service** | All traffic | WAF, rate limiting, traffic scrubbing. |

---

## 8.  Monitoring & Observability

| Metric | Description | Alert |
|--------|-------------|------|
| `api_latency_99p` | 99th percentile | > 200 ms |
| `kafka_producer_backlog` | # unacknowledged messages | > 10 k |
| `redis_cache_hit_rate` | Cache hit ratio | < 95 % |
| `followers_per_user` | Distribution | > 95th percentile > 1 M => switch to push |
| `db_replication_lag` | Seconds | > 30 s |
| `post_write_error_rate` | Error %, duplicates | > 0.1 % |

**Tools**: Prometheus + Grafana, OpenTelemetry, Jaeger (traces).  

---

## 9.  Security & Compliance

| Feature | Implementation |
|---------|----------------|
| **Authentication** | OAuth2 / JWT, introspection service. |
| **Authorization** | RBAC per endpoint, rate limits per user. |
| **DDoS Shield** | Cloudflare, Akamai or in‑house WAF. |
| **Data Encryption** | TLS for all network traffic; AES‑256 at rest (Cassandra). |
| **GDPR / CCPA** | Data retention policies (30 days for posts, 90 days for logs). |
| **Auditing** | Write‑through logs, immutable audit trail. |

---

## 10.  Capacity Planning (Year‑1)

| Service | # Nodes | CPU | RAM | Disk |
|---------|---------|-----|-----|------|
| **Kafka** | 3 | 16c | 64 GB | 5 TB (SSD) |
| **Post Persist** | 4 | 12c | 64 GB | 3 TB |
| **Follower Cache** | 6 | 8c | 128 GB | 2 TB |
| **Ranking Engine** | 2 | 16c | 64 GB | 1 TB |
| **Feed API** | 8 | 8c | 32 GB | 2 TB |
| **Auth** | 2 | 8c | 16 GB | 1 TB |
| **DB (PostgreSQL)** | 3 | 8c | 32 GB | 2 TB |
| **CDN** | Edge | N/A | N/A | N/A |

**Total**: ~ $2.3 M (hardware) + $0.6 M (cloud) per year.  

> Scaling is linear: add a node → ~ + 25 % capacity.

---

## 11.  Trade‑offs & Design Alternatives

| Decision | Reason | Opposite |
|----------|--------|----------|
| **Hybrid push/pull** | Balances latency vs. fan‑out cost. | Pure push (high storage) / Pure pull (high CPU). |
| **Cassandra** | Linear scalability, write throughput, good for time‑series. | PostgreSQL (ACID) but limited horizontal scale. |
| **Redis for follower sets** | O(1) set ops, in‑memory quick reads. | DynamoDB (stronger consistency) but higher latency. |
| **Sorted set ranking** | Allows efficient top‑N query. | B-tree index + full scan (slower). |
| **Eventual follower consistency** | Acceptable user experience; avoids blocking writes. | Strict consistency -> increased latency on follow/unfollow. |

---

## 12.  Future Improvements

* **Real‑time personalization** – Use streaming ML to adjust ranking weights per user.
* **Graph analytics** – Detect communities, influence scores.
* **Content moderation** – Real‑time flagging pipelines.
* **Feature flags** – Experiment with new feed algorithms via A/B tests.
* **Geo‑sharding** – Reduce cross‑region traffic.

---

### 13.  Take‑away

> A scalable news feed system hinges on **efficient ingestion**, **distributed storage**, **pre‑computed timelines**, and a **multi‑tier cache**. By combining a **push‑heavy** strategy for popular content with a **pull** approach for the rest, we keep write costs reasonable while guaranteeing sub‑100 ms read latency. The architecture outlined above is production‑ready yet flexible enough to grow from a few million to a billion users over time.