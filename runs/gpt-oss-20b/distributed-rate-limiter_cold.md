---

Design a distributed rate limiter to protect an API gateway.


---

---

## Distributed Rate‑Limiter for an API Gateway  
*(Self‑contained design – version 1.0 – 2026‑05‑08)*  

---  

### 1. Problem Statement  

The API gateway receives traffic from millions of end‑users.  
> *Goal:*  
- Enforce **per‑user**, **per‑IP**, and **global** request quotas.  
- Keep latency < **50 ms** (including rate‑limit decision).  
- Scale to **10 k qps** (and grow to 100 k qps if required).  
- Be tolerant to node failures, network partitions, and clock skew.  

---

### 2. Design Goals & Constraints  

| Goal | Why it matters | Metric / Target |
|------|----------------|-----------------|
| **Strong consistency** (no double‑spurious allowance) | Prevent traffic surges that defeat the quota. | Rate‑limiting result must be *>= 99.999 %* accurate compared to a single‑threaded baseline. |
| **Low latency** | Poor latency sucks users and increases churn. | < 40 ms per request (95 th percentile). |
| **Horizontal scalability** | 10 k qps is a small fraction of the expected **> 1 M qps** peak. | Add a node → ~ +30 % throughput. |
| **Minimal attack surface** | Rate limiter should *cannot* be easily spoofed. | Verify signed API key per request; only gateway can query limiter. |
| **Observability** | Operators need to detect abuse and failures quickly. | Emit per‑bucket counters, burst statistics, 5‑min rolling latency. |
| **Graceful degradation** | A cache or node outage should not crash the entire API service. | Fallback → block *and* log (or allow, as per policy). |
| **Cost‑Efficiency** | Data‑store and compute is a big part of infra cost. | 10–15 % of total infra budget. |

---

### 3. High‑Level Architecture  

```
                                         ┌───────────────────┐
                                         │   Auth/Config      │
                                         │   Service (Auth)  │
                                         └───────────────────┘
                                                   ▲
                                                   │
                                                   │
                                       ┌───────────┐└────────────┐
                                       │          │             │
                                       │          │             │
                                     ┌─▼─┐      ┌─▼─┐       ┌─▼─┐
                                     │DB│      │DB│       │DB│
                                     └─┬─┘      └─┬─┘       └─┬─┘
                                       │          │          │
                                       ▼          ▼          ▼
                          ┌───────────────────────────────────────────┐
                          │                                              
                          │   ┌─────────────────────────────┐           │
                          │   │  API Gateway (Edge)         │           │
                          │   └─────────────────────────────┘           │
                          │                 │                           │
                          │               HTTP                        │
                          │                 │                           │
                          │                 ▼                           │
                          │       ┌───────────────────────┐           │
                          │       │Rate‑Limiter LB       │           │
                          │       └───────────────────────┘           │
                          │                 │                           │
                          ▼                 ▼                           ▼     
                     ┌───────────────────────────────────────────────┐
                     │                                             │
                     │  ┌───────────────────────┐  ┌───────────────────────┐
                     │  │ Rate‑Limiter Node 1   │  │ Rate‑Limiter Node 2   │
                     │  └───────────────────────┘  └───────────────────────┘
                     │                 ▲                                 ▲
                     │                 │                                 │
                     │                 ▼                                 ▼
                     │       ┌────────────────────────────────────┐
                     │       │    Redis Cluster (sharded, HA)    │           ←••• Shared State
                     │       └────────────────────────────────────┘
                     │                ▲           ▲           ▲
                     │                │           │           │
                     │                │           │           │
                     └────────────────┘           └───────────┘
```

*Key Flow*

1. Client → **API Gateway** (TCP/TLS).  
2. Gateway enriches request with **API key**, **IP**, **timestamp**.  
3. Gateway forwards only the **rate‑limit token** to the **Rate‑Limiter Load‑Balancer** (LB).  
4. LB routes to an **RL Node** (consistent hashing on the key).  
5. RL Node runs a **Lua script** against **Redis Cluster** to atomically update/inspect the bucket.  
6. Result (`allow` / `deny`) is returned to gateway, which emits either `200` or `429`.  
7. Optional **fallback** path is invoked if RL node or Redis is unreachable.

---

### 4. Data Model & Algorithm  

| Key | Type | Dataset | Notes |
|-----|------|---------|-------|
| `rl_user:{api_key}` | `hash` | `tokens`, `last_ts` | per‑user bucket. |
| `rl_ip:{ip}` | `hash` | `tokens`, `last_ts` | per‑IP bucket. |
| `rl_global` | `hash` | `tokens`, `last_ts` | single global bucket. |
| `rl_limits:{api_key}` | `hash` | `rate` (req/s), `burst` | per‑user plan (can be cached). |

**Token‑Bucket parameters**

- `rate = request_limit / window` – tokens added per second.  
- `burst = max(1, window × 2)` – max tokens an empty bucket can hold (allow bursts).  

Example:  
- *Per‑user*: 120 req/min → `rate = 120/60 = 2 tps`, `burst = 240`.  
- *Per‑IP*: 300 req/min → `rate = 5 tps`, `burst = 600`.  
- *Global*: 10 k req/s → `rate = 10 k`, `burst = 20 k` (to absorb short spikes).

**Lua Script** (pseudocode, see `rate_limit.lua` below)

```lua
-- KEYS[1] = bucket key
-- ARGV[1] = rate (tokens per ms)
-- ARGV[2] = burst (max tokens)
-- ARGV[3] = key TTL in seconds

local key   = KEYS[1]
local rate_ms = tonumber(ARGV[1])                -- e.g., 0.002 tokens per ms
local burst = tonumber(ARGV[2])                  -- e.g., 240
local ttl    = tonumber(ARGV[3])                  -- e.g., 3600

-- Get current time from Redis (avoids clock skew)
local now = redis.call('TIME')
local now_ms = now[1] * 1000 + math.floor(now[2] / 1000)

-- Load or init bucket
local bucket = redis.call('HMGET', key, 'tokens', 'last_ts')
local tokens = tonumber(bucket[1]) or burst
local last_ts = tonumber(bucket[2]) or now_ms

-- Refill
local elapsed_ms = now_ms - last_ts
local added = elapsed_ms * rate_ms
tokens = math.min(burst, tokens + added)

-- Consume
local allow = 0
if tokens >= 1 then
  tokens = tokens - 1
  allow = 1
end

-- Persist bucket
redis.call('HMSET', key, 'tokens', tokens, 'last_ts', now_ms)
redis.call('EXPIRE', key, ttl)
return allow
```

*Why Redis?*  
- Atomic Lua execution → safe race‑free counter update without round‑trip.  
- Clustered – sharded data spreads load & offers HA.  
- Supports TTL → stale keys auto‑purge.  

*Why Token‑Bucket?*  
- Smooth traffic (eventually stable).  
- Allows short bursts (10× 1 s burst policy for a 60 s minute quota).  
- Considered standard for API gating.

---

### 5. Implementation Details  

#### 5.1 Redis Cluster  

| Feature | Configuration | Reason |
|---------|---------------|--------|
| **3‑node cluster** (primary+2 replicas) | Each node 8 CPU, 32 GB RAM | 10 k qps → < 5 k ops/s per node → well under wire 10 k ops/s |
| **Slot partitioning** | 16 K slots | Even data distribution; consistent hashing ensures same bucket → same node |
| **Persisted to SSD** | Append‑only log + RDB snapshots | Fast crash recovery (less than 1 s) |
| **High‑availability** | Automatic failover | Node failure → nearest replica takes over, no request loss. |

#### 5.2 Rate‑Limiter Node  

- Runs as a stateless micro‑service behind a simple LB.  
- Exposes a **REST** endpoint (`/rl`) that accepts `{api_key, ip, maybe plan_id}` and forwards to the script.  
- Uses **async I/O** (e.g., NodeJS, Go‑net/http, or Rust async) to avoid blocking on Redis.  
- Caches `rl_limits:{api_key}` using in‑memory LRU (TTL 10 min) to reduce look‑ups.

#### 5.3 Load Balancer  

- **Consistent‑hashing** on the *api_key* (or `ip` for IP‑based buckets).  
- Guarantees that successive requests from the same client hit the same RL Node, eliminating cross‑node race conditions on the same bucket.  

#### 5.4 Pipeline & Back‑pressure  

- Node keeps **async pipelines**: a request triggers a single TCP call to Redis – no two-stage ACK.  
- In case of a spike beyond 10 k×→ 30 k writes per second, nodes off‑load load to a second Redis shard or an eventual‑consistency store for *non‑critical* burst control.  

---

### 6. Capacity Planning & Math  

| Item | Value | Rationale |
|------|-------|-----------|
| **Target qps** | 10 k | Minimum requirement |
| **Avg WM request per qps** | 1 | Each request performs 1 Lua script |
| **Redis ops/s** | 10 k * 3 (HMGET, HMSET, EXPIRE) ≈ 30 k | 3 commands inside Lua but counted as 3 ops for replication |
| **Node ops/s** | 30 k ÷ 3 = 10 k | 3 nodes ⇒ 10 k ops/s each |
| **Peak per‑user active fraction** | 1 % | 1 M users, 10k active at any time |
| **Keys in cluster** | 10k (active) + standby (1 % of 1M) ~ 10,100 | 1 M? bigger reservation 1.5 M |
| **Memory per key** | 64 B | hash + overhead |
| **Total Redis memory** | 10k keys × 64 B ≈ 0.64 MiB (active) < 1 MiB; reserve 10 MiB for 1 M keys during flash crowds |
| **CPU Latency** | ~10 µs per key | Script runs in memory |
| **Network RTT** | 1–2 ms (latency to Redis) | Node→cluster across same datacenter |

> **Summary:**  
> Even with a **root‑level** config of **10 k qps** and **1 M** possible keys, one 3‑node Redis cluster with 8 core‑8 Gb‑RAM nodes comfortably satisfies the math. Scaling to 100 k qps only requires adding 2‑3 more Redis nodes **and** scaling the RL nodes linearly (4–5 RL nodes).

---

### 7. Failure Modes & Mitigation  

| Failure | Impact | Mitigation | Notes |
|---------|--------|------------|-------|
| **Redis partition** | All requests blocked or some may pass unbounded | *Fail‑fast* policy → deny (429) if Redis times‑out | Guarantees limits are not broken; may temporarily raise error rates. |
| **Redis node crash** | Temporary unavailability of 1/3 the data | Automatic failover to replica ⇒ traffic continues | Some old values may not be updated but replaced by replica on re‑join. |
| **Network latency spikes** | 429 on *time‑outs* | Circuit‑breaker in RL node → if RTT > 10 ms → reject request or fallback | Prevent waiting on slow cluster. |
| **Clock drift** | Tokens calculated wrong | Use Redis server time via `TIME` in Lua script | Eliminates reliance on local OS clocks. |
| **Rapid key churn (burst of new users)** | Redis memory bloom → OOM | Rolling TTL (3600 s) + redis eviction strategy `maxmemory-policy` = `allkeys-lru` | Evict least recently used keys automatically. |
| **Denial‑of‑Service to RL cluster (API‑key flood)** | Aggressive rate limits hit | Global bucket super‑caps (10 k req/s) + per‑key limits → traffic halts gracefully; TTL ensures not staying banned. | Add an additional safeguard like IP anomaly detection or client certificates. |
| **Configuration drift** | Inconsistent quota per user | Versioned config in Auth Service + cache invalidation on change | Auth service writes to a pub/sub topic; RL nodes subscribe & flush cache. |
| **UV de‑dup** | Same request delivered twice → double token consumed | Idempotent markers optional (e.g., API key + request id) – out of scope for core RL. | Might add duplicate detection if needed. |

---

### 8. Observability & Metrics  

| Metric | Collection Point | Sampling | Purpose |
|--------|------------------|----------|---------|
| `rate_limit_allow_total` | RL Node | Counter | Traffic routed & allowed |
| `rate_limit_deny_total` | RL Node | Counter | Violations |
| `rate_limit_bucket_tokens` | Redis | Gauge (via `INFO` & `SLOWLOG`) | Bucket health |
| `rate_limit_latency_ms` | RL Node | Histogram | Enforce SLA |
| `redis_op_latency_ms` | RL Node | Histogram | Cluster performance |
| `redis_conn_failures` | RL Node | Counter | Detect HA issues |
| `api_gateway_429s` | Gateway | Counter | End‑to‑end metric |

**Alerting**

- `rate_limit_deny_total / rate_limit_allow_total > 0.1` → likely abuse.  
- `redis_conn_failures > 5` in 1 min → cluster down.  
- `rate_limit_latency_ms 99th > 30 ms` → performance degradation.

---

### 9. Trade‑offs & Alternatives  

| Decision | Alternative | Cost | Latency | Consistency | Notes |
|----------|-------------|------|---------|-------------|-------|
| **Token Bucket (Redis Lua)** | **Leaky Bucket** | Similar | Slightly higher CPU | Strong | Allows prettier burst shaping. |
| | **Sliding Window Log** | Larger storage & CPU | Significant | Strong | Gives precise per‑second spread (no bursts). |
| **Centralized state (Redis)** | **Local in‑memory cells** | Reduces network cost | Lowest | Eventual | Inconsistent on node failover; hard to balance bursts. |
| | **Distributed consensus** (etcd/RAFT) | Highest | Highest | Strong | Overkill for per‑request counters. |
| **Lua script** | **Atomic increments via `INCRBYFLOAT`** | Cheaper | Slightly higher | Risk of race | Harder to implement generic bucket logic. |
| **Redis Cluster** | **Sharded Memcached** | Lower cost | Lower | Eventual | Memcached can't guarantee atomic multi‑field writes. |
| | **Relational DB** | High latency | High | Strong | Not suitable for request‑level atomicity. |
| **Fallback policy** | **Allow** instead of block | QoS improves | None | Risk of quota hit | Possibly mis‑rated. |
| | **Block** (current design) | Strict quota enforcement | Engineering overhead | Reasonable | Safer but leads to 429 traffic. |

**Why we stay with Redis + Token‑Bucket + Lua:**

- **Atomicity & speed** – Lua executes inside the server without round‑trips.  
- **Memory efficiency** – 64 B per key easily scales.  
- **Simple math & quotas** – We can change per‑user limits on the fly.  
- **Observability** – Redis exposes per‑key stats; RL nodes can instrument.  

---

### 10. Extension Ideas  

1. **Adaptive limits** – Dynamically tighten per‑IP quotas during a traffic spike.  
2. **User‑tier backed limits** – Store tier config in a fast key‑value (e.g., Consul).  
3. **Composite buckets** – Combine user & IP scopes in a single key to avoid double‑charge.  
4. **Worker‑based replenishment** – Periodic background tasks that pre‑fill buckets for heavy users.  
5. **Dynamic key TTL** – Longer TTL for known heavy hitters to reduce cluster churn.  

---

### 11. Mermaid Diagram  

```mermaid
graph TD
  %% Clients
  A[Client A] -->|HTTPS| B[API Gateway]
  B --> C[Rate‑Limiter LB]
  %% Inbound throttling
  C --> L1[RL Node 1]
  C --> L2[RL Node 2]
  C --> L3[RL Node 3]
  %% Redis cluster
  L1 -->|Lua Script| R1[Redis Node 1]
  L2 -->|Lua Script| R2[Redis Node 2]
  L3 -->|Lua Script| R3[Redis Node 3]
  %% Replication
  R1 --replica--> R2
  R2 --replica--> R3
  R3 --replica--> R1
  %% Auth / Config
  B --> D[Auth Service]      %% Issues API keys
  D --> E[Config Store]      %% Per‑user limits
  %% Metrics
  L1 --> F[Prometheus Exporter]:::metrics
  L2 --> F
  L3 --> F
  R1 --> F
  R2 --> F
  R3 --> F
  classDef metrics fill:#f4f4f4,stroke:#333,stroke-width:2px;
```

*Legend:*  
- **API Gateway** validates client credentials and forwards quota data to the Rate‑Limiter;  
- **RL Nodes** enforce limits via Redis atomic Lua;  
- **Redis cluster** is an HA 3‑node ring;  
- **Auth / Config** service supplies per‑user plan and bucket params;  
- All nodes expose metrics for **Prometheus**.

---

## Summary

By combining a centrally managed Redis cluster, token‑bucket logic implemented with fast Lua scripts, and a stateless, consistent‑hash‑based set of rate‑limiter nodes, we get:

- **< 40 ms** request latency  
- **≥ 10 k qps** throughput (scalable to > 100 k qps)  
- **Strong quota enforcement** (atomic per‑request)  
- **Graceful degradation** (block on failure)  
- **Observability** (end‑to‑end metrics & alerts)  

The design is intentionally simple yet robust against the common failure modes of distributed systems, while remaining amenable to future feature expansion such as dynamic tier adjustments or composite quotas.