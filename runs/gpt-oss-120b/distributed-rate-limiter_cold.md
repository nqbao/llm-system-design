---

Design a distributed rate limiter to protect an API gateway.


---

## Distributed Rate Limiter for an API Gateway  
**Target audience:** senior engineers, site‑reliability engineers, and product owners who need a concrete design that can be built, deployed and operated today.  

---

## 1. Problem Statement

> Protect a public‑facing API gateway from abusive traffic while keeping latency low and guaranteeing that legitimate clients experience no throttling beyond the configured limits.

**Typical limits to support**

| Dimension                     | Example values |
|------------------------------|-----------------|
| Global traffic (all clients) | ≤ 1 M RPS (requests per second) |
| Per‑API‑key (or per‑user)    | 10 RPS steady, 100 RPS burst (10 s) |
| Per‑IP (optional)            | 5 RPS steady |
| Global “system‑wide” limit   | 10 M RPS for all downstream services (protect them) |

The limiter must work **across many gateway instances** (horizontal scale‑out) and survive node failures, network partitions, and operational upgrades without losing correctness.

---

## 2. Functional Requirements (FR)

| # | Requirement |
|---|-------------|
| FR‑1 | Enforce **steady‑state** and **burst** limits per configurable identifier (API key, user‑id, IP, endpoint). |
| FR‑2 | Return HTTP 429 (Too Many Requests) with `Retry‑After` header when limit is exceeded. |
| FR‑3 | Provide a **real‑time** “used‑quota / remaining‑quota” API for dashboards. |
| FR‑4 | Support **dynamic policy changes** (add / modify / delete limits) without service restart. |
| FR‑5 | Operate with **sub‑millisecond latency** (≤ 2 ms added per request). |
| FR‑6 | Emit metrics: request count, throttle count, latency, error rates. |
| FR‑7 | Allow **graceful degradation** (fail‑open or fail‑closed) on datastore outage. |

---

## 3. Non‑Functional Requirements (NFR)

| # | Requirement |
|---|-------------|
| NFR‑1 | **Scalability** – handle > 1 M RPS globally. |
| NFR‑2 | **High availability** – ≥ 99.99 % uptime, no single point of failure. |
| NFR‑3 | **Strong consistency** for per‑client quotas (no over‑issuance). |
| NFR‑4 | **Low operational cost** – ≤ 100 GB RAM for the rate‑limit store in most deployments. |
| NFR‑5 | **Observability** – logs, metrics, traces. |
| NFR‑6 | **Security** – TLS, auth between components, ACL on data store. |

---

## 4. High‑Level Architecture Overview

```
+-------------------+       +-------------------+      +-------------------+
|   Client (Internet) ---> |   Load Balancer   | ---> |   API Gateway X   |
+-------------------+       +-------------------+      +-------------------+
                                           |            |
                                           | gRPC/HTTP   | (per‑request)
                                           v            v
                                    +-------------------+-------------------+
                                    |   Distributed Rate‑Limiter Service (RLS) |
                                    +-------------------+-------------------+
                                            |   |
                                            |   | (sharded)
                                            v   v
                                   +-------------------+  +-------------------+
                                   |   Redis Cluster   |  |  Config Store (etcd) |
                                   +-------------------+  +-------------------+

```

* **API Gateway** – e.g., Envoy, Kong, or a custom NGINX‑plus‑Lua. It forwards each request to the **Rate‑Limiter Service (RLS)** before routing to the backend.  
* **Rate‑Limiter Service** – stateless gRPC/HTTP microservice that implements the token‑bucket algorithm atomically using Redis Lua scripts.  
* **Redis Cluster** – the authoritative store of token‑buckets, sharded by identifier (consistent hashing). Replicated × 2 for HA.  
* **Config Store** – holds per‑identifier limit policies (size, refill rate, TTL) and is watched by RLS for hot‑reloading.  

---

## 5. Core Algorithm: Token Bucket (Burst‑aware)

* **Bucket size** = `burst_seconds × steady_rate`.  
* **Refill rate** = `steady_rate` tokens per second (continuous).  
* **Operation per request** (atomic Lua script):
  1. Fetch bucket state (`tokens`, `last_refill_ts`).  
  2. Compute elapsed time `Δt`.  
  3. `tokens = min(bucket_size, tokens + Δt * refill_rate)`.  
  4. If `tokens >= 1` → `tokens -= 1` and **allow**.  
  5. Else → **reject** (return remaining TTL for `Retry‑After`).  

The script runs in a single Redis call ⇒ **strong consistency** without distributed locks.

### Lua Script (simplified)

```lua
-- KEYS[1] = bucket key
-- ARGV[1] = bucket_size
-- ARGV[2] = refill_rate (tokens per ms)
-- ARGV[3] = current_time_ms

local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(bucket[1]) or ARGV[1]   -- missing key => full bucket
local last_ts = tonumber(bucket[2]) or ARGV[3]

local elapsed = ARGV[3] - last_ts
tokens = math.min(tonumber(ARGV[1]), tokens + elapsed * tonumber(ARGV[2]))

if tokens >= 1 then
    tokens = tokens - 1
    redis.call('HMSET', KEYS[1], 'tokens', tokens, 'ts', ARGV[3])
    redis.call('PEXPIRE', KEYS[1], 2 * 60 * 1000)   -- keep key alive 2min
    return {1, tokens}
else
    redis.call('HMSET', KEYS[1], 'tokens', tokens, 'ts', ARGV[3])
    redis.call('PEXPIRE', KEYS[1], 2 * 60 * 1000)
    return {0, tokens}
end
```

*All “tokens” are stored as floating point numbers to allow sub‑second refill granularity.*

---

## 6. Data Model

| Redis Key Pattern | Fields | Description |
|-------------------|--------|-------------|
| `rate:{policy_id}:{client_id}` | `tokens` (float), `ts` (epoch‑ms) | Current bucket state. |
| `policy:{policy_id}` | `bucket_size`, `refill_rate_per_ms`, `burst_seconds` | Immutable policy (cached locally by RLS). |
| `policy_meta` (Hash) | `version` | Incremented on any policy change (used for hot‑reload). |

*Key TTL*: 2 min (or `burst_seconds + safety_margin`). TTL guarantees that unused keys are evicted, keeping memory bounded.

---

## 7. Capacity Planning & Math

### 7.1 Traffic Assumptions

| Metric                              | Value |
|------------------------------------|-------|
| Global peak traffic                 | 1 M RPS |
| Avg. requests per API‑gateway node  | 100 k RPS (10 nodes) |
| Requests per second per Redis shard | 250 k RPS (4 shards) |
| Average request size (to Redis)    | 36 B (key) + 24 B (args) ≈ 60 B |
| Redis response size                | 20 B |
| Network round‑trip latency (in‑datacenter) | ≤ 0.3 ms |

**Redis bandwidth per shard**

```
(60 B request + 20 B response) * 250,000 RPS = 20 MB/s ≈ 160 Mbit/s
```

Even a modest 1 GbE NIC per shard can handle > 5× this load, leaving headroom for spikes.

### 7.2 Memory Footprint

Assume **per‑client** limits (worst‑case 10 M active client identifiers).  

*Per key data* (≈ 24 B overhead + 2 fields × 8 B) ≈ 40 B  

```
10,000,000 keys * 40 B ≈ 400 MB
```

Add Redis internal hash table overhead (≈ 30 %) → **≈ 520 MB**.  
A 4‑shard cluster with 512 MB per node is well inside a 2 GB memory allocation (typical Redis VM size).  

**Conclusion:** 10 M concurrent limit scopes comfortably fits a 4‑shard Redis cluster with 8 GB total RAM (provides safety margin and room for other data structures).

### 7.3 CPU Load

Each request triggers a Lua script (~ 1 µs CPU in Redis).  

```
250,000 RPS * 1 µs = 0.25 CPU‑seconds per second = 0.25 cores
```

Add overhead for networking, persistence, background tasks → **~ 1 vCPU per shard**. A 2‑vCPU instance per shard is more than sufficient.

### 7.4 Latency Budget

| Component                      | Expected latency |
|-------------------------------|------------------|
| Gateway → RLS (gRPC)          | 0.2 ms (in‑datacenter) |
| RLS → Redis (single command)  | 0.3 ms (client library + network) |
| Redis Lua execution            | 0.1 ms |
| Total added per request        | **≈ 0.6 ms** (well under 2 ms budget) |

---

## 8. Failure Modes & Mitigations

| Failure | Impact | Mitigation |
|---------|--------|------------|
| **Redis shard crash** | All requests for IDs mapped to that shard are blocked (or may over‑issue). | • Replication × 2 with automatic fail‑over (Redis Sentinel / Cluster).<br>• RLS retries with exponential back‑off; if both replicas unavailable, **fail‑closed** (return 429) or **fail‑open** based on policy. |
| **Network partition between RLS and Redis** | Requests cannot be accounted → possible over‑issuance. | • Detect timeout (> 5 ms) → switch to *local bucket* with conservative limits (e.g., 10 % of steady rate).<br>• Send a heartbeat to monitoring; alert on partition. |
| **Hot key (single client spikes)** | One client consumes most Redis QPS → tail latency for others. | • Use **consistent hashing** + **virtual nodes** to distribute keys evenly.<br>• Apply **client‑side leaky bucket** fallback after N consecutive throttles for that client (rate‑limit locally). |
| **Policy update race** | New limits not applied instantly, causing either over‑ or under‑throttling. | • Policy changes stored in Etcd versioned key.<br>• RLS watches version; on change it atomically updates in‑memory cache and **resets bucket_size** for affected keys on next request. |
| **Redis persistence lag (AOF or RDB)** | In case of crash, last‑second state may be lost → over‑issuance on restart. | • Use **AOF with every‑second fsync** (safe‑sync) for production; optionally **replicate** to a second cluster for disaster recovery. |
| **Clock skew** between API gateway and Redis | Tokens may be refilled incorrectly. | • Use Redis server time (`TIME` command) inside Lua script; avoids client clock reliance. |
| **RLS process crash** | No rate‑limit evaluation → either all pass (fail‑open) or all block (fail‑closed). | • Deploy under a supervisor (systemd/K8s) with **readiness probe**; traffic is sent only to healthy pods. |
| **Exhausted connection pool** | Requests queue up → high latency. | • Size pool based on QPS (e.g., `max_connections = 2 * concurrent_requests / expected_rtt`).<br>• Enable **connection pooling** in the RLS client library. |

---

## 9. Trade‑off Discussion

| Design Choice | Pros | Cons | When to pick |
|---------------|------|------|--------------|
| **Centralized Redis bucket** (single atomic Lua) | *Strong consistency* – no over‑issuance; simple implementation. | Adds **network hop** per request; Redis becomes a potential hotspot. | Traffic ≤ 5 M RPS, need strict quota enforcement. |
| **Local token bucket + periodic sync** | Near‑zero added latency; reduces Redis load. | *Eventual consistency* – may allow brief bursts beyond limit; requires background drift correction. | Very high traffic (≥ 10 M RPS) where sub‑ms added latency is crucial and small over‑issuance is acceptable. |
| **Fixed‑window counter** | Simpler; can be implemented with Redis `INCR` & `EXPIRE`. | “Thundering herd” at window boundary; less smooth throttling. | Use for non‑critical limits (e.g., analytics throttling). |
| **Sliding‑window with sorted set** | Precise per‑second granularity; no burst “reset” effect. | Higher memory + CPU (zset O(log N)). | Needed for *fairness* across many short‑lived clients. |
| **Redis Cluster vs. Sharded Standalone** | Cluster provides automatic sharding & HA. | More complex ops, cross‑slot scripting restrictions. | Production with > 2 M keys; otherwise a single large Redis with replication may suffice. |
| **Fail‑closed vs. Fail‑open on Redis outage** | *Fail‑closed* protects downstream services, prevents abuse during outage. | May block legitimate traffic, hurting SLA. | When downstream services are capacity‑constrained (e.g., costly DB). |
| | *Fail‑open* preserves availability but can allow abuse. | Risks overload during outage. | When availability > availability, e.g., public static assets. |

---

## 10. Detailed Component Design

### 10.1 API Gateway Integration

| Option | Implementation |
|--------|----------------|
| **Envoy Rate Limit Service (RLS)** | Configure `rate_limit_service` filter → calls our `rls` gRPC for each request. Envoy passes a **descriptor** (policy_id, client_id). |
| **Kong Plugin (Lua)** | Calls Redis directly via `lua-resty-redis` & embedded Lua script.<br>Pros: one‑hop; cons: limited scaling of Kong workers. |
| **Custom NGINX+Lua** | Same as Kong, but embedded in existing stack. |

**Chosen approach:** **Envoy + external RLS** (language‑agnostic, decouples scaling, easier to instrument).  

**Flow per request**

1. Envoy extracts identifiers (e.g., `X-API-Key`, client IP, endpoint) → builds descriptor.  
2. Envoy calls RLS via gRPC (`RateLimitService.Check`).  
3. RLS runs the Lua script on Redis.  
4. RLS returns `OK` or `OVER_LIMIT` + `Retry-After`.  
5. Envoy either forwards the request or returns 429.

### 10.2 Rate‑Limiter Service (RLS)

* **Language**: Go (fast, good concurrency, native gRPC).  
* **Concurrency model**: one goroutine per request, uses a **bounded connection pool** to Redis (e.g., `github.com/go-redis/redis/v9`).  
* **Hot‑reload**: watches Etcd for `policy_meta/version`; when it changes, reloads all policies into an in‑memory map (`sync.RWMutex`).  
* **Observability**:  
  * **Prometheus** counters: `rls_requests_total`, `rls_allowed_total`, `rls_throttled_total`.  
  * **Histograms**: request latency, Redis latency.  
  * **Tracing**: OpenTelemetry spans that include Redis command metadata.  

#### Sample gRPC contract (simplified)

```proto
service RateLimiter {
  rpc Check (CheckRequest) returns (CheckResponse);
}
message CheckRequest {
  string policy_id = 1;      // e.g., "api_key"
  string client_id = 2;      // e.g., API key value
  string endpoint   = 3;     // optional
}
message CheckResponse {
  enum Decision { ALLOWED = 0; OVER_LIMIT = 1; }
  Decision decision = 1;
  int32 retry_after_seconds = 2; // when OVER_LIMIT
}
```

### 10.3 Redis Cluster

* **Cluster size**: 4 primary shards (each with 1 replica).  
* **Key slot allocation**: use `hash_tag` `{client_id}` to force all buckets for a client onto the same shard.  
* **Persistence**: AOF (`appendonly yes`) with `fsync everysec`.  
* **Memory policy**: `maxmemory-policy allkeys-lru` (evicts least‑recently‑used bucket keys when approaching limit).  
* **Security**: TLS (`stunnel` or built‑in TLS), ACL per RLS service (`user rls on >password ~rate:* +@all`).

### 10.4 Config Store (Etcd)

* Stores **policy objects**:

```json
{
  "policy_id": "api_key",
  "steady_rate_rps": 10,
  "burst_seconds": 10,
  "enabled": true
}
```

* **Versioned key**: `/rate_limiter/policy_meta/version` – an integer increased on any change.  
* RLS watches this key; on change it reloads policies atomically.

---

## 11. Scaling Strategy

1. **Horizontal scaling of API Gateways** – add more instances behind the load balancer; each instance contacts the same RLS cluster (or a local RLS replica).  
2. **RLS auto‑scales** – Kubernetes Horizontal Pod Autoscaler (HPA) based on CPU and request latency (`rls_request_latency_seconds`).  
3. **Redis sharding** – increase shards when `keys_per_shard` > 5 M (memory or QPS). Adding a node triggers rebalancing; key migration is handled by Redis Cluster metadata.  
4. **Burst handling** – keep a **small per‑node cache** of tokens for the hottest keys (top 0.1 % of traffic) using an in‑process LRU; updates are flushed to Redis every 50 ms, reducing round‑trips.  

---

## 12. Observability & Alerting

| Metric (Prometheus) | Alert Threshold |
|---------------------|-----------------|
| `rls_requests_total` (rate) | > 5 M RPS (unexpected traffic) |
| `rls_throttled_total` (rate) | > 0.5 % of total requests (possible abuse) |
| `redis_latency_seconds` (p99) | > 2 ms |
| `redis_cluster_down_nodes` | > 0 |
| `rls_error_total` (rate) | > 0.1 % (internal errors) |
| `gateway_429_total` (rate) | spikes > 10× baseline |

Logs include request ID, policy ID, decision and bucket state (debug level). All logs and metrics are shipped to a centralized **ELK/EFK** stack and **Grafana** dashboards.

---

## 13. Security Considerations

* **Transport security** – Envoy ↔ RLS (mTLS), RLS ↔ Redis (TLS + client certificate).  
* **Authentication** – RLS authenticates to Redis via ACL users; Envoy authenticates clients via JWT/API keys before hitting RLS.  
* **Denial‑of‑service protection** – rate‑limit *pre‑RLS* at the edge (e.g., CDN, WAF) to avoid flooding the RLS itself.  
* **Data privacy** – bucket keys contain only opaque identifiers (hashed API keys) to avoid leaking personally identifiable information (PII).  
* **Audit** – every policy change is logged in Etcd with `who` and `when`.

---

## 14. Deployment Blueprint (Kubernetes)

```yaml
# Redis Cluster (4 sharded masters + 1 replica each)
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: redis-cluster
spec:
  serviceName: redis-headless
  replicas: 8        # 4 masters + 4 replicas
  selector:
    matchLabels:
      app: redis
  template:
    metadata:
      labels:
        app: redis
    spec:
      containers:
      - name: redis
        image: redis:7.2-alpine
        command: ["redis-server", "/etc/redis/redis.conf"]
        ports:
        - containerPort: 6379
        volumeMounts:
        - name: config
          mountPath: /etc/redis
        - name: data
          mountPath: /data
      volumes:
      - name: config
        configMap:
          name: redis-config
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 10Gi
---
# Rate Limiter Service
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rls
spec:
  replicas: 6
  selector:
    matchLabels:
      app: rls
  template:
    metadata:
      labels:
        app: rls
    spec:
      containers:
      - name: rls
        image: myorg/rls:latest
        ports:
        - containerPort: 9090   # gRPC
        env:
        - name: REDIS_SENTINEL_ADDR
          value: "redis-sentinel:26379"
        - name: ETCD_ENDPOINTS
          value: "etcd-0.etcd:2379,etcd-1.etcd:2379"
        resources:
          limits:
            cpu: "1"
            memory: "512Mi"
          requests:
            cpu: "500m"
            memory: "256Mi"
        readinessProbe:
          grpc:
            port: 9090
          initialDelaySeconds: 5
          periodSeconds: 10
---
# Envoy (gateway) sidecar uses the RLS service via Kubernetes DNS:
# rate_limit_service:
#   grpc_service:
#     envoy_grpc:
#       cluster_name: rls_cluster
#   timeout: 0.5s
```

*Network policies* restrict traffic so only the gateway namespace can talk to the `rls` service, and only `rls` can reach Redis.

---

## 15. Summary Checklist

- **Correctness** – token bucket executed atomically in Redis Lua → no over‑issuance.  
- **Scalability** – 1 M RPS with 4‑shard Redis cluster, 6 RLS pods, 10 gateway nodes, each ≤ 0.6 ms added latency.  
- **HA** – Redis replication, RLS pod restarts, Etcd watch for policy reload.  
- **Observability** – Prometheus metrics, OpenTelemetry traces, Grafana dashboards.  
- **Security** – TLS everywhere, ACLs, hashed identifiers.  
- **Operational simplicity** – all state lives in Redis (no custom sharding logic), policies in Etcd (single source of truth).  

---

## 16. Mermaid Diagram

```mermaid
flowchart TD
    subgraph ClientNet[Internet / Clients]
        C1[Client]
        C2[Client]
        C3[Client]
    end

    subgraph LB[Load Balancer]
        LB1[LB]
    end

    subgraph GW[API Gateway (Envoy) × N]
        GW1[Envoy Instance 1]
        GW2[Envoy Instance 2]
        GW3[Envoy Instance 3]
    end

    subgraph RLS[Rate‑Limiter Service (RLS) × M]
        RLS1[rls pod 1]
        RLS2[rls pod 2]
        RLS3[rls pod 3]
    end

    subgraph RedisCluster[Redis Cluster (4 shards × 2 replicas)]
        R1[Shard‑1 Master]
        R1R[Shard‑1 Replica]
        R2[Shard‑2 Master]
        R2R[Shard‑2 Replica]
        R3[Shard‑3 Master]
        R3R[Shard‑3 Replica]
        R4[Shard‑4 Master]
        R4R[Shard‑4 Replica]
    end

    subgraph Config[Etcd Config Store]
        ETCD[etcd]
    end

    C1 --> LB1
    C2 --> LB1
    C3 --> LB1

    LB1 --> GW1
    LB1 --> GW2
    LB1 --> GW3

    GW1 -->|gRPC Check| RLS1
    GW2 -->|gRPC Check| RLS2
    GW3 -->|gRPC Check| RLS3

    RLS1 -->|Lua script| R1
    RLS1 -->|Lua script| R2
    RLS1 -->|Lua script| R3
    RLS1 -->|Lua script| R4

    RLS2 -->|Lua script| R1
    RLS2 -->|Lua script| R2
    RLS2 -->|Lua script| R3
    RLS2 -->|Lua script| R4

    RLS3 -->|Lua script| R1
    RLS3 -->|Lua script| R2
    RLS3 -->|Lua script| R3
    RLS3 -->|Lua script| R4

    R1  <--> R1R
    R2  <--> R2R
    R3  <--> R3R
    R4  <--> R4R

    RLS1 -->|watch| ETCD
    RLS2 -->|watch| ETCD
    RLS3 -->|watch| ETCD
```

*Arrows represent network calls; RLS ↔ Redis is a single Redis command containing the Lua script. The diagram captures the flow from client request → rate‑limit check → token bucket update → final decision.*

---

**Next steps for implementation**

1. Prototype the Lua script and benchmark against a 4‑shard Redis cluster (use `redis-benchmark`).  
2. Build the Go gRPC RLS service (include health checks, metrics).  
3. Deploy a minimal Envoy+RLS stack in a dev cluster, drive traffic with `hey` or `wrk2` to validate latency & throughput.  
4. Iterate on scaling (add shards, introduce local cache for hot keys).  
5. Harden security and set up alerting before moving to production.  

Feel free to ask for deeper dive into any component (e.g., Redis Lua optimization, Envoy config, or Kubernetes rollout strategy).