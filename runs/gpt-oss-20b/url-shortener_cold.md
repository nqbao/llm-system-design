---

Design a URL shortener like bit.ly.


---

## 1. Problem Statement & Requirements  

| **Feature** | **What it means** | **Objective** |
|-------------|-------------------|----------------|
| **Short URL Creation** | POST `/shorten` – accept a long URL (≤ 2 KB) and return a < 10‑char code | Fast, low‑latency creation (< 50 ms) |
| **Redirection** | GET `https://tiny.io/<code>` – respond with a 301/302 to the original URL | Sub‑millisecond redirect (~5 ms on edge) |
| **Analytics** | GET `/stats/<code>` – return click count, referrers, geolocation | Provide accurate stats with 1‑minute latency |
| **Quota / Rate‑Limiting** | 10 k RPS per IP for creation, 50 k RPS per domain per day | Protect backend, prevent spam |
| **Scalability** | ≥ 10 M read RPS, ≥ 1 k write RPS per region, service must grow 5 %/day | Handle large global traffic |
| **Availability** | 99.999% uptime (5 nines) | Users get redirects even in AZ failures |
| **Security** | Prevent malicious URLs, abuse, click fraud, DDoS | Keep service reputation and compliance |
| **Data Model** | Permanent mapping of `code → {original_url, created_at, stats}` | 10 B records over 3 yrs ≈ 2 TB raw data |

---

## 2. Constraints & Real‑World Limits  

| **Constraint** | **Impact** |
|---------------|------------|
| **Key space** | 62 chars (A–Z, a–z, 0–9). 6‑char keys ⇒ 62⁶ ≈ 56.8 B unique codes.|
| **DB performance** | Each region can support ≈ 10 k writes/sec & > 10 M reads/sec with a distributed NoSQL (Cassandra / DynamoDB).|
| **Cost** | Balanced between RAM (cache) and disk (DB). 1 TB of data cost ≈ $200/month on DynamoDB Streams + GCP Bigtable, BigQuery.|
| **Latency targets** | Create ≤ 50 ms, redirect ≤ 5 ms (edge), stats ≤ 1 s.|

---

## 3. High‑Level Solution Overview  

```
Users →  CDN/ALB → API Gateway → Auth & Rate‑Limiter
          ↓
  ────────|───────
          ↓
   Shortener Service (stateless)
          ↙          ↘
   ↙ Cache (Redis)   ↘ DB (Cassandra/DynamoDB)
   ↘ Stats Collector  ↙
     (Kafka) ←→ Analytics Pipeline (Spark/Kafka‑Streams) → Analytics Store
```

1. **Front‑End** – static HTML/JS from a CDN (pay‑as‑you‑go, zero‑cost).
2. **API Gateway** –  load‑balance, TLS termination, OAuth / API keys.
3. **Shortener Service** – stateless workers that:
   * Generate short codes (atomic counter or random + dedupe).
   * Persist to DB with *strong write consistency*.
   * Publish *event* to Kafka for analytics.
4. **Redirect Service** – same pool of workers; on request:
   * First, check Redis cluster for hot key.
   * On miss, read from DB, then cache the result.
5. **Cache** – 8‑sharded Redis (≈ 8 GB per shard).  
   * Hotness: 70 % of queries are for a small set of popular
     URLs → 5‑second TTL, 30‑min TTL for the rest.
6. **Analytics** – every redirect and creation event streams to Kafka.  
   * Real‑time processing (Spark Streaming/Kafka Streams) updates counters every second.  
   * Bulk write to a column‑store (ClickHouse/BigQuery) for ad‑hoc BI.
7. **Data Stores**  
   * **Cassandra / DynamoDB** – primary key = `code`.  
     *Schema*:
     ```
     code: string (PK)
     original_url: string
     created_at: timestamp
     click_count: counter (atomic)
     user_id: string (nullable)
     status: enum (ACTIVE, DELETED, BANNED)
     ```
   * **Redis** – key: `code`, value: `original_url`.  

---

## 4. Detailed Data Models & Capacity Math  

| **Entity** | **Size (avg)** | **Number** | **Raw Size** |
|------------|----------------|------------|--------------|
| URL Mapping (code + original + meta) | 200 B | 10 B | 2 TB |
| Analytics Event (per click) | 200 B | 1 T per day (10 M read RPS × 86 400 s) | 200 GB |
| Cache Entry (code → url) | 100 B | 2 M hot | 200 MB |
| Total |  |  | **≈ 2.4 TB** (on‑disk) |

*Shard calculations:*  
Let us assume **4‑AZ availability**; each AZ will have 1/4 of traffic.  
* **DB** – 10 G operations/day/region → 1 K writes/s + 2.5 M reads/s.  
  * *Cassandra* replication factor = 3 → 12 K writes/s to CF, each node handles ~3 K.  
* **Redis** – 7 M hot reads/s → 7 M req/s ÷ 8 shards ≈ 875 K RPS per shard, within a 1.5 GB memory limit per shard.

**Previous usage:**

* 1 M registrations → **10 M URLs** → 2 GB raw storage.  
* 100 M hits/day → 3 TB query logs, but aggregated metrics keep 1 GB of counters.

---

## 5. Key Generation & Collision Handling  

| **Strategy** | **Pros** | **Cons** |
|--------------|----------|----------|
| **Atomic counter** (global across AZs) | 100 % unique, no collision, predictable length | Requires distributed lock (e.g., DynamoDB + Optimistic Locking) |
| **Random + check** | Simpler, can be local | Must query DB for existence → 2‑TX overhead per generate |

We use a **globally‑distributed counter** stored in DynamoDB with *conditional writes*:  

```python
# pseudo‑code
while True:
    current = db.get_item('Counter', 'Global')
    candidate = encode_base62(current + 1)
    success = db.put_item(
        'URLs', candidate, url, condition='attribute_not_exists(code)'
    )
    if success:  # first seen
        db.update_item('Counter', 'Global', current + 1)
        return candidate
```

* `candidate` is 6 chars with ≈ 0 % collision probability until 56 B URLs.  
* Only **one read + one conditional write** per request.

---

## 6. Read / Write Workflows  

### 6.1 Creation Path  

1. **API Layer** – Validate URL length & scheme, enforce **per‑IP** rate limit.  
2. **Service** – Generate code via counter, store mapping (strong consistency) and publish *`Created`* event to Kafka.  
3. **Response** – Return `{shortUrl: "https://tiny.io/<code>"}`.  
4. **Async** – Mark entry in Redis with short TTL (**1 s**) to warm first crawler hit.

### 6.2 Redirect Path  

1. **Client** – HTTP GET `/abc123`.  
2. **Load‑Balancer** – Forward to registered instance.  
3. **Service** – Check **Redis**:  
   * **Cache hit** → 301 redirect, update local cache stats (atomic `INCR`).  
   * **Cache miss** → Read from **Cassandra** with *strong read* (read‑repair disabled for speed).  
4. **Cache** – Store URL for 30 min (`EXPIRE`).  
5. **Publish** – Produce *`Redirected`* event to Kafka (includes IP, UA).  
6. **Response** – 301/302 to original URL.

---

## 7. Analytics Pipeline  

| **Step** | **Component** | **Purpose** |
|----------|---------------|-------------|
| 1 | Kafka Topic `clicks` | High‑throughput stream of click events. |
| 2 | Kafka Streams / Spark Structured | Every 1 s, **aggregate** `click_count` by `code` and `country`. |
| 3 | Upsert to Analytics Store (ClickHouse, BigQuery) | Persist aggregated snapshots daily. |
| 4 | REST API `/stats/{code}` | Reads from analytics store (read‑optimized). |

The stream keeps a *counter* per key in a state store; you can sidestep the cost of Cassandra writes for every click.

---

## 8. Capacity Planning & Performance Targets  

| **Metric** | **Target** | **Reason** |
|------------|-------------|------------|
| **Create requests** | 1 k RPS | 10 k per IP * 10 000 IPs (peak) |
| **Redirect requests** | 10 M RPS | Global traffic (e.g., 400 k per 10 k unique readers) |
| **Cache traffic** | 70 % hot => 7 M RPS | Redis cluster with 8 shards, each ≈ 0.9 M RPS (well below 1 M RPS per node limit). |
| **DB write throughput** | 30 K writes/s across 4 AZs | DynamoDB/OCP: 10 k writes/s /AZ → 40 k /day. |
| **DB read throughput** | 2.5 M reads/s /AZ | 2.5 M / 4 AZs = 625 K reads/s each. |
| **Analytics store** | 200 GB/day writes | 200 B × 1 T events = 200 GB |

**Autoscaling:**  

* **API tier** – 10‑20 instances per AZ, horizontal scaling by CPU/memory.  
* **Buffer** – Keep 2× CPU of predicted traffic as buffer for burst.  
* **Redis** – Add nodes in horizontal cluster as size grows; no sharding changes required.

**Cost estimate (cloud‐agnostic):**  

| Component | Monthly Cost (US$) |
|-----------|-------------------|
| DNS / CDN (Cache) | 200 |
| API Gateways | 300 |
| Workers (x80 VMs) | 4000 |
| Redis Cluster (8 GB each × 8) | 800 |
| Cassandra / DynamoDB (backend) | 2000 |
| Kafka (broker + storage) | 1000 |
| Analytics (ClickHouse) | 800 |
| Monitoring + Alerts | 500 |
| **Total** | **≈ $8 800** |

*Can be cut by using reserved instances and freezing idle nodes.*

---

## 9. Reliability, Recovery & Availability  

| **Failure** | **Detection** | **Mitigation** |
|-------------|---------------|----------------|
| **AZ outage** | Cloud HA / health‑checks | Traffic automatically reroutes; data replicated across AZ. |
| **DB node failure** | Liveness + read‑repair | Secondary node takes over; no data loss due to replication factor 3. |
| **Cache node crash** | Redis cluster heartbeat | Partition re‑epithet; clients fall back to DB. |
| **Kafka partition loss** | Replication factor 3 | Replay from DLQ; at most a few minutes of analytics loss. |
| **Write throttling** | Cloud throttles API Gateway | Back‑pressure; *Client‑side* queue until ack. |

**Backup & Restore** – Snapshot of Cassandra every 6 h → cold‑storage (S3). 1 hour recovery.

**Disaster Recovery** – Dual‑region active‑active; weighted automatic failover.

---

## 10. Security & Abuse Controls  

| Guard | Mechanism | Throttle / Quota |
|-------|-----------|------------------|
| **IP rate limiting** | API Gateway header (`x-rate-limit`) | 10 k create RPS per IP; 1 M redirection per 24 h per IP. |
| **CAPTCHA on bulk** | JS challenge + reCAPTCHA | Bypass for verified API keys. |
| **Domain locking** | User’s domain config | Pre‑approved domains only. |
| **URL validation** | Regex scheme + AV scanning | Reject suspicious protocols (javascript:). |
| **Click fraud detection** | Two‑factor source validation (IP+referer) + anomaly detector. | Flag & quarantine on 10‑hour trend of > 1 M clicks. |
| **Audit logs** | CloudWatch + Kafka | Immutable logs for compliance. |

---

## 11. Trade‑offs & Alternatives  

| Decision | Option A | Option B | Trade‑Off |
|----------|----------|----------|-----------|
| **Primary DB** | Cassandra (open‑source) | DynamoDB (managed) | 1) Cassandra → DIY, 2) DynamoDB → managed, more consistent latency. |
| **Cache** | Redis cluster | Memcached | Redis – persistence & atomic ops; Memcached – lighter memory usage. |
| **Analytics Store** | ClickHouse | BigQuery | ClickHouse – real‑time counts; BigQuery – multi‑dim analytics but higher query cost. |
| **Code length** | 6 chars | 5 chars | 5 → ~916 B collisions; 6 → safe until 56 B links. |
| **Traffic routing** | Edge CDN silo | Global load balancer | CDN guarantees < 5 ms; GLB may expose all traffic to origin. |
| **Event sourcing** | Kafka Streams | Pulsar | Pulsar adds schema support, but Kafka more mature. |

---

## 12. Failure Modes & Mitigations – Checklist  

1. **DB Layer** – Deadlock → Use *lightweight transactions* & `CONDITION` to avoid stalls.  
2. **Cache Eviction** – TTL mis‑config → accidental cache purge. Mitigation: *Lfu* eviction + *hybrid* TTL.  
3. **Race Conditions** – Double‑creation of same code. Mitigation: *Conditional writes* on `PUT`.  
4. **Bad Traffic (DDOS)** – Managed WAF + IP blacklisting.  
5. **Inconsistent Stats** – Flaky Kafka consumer → Use *exactly‑once* semantics.  
6. **Long‑Running Requests** – Slow DB read = redirect delay → fallback to *stale* cached result (max 1 h).  
7. **Analytics Lag** – Backlog of 10k events per second → increase partitions or add consumer.  

---

## 13. Diagram (Mermaid Syntax)  

```mermaid
graph TD
  A[Users] -->|HTTPS| B(CDN / Cloudflare)
  B -->|w/ IP Auth| C(API Gateway)
  C -->|Auth & Rate‑Limit| D(Shortener Service)
  D -->|Create| E[Redis (Cache)]
  D -->|Create| F[Cassandra / DynamoDB]
  D -->|Created Event| G(Kafka Topic: Created)
  D -->|Redirect| H(Kafka Topic: Clicked)
  H -->|Analytics| I(Analytics Stream)
  I -->|Persisted View| J(ClickHouse / BigQuery)
  G -->|Used for Stats| K(Stats Service)

  subgraph "Redirect Path"
    C -->|GET /<code>| D
    D -->|Cache Hit| E
    D -->|Cache Miss| F
    E -->|Return URL| L[Client]
    F -->|Return URL| L
  end

  subgraph "Storage & Compute"
    E[Redis] -.-> F
    G -.-> F
  end

  subgraph "Monitoring"
    D -->|Metrics| M(Prometheus)
    E -->|Cache Hit Rate| M
    F -->|DB Latency| M
  end
```

> **Legend**  
> * ↔︎ Bidirectional edges denote request/response flows.  
> ➔  One‑way service calls.  
> 🔃  Periodic metrics push.

---

## 14. Summary  

* **Short code**: 6 char, ≈ 56 B unique keys → collision‑free for > 50 B URLs.  
* **Database**: Cassandra or DynamoDB – strong writes, tunable consistency.  
* **Cache**: 8‑sharded Redis – 70 % hot request reduction.  
* **Analytics**: Kafka → Spark/Kafka‑Streams → ClickHouse.  
* **Scalability**: 10 k write RPS, 10 M read RPS across 4 AZs, 2 TB storage.  
* **Availability**: 99.999% with multi‑AZ replication, auto‑scaling, graceful degradation.  
* **Security**: IP rate limits, CAPTCHA, SA‑AS domain locking, click‑fraud detection.  

With this architecture we can comfortably ship a high‑performance, highly‑available URL shortener that scales linearly with global demand while keeping operational costs manageable.