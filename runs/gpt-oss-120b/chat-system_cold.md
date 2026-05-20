---

Design a real-time chat system like WhatsApp or Slack.


---

Below is a **complete, end‑to‑end design** for a real‑time chat platform that can be used for a consumer‑grade product (WhatsApp‑style) **or** an enterprise‑grade product (Slack‑style).  
The document is divided into sections that cover **requirements, high‑level architecture, data model & storage, scaling calculations, trade‑offs, failure modes & mitigation, security, and operational concerns**.  
Every diagram is expressed in Mermaid syntax inside fenced blocks, as requested.

---

## 1️⃣ System Overview & Core Requirements  

| Category | Requirement | Notes |
|----------|-------------|-------|
| **Functional** | • User registration / login (phone/email + password) <br>• 1‑to‑1 direct messages (DM) <br>• Group / channel messaging (up to 10 k members) <br>• Presence (online/away/offline) <br>• Message read receipts & typing indicator <br>• Media & file sharing (images, video, PDFs, etc.) <br>• Push notifications (mobile/desktop) <br>• Search (by keyword, sender, date) | Most features are **eventually consistent** except presence & delivery acknowledgements which need low latency. |
| **Non‑functional** | • **Latency**: < 200 ms for message delivery (95th percentile) <br>• **Throughput**: ≥ 10 k msgs/sec for a medium‑size deployment (scale to > 200 k msgs/sec) <br>• **Durability**: No message loss, at‑least‑once delivery guarantees <br>• **Scalability**: Horizontal scaling on all stateless layers <br>• **Availability**: 99.9 % uptime (≈ 8.76 h downtime / year) <br>• **Security**: End‑to‑end encryption (E2EE) for private chats, TLS for all in‑flight traffic <br>• **Compliance**: GDPR/CCPA data‑subject rights, data‑locality options | The design purposely separates **real‑time path** from **storage & analytics** to avoid coupling latency‑sensitive traffic with heavy background workloads. |

---

## 2️⃣ High‑Level Architecture  

```
graph TD
    subgraph Clients
        A[Mobile App (iOS/Android)] -->|WebSocket| LB1[Load Balancer (WS)]
        B[Web Client (React)] -->|WebSocket| LB1
        C[Desktop Client (Electron)] -->|WebSocket| LB1
    end

    subgraph API
        LB2[Load Balancer (HTTP/HTTPS)] --> APIGW[API Gateway]
        APIGW --> AuthS[Auth Service]
        APIGW --> UserS[User Service]
        APIGW --> ChatS[Chat Service]
        APIGW --> GroupS[Group Service]
        APIGW --> MediaS[Media Service]
        APIGW --> NotifyS[Notification Service]
        APIGW --> SearchS[Search Service]
    end

    subgraph Real‑time Delivery
        LB1 --> WSRouter[WebSocket Router]
        WSRouter --> PresenceS[Presence Service]
        WSRouter --> DeliveryS[Message Delivery Service]
        DeliveryS --> PubSub[Pub/Sub (Kafka/NATS)]
        PubSub --> MsgStore[Message Store (Cassandra)]
        DeliveryS --> Cache[Redis (Cache & Presence)]
    end

    subgraph Asynchronous Workers
        MediaS --> MediaProc[Media Processor Workers]
        NotifyS --> PushWorker[Push Notification Workers]
        SearchS --> IndexWorker[Search Index Workers]
    end

    subgraph Storage
        UserDB[(PostgreSQL)] -->|User profile, auth| UserS
        MsgStore -->|Message rows| ChatS
        MediaDB[(S3/MinIO)] -->|Media blobs| MediaS
        SearchIdx[(Elasticsearch)] --> SearchS
        LogDB[(ClickHouse)] -->|Analytics| Logs
    end
```

**Key Components**

| Component | Role | Typical Tech Choices |
|---|---|---|
| **Load Balancers** | Terminates TLS, distributes traffic, health‑checks | HAProxy / Envoy / Cloud LB |
| **API Gateway** | Central entry point, request routing, rate‑limiting, authentication enforcement | Kong / AWS API GW / Ambassador |
| **Auth Service** | Issue/verify JWT, password hashing, 2FA, token revocation | Spring Security, Go‑Auth, OIDC |
| **User Service** | CRUD for user profiles, contacts, device tokens | PostgreSQL + Redis cache |
| **Chat Service** | Core business logic: create conversation, enforce ACL, produce messages to Pub/Sub | Java/Go microservice |
| **Group Service** | Manage group/channel metadata, membership, admin rights | Same stack as Chat Service |
| **WebSocket Router** | Stateless routing layer; maps a **conversation‑id** → set of connected sockets | NGINX‑RTMP‑like or custom **socket.io** server |
| **Message Delivery Service** | Subscribes to Pub/Sub topics, pushes to online sockets, writes ack state | Node.js / Go, uses **Redis** for connection lookup |
| **Presence Service** | Stores online/offline state (heart‑beats) in Redis, pushes presence updates via Pub/Sub | Redis + WebSocket |
| **Message Store** | Write‑once, immutable log of messages: high‑throughput, wide‑row store | Cassandra / ScyllaDB / DynamoDB (CQL) |
| **Media Service** | Handles file upload, generation of signed URLs, asynchronous transcoding | S3 (or self‑hosted MinIO) + Lambda/FAAS workers |
| **Notification Service** | Sends push (FCM/APNs) and email notifications, de‑duplicates per device | Firebase Cloud Messaging, SNS |
| **Search Service** | Indexes messages for full‑text search, respects privacy flags | Elasticsearch / OpenSearch |
| **Durable Log (optional)** | Immutable backup of message stream for compliance/analytics | Kafka topics with Replication factor=3 |
| **Analytics Store** | Columnar store for usage metrics, reporting | ClickHouse / Snowflake |

---

## 3️⃣ Data Model & Storage Choices  

### 3.1 User & Authentication  

| Table | Primary Key | Important Columns |
|-------|------------|-------------------|
| `users` | `user_id (UUID)` | `phone/email`, `hashed_password`, `display_name`, `profile_pic_url`, `created_at`, `last_login_at` |
| `devices` | `(user_id, device_id)` | `fcm_token`, `last_seen`, `platform`, `push_enabled` |
| `contacts` | `(owner_id, contact_user_id)` | `status (pending/accepted)`, `created_at` |

*Store in **PostgreSQL** for strong consistency & relational queries.*  

### 3.2 Conversations & Membership  

| Table | Primary Key | Important Columns |
|-------|------------|-------------------|
| `conversations` | `conv_id (UUID)` | `type (DM/GROUP)`, `created_at`, `last_message_at`, `is_encrypted` |
| `conv_members` | `(conv_id, user_id)` | `role (owner/member/admin)`, `joined_at`, `muted_until` |

Again hosted in **PostgreSQL** – size is modest ( ≤ 10 M rows for a 10 M user system).  

### 3.3 Message Store  

A **wide‑row** per conversation, stored in a **column‑family NoSQL** (Cassandra) to support:

* **Write‑heavy** pattern – 1‑to‑many inserts per second.
* **Time‑ordered reads** – range scans for recent messages.
* **Horizontal scalability** via partition key = `conv_id`.

Schema (Cassandra CQL):

```sql
CREATE TABLE messages (
    conv_id          uuid,        -- partition key
    msg_timestamp    timeuuid,    -- clustering column (ordered)
    msg_id           uuid,
    sender_id        uuid,
    content_type     text,        -- "text", "image", "video", "file"
    content          blob,        -- encrypted payload (small for text)
    media_url        text,        -- S3 URL if applicable
    read_by          set<uuid>,   -- set of user_ids who have read
    edit_history     list<blob>,  -- optional, encrypted edits
    PRIMARY KEY (conv_id, msg_timestamp)
) WITH CLUSTERING ORDER BY (msg_timestamp DESC);
```

**Capacity calculation (see §4)** shows this easily handles petabytes of data with modest node counts.  

### 3.4 Media Storage  

*Use **object storage** (S3 or on‑prem MinIO) with **per‑object encryption** (SSE‑KMS).  
*The `media_url` stored in the message row is a **presigned** URL limited to a short TTL (e.g., 15 min).  

### 3.5 Presence & Connection Mapping  

*All **online** sockets are stored in **Redis** hashes:*  

```text
hash:socket:<user_id> => { socket_id: server_id, last_heartbeat_ts }
set:conv_sockets:<conv_id> => { socket_id1, socket_id2, … }
```

*Redis TTL = 30 s; clients send heartbeat every 10 s.*  

### 3.6 Search Index  

*Elasticsearch document per message (or per batch for large groups).  
*Only **non‑encrypted metadata** (sender_id, timestamp, content preview) is indexed – the actual text stays encrypted in Cassandra.  
*Compliance flag `searchable: false` removes document from the index.

---

## 4️⃣ Capacity Planning & Math (Real‑World Numbers)  

Assume a **medium‑size launch** that grows to **10 M daily active users (DAU)** within 12 months.  

| Metric | Assumption | Result |
|--------|------------|--------|
| **Average messages per active user per day** | 35 (mix of DM & groups) | **350 M msgs/day** |
| **Peak hour factor** | 3× daily avg (social spikes) | 105 M msgs/hr ≈ 29 k msgs/s |
| **Peak 5‑minute burst** | 1.5× 5‑min average of peak hour | ~2.4 k msgs/s (consistent with 29 k/hr) |
| **Message size (text)** | 200 bytes (UTF‑8 JSON + encryption overhead) | 70 GB/day ≈ **2.1 TB/month** |
| **Media (average 0.2 MB per user per day)** | 2 GB/day ≈ **60 GB/month** |
| **Total ingest** | Text + Media | **~2.2 TB/month** |
| **Write throughput to message store** | 29 k msgs/s | **~6 k writes/sec** to Cassandra (each write ≈ 1 row) |
| **Read queries (scroll forward/backward)** | Assume 30 % of active users load chat history each day → 3 M reads/day ≈ 35 read/s (light) + 3 k reads/s for “jump to latest” in active sessions |
| **WebSocket connections (concurrent online)** | 15 % of DAU ≈ 1.5 M connections | **~1.5 M persistent sockets** |
| **WebSocket server capacity** | 20 k connections per node (≈ 64 MiB RAM, 2 vCPU) using epoll & async I/O | **75 nodes** (1.5 M / 20 k) |
| **Redis presence store** | 1.5 M keys, each ~200 B → 300 MiB; add overhead → 1 GiB | 2‑node Redis cluster (master‑replica) for HA |
| **Kafka (or NATS) throughput** | 29 k msgs/s × 1 KB ≈ 30 MiB/s | 3‑node Kafka cluster (10 MiB/s per broker) with replication factor 3 gives 9 MiB/s * 3 = 27 MiB/s * 2 (over‑provision) ≈ 54 MiB/s – sufficient |
| **Media processing workers** | Assume 5 % of msgs contain media = 1.75 M media uploads/day ≈ 20 uploads/s | 40 transcode workers (2×CPU per worker) handle this comfortably |
| **Push notifications** | 10 % of msgs trigger push (offline recipients) ≈ 3 k pushes/s | 3 × 10‑core workers → < 500 ms latency using FCM/APNs batch API |

### Scaling Headroom  

| Target | Scaling factor | Required resources |
|--------|----------------|--------------------|
| **100 M DAU** (10×) | 10× traffic | 10× WebSocket nodes (≈ 750), 10× DB nodes (Cassandra 30‑node ring), 10× Kafka (30‑node) – still within a single cloud region. |
| **Geographically distributed** | Add **regional edge clusters** (e.g., 2‑3 zones) with data‑replication using **Cassandra multi‑DC** and **Redis Geo‑replication** for low latency. |

---

## 5️⃣ Trade‑offs & Design Decisions  

| Decision | Pro | Con | When to Re‑evaluate |
|----------|-----|-----|----------------------|
| **Cassandra for message store** | Linear scalability, high write throughput, built‑in replication, immutable rows suit audit logs. | Strong consistency is limited (tunable consistency). No secondary indexes – need external search. | If you need strict transactional guarantees (e.g., banking‑grade) → consider **CockroachDB** or **Aurora** (but at higher cost). |
| **WebSocket + Pub/Sub** vs **Long‑polling** | Low latency, bi‑directional, single TCP connection, efficient for mobile. | Requires sticky routing or shared state (Redis), more complex scaling. | For low‑traffic SaaS MVP, plain **REST + Long‑polling** could be enough. |
| **End‑to‑End Encryption (E2EE)** | Guarantees confidentiality, differentiates product (WhatsApp). | Server cannot read content → limited server‑side features (search, compliance retention). | For enterprise Slack‑style product, you may choose **server‑side encryption** (SSE‑KMS) to enable indexing while still providing TLS in‑flight. |
| **Separate Notification Service** | Decouples push delivery from chat path → smoother scaling. | Additional latency & operational surface. | If push volume is negligible (internal use only), embed it in the Chat Service. |
| **Relational DB for user & group metadata** | ACID transactions for membership changes, easy joins. | Poor horizontal scaling for very large user bases. | For a **massive consumer product** (>100 M users) you could move to **Vitess/MySQL sharding** or **CockroachDB**. |
| **Search via Elasticsearch** vs **Lexical indexes in Cassandra** | Full‑text, relevance ranking, fuzzy search. | Extra cluster, data duplication, eventual consistency. | If product only needs simple keyword search on last 100 messages, a **materialized view** in Cassandra could replace Elasticsearch. |
| **Single‑region vs Multi‑region deployment** | Simpler ops, no cross‑region latency. | Higher latency for distant users, regional outages affect all. | Once latency > 150 ms for key markets, split into **regional edge clusters** with **Cassandra multi‑DC** and **global load balancers**. |
| **Message TTL & Archival** | Automatic deletion reduces storage cost. | Users may lose history; compliance may require retention. | Add **archival pipeline** to cold storage (Glacier, HDFS) for compliance. |

---

## 6️⃣ Failure Modes & Mitigations  

| Failure Scenario | Impact | Detection | Mitigation |
|------------------|--------|-----------|------------|
| **Cassandra node crash** | Loss of write availability for its token ranges (if replication factor < 2). | Node health check, gossip alerts. | Use **RF=3**; automatically reroute writes to other replicas. Run **repair** jobs nightly. |
| **Redis cache loss** (e.g., master fails) | Presence look‑ups stale → users appear offline; message routing may miss sockets. | Redis sentinel alerts, TTL expiration. | **Redis Sentinel** / **Cluster** with automatic failover; on failover rebuild presence state from WebSocket heartbeats. |
| **WebSocket server overload** (CPU/Memory spike) | Increased latency, dropped messages, disconnects. | High CPU/Memory metrics, request queue length. | Deploy **autoscaling** based on connection count & CPU; use **circuit breaker** in router; fallback to **HTTP polling** for affected users. |
| **Message duplication** (producer retry) | Users see same message twice. | Duplicate detection logic in consumer (idempotent writes). | Include **client‑generated UUID** per message; enforce **INSERT IF NOT EXISTS** in Cassandra. |
| **Network partition** separating a data‑center | Users in one DC cannot talk to others; possible split‑brain. | Heartbeat loss between clusters, high latency. | **Quorum writes** (W+R > RF) ensure consistency; route cross‑DC via **gossip** and **raft** for config changes. |
| **Media service outage** (S3 not reachable) | Media upload fails, older media cannot be fetched. | HTTP 5xx responses, storage metrics. | **Fallback to secondary bucket** (multi‑region replication). Notify user with retry UI. |
| **Push provider throttling (FCM/APNs)** | Delayed/offline notifications → poorer UX. | Error rates in push worker logs. | **Rate‑limit** pushes per device; batch send; graceful degradation to **in‑app notifications** when offline. |
| **Data‑corruption in message store** | Permanent loss of messages. | Cassandra checksum mismatches, repair failures. | **Snapshot backups** (incremental) + **anti‑entropy repair**; test restores regularly. |
| **Compromised JWT** (private key leak) | Unauthorized API access. | Security monitoring alerts (unexpected token signatures). | Rotate keys, invalidate existing tokens, enforce **short token TTL** (15 min) with refresh tokens. |
| **Clock skew** (servers out of sync) | Incorrect ordering of messages (timeuuid). | NTP drift alerts. | Use **Chrony** with multiple NTP sources; fall back to **monotonic counters** for ordering. |

---

## 7️⃣ Security & Privacy  

| Area | Controls |
|------|----------|
| **Transport** | Enforce TLS 1.3 on all inbound/outbound traffic. Use **HSTS** & **OCSP Stapling**. |
| **Authentication** | JWT signed with **RSA‑2048** or **Ed25519**; token lifetime ≤ 15 min; refresh token with revocation list. |
| **Authorization** | ACL checks in **Chat Service** (owner, member, admin). Centralized policy engine (OPA) optional. |
| **End‑to‑End Encryption** | **Signal Protocol** (double ratchet) for private chats; key exchange via **X3DH** over TLS. Server stores only **encrypted blobs**. |
| **Server‑Side Encryption** | Media objects encrypted using **AES‑256‑GCM** with per‑object keys managed by **KMS**. |
| **Data at Rest** | Full‑disk encryption (dm‑crypt) on all VMs; **SSE‑KMS** for object storage. |
| **Key Management** | Centralized **HSM** (AWS CloudHSM, Azure Key Vault) for master keys; per‑conversation keys derived via HKDF. |
| **Compliance** | Ability to **export/delete** all user data (`right to be forgotten`). Store **metadata** (e.g., conversation IDs) in a searchable tag that can be removed without touching encrypted payload. |
| **Audit Logging** | Immutable logs in **ClickHouse** + **Kafka**; signed logs using **Hash‑Based Message Authentication Code (HMAC)**. |
| **Rate Limiting & Abuse Prevention** | Token bucket per IP/device; CAPTCHA for suspicious sign‑ups; automated spam detection via ML on message patterns. |
| **Pen‑Testing & Bug Bounty** | Periodic third‑party audits; public bug bounty program (e.g., HackerOne). |

---

## 8️⃣ Operational Concerns  

1. **CI/CD** – Blue/Green deployments per microservice; canary releases for new protocol versions.  
2. **Observability** –  
   * **Metrics**: Prometheus (per‑service latency, error rates, connection counts).  
   * **Logs**: Structured JSON → Elastic/Logstash → Kibana.  
   * **Tracing**: OpenTelemetry spans across API Gateway → Chat Service → Delivery Service.  
3. **Backup & Disaster Recovery** –  
   * **Cassandra**: Snapshots + incremental backups to object storage; cross‑region restore scripts.  
   * **PostgreSQL**: PITR (Point‑In‑Time Recovery) using WAL archiving.  
   * **Redis**: AOF + RDB snapshots; replicates to standby region.  
4. **Capacity Planning** – Automated scaling based on **CPU**, **connection count**, **Kafka lag**, **Redis memory usage**.  
5. **Feature Flags** – Use **LaunchDarkly** or internal flag system for gradual rollout (e.g., new E2EE protocol version).  

---

## 9️⃣ Sample End‑to‑End Flow (Message Send)  

```
sequenceDiagram
    participant C as Client (Mobile)
    participant LB as LB (WS)
    participant WS as WebSocket Router
    participant PD as Presence Service (Redis)
    participant Pub as Pub/Sub (Kafka)
    participant MS as Message Service (Cassandra)
    participant DL as Delivery Service
    participant N as Notification Service

    C->>LB: Upgrade to WebSocket (JWT)
    LB->>WS: Connection established
    WS->>PD: Register presence (user_id, socket_id)
    C->>WS: SEND_MESSAGE {conv_id, client_msg_id, payload}
    WS->>Pub: PRODUCE(topic=conv_id, payload)
    Pub-->>MS: (Consumer) Write message row (idempotent)
    Pub-->>DL: (Consumer) Lookup online sockets for conv_id
    DL->>PD: Get socket_ids for members
    DL->>WS: PUSH payload to socket_id(s)
    WS->>C: ACK (server_msg_id, timestamp)
    alt recipients offline
        DL->>N: ENQUEUE push notification
    end
```

*Key Guarantees:*  

* **Exactly‑once** delivery to the **Message Store** (idempotent write using `client_msg_id`).  
* **At‑least‑once** push to online sockets (retries on failure).  
* **Read receipt** is a separate message type (sent from client → WS → Pub → Delivery).  

---

## 10️⃣ Summary  

*The design balances low latency, high durability, and strong privacy.*  

* **Stateless front‑ends (API Gateway, WebSocket Router) + horizontal autoscaling** give us the ability to serve millions of concurrent users.  
* **Cassandra** offers a write‑optimized, partition‑friendly message log that scales linearly and survives node failures.  
* **Redis** handles ultra‑fast presence lookups and socket‑to‑user mapping without persisting huge amounts of state.  
* **Kafka/NATS** decouples the real‑time delivery path from storage, enabling replay for compliance and analytics.  
* **End‑to‑end encryption** isolates user data from the server, while **optional server‑side encryption** provides richer features (search, compliance) for enterprise deployments.  
* Comprehensive **observability, automated scaling, and disaster‑recovery** procedures ensure the target SLA (99.9 % uptime) and a graceful degradation path under overload or partial outages.

With the capacity numbers shown, the platform comfortably handles **~30 k messages/s and ~1.5 M concurrent sockets** on a modest cluster, and can be expanded **10×** to support a **global‑scale product** with minimal architectural changes.  

--- 

*Feel free to ask for deeper dives into any sub‑system (e.g., key‑exchange for E2EE, sharding strategy for Cassandra, or cost models per cloud provider).*