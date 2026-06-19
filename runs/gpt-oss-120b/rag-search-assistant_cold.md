---

Design a retrieval-augmented generation (RAG) assistant that answers user questions based on a company's internal documents.


---

# Retrieval‑Augmented Generation (RAG) Assistant for Internal Company Documents  

**Goal** – Provide employees a conversational assistant that answers questions using the company’s up‑to‑date internal knowledge (policy docs, engineering wikis, product specs, support tickets, etc.) while guaranteeing privacy, low latency, and high availability.

---

## 1. Scope & Functional Requirements  

| # | Requirement | Details |
|---|-------------|---------|
| 1️⃣ | **Document ingestion** | Periodic (daily) bulk import of PDFs, DOCX, HTML, Markdown, emails, Confluence/SharePoint wikis. Extract text, clean, split into “chunks” (≈200 tokens). |
| 2️⃣ | **Embedding generation** | Produce dense vectors for each chunk (e.g., 1536‑dim OpenAI `text-embedding-ada-002` or a local Sentence‑Transformer). |
| 3️⃣ | **Vector store** | Store embeddings + metadata, enable nearest‑neighbour (ANN) search. |
| 4️⃣ | **Hybrid retrieval** | Combine semantic ANN search (embedding similarity) with lexical BM25 fallback. |
| 5️⃣ | **Reranking (optional)** | Small cross‑encoder to reorder top‑K results by relevance. |
| 6️⃣ | **Prompt construction** | Insert system prompt, retrieved snippets (≤ L‑LM token limit), and user question into a single prompt. |
| 7️⃣ | **LLM inference** | Call a generative model (GPT‑4‑Turbo or an on‑prem GPU‑hosted model) to synthesize an answer. |
| 8️⃣ | **Citation & provenance** | Attach source IDs (doc‑title, page, chunk‑offset) so users can open the original material. |
| 9️⃣ | **Access control** | Role‑Based Access Control (RBAC) – every request is authorized before any retrieval/LLM call. |
| 🔟 | **Observability** | End‑to‑end tracing, request latency, error rates, token usage, storage health. |
| 1️⃣1️⃣ | **Reliability** | 99.9 % uptime, graceful degradation if any component fails. |
| 1️⃣2️⃣ | **Compliance** | Data‑at‑rest encryption (AES‑256), in‑flight TLS, audit logs, GDPR‑/CCPA‑compatible retention. |
| 1️⃣3️⃣ | **Scalability** | Support up to **1 k QPS** (peak) with average latency ≤ 2 s per answer. |
| 1️⃣4️⃣ | **Cost awareness** | Keep operation cost < $5 k/month for a mid‑size company (≈10 k docs). |

---

## 2. Non‑Functional Requirements  

| Category | Target |
|----------|--------|
| **Latency** | 95 % of queries ≤ 1.5 s, 99 % ≤ 2 s (including embedding + retrieval + LLM). |
| **Throughput** | 1 000 queries / s (≈86 M queries / month). |
| **Availability** | 99.9 % SLA (≈43 min downtime / month). |
| **Security** | Mutual TLS between services, JWT‑based auth, audit logging, data encryption. |
| **Observability** | OpenTelemetry metrics + logs, alerts on error‑rate > 0.5 % or latency > 3 s. |
| **Maintainability** | CI/CD pipelines, blue‑green deployments, automated schema migrations. |
| **Extensibility** | Plug‑in for alternative embedding models or LLMs. |

---

## 3. High‑Level Architecture  

```mermaid
flowchart TB
    subgraph Edge["Client Edge"]
        User[User (Web/Slack/CLI)]
    end

    subgraph API["API Layer"]
        GW[API Gateway (Auth, Rate‑limit)]
        RAG[RAG Service (FastAPI)]
    end

    subgraph Retrieval["Retrieval Pipeline"]
        QEmb[Query Embedding Service]
        VecDB[(Vector Database<br/>FAISS/Weaviate)]
        LexDB[(Lexical Store<br/>ElasticSearch)]
        Rerank[Reranker (optional)]
        Docs[Document Store (PostgreSQL + S3)]
    end

    subgraph Generation["Generation Pipeline"]
        Prompt[Prompt Builder]
        LLM[LLM Inference<br/>(OpenAI API or Local GPU)]
        Cite[Source Citation Formatter]
    end

    subgraph Ops["Operations"]
        Mon[Monitoring & Alerting]
        Log[Centralized Logging]
        Tracing[OpenTelemetry Tracing]
    end

    User --> GW --> RAG
    RAG --> QEmb --> VecDB
    RAG --> LexDB
    VecDB --> Rerank --> Docs
    LexDB --> Docs
    Docs --> Prompt
    RAG --> Prompt
    Prompt --> LLM --> Cite --> RAG --> GW --> User

    style Edge fill:#f9f,stroke:#333,stroke-width:2px
    style API fill:#bbf,stroke:#333,stroke-width:2px
    style Retrieval fill:#dfd,stroke:#333,stroke-width:2px
    style Generation fill:#ffd,stroke:#333,stroke-width:2px
    style Ops fill:#eee,stroke:#333,stroke-dasharray: 5 5
```

---

## 4. Component Deep‑Dive  

### 4.1 Document Ingestion Service  

| Sub‑component | Tech | Rationale |
|---------------|------|-----------|
| **File crawler / webhook** | Python (Apache Tika, office‑parser) | Handles PDFs, DOCX, HTML, MD, emails, wiki APIs. |
| **Text normalizer** | spaCy + custom regex | Strip boilerplate, de‑duplicate, language detection. |
| **Chunker** | Fixed‑size sliding window (≈200 tokens) with 50 % overlap | Guarantees each knowledge fact appears in ≥ 1 chunk; fits LLM context window. |
| **Metadata collector** | UUID, source URL, creation/mod‑date, department tags | Enables RBAC filtering at query time. |
| **Chunk store** | PostgreSQL (metadata) + S3 (raw chunk text) | Cheap, durable storage; easy backup. |
| **Embedding job** | Async queue (Celery + Redis) → Embedding Service | Decouples ingestion from user‑facing path; supports retries. |
| **Versioning** | Incremental snapshot per day; soft‑delete flag | Allows rollback, audit trail. |

**Throughput estimate** – 10 k documents per day × avg 2 k tokens ⇒ 20 M tokens. Embedding call (1 k tokens → 0.001 s on OpenAI) → ~20 s total compute, easily parallelized across 20 workers.

### 4.2 Embedding Service  

* **Option A – Managed**: OpenAI `text-embedding-ada-002` (1536‑dim, $0.0001 per 1 k tokens).  
* **Option B – Self‑hosted**: `sentence‑transformers/all‑mini‑lm‑l6‑v2` (384‑dim) on CPU/GPU.  

| Metric | Managed | Self‑hosted |
|--------|---------|------------|
| Cost per 1 M tokens | $0.10 | EC2‑GPU $0.28 hr (p3.2xlarge) → ~$200 / month for 10 M tokens |
| Latency (avg) | 30 ms (network) | 8 ms (GPU) |
| Ops overhead | Minimal | Model updates, scaling, GPU health |
| Data privacy | Tokens leave OpenAI | Fully on‑prem (preferred for sensitive docs) |

**Decision** – Start with managed service for speed; migrate to self‑hosted after data‑privacy review.

### 4.3 Vector Database  

* **FAISS + IVF‑PQ** (on‑prem) or **Weaviate Cloud** (managed).  
* **Dimensions**: 1536 (OpenAI) or 384 (local).  
* **Index type**: IVF‑SQ8 (fast build) → ~0.4 ms per query for 10 k – 100 k vectors.  

**Storage sizing** (example 1 M chunks, 1536‑dim, 4‑byte float):

```
vector bytes = 1536 * 4 = 6,144 B ≈ 6 KB
metadata (doc_id, offset, tags) ≈ 1 KB
total per chunk ≈ 7 KB
1 M chunks → 7 GB
```

Add 30 % overhead for IVF tables → **≈ 9 GB** on SSD (NVMe).  

**Replication** – 2‑node replica set (active‑passive) for HA; each node holds full index.

### 4.4 Lexical Store (BM25)

* **ElasticSearch 8.x** with `text` analyzer.  
* Stores same chunk documents (same IDs) for fallback when semantic similarity fails or for hybrid scoring.

### 4.5 Reranker (Optional)

* Small cross‑encoder (`cross‑encoder/ms‑marco-MiniLM-L-12-v2`) runs on CPU; scores top‑K=20 from ANN → final top‑5 sent to LLM.  
* Adds ~10 ms latency per query; improves relevance on ambiguous queries.

### 4.6 Prompt Builder & Citation Formatter  

* **System prompt** (static) instructs LM: “Answer using only the provided context; cite sources as `[doc‑title#section]`”.  
* **Context assembly** – concatenate retrieved chunks, truncate to stay under model’s max context (e.g., 8 k tokens for GPT‑4‑Turbo). Uses **Maximal Marginal Relevance (MMR)** to maximize coverage while minimizing redundancy.  
* **Citation mapping** – keep mapping `chunk_id → source_ref` and inject markers (`[[C1]]`) into the prompt; after generation replace with human‑readable links.

### 4.7 LLM Inference  

| Option | Cost per 1 k output tokens | Latency (avg) | Deployment |
|--------|---------------------------|----------------|------------|
| OpenAI GPT‑4‑Turbo | $0.015 (completion) | 300 ms (network) | Managed API |
| Local LLaMA‑2‑70B (quant‑4bit) on A100 | $0 (compute cost only) | 500 ms (GPU) | Kubernetes pod, auto‑scale |
| Claude‑3.5‑Sonnet (API) | $0.003 (prompt) + $0.015 (completion) | 250 ms | Managed API |

**Chosen baseline** – OpenAI GPT‑4‑Turbo (balance of quality & cost). Later switch to local LLaMA‑2 when token‑budget grows.

### 4.8 API Gateway & Auth  

* **API‑gateway** (Kong/Envoy) performs JWT validation, rate‑limit per user (e.g., 30 QPM).  
* Passes user’s department/role claims to RAG service for **document‑level ACL** filtering (`WHERE department IN user.roles`).

### 4.9 Observability Stack  

* **Metrics** – Prometheus (request latency, error counters, token usage).  
* **Tracing** – OpenTelemetry (spans: ingest → embed → retrieve → generate).  
* **Logs** – Loki/ELK (structured JSON).  
* **Alerting** – Alertmanager (latency > 3 s, vector‑DB node down).

---

## 5. Data Flow (Happy Path)

1. **User** sends a question → **API GW** (auth).  
2. **RAG Service** receives request, extracts user ID + roles.  
3. **Query Embedding Service** creates a dense vector for the question (≈ 30 ms).  
4. **Vector DB** performs ANN search → top‑K=50 chunk IDs (≈ 0.4 ms).  
5. **Lexical Store** runs BM25 fallback in parallel → top‑K=50 (≈ 1 ms).  
6. **Hybrid Scorer** merges results (weighted sum).  
7. **Reranker** (if enabled) re‑scores → final top‑5 chunks (≈ 10 ms).  
8. **Document Store** fetches raw text for the selected chunks (≈ 5 ms).  
9. **Prompt Builder** assembles system prompt + retrieved context (≤ 8 k token window).  
10. **LLM** generates answer + citation placeholders (≈ 300 ms).  
11. **Citation Formatter** replaces placeholders with source links.  
12. **RAG Service** returns JSON `{answer, sources[], usage}` → **API GW** → **User**.  

**Total latency** (worst‑case) ≈ **1.3 s** – comfortably below the 2 s SLA.

---

## 6. Capacity & Scaling Calculations  

### 6.1 Query Traffic  

| Metric | Value |
|--------|-------|
| Peak QPS | 1 000 |
| Avg question length | 30 tokens |
| Avg retrieved context (5 chunks × 200 tokens) | 1 000 tokens |
| Prompt + system (≈ 150 tokens) | 150 |
| Total input tokens to LLM | 1 180 ≈ 1.2 k |
| Expected LLM output | 200 tokens |
| Total LLM tokens per query | 1.4 k |

**LLM cost per query** (GPT‑4‑Turbo):  
`prompt: 1.2 k × $0.003 = $0.0036`  
`completion: 0.2 k × $0.015 = $0.0030`  
**≈ $0.0066 per query**  
**Monthly (86 M queries)** → **$568 k** – far above budget.  

**Realistic target** – 100 k queries/month (≈ 3 QPS avg) → **$660**.  
Thus we must **cap usage** (e.g., internal only, per‑user quota) and **optimize context** (lower token count) or **use cheaper LLM** (Claude‑3.5‑Sonnet = $0.0036 per query).  

### 6.2 Embedding Service Load  

*Embedding latency* ≈ 30 ms per request (managed).  
At 1 k QPS → 30 ms × 1 k = **30 seconds of compute per second** → **30 × 1000 = 30 000 ms** = 30 CPU‑seconds per second → 30 cores needed (each core ~1 s per 30 ms).  

**Provision** – Deploy **4 × c5.2xlarge (8 vCPU each)** → 32 vCPU; headroom for bursts and retries.  

If self‑hosted on GPU (e.g., `sentence‑transformers/all‑mini‑lm‑l6‑v2`), a single A100 can embed ~~10 k queries/sec; a single GPU would be enough for 1 k QPS.

### 6.3 Vector DB Throughput  

ANN search cost ≈ 0.4 ms per query (FAISS IVF‑SQ8).  

*CPU required*: 0.4 ms × 1 k = 0.4 seconds of CPU per second ⇒ **0.4 cores**.  

Add overhead for replica sync, indexing, cold‑start → **2 vCPU** per node.  

**Memory** – For 10 M chunks (≈ 70 GB) → need **≥ 128 GB RAM** per node (FAISS can mmap). Use **r5.4xlarge (128 GB)**.

### 6.4 LLM Inference Cost (Managed API)  

Assuming **100 k queries/month** (budget ~ $5 k).  

| Component | Tokens/month | Cost (USD) |
|-----------|--------------|------------|
| Prompt (1.2 k) | 120 M | $360 |
| Completion (0.2 k) | 20 M | $300 |
| Total | – | **$660** |

Leaves ample margin for monitoring, ingestion, and other services.  

If usage spikes to 200 k queries, cost ≈ **$1.3 k** – still acceptable.

### 6.5 Storage  

| Item | Size |
|------|------|
| Embedding vectors (10 M chunks, 1536‑dim) | 60 GB |
| Chunk metadata (PostgreSQL) | 2 GB |
| Raw chunk text (S3, avg 200 tokens ≈ 1 KB) | 10 GB |
| ElasticSearch inverted index | 5 GB |
| Logs (30 days) | 50 GB |
| **Total** | **≈ 130 GB** |

All comfortably fit on a **2‑TB SSD** (for faster retrieval) with weekly backups to S3 Glacier.

---

## 7. Trade‑offs  

| Decision | Pro | Con | When to Re‑evaluate |
|----------|-----|-----|----------------------|
| **Managed embedding (OpenAI)** | No ops, high quality, low latency | Sent data leaves network → compliance, per‑token cost | If policy requires on‑prem data, switch to self‑hosted. |
| **FAISS on‑prem vs. Weaviate SaaS** | Full control, cheap | More ops complexity, need GPU/large RAM | If scaling beyond 50 M vectors, consider SaaS for easier scaling. |
| **Hybrid retrieval (semantic + BM25)** | Better recall for rare terms | Slightly higher latency & compute | If latency budget is extremely tight, drop BM25. |
| **Cross‑encoder reranker** | Improves relevance, reduces hallucination | Extra CPU & latency (~10 ms) | If relevance already high, remove to simplify. |
| **GPT‑4‑Turbo** | Best answer quality | Highest token cost | If cost spikes, move to Claude‑3.5‑Sonnet or a local LLaMA‑2. |
| **Citation markup** | Transparency, trust | Slight prompt length increase | If token budget tight, use short numeric IDs only. |
| **Full‑text storage in PostgreSQL** vs. S3 | Simple ACID reads | Higher I/O | For very large corpora, shift to object store + CDN. |

---

## 8. Failure Modes & Mitigations  

| Failure | Impact | Detection | Mitigation |
|---------|--------|-----------|------------|
| **Embedding service outage** | No new queries can be vector‑searched → fallback to BM25 only. | 5xx from embedding endpoint, increased latency metric. | Circuit breaker → use cached query‑embedding (if same query repeats) or skip embedding and rely on lexical search. |
| **Vector DB node crash / partition** | Lost ANN results → degraded relevance. | Health check failure, increased query latency. | Replicated active‑passive nodes; auto‑failover to replica; warm‑up new node from persisted vectors. |
| **LLM API rate‑limit / error** | No answer generated. | HTTP 429/5xx from LLM, spike in error metric. | Exponential back‑off + fallback to a “knowledge‑base only” answer (concatenate top chunks without generation). |
| **Network partition between services** | End‑to‑end request stalls. | Timeout alerts, dropped connections. | Retries with timeout caps, return a friendly “system busy, try later” message after 3 retries. |
| **RBAC mis‑configuration** | Unauthorized data leakage. | Audit log shows user accessed disallowed doc_id. | Real‑time policy evaluation; deny‑by‑default; periodic security audit. |
| **Data corruption in chunk store** | Wrong context → hallucination. | Checksum mismatch, DB integrity alerts. | Immutable backups, restore from latest snapshot, alert. |
| **Burst traffic > capacity** | Latency spikes, queue buildup. | Queue length metric, response time > 2 s. | Autoscaling (K8s HPA) + request throttling per user; graceful degradation (reduce K from 5→3). |
| **Prompt injection (user supplies malicious text)** | LLM may produce undesirable output. | Content moderation logs, toxicity classifier. | Pre‑process user input: strip code blocks, limit special characters, enforce system prompt that overrides. |

---

## 9. Security & Compliance  

| Area | Controls |
|------|----------|
| **Transport** | mTLS between services; external traffic via API GW with TLS 1.3. |
| **At‑rest** | AES‑256 encrypted volumes (EBS, S3 SSE‑S3). |
| **Identity** | OAuth2/JWT issued by corporate IdP (Okta, Azure AD). Claims contain dept/role → filtered at query time. |
| **Authorization** | ABAC – each document carries `allowed_roles` attribute; RAG service adds `WHERE role IN $user.roles`. |
| **Audit** | Immutable audit log (Elastic + S3) with request ID, user, doc IDs accessed, timestamps. |
| **Data retention** | Ingestion pipeline tags documents with `retention_period`; periodic job deletes/archives after expiry. |
| **PII handling** | PII detection (Presidio) during ingestion; flagged chunks are **redacted** or stored in a separate “restricted” bucket with stricter ACL. |
| **Regulatory** | GDPR: provide “right to be forgotten” → delete all chunks & embeddings for a given document ID on request. |
| **Pen‑test** | Annual external security assessment, + continuous vulnerability scanning (Trivy). |

---

## 10. Monitoring, Logging & Alerting  

1. **Metrics (Prometheus)**  
   - `rag_query_latency_seconds` (histogram)  
   - `embed_latency_seconds`  
   - `vector_search_latency_seconds`  
   - `llm_token_usage_total` (prompt/completion)  
   - `requests_total{status="5xx"}`  

2. **Tracing (OpenTelemetry)** – spans: `http.request → embed → ann_search → rerank → llm_inference`.  

3. **Logs (Loki/ELK)** – JSON fields: `request_id`, `user_id`, `doc_ids`, `prompt_tokens`, `completion_tokens`, `error`.  

4. **Alert thresholds**  
   - Latency P95 > 2 s → page on-call.  
   - Error rate > 0.5 % over 5 min → page.  
   - Vector DB replica lag > 30 s → alert.  
   - LLM cost exceed budget forecast → notify finance.  

5. **Dashboards** – Grafana panels for QPS, token usage, per‑department request breakdown, storage growth.

---

## 11. Deployment & Operations  

| Layer | Tech | CI/CD |
|-------|------|-------|
| **API GW** | Kong (Docker) | GitHub Actions → Helm upgrade |
| **RAG Service** | FastAPI (Python 3.11) + Uvicorn workers | Docker build → K8s Deployment (HPA) |
| **Embedding Worker** | Celery + Redis | Separate Helm chart |
| **Vector DB** | FAISS + Faiss‑srv (gRPC) on dedicated nodes | Ansible/K8s DaemonSet |
| **LLM Proxy** | Node.js thin wrapper (rate‑limit, retries) | Deploy as side‑car |
| **Observability** | Prometheus Operator, Loki, Tempo, Alertmanager | Helm charts |
| **Secrets** | HashiCorp Vault + K8s Secrets Store CSI | Auto‑rotation pipeline |

**Blue‑Green Release** – Deploy new model version (e.g., new embedding model) to a canary subset (5 % traffic) with separate index; monitor quality before full cut‑over.

**Disaster Recovery** – Nightly snapshots of vector index and Postgres → replicated to a different AWS region; a fail‑over script can spin up a new cluster within 15 min.

---

## 12. Cost Estimate (Monthly, 10 k documents, 100 k queries)  

| Component | Approx. Cost |
|-----------|--------------|
| **Embedding (OpenAI, 20 M tokens)** | $2 |
| **LLM (GPT‑4‑Turbo, 140 M tokens)** | $660 |
| **Vector DB Nodes (2× r5.4xlarge)** | $730 (≈ $0.10/h each) |
| **ElasticSearch (2× c5.large)** | $120 |
| **K8s Compute (RAG + API GW)** | $300 |
| **Storage (EBS SSD 2 TB)** | $150 |
| **Observability (Prometheus + Grafana Cloud)** | $80 |
| **Support / Misc** | $200 |
| **Total** | **≈ $2,252** |

*If query volume grows to 300 k/month, the biggest variable is LLM cost (~$2 k). Switching to a cheaper LLM or self‑hosted model would bring the total back under $2 k.*

---

## 13. Future Enhancements  

| Idea | Benefit |
|------|---------|
| **Fine‑tune the LLM** on internal Q&A pairs → higher factuality, lower token usage. |
| **Dynamic chunk sizing** (adaptive to semantic boundaries) → fewer tokens needed for context. |
| **Graph‑based retrieval** (knowledge graph of entities) for multi‑hop reasoning. |
| **User‑feedback loop** – allow users to up‑vote/down‑vote answers; retrain reranker. |
| **Multilingual support** – multilingual embedding (LaBSE) + language detection. |
| **Edge caching** – cache popular query‑embedding + results in Redis for sub‑100 ms response. |
| **Data‑Lineage UI** – visual tool showing which docs contributed to an answer. |

---

## 14. TL;DR Architecture Summary  

- **Ingestion** → chunk → embed → store in **FAISS** + **ElasticSearch**.  
- **Query** → embed → **ANN** (FAISS) + BM25 → **rerank** → fetch raw chunks → **MMR** → build prompt → LLM → **citation** → answer.  
- **Security** via JWT RBAC, encrypted storage, audit logs.  
- **Scalable** with K8s, HPA; capacity for **1 k QPS** and **≤ 2 s** latency.  
- **Cost‑effective** for typical internal use (≈ $2 k/month at 100 k queries).  

The design balances **high relevance** (semantic + lexical retrieval + reranking) with **low latency**, **privacy**, and **operational simplicity**, while providing clear pathways to upgrade model quality or scale to larger corpora.