---

Design a retrieval-augmented generation (RAG) assistant that answers user questions based on a company's internal documents.


---

Below is a practical design for an internal-docs RAG assistant that is secure, fresh, and production-ready.

---

## 1) Goals

Build an assistant that:

- answers employee questions using **internal documents**
- provides **citations**
- respects **document permissions / ACLs**
- stays **fresh** as docs change
- avoids hallucinating when evidence is weak
- scales to enterprise usage

---

## 2) Requirements

### Functional
1. Ingest documents from internal sources:
   - Confluence / SharePoint / Google Drive / Notion / PDFs / wiki pages
2. Parse and chunk documents for retrieval
3. Retrieve relevant content for a user query
4. Generate an answer grounded in retrieved chunks
5. Show citations linking back to source docs
6. Respect per-user and per-group access controls
7. Support follow-up questions in a session
8. Collect feedback for quality improvement

### Non-functional
1. **Security first**: zero unauthorized disclosure
2. **Freshness**: changed docs searchable within minutes
3. **Latency**: fast enough for chat UX
4. **Availability**: 99.9% target
5. **Auditability**: know what sources were used for each answer
6. **Cost control**: LLM calls dominate cost, so optimize them

---

## 3) Assumptions and capacity planning

Let's size a realistic mid/large enterprise deployment.

### Corpus assumptions
- **500,000 documents**
- average **3,000 tokens/doc**
- total corpus = **1.5B tokens**
- 10% are PDFs/scans needing OCR

### Chunking
Use:
- chunk size = **400 tokens**
- overlap = **80 tokens**
- stride = **320 tokens**

Estimated chunks:

\[
1.5B / 320 \approx 4.69M \text{ chunks}
\]

Call it **4.7 million chunks**.

### User traffic assumptions
- **10,000 DAU**
- **20 queries/user/day**
- total = **200,000 queries/day**

Average QPS:

\[
200,000 / 86,400 \approx 2.3 \text{ QPS}
\]

Enterprise traffic is bursty. Design for:
- **15 QPS steady peak**
- **50 QPS short bursts**

### Latency targets
- p95 first token: **< 1.5 s**
- p95 full answer: **< 6 s**
- retrieval stage: **< 300 ms**
- freshness:
  - 95% doc updates searchable within **5 min**
  - ACL tightening effective within **1 min**

---

## 4) High-level architecture

```mermaid
graph LR
  subgraph Sources
    C1[Confluence]
    C2[SharePoint / Google Drive]
    C3[PDFs / Wikis / Notion]
    C4[Jira / Internal KB]
  end

  subgraph Ingestion
    W[Connectors + Webhooks]
    Q[(Queue / Kafka)]
    P[Parser + OCR]
    N[Normalizer + Deduper]
    CH[Chunker + Metadata Enricher]
    EM[Embedding Service]
    IW[Index Writer]
  end

  subgraph Storage
    OBJ[(Object Store: raw docs)]
    PG[(Postgres: metadata, versions, ACL cache)]
    SRCH[(Search Cluster: BM25 + Vector Index)]
    REDIS[(Redis: session/cache)]
  end

  subgraph Serving
    UI[Web / Slack / API Client]
    GW[API Gateway]
    AUTH[SSO / OIDC]
    ORCH[RAG Orchestrator]
    ACL[ACL Resolver]
    QR[Query Rewriter]
    RR[Reranker]
    PB[Prompt Builder]
    LLM[Private LLM Endpoint]
    OBS[Logs / Metrics / Traces / Eval]
  end

  C1 --> W
  C2 --> W
  C3 --> W
  C4 --> W
  W --> Q --> P --> N --> CH --> EM --> IW --> SRCH
  P --> OBJ
  CH --> PG

  UI --> GW --> AUTH --> ORCH
  ORCH <--> REDIS
  ORCH --> ACL
  ORCH --> QR
  ORCH --> SRCH
  SRCH --> RR --> PB --> LLM
  LLM --> UI
  ORCH --> OBS
  IW --> OBS
```

---

## 5) Core design decisions

### Decision 1: Use **hybrid retrieval**
Use both:
- **BM25 / lexical search**
- **dense vector search**

Why:
- internal enterprise queries often contain acronyms, product names, incident IDs, policy codes
- vector search catches semantics
- BM25 catches exact terms and rare keywords

**Tradeoff:** a bit more complexity, but usually materially better recall than vector-only.

### Decision 2: Enforce **ACL twice**
1. **Pre-filter** in search index using indexed ACL metadata
2. **Final live ACL re-check** before sending chunks to the LLM

Why:
- stale index ACLs can leak data
- final check makes system fail-closed

### Decision 3: Always answer with **citations or abstain**
If retrieved evidence is weak, return:
- “I couldn’t find enough support in authorized documents”
- plus top sources or clarifying question

This is critical for trust.

### Decision 4: Separate **ingestion** from **serving**
Ingestion is asynchronous and versioned.
Serving only reads “active” indexed versions.

This prevents partially indexed docs from producing broken answers.

---

## 6) Ingestion and indexing pipeline

## 6.1 Connectors
Sources use:
- webhook/event-driven updates when supported
- periodic delta sync as backup
- nightly full reconciliation for correctness

Each discovered doc produces an event:
- `source_id`
- `doc_id`
- `version`
- `modified_at`
- `acl_info`

### Why both webhook + polling?
Webhooks miss events sometimes. Polling repairs gaps.

---

## 6.2 Parsing and normalization
For each document:
1. fetch raw content
2. extract text
3. OCR scanned pages if needed
4. preserve structure:
   - title
   - headings
   - table boundaries
   - links
   - page numbers
5. normalize:
   - remove boilerplate
   - detect language
   - compute checksum
   - dedupe near-identical copies

Store raw/original files in object storage.

### Important
A lot of RAG quality comes from parsing quality. Slides, tables, and PDFs are common failure points.

---

## 6.3 Chunking strategy
Use **structure-aware chunking**, not fixed blind splits.

Rules:
- target **300–500 tokens**
- prefer heading/paragraph boundaries
- don't split tables/code blocks if possible
- attach metadata to each chunk:
  - `doc_id`
  - `version_id`
  - `chunk_id`
  - `title`
  - `heading_path`
  - `source_type`
  - `owner`
  - `modified_at`
  - `security_label`
  - `acl_principals`
  - `canonical_url`
  - `authority_score`

### Why not giant chunks?
Large chunks:
- reduce retrieval precision
- waste context window
- increase generation cost

### Why not tiny chunks?
Tiny chunks:
- lose context
- hurt answer completeness

400 tokens is a good default.

---

## 6.4 Embeddings
Use a **multilingual enterprise-safe embedding model**, ideally private-hosted.

Assume:
- 1024-dimensional vectors
- float16 storage

Storage per vector:

\[
1024 \times 2 \text{ bytes} = 2048 \text{ bytes} = 2 KB
\]

For 4.7M chunks:

\[
4.7M \times 2KB \approx 9.4GB
\]

---

## 6.5 Indexing
Use a search engine that supports:
- BM25
- ANN vector search
- metadata filters
- ACL filters

Good fits:
- **OpenSearch**
- **Vespa**
- Elasticsearch with vector support

For this design, I’d choose **OpenSearch** for simplicity and mature hybrid support.

### Why one search system instead of separate vector DB + lexical engine?
Pros:
- simpler hybrid ranking
- simpler ACL filtering
- fewer moving parts
- easier ops

Cons:
- a dedicated vector DB may outperform at very large scale

At **4.7M chunks**, one search cluster is reasonable.

---

## 6.6 Versioning and activation
Each doc has states:
- discovered
- parsed
- chunked
- embedded
- indexed
- active

Only **active** versions are queryable.

For updates:
- index new version fully
- atomically flip active pointer
- retire old version later

This avoids partial answers from half-indexed docs.

---

## 6.7 Deletions and ACL changes
Two critical paths:

### Document deletion
- mark doc tombstoned immediately in metadata DB
- remove from search index asynchronously
- query service excludes tombstoned docs immediately

### ACL tightening
- update ACL metadata immediately
- push invalidation to ACL cache
- final live ACL check blocks access even if search index is stale

This is one of the most important leak-prevention mechanisms.

---

## 7) Query serving pipeline

```mermaid
sequenceDiagram
  actor U as User
  participant API as API Gateway
  participant O as RAG Orchestrator
  participant A as Auth/ACL
  participant S as Search
  participant R as Reranker
  participant L as LLM

  U->>API: Ask(query, session_id)
  API->>A: Validate JWT / fetch groups
  API->>O: user, groups, query

  O->>O: classify query / rewrite follow-up
  par Hybrid retrieval
    O->>S: BM25 search + ACL prefilter
    O->>S: Vector search + ACL prefilter
  end
  S-->>O: top candidates
  O->>R: rerank top 50
  R-->>O: top 8-10 chunks
  O->>A: final live ACL check
  A-->>O: allowed chunks only
  O->>L: grounded prompt with citations required
  L-->>U: streamed answer + citations
  O->>O: log traces / feedback / metrics
```

---

## 8) Retrieval and ranking design

## 8.1 Query preprocessing
Before retrieval:
- normalize query
- expand company acronyms using glossary
- rewrite follow-up questions into standalone form
- detect filters if obvious:
  - date range
  - source type
  - department
  - “policy only”, “latest doc”, etc.

Use a small model or rules for this step.

---

## 8.2 Candidate retrieval
Retrieve:
- top **100 BM25** chunks
- top **100 vector** chunks

Fuse them with **Reciprocal Rank Fusion (RRF)**.

Why RRF?
- simple
- robust
- works well when combining retrieval methods

---

## 8.3 Reranking
Take top **50 fused candidates** and rerank with a cross-encoder.

Why rerank?
- first-stage retrieval optimizes recall
- reranker improves precision significantly

Typical extra latency:
- ~100–250 ms depending on hardware/model

Tradeoff:
- better answers
- more latency/cost

For chat UX, it's usually worth it.

---

## 8.4 Diversification and context packing
After reranking:
- remove duplicates
- apply MMR/diversification
- prefer authoritative and recent sources
- merge adjacent chunks from same section if useful

Pack final context:
- **6–10 chunks**
- target **1,500–2,000 input context tokens**

Why not dump 20+ chunks into the LLM?
- cost rises quickly
- answer quality often gets worse
- context noise increases hallucination risk

---

## 8.5 Prompting
Prompt rules:
- only answer from retrieved documents
- treat retrieved docs as **untrusted content**, not instructions
- cite every key claim
- if evidence is insufficient, say so
- mention conflicting sources if found

This is also where we defend against **prompt injection inside documents**.

---

## 8.6 Output format
Prefer structured model output internally:

```json
{
  "answer": "...",
  "citations": [
    {"chunk_id": "c123", "doc_id": "d12", "url": "..."}
  ],
  "confidence": 0.81,
  "needs_clarification": false
}
```

UI can render:
- natural language answer
- source cards
- “open document” links
- warning if evidence is weak

---

## 9) Access control design

This system lives or dies on permission correctness.

## 9.1 ACL model
Index ACL metadata at document/chunk level:
- allowed users
- allowed groups
- security label

In practice, most enterprise docs are group-based, not user-based.

Assume:
- average user belongs to **30 groups**
- average doc has **5 ACL principals**

That makes query-time filtering feasible.

---

## 9.2 Enforcement strategy
### At retrieval time
Search query includes ACL filter:
- `allowed_groups intersects user_groups`
- or `allowed_users contains user_id`

### Before LLM call
For final top chunks, perform fresh authorization against metadata/ACL service.

**Fail closed**:
- if ACL service is unavailable, do not send that chunk to the LLM

This may reduce answer quality temporarily, but prevents leakage.

---

## 9.3 Why final re-check matters
If a SharePoint permission changed 30 seconds ago:
- search index may still be stale
- metadata DB / ACL service may already be updated

Without final re-check, you can leak confidential content.

---

## 10) Storage sizing

## 10.1 Search index footprint
For **4.7M chunks**:

### Stored chunk text
Assume ~2 KB/chunk average text:

\[
4.7M \times 2KB \approx 9.4GB
\]

### Embeddings
\[
4.7M \times 2KB \approx 9.4GB
\]

### ANN graph overhead
Roughly **8 GB**

### BM25 / inverted index
Roughly **15 GB**

### Metadata
Roughly **5 GB**

### Total primary data
\[
9.4 + 9.4 + 8 + 15 + 5 \approx 46.8GB
\]

Call it **47 GB primary**.

With **1 replica**:
- **94 GB**

With **30% headroom**:
- **122 GB**

### Search cluster recommendation
Start with:
- **6 data nodes**
- spread across **3 AZs**
- each node: **16 vCPU, 64 GB RAM, 500 GB SSD**

This is more than enough for current size and gives room for growth, reindexing, and failures.

---

## 10.2 Object storage
Assume average original file size **1 MB/doc**:

\[
500,000 \times 1MB = 500GB
\]

With version history and OCR artifacts, plan for **1–2 TB**.

---

## 10.3 Metadata DB
Postgres stores:
- doc registry
- versions
- chunk metadata
- source sync checkpoints
- ACL cache
- audit records pointers

This is modest: likely tens of GB, not hundreds.

---

## 11) LLM throughput and cost

Assume per query:
- input context + prompt = **2,000 tokens**
- output = **300 tokens**
- total = **2,300 tokens/query**

At **200,000 queries/day**:

\[
200,000 \times 2,300 = 460M \text{ tokens/day}
\]

Split:
- input = **400M/day**
- output = **60M/day**

If using a private endpoint priced roughly at:
- $1 / 1M input tokens
- $3 / 1M output tokens

Daily cost:

- input: **$400/day**
- output: **$180/day**
- total: **$580/day**, about **$17K/month**

This is why:
- retrieval optimization matters
- context packing matters
- query classification matters
- smaller default models often make sense

### Cost optimization levers
1. use a smaller default answer model
2. use a better reranker to reduce context size
3. cache popular answers
4. return search results instead of full generation for navigational queries
5. escalate to bigger model only when needed

---

## 12) Reliability and failure modes

## 12.1 Likely failure modes

### 1. Unauthorized data leakage
Cause:
- stale ACLs
- bad filters
- model sees restricted chunk

Mitigations:
- retrieval ACL filter
- live ACL re-check before LLM
- fail closed
- audit every cited chunk

### 2. Stale or deleted docs still appear
Cause:
- connector missed delete event
- index lag

Mitigations:
- tombstone in metadata immediately
- nightly reconciliation
- serving excludes tombstoned docs even before physical index delete

### 3. Retrieval misses the right doc
Cause:
- acronyms
- exact codes
- bad chunking
- embeddings weak on enterprise jargon

Mitigations:
- hybrid retrieval
- glossary/synonym expansion
- better chunking
- reranker
- source authority weighting

### 4. Hallucinated answers
Cause:
- weak retrieved evidence
- too much noisy context
- model overconfident

Mitigations:
- citations required
- confidence thresholds
- abstain when support weak
- keep context compact
- optional answer verifier

### 5. Prompt injection from documents
Cause:
- malicious or accidental text inside docs: “ignore prior instructions”

Mitigations:
- instruct model docs are data, not instructions
- sanitize HTML/scripts
- source trust weighting
- block sensitive tool use in answer path

### 6. OCR/parsing errors
Cause:
- scans, tables, images

Mitigations:
- parser quality monitoring
- source preview in UI
- specialized PDF/table parsing
- flag low-quality extraction

### 7. LLM outage or high latency
Mitigations:
- fallback to top source snippets
- lexical-only fallback mode
- circuit breaker
- multiple model endpoints if possible

### 8. Search cluster hot spots
Cause:
- poor shard distribution
- skew from popular terms

Mitigations:
- balanced sharding
- query/result cache
- autoscaling
- load testing

---

## 13) Caching

### Good candidates
1. **Query embedding cache**
   - key: normalized query text
   - high hit rate for common queries

2. **Retrieval cache**
   - key: normalized query + filter set + ACL hash
   - TTL short, e.g. 5 minutes

3. **Session summary cache**
   - Redis
   - TTL 24h

### Answer caching
Possible but risky because of:
- freshness
- ACL differences
- changing source documents

If used, key it by:
- normalized query
- user/group ACL signature
- source freshness watermark

Short TTL only.

---

## 14) Observability and evaluation

## 14.1 Metrics
Track:
- QPS
- p50/p95 latency per stage
- empty result rate
- ACL-filtered candidate count
- citation count per answer
- low-confidence abstain rate
- index freshness lag
- connector error rate
- hallucination / unsupported answer rate from audits

## 14.2 Offline evaluation
Build a golden set:
- ~1,000 real internal questions
- human-validated relevant docs
- target answers

Measure:
- Recall@20
- nDCG@10
- citation precision
- answer correctness
- groundedness
- ACL correctness

## 14.3 Online evaluation
Collect:
- thumbs up/down
- “opened source doc”
- reformulation rate
- abandonment rate
- escalation to human/support
- time saved

---

## 15) Security and compliance

1. **SSO/OIDC** for user auth
2. **mTLS** for service-to-service calls
3. **KMS-backed encryption at rest**
4. private networking to model endpoints
5. audit log:
   - user
   - query
   - docs retrieved
   - docs cited
   - model version
6. configurable retention for conversations
7. default policy: **customer data not used for model training**
8. redact secrets/PII from logs

If compliance is very strict, host embeddings + answer model entirely inside company VPC or on-prem.

---

## 16) Suggested API surface

### User ask
`POST /v1/answer`
```json
{
  "session_id": "s1",
  "query": "What is the remote work reimbursement policy?",
  "filters": {
    "source_types": ["policy", "wiki"]
  }
}
```

Response: streaming tokens + final citations.

### Feedback
`POST /v1/feedback`
```json
{
  "session_id": "s1",
  "answer_id": "a55",
  "rating": "down",
  "reason": "outdated"
}
```

### Citation fetch
`GET /v1/citations/{id}`

---

## 17) Recommended rollout plan

### Phase 1: MVP
- Confluence + Drive
- batch + webhook ingestion
- hybrid retrieval
- citations
- ACL enforcement
- one answer model
- feedback buttons

### Phase 2
- reranker
- better PDF/table extraction
- session-aware follow-ups
- glossary expansion
- admin dashboards

### Phase 3
- answer verifier
- source trust scoring
- multilingual support
- specialized retrievers for code/tables/tickets

---

## 18) Final recommended architecture

If I were building this today, I would choose:

- **Connectors**: webhook + periodic sync
- **Raw storage**: object store
- **Metadata**: Postgres
- **Search**: OpenSearch for BM25 + vector + filters
- **Queue**: Kafka/SQS
- **Cache/session**: Redis
- **Reranker**: cross-encoder service
- **LLM**: private-hosted or private endpoint
- **Security**: double ACL enforcement, fail closed
- **Quality bar**: answer only with citations; otherwise abstain

The most important product decisions are:

1. **hybrid retrieval, not vector-only**
2. **final live ACL check before generation**
3. **structured chunking and authoritative source ranking**
4. **compact context + reranking**
5. **citations-or-abstain behavior**

That gives you a RAG assistant that is not just impressive in demos, but safe and reliable in a real company.

If you want, I can also turn this into:
- an **interview-style system design answer**
- a **1-page architecture summary**
- or a **deep dive on ACL-safe retrieval**, which is usually the hardest part.