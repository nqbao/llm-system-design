---

Design a terminal-based AI coding agent that can complete software engineering tasks.


---

## Terminal‑Based AI Coding Agent – System Design Document  

**Goal** – Build a CLI‑driven “AI coding assistant” that can understand a software project, answer developer questions, write/modify code, run tests, and optionally push changes. It must feel instantaneous, be usable on a developer’s workstation, and optionally scale to a shared service for many engineers.

---

### 1.  High‑Level Overview  

| Component | Primary Responsibility |
|-----------|------------------------|
| **CLI Front‑End** | Parses commands, streams chat‑style I/O, shows diffs, invokes actions (`ask`, `edit`, `test`, `commit`). |
| **Session Orchestrator** | Maintains per‑user context (history, open files), routes requests to downstream services, handles retries. |
| **Code‑Base Manager** | Clones/pulls the target repository, watches files, chunk‑splits source code, produces embeddings, keeps the index in sync. |
| **Embedding Service** | Runs a lightweight embedding model (e.g., MiniLM‑v2, sentence‑transformers) to produce vectors for each code chunk. |
| **Vector Store** | Stores vectors and metadata; enables similarity search for “relevant context”. |
| **Prompt Builder** | Collects relevant chunks, recent chat history, system instructions, and constructs the full prompt for the LLM. |
| **LLM Inference Layer** | Calls either a remote LLM (OpenAI, Anthropic, Cohere) **or** a locally hosted model (e.g., LLaMA‑2‑70B‑Chat, CodeLlama‑34B). |
| **Execution Sandbox** | Isolated Docker container (or gVisor/Firecracker) that can compile / run the code and unit tests safely. |
| **Test Runner** | Detects the project’s test framework (pytest, jest, go test…) and invokes it inside the sandbox, returning a structured report. |
| **CI/VD Integration** | Optional step that pushes a branch and opens a pull‑request, or triggers a remote CI pipeline. |
| **Cache & Rate‑Limiter** | Redis cache for recent prompts & LLM responses, and a token‑rate limiter to protect downstream APIs. |
| **Telemetry & Logging** | Centralised logs (ELK / Loki), metrics (Prometheus) for latency, error rates, token usage, sandbox crashes, etc. |
| **Auth & Security** | SSH key handling for repo access, signed JWT for API calls, sandbox hardening, secret redaction. |

The data flow is illustrated in the mermaid diagram below.

---

### 2.  Functional Requirements  

1. **Project Understanding** – Provide accurate answers about definitions, call graphs, and architecture.  
2. **Code Generation / Editing** – Write new files, modify existing ones, and show a color‑coded diff.  
3. **Test Execution** – Run unit/integration tests after a modification and surface failures.  
4. **Version Control** – Stage, commit, create branches, and optionally open a PR.  
5. **Contextual Recall** – Preserve conversation history and automatically augment prompts with relevant code snippets.  
6. **Multi‑Language Support** – At least JavaScript/TypeScript, Python, Go, Java, and C#.  
7. **Offline Mode** – Ability to run with a locally hosted LLM for data‑privacy or disconnected environments.  
8. **Safety** – Prevent the LLM from suggesting insecure patterns (e.g., hard‑coded secrets) and sandboxed execution must be escape‑proof.  

---

### 3.  Non‑Functional Requirements  

| Requirement | Target |
|-------------|--------|
| **Latency** (user sees a suggestion) | ≤ 2 s for most queries, ≤ 5 s for heavy generation (≥ 500 tokens). |
| **Throughput** | 100 concurrent sessions × 2 requests / sec (≈ 200 RPS) without degradation. |
| **Availability** | 99.9 % (≤ 8.8 h downtime per year). |
| **Scalability** | Horizontal scaling of LLM inference nodes and vector store. |
| **Security** | Zero‑network sandbox, limited‑privilege repo access, secrets redaction, audit logs. |
| **Cost** | ≤ $0.10 per user‑hour of active usage (including LLM API cost, compute, storage). |
| **Observability** | 99 % of errors captured in metrics and logs; alert on latency > 5 s, sandbox crash rate > 1 %. |
| **Extensibility** | Plug‑in architecture for new LLM providers, test frameworks, or language servers. |

---  

### 4.  Detailed Architecture  

```mermaid
flowchart TD
    subgraph User
        A[Terminal CLI] -->|JSON RPC| B[Session Orchestrator]
    end

    subgraph Core
        B --> C[Command Router]
        C -->|Ask/Edit| D[Prompt Builder]
        C -->|Test| E[Execution Sandbox]
        C -->|Commit| F[Git Ops]

        D --> G[Vector Store]
        D --> H[LLM Inference Layer]

        G --> I[Embedding Service]
        I --> J[Code‑Base Manager]

        H --> K[Remote LLM API]
        H --> L[Local LLM Server]

        E --> M[Sandbox Runtime (Docker)]
        M --> N[Test Runner]
    end

    subgraph Persistence
        J --> O[Repo Storage (Git)]
        G --> P[Vector DB (PGVector/Weaviate)]
        B --> Q[Redis Cache]
        B --> R[PostgreSQL Metadata]
        B --> S[Telemetry (Prometheus/ELK)]
    end

    style User fill:#E3F2FD
    style Core fill:#E8F5E9
    style Persistence fill:#FFF3E0
```

#### Component Details  

| Component | Tech Choices (pros/cons) | Capacity / Sizing |
|-----------|--------------------------|-------------------|
| **CLI Front‑End** | Python Click / Typer, or Rust `clap`. <br>Pros: fast startup, easy packaging. <br>Cons: Rust binary larger; Python easier for plugin dev. | Minimal – < 10 ms per command parse. |
| **Session Orchestrator** | Node.js/Express or Go Gin (HTTP‑JSON RPC). <br>Pros: excellent concurrency (Go). <br>Cons: Node easier for async LLM calls. | 1 vCPU & 2 GB RAM per 1 k concurrent sessions (state stored in Redis). |
| **Code‑Base Manager** | `git2-rs` (Rust) or `GitPython`. <br>Runs on a file‑watcher (inotify/FSNotify). | Pull & index a 2 M LOC repo in < 30 s on a single CPU core. |
| **Embedding Service** | MiniLM‑v2 (384‑dim) via ONNX Runtime (CPU). <br>Cost: ~0.5 ms / 1 KB chunk. | For 10 k chunks → 5 s total; 200 RPS feasible on 4‑core CPU. |
| **Vector Store** | **PGVector** (PostgreSQL) or **Qdrant**. <br>Pros: ACID, easy backup. <br>Cons: Qdrant faster for > 100 k vectors. | 10 k vectors × 384×4 B = 15 MB. < 1 GB RAM with 100 k vectors. |
| **Prompt Builder** | Python Jinja2 templating; token‑aware truncation using `tiktoken`. | O(1 ms) per request. |
| **LLM Inference Layer** | 1️⃣ Remote: OpenAI GPT‑4‑32k (price $0.03/1k prompt, $0.06/1k completion). <br>2️⃣ Local: LLaMA‑2‑70B‑Chat (8‑bit quantized) on Nvidia A100. | **Remote** – throughput limited by rate‑limit (≈ 350 RPS per API key). <br>**Local** – 1 A100 ≈ 30 gen‑RPS for 4k‑token prompts. |
| **Execution Sandbox** | Docker + `gVisor` + seccomp + cgroup limits (CPU 2 cores, RAM 4 GB, timeout 30 s). | 0.1 s start‑up (container reuse pool) + test time. |
| **Cache** | Redis (cluster mode) with LRU eviction. | 256 GB RAM can hold ~1 M prompt‑response pairs (average 2 KB each). |
| **Telemetry** | Prometheus + Grafana dashboards. Alertmanager for SLA breaches. | Minimal overhead (< 5 ms per request). |
| **Auth** | SSH‑agent forwarding for repo clone; JWT signed with RSA‑2048 for internal services. | Negligible compute. |
| **CI/VD** | GitHub Actions API or Azure DevOps; just a webhook trigger. | Async – not part of latency budget. |

---

### 5.  Capacity & Cost Calculations  

#### 5.1  Typical Workload  

| Metric | Assumption | Value |
|--------|------------|-------|
| **Average Codebase** | 2 M LOC ≈ 15 GB plain text | 15 GB |
| **Chunk Size** | 500 lines ≈ 2 KB per chunk | 7 500 chunks |
| **Embedding Dim** | 384 (MiniLM‑v2) | 384 × 4 B = 1.5 KB per vector |
| **Vector DB Size** | 7 500 × 1.5 KB ≈ **11 MB** | + metadata ≈ 20 MB |
| **LLM Prompt Length** | 3 k tokens (≈ 12 KB) – includes 10 relevant chunks + 1 k chat history | 12 KB |
| **LLM Completion** | 500 tokens (≈ 2 KB) | 2 KB |
| **Tokens per request** | 3 500 tokens | 0.0035 M tokens |

#### 5.2  Throughput & Latency  

| Component | Latency (p95) | Max RPS (single instance) |
|-----------|---------------|---------------------------|
| **Embedding** (CPU) | 0.5 ms per chunk | 2 k chunk‑embeds / s (≈ 100 RPS) |
| **Vector Search** (PGVector) | 8 ms (10 k vectors) | 100 RPS |
| **LLM Generation (remote)** | 800 ms (GPT‑4 8k) | 1.2 RPS per API key |
| **LLM Generation (local A100)** | 1.2 s (70B, 4k) | ≈ 30 RPS |
| **Sandbox + Test** | 0.1 s spin‑up + avg test 1 s | 10 RPS per container pool (size = 4) |

**Target 200 RPS**  

*Remote mode*: 200 RPS × 0.0035 M token = 0.7 M tokens/s ≈ 0.7 × $0.06 / 1k ≈ $42 / hour → too expensive.  
**Solution** – pool multiple API keys (≈ 50 keys) or **use local inference**.

*Local mode*: 2 A100 GPUs = 2 × 30 RPS = 60 RPS. To hit 200 RPS we need **~7 A100** nodes (or a mix of smaller GPUs). However, the practical usage pattern for a terminal agent is bursty: a single developer rarely exceeds 1 RPS, so a **single A100** comfortably serves **≤ 30 simultaneous developers**. For a SaaS offering with 100 concurrent devs, a **2‑node (2 A100 each) cluster** provides headroom and redundancy.

#### 5.3  Storage  

| Data | Size |
|------|------|
| Repo clone (bare) | 15 GB |
| Vector store | 30 GB (including indexes, backups) |
| Redis cache | 256 GB (max) |
| Logs (30 days) | 10 GB |
| **Total** | **~311 GB** on SSD (NVMe) → 0.5 TB provisioned for safety. |

#### 5.4  Cost (AWS example)  

| Service | Qty | Unit Cost (per hour) | Hourly Total |
|---------|-----|----------------------|--------------|
| **c6i.large** (Orchestrator) | 2 | $0.085 | $0.17 |
| **r6g.xlarge** (Vector DB) | 1 | $0.266 | $0.27 |
| **p4d.24xlarge** (8 × A100) | 1 | $32.77 | $32.77 |
| **ElasticCache (Redis)** | 1 (cache.t4g.medium) | $0.053 | $0.05 |
| **EBS gp3 1 TB** | 1 | $0.08/GB‑mo ≈ $0.11/hr | $0.11 |
| **Data Transfer** | – | – | $0.05 |
| **Total** | – | – | **≈ $33.5 / hr** |

If using **OpenAI GPT‑4** instead of local GPU (cost $0.06/1k tokens), typical usage of **0.5 M tokens/hr** = **$30/hr** in API charges, plus much cheaper compute. The sweet‑spot is therefore **remote LLM + cheap CPU for embeddings**, unless strict data‑privacy or offline requirements demand local models.

---

### 6.  Trade‑off Analysis  

| Decision | Pro | Con | When to Choose |
|----------|-----|-----|----------------|
| **Remote LLM (API)** | • No infra to manage.<br>• Latest model updates.<br>• Pay‑as‑you‑go. | • Data leaves premises.<br>• Variable latency, rate‑limit.<br>• Higher per‑token cost at scale. | Small teams, privacy‑neutral code, low budget, quick MVP. |
| **Local LLM (GPU)** | • Full control of data.<br>• Predictable latency.<br>• Fixed cost after purchase. | • High upfront GPU cost.<br>• Need to keep model updated.<br>• Inferencing engineering effort. | Enterprises with IP constraints, heavy usage (> 10 k RPS), or offline environments. |
| **Full‑repo embedding vs On‑the‑fly** | • Fast retrieval, constant‑time search.<br>• Enables “semantic code search”. | • Index stale after commits → need background re‑index.<br>• Storage overhead. | Large projects (> 500 k LOC) where search speed matters. |
| **Docker sandbox vs Firecracker micro‑VM** | Docker is easier to spin up; many devs already trust it. | Slightly larger attack surface. | Development / low‑risk environments. |
| **Cache LLM responses** | Reduces latency on repeated queries; saves tokens. | Risk of serving stale answers if code changes. | For static “explain this function” queries; cache TTL 5–10 min. |
| **Prompt truncation vs Summarization** | Truncation cheap, deterministic. | May lose critical context. | Use summarization (LLM‑generated) for > 4 k token contexts; trade latency for relevance. |
| **Single‑node vs Multi‑region deployment** | Simpler ops. | Higher latency for remote devs; no geo‑redundancy. | Internal corporate network → single‑node is fine. |

---

### 7.  Failure Modes & Mitigation Strategies  

| Failure Scenario | Impact | Detection | Mitigation |
|------------------|--------|-----------|------------|
| **LLM API outage / rate‑limit** | No new suggestions, loss of function. | HTTP 5xx / 429; latency > 5 s. | – Switch to cached responses.<br>– Fallback to smaller locally hosted model (e.g., LLaMA‑2‑7B).<br>– Exponential back‑off & retry. |
| **Local GPU OOM / driver crash** | Generation stalls, node unusable. | GPU utilisation 100 % + kernel logs.<br>Prometheus alerts. | – Auto‑restart pod/instance.<br>– Use GPU health‑check sidecar.<br>– Keep spare node for fail‑over. |
| **Embedding index corruption** | Retrieval returns wrong context → bad code suggestions. | Vector DB health‑check fails; checksum mismatch. | – Periodic snapshot & verification.<br>– Re‑index on demand (background job). |
| **Sandbox escape / long‑running process** | Potential host compromise. | Container exit code non‑zero, resource usage spikes. | – Use gVisor + seccomp + setuid restrictions.<br>– Enforce strict timeout (30 s).<br>– Monitor cgroup CPU/RAM usage; kill on breach. |
| **Git repository divergence** | Agent modifies wrong version, merge conflicts. | Commit hash mismatch between cached index and HEAD. | – Verify git HEAD before each edit.<br>– Auto‑rebase or prompt user to pull. |
| **Secrets leakage via prompts** | Exfiltration of API keys, passwords. | Regex scanning of prompt content for patterns like `AKIA...` before sending to LLM. | – Redaction engine applied to all outgoing prompts.<br>– Disallow `env` variables in code that are not explicitly whitelisted. |
| **High latency > 5 s** | Poor UX, user abandons. | Prometheus latency SLO violation. | – Auto‑scale LLM pods or add more API keys.<br>– Serve “partial suggestions” from cache.<br>– Show progress bar & allow cancel. |
| **Disk exhaustion** | System crashes, loss of repo. | Disk usage > 80 % alerts. | – Rotate old repo clones, keep shallow clones.<br>– Use auto‑purge policy for caches older than 30 days. |

---

### 8.  Security & Privacy  

1. **Zero‑Network Sandboxing** – All code execution happens inside a container with **`--network=none`**. No outbound connections are allowed.  
2. **Secret Redaction** – Before any prompt leaves the orchestrator, a pipeline runs:  
   - Regex for common secret patterns (AWS keys, JWT, RSA private key blocks).  
   - `git-crypt` detection for encrypted files.  
   - Replace with `<REDACTED>` tokens.  
3. **Principle of Least Privilege** – The service account used for repo cloning only has read access; commit‑push rights require explicit user OAuth token passed as a one‑time secret.  
4. **Audit Trail** – Every LLM request/response is logged with a hash of the user‑ID, timestamp, and repository snapshot identifier (not the raw code). Logs are immutable (write‑once storage).  
5. **Model Data Isolation** – In local‑model mode, the model weights are stored on encrypted EBS volumes; only the inference process can read them.  
6. **Compliance** – The system can be configured to run entirely within a VPC with no internet egress (local mode) to meet GDPR/CCPA where code is personal data.  

---

### 9.  Deployment & Operations  

| Step | Tooling | Reason |
|------|---------|--------|
| **Containerization** | Docker multi‑stage build (CLI binary + runtime). | Guarantees identical dev/prod environment. |
| **Orchestration** | Kubernetes (EKS/GKE) with **Helm** chart. | Auto‑scaling, self‑healing, secrets management via KMS. |
| **CI for Agent** | GitHub Actions – build, unit tests, integration tests (run sandbox). | Fast feedback loop. |
| **Blue‑Green Deploy** | Use separate namespace for new version; traffic split via Ingress. | Zero‑downtime upgrades. |
| **Observability Stack** | Prometheus + Grafana dashboards for latency, error rates, token usage; Loki for logs; Alertmanager for SLA breaches. | Rapid incident detection. |
| **Backup & DR** | Daily snapshots of PostgreSQL + EBS volume; cross‑region replication. | Data durability. |
| **Scaling Policies** | – **Horizontal Pod Autoscaler (HPA)** on CPU & request latency for orchestrator.<br>– **Cluster Autoscaler** for GPU nodes based on LLM queue length. | Cost‑efficient elasticity. |
| **Secrets Management** | AWS Secrets Manager / HashiCorp Vault – stores OpenAI API keys, repo OAuth tokens, SSH private keys. | Rotation & audit. |

---

### 10.  Example Interaction Flow  

1. **User** runs: `ai-code ask "How does the authentication middleware work?"` in the repo root.  
2. **CLI** sends a JSON RPC request to the **Session Orchestrator** (`/ask`).  
3. **Orchestrator** checks recent chat history, asks **Embedding Service** for the vector of the user query (using the same model).  
4. **Vector Store** returns the top‑5 most similar code chunks (each ≤ 2 KB).  
5. **Prompt Builder** creates a system prompt: *“You are a senior software engineer. Use the following code snippets as context…”* and appends the user question.  
6. **LLM Inference Layer** calls the selected model (remote or local).  
7. **LLM** returns a Markdown answer, possibly with a code excerpt.  
8. **CLI** prints the answer, highlights code blocks, and asks *“Apply the suggested change? (y/N)”*.  
9. If the user accepts, the **CLI** sends an `edit` command:  
   - **Prompt Builder** now includes a *“generate diff for file X”* instruction.  
   - **LLM** returns a diff.  
   - **CLI** displays a colorized diff, asks for confirmation.  
   - On confirm, **Git Ops** stages the change, makes a commit.  
10. **User** runs `ai-code test` → Orchestrator launches **Execution Sandbox**, runs the project’s test suite, streams back a JUnit‑style report.  
11. If all tests pass, user can run `ai-code push` to open a PR via GitHub API.  

All steps respect the latency budget (< 2 s for generation, ≤ 1 s for search, ≤ 3 s for test runner on small suites).

---

### 11.  Extensibility & Future Work  

| Feature | How to Add |
|---------|------------|
| **Support for additional LLM providers** | Implement a new `LLMAdapter` conforming to the internal `generate(prompt, maxTokens)` interface. |
| **GraphQL code‑query language** | Build a resolvers layer on top of the vector store, expose a `/graphql` endpoint for IDE plugins. |
| **IDE Plugin** (VS Code) | Reuse the same backend API via WebSocket; UI for inline suggestions. |
| **Continuous Learning** | Capture corrected diffs, feed back via supervised fine‑tuning pipeline (privacy‑preserving). |
| **Advanced Refactoring** | Combine LLM output with static analysis (AST transformation) using `tree‑sitter`. |
| **Multi‑repo context** | Extend the index to include dependency graphs, fetch remote packages on demand. |
| **Policy Enforcement** | Plug‑in a security‑policy engine (e.g., OPA) that validates LLM‑generated code before commit. |

---

### 12.  Summary  

- **Core idea** – a CLI that orchestrates **(a) code‑base semantic search**, **(b) LLM‑driven generation**, and **(c) safe execution**.  
- **Latency** ≤ 2 s for most developer interactions; **throughput** scalable from a single‑user workstation to a 100‑user SaaS cluster.  
- **Cost** can be kept under **$0.10 per active hour** by using a local GPU for inference or by batching remote‑LLM calls.  
- **Safety** is enforced at three levels: prompt redaction, sandboxed execution, and audit‑logging.  
- **Trade‑offs** are clearly mapped (remote vs local LLM, indexing vs on‑the‑fly, Docker vs micro‑VM).  
- **Failure handling** includes fall‑backs, health‑checks, and observability that keep the agent reliably usable in production.  

With this design, a software engineer can stay inside the terminal and ask the AI to *explain*, *write*, *test*, and *commit* code—all while the system guarantees low latency, data privacy, and robust operation.