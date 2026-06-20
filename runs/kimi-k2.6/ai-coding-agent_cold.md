---

Design a terminal-based AI coding agent that can complete software engineering tasks.


---

**System Design: Terminal-Based Autonomous Coding Agent (Codex-CLI)**

---

## 1. Executive Summary

This is a **single-user, terminal-native AI software engineering agent**. It runs as a local CLI/TUI process that accepts natural language tasks, plans multi-step solutions, executes tools in a sandboxed environment, and mutates the user's codebase under human oversight. The design prioritizes **deterministic safety** (git-guarded writes), **finite context budgets** (hybrid RAG + monolithic context), and **fast local feedback loops** (sub-200ms search, streaming LLM output).

---

## 2. Architecture Overview

```mermaid
graph TB
    subgraph UserSpace ["User Interface"]
        UI ["Terminal UI / CLI<br/>(Ratatui / Streaming Stdout)"]
    end

    subgraph AgentCore ["Agent Core"]
        ORC ["Orchestrator<br/>(State Machine)"]
        PLN ["Planner<br/>(Reasoning LLM)"]
        EXC ["Executor<br/>(Tool Loop)"]
        REV ["Reviewer<br/>(Verification LLM)"]
        CTX ["Context Engine<br/>(RAG + LSP + Summarizer)"]
    end

    subgraph LLMBackend ["LLM Backend"]
        ROUT ["LLM Router<br/>(Token Budget + Fallback)"]
        CACHE ["Response Cache<br/>(Exact Prompt TTL 1h)"]
    end

    subgraph Memory ["Memory Layer"]
        VDB ["Vector Store<br/>(1536-dim / ~10k chunks)"]
        HIST ["Message History<br/>(20-msg window / 80k tok)"]
        WMEM ["Working Memory<br/>(Compressed Session Notes)"]
    end

    subgraph ToolLayer ["Tool & Sandbox Layer"]
        FILE ["File Ops<br/>(Read / Write / Atomic Diff)"]
        SHL ["Shell Sandbox<br/>(Rootless Docker + seccomp)"]
        SRC ["Code Search<br/>(Tree-sitter + Ripgrep)"]
        LSP ["LSP Client<br/>(Type-accurate Refactor)"]
        TST ["Test Runner<br/>(pytest / jest / cargo)"]
    end

    FS ["Project Filesystem<br/>(Git Monitored)"]
    GIT ["Git Guardian<br/>(Auto-branch + Stash)"]

    UI <-->|Query / Approval / Ctrl-C| ORC
    ORC -->|Plan Task| PLN
    ORC -->|Execute Tools| EXC
    EXC -->|Raw Output| REV
    REV -->|Verdict| ORC

    PLN -->|Fetch Context| CTX
    CTX -->|Embed Search| VDB
    CTX -->|History| HIST
    CTX -->|Notes| WMEM
    CTX -->|Symbols| LSP

    PLN -->|Generate| ROUT
    REV -->|Verify| ROUT
    ROUT -->|Cached?| CACHE

    EXC -->|Dispatch| FILE
    EXC -->|Dispatch| SHL
    EXC -->|Dispatch| SRC
    EXC -->|Dispatch| LSP
    EXC -->|Dispatch| TST

    FILE -->|Atomic Write| GIT
    SHL -->|Isolated Exec| GIT
    TST -->|Test Exec| GIT
    SRC -->|Index / Query| FS
    LSP -->|Analyze| FS
    GIT -->|Apply| FS
```

---

## 3. Component Deep Dive

### 3.1 Orchestrator (State Machine)
The Orchestrator is a deterministic state machine with six states:
- **IDLE**: Awaiting user input.
- **PLANNING**: LLM generates a task graph (max 10 steps).
- **EXECUTING**: Dispatches one tool per LLM turn; no parallel tool execution to preserve determinism.
- **REVIEWING**: A lighter LLM verifies the tool output (e.g., "did the test actually pass?").
- **AWAITING_USER**: Blocked on destructive or high-confidence operations.
- **DONE / ERROR**: Terminal state with exit code.

**Guardrails**: Hard ceiling of **20 turns per task**. If reached, the agent serializes state to `.agent/resume.json` and yields control. A loop detector hashes the last 5 action sequences; a collision forces a pause.

### 3.2 Planner & Reviewer (LLM Tiering)
- **Planner**: Heavy model (Claude 3.5 Sonnet or GPT-4o). Enforced JSON mode emitting a plan schema: `steps: [{tool: "view", path: "..."}, {tool: "edit", ...}]`.
- **Reviewer**: Light model (Claude 3.5 Haiku / GPT-4o-mini). Consumes **1k output tokens** to verify diffs. Latency: **~3s**. This offloads cheap verification from the expensive planner.

### 3.3 Context Engine
The Context Engine is the bottleneck for both cost and accuracy.

**RAG Pipeline**:
- **Chunking**: 100-line chunks with 5-line overlap. For a 500k-line repository, this yields **~5,000 chunks**.
- **Embeddings**: 1536-dim `text-embedding-3-small` or local `nomic-embed-text`. Stored in an in-process vector DB (Qdrant lightweight or `sqlite-vec`).
- **Retrieval**: Query latency **<150ms** (50ms embedding + 100ms HNSW search). Top-10 chunks retrieved.
- **Storage math**: 5,000 chunks × 1,536 dims × 4 bytes = **~30 MB** raw. With metadata and HNSW index, **~70 MB** on disk.

**LSP Integration**:
- Spawns language servers per filetype (e.g., `pylsp`, `typescript-language-server`) via JSON-RPC over stdio.
- Used only for **write-time verification** (e.g., "rename symbol") and **dependency graph resolution**. Cold start: **2–4s**. Warm queries: **<100ms**.

**Summarizer**:
- When message history exceeds 20 exchanges, the oldest 10 are compressed by a 4B-parameter local model into a 500-token summary appended to Working Memory.

### 3.4 LLM Router & Token Budget
Because the agent is terminal-based and iterative, token spend is the primary cost driver.

**Context Budget (Claude 3.5 Sonnet, 200k window)**:
| Component | Tokens | Cost per 1k |
|---|---|---|
| System Prompt (tool defs + rules) | 4,000 | $3 input |
| Pinned Working Memory | 1,000 | $3 input |
| Rolling History (last 20 msgs) | 20,000 | $3 input |
| Active File Cache ("open tabs") | 30,000 | $3 input |
| RAG-retrieved Chunks | 10,000 | $3 input |
| **Scratch / LLM Output Reserve** | **135,000** | — |

If a prompt exceeds **180,000 tokens**, the Orchestrator triggers an emergency summarization of the oldest non-pinned history.

**Fallbacks**:
1. **Primary**: Claude 3.5 Sonnet via API.
2. **Fallback**: GPT-4o via API (different rate-limit bucket).
3. **Emergency**: Local `Llama-3.3-70B` (Q4_K_M, requires ~40 GB VRAM / 64 GB RAM) for offline summarization and simple edits.

**Cache**: Exact prompt matches (including system prompt and history hash) are cached in an LRU with **1-hour TTL**. Hit rate expected: **15–20%** for repetitive review steps.

### 3.5 Tool Registry & Sandbox

**File Operations**:
- `view`: Read file or directory tree.
- `edit`: **Search-and-replace block** (not line numbers). Requires a 3-line unique prefix and suffix. If ambiguous, the tool rejects and returns an error to the LLM.
- `create`: Idempotent file creation.

**Shell Sandbox**:
- **Rootless Docker** with the following restrictions:
  - `--read-only` root filesystem.
  - Writable overlay only at `/tmp/agent-scratch`.
  - `--security-opt seccomp=agent-profile.json` (no `execve` of setuid binaries, no `ptrace`).
  - `--network none` by default.
  - Memory limit: **2 GB**. CPU limit: **2 cores**. Timeout: **60s** for shell, **300s** for tests.
- User ID mapped to host UID to prevent permission issues on bind mounts.

**Search**:
- **Ripgrep**: Text search over the full repo. 1M lines in **<1s**.
- **Tree-sitter**: Structural queries (e.g., "find all function definitions named `auth*`"). Parsed on the fly; no persistent index needed for reads.

**Test Runner**:
- Parses JUnit / pytest / cargo JSON output. Returns structured `{passed, failed, stdout, duration}` to the LLM. If tests hang, the sandbox OOM killer terminates the container.

### 3.6 Git Guardian
Every agent session begins with:
1. `git stash push -m "agent-pre-$(date +%s)"` — snapshot of dirty state.
2. `git checkout -b agent-session-$(uuid)` — isolated feature branch.
3. File writes are committed atomically with messages like `agent: edit auth.py`.

The user can `git diff main..agent-session-xxx` to review all changes. If the agent crashes, the branch persists; the user can merge or discard. This makes destructive operations **recoverable in <1s** via `git reset`.

---

## 4. Capacity Planning & Math

### 4.1 Latency per Turn
| Step | Duration |
|---|---|
| Context Retrieval (RAG + LSP) | 150–300 ms |
| LLM Prompt Transfer + TTFB | 2–5 s |
| LLM Generation (4k output) | 10–20 s |
| Tool Execution (shell / test) | 1–30 s |
| Review LLM Call | 3–5 s |
| **Total per Turn** | **16–60 s** |

A typical 10-step task completes in **3–8 minutes**.

### 4.2 Cost per Task
Assuming **Claude 3.5 Sonnet** pricing ($3/1M input, $15/1M output):

- **Input per turn**: 30k tokens (code + history + tool output) = $0.09
- **Output per turn**: 2k tokens (plan + tool call JSON) = $0.03
- **Per-turn cost**: **$0.12**
- **10-turn task**: **$1.20**
- **Reviewer** (Haiku, $0.25/1M in, $1.25/1M out): $0.01 per turn → negligible.

**Embedding costs** (indexing a 500k-line repo once):
- ~750k tokens at `text-embedding-3-small` = **$0.075**.

### 4.3 Local Resource Footprint
| Resource | Usage |
|---|---|
| Agent Binary + TUI | ~100 MB RAM |
| Vector DB (5k chunks) | ~70 MB RAM/Disk |
| Message History | ~10 MB RAM |
| Sandbox Container (warm) | ~50 MB RAM |
| **Total Resident Memory** | **~250–300 MB** |
| Disk (logs, checkpoints) | ~50 MB/day |

---

## 5. Explicit Tradeoff Analysis

### 5.1 Autonomy vs. Safety: Yolo Mode vs. Guardrails
- **Yolo Mode**: Auto-approves all writes and shell commands. Throughput is **1 task per 3 minutes**, but the risk of catastrophic data loss or sandbox escape is non-zero.
- **Guardrail Mode**: User must approve every `edit`, `create`, and `shell` invocation. Throughput drops to **1 task per 8–12 minutes** due to human latency.
- **Decision**: Default to **"Git-Guarded Auto"**. The agent auto-commits after every successful tool, but destructive operations (`rm`, `git push`, `pip install`) trigger an `AWAITING_USER` pause. Safety is restored via `git reset`, not user friction.

### 5.2 Monolithic Context vs. RAG
- **Full Repo Context**: Sending the entire codebase eliminates retrieval errors. However, a 500k-line repo exceeds the 200k context window. Even a 50k-line repo consumes **~75k tokens**, costing **$2.25 per turn** and increasing LLM latency by 5–10s due to long-context attention.
- **RAG Hybrid**: Retrieves only 10 relevant chunks (~5k tokens) plus 5 "open tab" files in full. This reduces cost by **60%** and handles repos up to **5M lines**. The tradeoff is a **200ms** retrieval latency and a **5–10% chance** of missing cross-file dependencies.
- **Decision**: Hybrid RAG + Cache. Keep recently edited files in full context; retrieve the rest. For small repos (<20k lines), allow a `--full-context` flag.

### 5.3 Tree-sitter vs. LSP for Structural Edits
- **Tree-sitter**: Universal, offline, language-agnostic. Parses a 500-line file in **<5ms**. Ideal for search, outlining, and syntax-aware chunking. However, it lacks type resolution; a "rename" might miss dynamic imports.
- **LSP**: Type-accurate and cross-file-aware. A rename in TypeScript correctly updates string literal references. However, it requires per-language binaries, 2–4s cold starts, and RPC fragility (servers crash).
- **Decision**: Tree-sitter is the default for all read/search operations. LSP is invoked **only** for `edit` operations where the file has a running language server and the edit is a semantic refactor (rename, extract method).

### 5.4 Remote API vs. Local LLM
- **Remote (Claude/GPT)**: High reasoning quality, fast inference (20s), but requires internet, costs **$1–2/task**, and sends proprietary code to a third party.
- **Local (Llama 3.3 70B Q4)**: Zero marginal cost, full privacy. Requires **40GB+ VRAM** (or slow CPU inference at 120s/turn). Quality drops on complex multi-file planning tasks.
- **Decision**: Remote primary for planning and reasoning. Local 4B–8B models (e.g., `Qwen2.5-Coder-7B`) used for summarization, embedding, and syntax highlighting. The agent can run in **air-gapped mode** with local 70B, but tasks are limited to <5-file refactors.

### 5.5 Search/Replace Blocks vs. Line-Number Edits
- **Line Numbers**: Compact for the LLM to generate. Brittle: if a user edits the file simultaneously, line numbers drift and the agent destroys code.
- **Search/Replace Blocks**: Robust to line shifts, but the LLM must hallucinate less text to match. A 40-line edit might require 120 tokens of context.
- **Decision**: Mandate search/replace with 3-line unique anchors. This increases token usage by **~8%** but reduces edit failure rates from **15% to <2%**.

---

## 6. Failure Modes & Mitigations

### 6.1 Context Window Overflow
- **Failure**: After 30 turns, the LLM drops the original user instruction or forgets constraints.
- **Mitigation**: Hard token counter in the LLM Router. At >110k tokens, oldest history is compressed by the local summarizer. System prompt and Working Memory are **pinned** (never evicted).

### 6.2 Hallucinated Tool Calls
- **Failure**: LLM generates `tool: "run_testx"` (typo) or passes a string where an integer is required.
- **Mitigation**: Pydantic v2 JSON schema validation on every LLM output. Invalid schemas are rejected with `ToolValidationError` and fed back to the LLM as a system note. Max **3 retries** before the Orchestrator enters `AWAITING_USER`.

### 6.3 Infinite Loop / Thrashing
- **Failure**: Agent applies a fix, runs tests, sees the same error, and repeats.
- **Mitigation**: Loop detector hashes the last 5 action sequences. On collision, the Orchestrator breaks the loop and presents the user with a summary of the stuck state. Max 20 turns per task.

### 6.4 Data Destruction
- **Failure**: Agent overwrites `config/production.yml` or deletes source files.
- **Mitigation**: Git Guardian (Section 3.6). All writes happen on a feature branch. The filesystem bind mount is **read-only** outside the workspace root. The agent cannot `rm -rf /` because the sandbox root is read-only.

### 6.5 Partial / Misaligned Edits
- **Failure**: Search/replace block matches the wrong occurrence of a common pattern (e.g., `return None`).
- **Mitigation**: The `edit` tool requires **both** a 3-line prefix and a 3-line suffix that must be unique within the file. If multiple matches exist, the tool returns `AMBIGUOUS_MATCH` and the LLM must provide more context.

### 6.6 Sandbox Escape / Malicious Dependency
- **Failure**: A test dependency or shell command attempts a network egress or privilege escalation.
- **Mitigation**: Rootless Docker with `--network none`, seccomp default deny for `socket`, `ptrace`, and `mount`. No `sudo` or setuid binaries in the container image. The workspace is mounted with `nosuid,nodev`.

### 6.7 LLM API Failure / Rate Limiting
- **Failure**: 429 or 524 errors from the LLM provider mid-task.
- **Mitigation**: Exponential backoff (1s, 2s, 4s, 8s, 16s). Automatic fallback to the secondary provider. If both fail, the Orchestrator writes a `RESUME_TOKEN` to `.agent/resume.json` and tells the user: *"Stashed state at turn 14. Run `codex --resume` to continue."*

---

## 7. Operational & Deployment Considerations

- **Packaging**: Single static binary (Rust recommended for Tree-sitter bindings and TUI performance) plus a Docker engine for the sandbox. No Python environment required on the host.
- **Configuration**: `.agent/config.toml` in the repo root. Defines:
  - `llm.primary = "claude-3-5-sonnet-20241022"`
  - `auto_approve = ["view", "search", "test"]` (default); `shell` and `edit` require approval unless in `--yolo` mode.
  - `ignore_patterns = ["*.min.js", "node_modules/"]` for RAG indexing.
- **Extensibility**: New tools are registered by dropping a JSON schema file into `.agent/tools/` and a binary in `$PATH`. The Orchestrator dynamically appends them to the system prompt.
- **Session Recovery**: If the terminal is killed (`SIGKILL`), the agent's state (turn count, branch name, working memory) is flushed to disk after every turn. Resume with `codex --resume`.

---

## 8. Summary

This design produces a **deterministic, auditable, and economically bounded** terminal agent. It handles **500k-line repositories** with a **$1.20 average task cost**, recovers from crashes via git and disk serialization, and fails safely via sandboxing and validation rather than hallucinating confidently.