---

Design a terminal-based AI coding agent that can complete software engineering tasks.


---

A strong terminal-based AI coding agent is **not just a chatbot in a shell**. It is a **local-first orchestration runtime** that can:

1. understand a software task,
2. inspect a repository,
3. edit code safely,
4. run builds/tests,
5. iterate based on failures,
6. and stop with a verifiable result.

The key design choice is:

> **Keep code access and execution local. Use the cloud mainly for model inference, auth, and policy.**

That gives better safety, privacy, and performance than shipping full repos to a remote service.

---

# 1) Requirements

## Functional requirements
The agent should be able to:

- accept a task in a terminal:
  - “fix the failing tests”
  - “add pagination to this API”
  - “refactor this module”
  - “explain why build is broken”
- inspect the repo:
  - list files
  - search symbols/text
  - read code
  - understand imports/tests/config
- modify code:
  - create/update/delete files
  - apply patches
  - run formatters
- validate work:
  - run lint/build/test commands
  - inspect failures
  - retry
- use git:
  - diff/status/branch/commit
- ask for approval before risky actions
- resume an interrupted session
- explain what it changed and why

## Non-functional requirements
- **Low latency**: first useful action in a few seconds
- **Safety**: no silent destructive commands
- **Auditability**: every tool call and file change is traceable
- **Privacy**: repo stays local by default
- **Scalability**: support tens of thousands of daily users
- **Portability**: macOS/Linux first, Windows via WSL or PowerShell adapter
- **Reliability**: recover from model/tool/network failure

## Explicit non-goals for v1
- autonomous prod deployment
- arbitrary browser automation
- direct database writes without approval
- fully unattended execution across the whole machine

---

# 2) High-level design

Use a **hybrid architecture**:

- **Local client/runtime** on the developer machine
  - terminal UI
  - repo indexing
  - tool execution
  - patch application
  - sandboxing
  - session state
- **Cloud control plane**
  - auth / org policy
  - model gateway / routing
  - optional telemetry
  - optional synced memory

This is the right split because the “hard” part of software tasks is usually not just generation—it is **grounded interaction with the real repo and environment**.

---

# 3) Architecture diagram

```mermaid
flowchart LR
    U[Developer in Terminal] --> TUI[CLI / TUI]

    subgraph LOCAL[Developer Machine]
        TUI --> SM[Session Manager / Agent Loop]
        SM --> CE[Context Engine]
        CE --> IDX[Repo Index\nmanifest + symbols + lexical search + optional embeddings]

        SM --> TB[Tool Broker]
        TB --> FS[File Tools\nread/write/patch]
        TB --> SH[Shell Runner]
        TB --> GT[Git Adapter]
        TB --> TV[Test/Build Runner]
        TB --> LSP[LSP Adapter]
        TB --> EXT[Plugin / MCP-style Adapters]

        SM --> PE[Policy & Approval Engine]
        SM --> CP[Checkpoint Store\nSQLite + local artifacts]
        SM --> RD[Secret Scanner / Redactor]
    end

    RD --> MGW[Model Gateway]

    subgraph CLOUD[Cloud Control Plane]
        MGW --> AUTH[Auth / Org Policy]
        MGW --> ROUTER[Prompt Manager + Model Router]
        ROUTER --> LLM1[Primary LLM Provider]
        ROUTER --> LLM2[Backup LLM Provider]
        ROUTER --> LLM3[Optional Local/On-prem Model]
        MGW --> OBS[Telemetry / Session Metadata]
        MGW --> MEM[Optional Remote Memory]
        MGW --> RATE[Rate Limits / Budgeting]
    end
```

---

# 4) Core execution model

The agent runs a **plan → inspect → edit → validate → summarize** loop.

## Sequence diagram

```mermaid
sequenceDiagram
    participant User
    participant CLI as Local CLI
    participant Agent as Session Manager
    participant Index as Context Engine
    participant Tools as Tool Broker
    participant Model as Model Gateway/LLM

    User->>CLI: "Fix the failing auth tests"
    CLI->>Agent: Start session
    Agent->>Index: Search relevant files/tests/config
    Index-->>Agent: Ranked snippets + symbols + repo summary
    Agent->>Model: Task + context + tool schemas
    Model-->>Agent: Plan + next tool call
    Agent->>Tools: Run search/read/git/test commands
    Tools-->>Agent: Structured outputs
    Agent->>Model: Results + updated context
    Model-->>Agent: Patch proposal
    Agent->>Tools: Apply patch
    Agent->>Tools: Run targeted tests/lint
    Tools-->>Agent: Pass/fail + logs
    alt validation fails
        Agent->>Model: Failure summary
        Model-->>Agent: Fix or alternate approach
    end
    Agent-->>User: Diff + summary + final status
```

---

# 5) Core design choices

## 5.1 Local-first data plane
All file access and command execution happen locally.

### Why
- safer
- faster for file scanning/tests
- avoids uploading entire codebases
- simpler enterprise adoption

### Tradeoff
- client is more complex than a thin terminal wrapper
- must handle cross-platform execution and sandboxing

This is worth it.

---

## 5.2 Structured tools first, shell second
The model should prefer **typed tools** like:

- `search_code(query, glob, limit)`
- `read_file(path, start, end)`
- `apply_patch(diff)`
- `run_tests(targets)`
- `git_status()`

Use raw shell only as an escape hatch.

### Why
Structured tools are:
- safer
- easier to validate
- easier to summarize back to the model
- more deterministic than free-form shell commands

### Tradeoff
Some tasks require arbitrary shell commands. So shell must still exist, but behind stronger policy gates.

---

## 5.3 Verification-driven agent loop
The agent should not stop at “I made the change.” It should try to prove correctness by:

1. syntax/parse check
2. lint/typecheck
3. targeted tests
4. broader tests if cheap enough

### Why
In coding tasks, **validation is the core product**.

### Tradeoff
More tool calls and latency, but much higher task completion quality.

---

# 6) Detailed component design

## 6.1 Terminal UI / CLI

### Responsibilities
- interactive chat
- show streaming model output
- show current step (“searching…”, “running tests…”)
- display diffs
- prompt for approval
- allow interrupt/resume
- store minimal local session history

### Useful commands
- `/plan`
- `/approve`
- `/deny`
- `/undo`
- `/resume <session-id>`
- `/diff`
- `/logs`
- `/budget`

### UX principle
Expose:
- plan
- actions
- results
- diff

Do **not** expose raw chain-of-thought. Internally maintain structured working memory instead.

---

## 6.2 Session Manager / Agent Loop

This is the core orchestrator.

### State machine
A session moves through states like:
- `Idle`
- `Planning`
- `Inspecting`
- `AwaitingApproval`
- `Editing`
- `Validating`
- `Blocked`
- `Done`

### Session state
Keep:
- user goal
- repo root / branch / commit
- permission mode
- current plan
- completed steps
- relevant files/symbols
- token/cost budget
- checkpoints
- validation results

### Working memory
Use a structured scratchpad, not raw reasoning text:

```json
{
  "goal": "Fix failing auth tests",
  "acceptance_criteria": [
    "auth tests pass",
    "no new lint errors",
    "minimal code changes"
  ],
  "current_hypothesis": "token refresh logic broke when null session is returned",
  "open_questions": [
    "is failure due to mock or prod code?"
  ],
  "done": [
    "located failing tests in tests/auth/test_refresh.py"
  ],
  "blocked": []
}
```

This is compact, auditable, and cheaper than replaying the full transcript.

---

## 6.3 Context Engine: repository understanding

A coding agent lives or dies by context selection.

## Why retrieval is mandatory
A 1M LOC repo does not fit in context.

Rough math:
- assume 1M LOC
- ~40 chars per line average
- 40M chars total
- ~4 chars/token average
- ≈ **10M tokens**

Even a 200k-token model can see only **~2%** of that at once.

So the system must retrieve the right slices.

### Indexes to build locally
On repo open, build:

1. **File manifest**
   - path
   - language
   - size
   - last modified
   - git status

2. **Lexical index**
   - via `ripgrep`/trigram/BM25
   - best for exact symbols, error strings, config names

3. **Symbol graph**
   - via tree-sitter or LSP
   - functions, classes, methods, imports, tests

4. **Optional semantic index**
   - chunk embeddings for fuzzy search
   - useful for “where do we handle retries?” type queries

5. **Repo summaries**
   - one summary per package/module
   - cached by git SHA

### Index size math for a large repo
Assume:
- 40k files
- 1M LOC
- 60k semantic chunks
- 300k symbols

Approx storage:
- file manifest: `40,000 * 250B ≈ 10 MB`
- symbol table: `300,000 * 100B ≈ 30 MB`
- embeddings: `60,000 * 768 * 2 bytes ≈ 92 MB` using fp16
- overhead/cache ≈ `20 MB`

Total local index: **~150 MB/repo**, which is reasonable.

### Retrieval strategy
For each turn:

1. parse task and extract entities
2. search filenames, symbols, errors, tests
3. expand neighbors:
   - imports
   - callers/callees
   - adjacent tests
4. rank snippets
5. pack prompt under token budget

### Ranking signals
A simple weighted rank works well:
- lexical match: 40%
- symbol/path match: 25%
- dependency proximity: 15%
- test adjacency: 10%
- recent edits / git diff proximity: 10%

### Context window packing
For a 200k-token model, reserve roughly:
- 15k: system/policy/tool schemas
- 10k: session summary + plan
- 20k: recent tool results
- 125k: repo snippets
- 30k: output reserve

If the prompt is too large:
- drop low-score snippets
- summarize older tool outputs
- compress repeated log lines

---

## 6.4 Tool Broker

The tool broker provides capabilities with explicit schemas and policies.

### Built-in tools
- `list_dir(path)`
- `search_code(query, glob, max_results)`
- `read_file(path, start_line, end_line)`
- `read_symbol(symbol_name)`
- `apply_patch(diff)`
- `write_file(path, contents)` for new files only
- `git_status()`
- `git_diff(paths?)`
- `git_checkout_branch(name)`
- `run_command(argv, cwd, timeout, network=false)`
- `run_tests(targets, timeout)`
- `get_diagnostics(path)` via LSP
- `fetch_issue(id)` / plugins

### Tool result format
Return structured data:
- exit code
- duration
- touched files
- stdout/stderr tail
- machine-readable diagnostics

Example:

```json
{
  "tool": "run_tests",
  "target": "tests/auth/test_refresh.py",
  "exit_code": 1,
  "duration_ms": 18342,
  "failures": [
    {
      "test": "test_refresh_rejects_null_session",
      "message": "Expected 401, got 500",
      "file": "src/auth/refresh.py",
      "line": 87
    }
  ],
  "stdout_tail": "...",
  "stderr_tail": "..."
}
```

This is much more useful than dumping 10,000 lines into the prompt.

---

## 6.5 Sandbox and command execution

The dangerous part of a coding agent is command execution.

### Execution policy
Support 4 modes:

1. **Read-only**
   - no file writes
   - no shell side effects

2. **Workspace-write**
   - may edit files inside repo
   - no network
   - no writes outside repo

3. **Workspace-write + test/build**
   - may run local test/build commands
   - resource-limited
   - no network by default

4. **Full-access**
   - explicit opt-in
   - can use network / system tools / external services

### Safety controls
- parse shell AST before execution
- classify commands:
  - read-only
  - workspace write
  - external network
  - privileged/system-wide
- deny or require approval for:
  - `sudo`
  - package publishing
  - writes outside allowed roots
  - network egress in restricted mode
  - destructive patterns like broad `rm`, `mv`, shell redirects to sensitive paths

### Process isolation
Use:
- subprocess groups
- timeouts
- output caps
- resource limits where available (`cgroups`, `ulimit`, containers)

Defaults:
- 120s timeout for normal commands
- 10MB log cap
- truncate and summarize output above threshold
- kill entire process tree on cancel

### Why not always use Docker?
Docker/container isolation is safer, but:
- startup overhead hurts latency
- many repos assume host tooling
- dev environments differ

Good compromise:
- use repo’s devcontainer/nix/venv if available
- otherwise host shell with strict policy
- optional “sandboxed session” flag for higher isolation

---

## 6.6 Patch / edit engine

The edit engine converts model intent into safe code changes.

### Preferred edit workflow
1. read smallest necessary snippet
2. ask model for a unified diff or symbol-scoped change
3. verify file hash still matches expected base
4. apply patch atomically
5. run formatter if configured
6. store reverse patch for undo

### Why unified diffs
Pros:
- language-agnostic
- reviewable
- easy to apply/revert
- integrates naturally with git

Cons:
- can fail on shifting line numbers
- less semantically robust than AST transforms

### Hybrid approach
- **default:** unified diff
- **when available:** tree-sitter/LSP-assisted symbol replacement
- **fallback:** full-file rewrite only if patching fails and user approves

### Concurrency handling
Before applying:
- check current file hash
- if changed externally, re-read and rebase patch
- if conflict persists, ask user

This prevents stomping on user edits.

---

## 6.7 Validation engine

The validation engine is what turns “code generation” into “task completion”.

### Validation stages
1. **Fast checks**
   - parse/compile
   - formatter
   - linter for touched files
   - LSP diagnostics

2. **Targeted tests**
   - tests near changed files
   - impacted package/module tests

3. **Broader suite**
   - package-wide or full suite if affordable

### Test selection
Use heuristics:
- nearest test path
- import graph / symbol references
- known test commands from config
- historical mapping from source paths to tests

### Why targeted first
Running a full test suite can take 10–40 minutes. A targeted pass may take 10–60 seconds and catches most local regressions.

### Flaky tests
If a test fails in a known flaky area:
- rerun once
- mark as flaky if inconsistent
- do not loop forever

### Stop conditions
Agent stops when:
- acceptance criteria met, or
- budget exceeded, or
- no progress for N turns, or
- waiting on user clarification/approval

Good defaults:
- max 25 model calls per task
- max 100 tool calls per task
- max 20 minutes wall clock
- max $2 spend unless overridden

---

## 6.8 Model Gateway

The gateway abstracts providers and gives central control.

### Responsibilities
- auth
- rate limiting
- provider routing
- fallback
- prompt templates
- structured output enforcement
- budgeting and cost tracking
- telemetry

### Model routing strategy
Use different models for different work:

- **Large reasoning model**
  - plan creation
  - tricky refactors
  - bug fixing with ambiguous signals

- **Small/fast model**
  - log summarization
  - retrieval ranking
  - command explanation
  - session compression
  - low-risk edits

### Why routing matters
If every call uses the most expensive model, costs explode.

### Output constraints
Require model outputs to match a schema:
- `assistant_message`
- `tool_call`
- `patch_proposal`
- `final_report`

Reject malformed outputs and retry with a repair prompt if needed.

### Provider failover
If provider A fails:
- retry once if idempotent
- switch to provider B
- if all fail, degrade to local model for read-only assistance

Do **not** automatically replay non-idempotent tool actions.

---

## 6.9 Memory

There are three useful memory scopes:

### 1. Session memory
Short-lived, per task:
- what was tried
- what failed
- current plan
- relevant files

### 2. Repo memory
Cached summaries:
- architecture notes
- test/build commands
- common module relationships

### 3. User/org memory
Opt-in preferences:
- coding style
- preferred test commands
- branch naming
- approval defaults

### Important rule
Do not automatically store:
- secrets
- tokens
- raw env files
- arbitrary command outputs with sensitive data

---

# 7) Storage design

## Local storage
Use a local SQLite DB plus artifact files.

### Suggested local tables
- `sessions`
- `turns`
- `tool_calls`
- `checkpoints`
- `repo_indexes`
- `policies`
- `memories`

### Artifact files
Keep larger blobs on disk:
- raw tool logs
- repo summaries
- patch backups
- diff snapshots

Why local?
- privacy
- easy resume
- low latency
- avoids central storage of code

## Cloud storage
Keep only:
- auth data
- org policy
- usage counters
- optional compact session metadata
- optional opt-in telemetry

Use:
- Postgres for metadata
- Redis for rate limits / short-lived cache
- object storage for optional uploaded traces

At this scale, this is enough; no need for Kafka/microservice sprawl in v1.

---

# 8) Capacity planning with real numbers

I’ll design for a serious but plausible scale.

## Assumptions
- **25,000 DAU**
- **1.2 tasks/user/day**
- **30,000 tasks/day**
- **18 minutes median task length**
- **14 agent turns/task**
- **1.2 model calls/turn**
- **4.5 tool calls/turn**
- **9,000 input tokens/model call**
- **1,200 output tokens/model call**

## Derived load

### Concurrent sessions
Total active session minutes/day:

`30,000 tasks * 18 min = 540,000 session-min/day`

Average concurrency:

`540,000 / 1,440 ≈ 375 sessions`

Developer usage is bursty, so assume ~5.3x peak:

`375 * 5.3 ≈ 1,988`

Design for **2,000 peak concurrent sessions**.

### Model calls
Calls/day:

`30,000 * 14 * 1.2 = 504,000 model calls/day`

Average RPS:

`504,000 / 86,400 ≈ 5.8 req/s`

Peak RPS from concurrency:

`2,000 sessions * 14 turns / 1,080 sec * 1.2 ≈ 31 req/s`

Design gateway for **~40 req/s** sustained headroom.

### Tool calls
Tool calls/day:

`30,000 * 14 * 4.5 = 1,890,000 tool calls/day`

These are mostly local, so central infra does not need to absorb them.

### Token volume
Input/day:

`504,000 * 9,000 = 4.536B input tokens/day`

Output/day:

`504,000 * 1,200 = 604.8M output tokens/day`

Peak token rates:
- input: `31 * 9,000 ≈ 279k tok/s`
- output: `31 * 1,200 ≈ 37k tok/s`

This is why external inference providers or a mixed routing strategy are attractive. Self-hosting all inference at this throughput would be expensive.

---

## Gateway sizing
The gateway is mostly doing:
- auth checks
- prompt assembly
- streaming
- provider calls
- telemetry

If one request costs roughly **50 ms CPU** of gateway work on average, then at 31 req/s:

`31 * 0.05 = 1.55 CPU cores`

With 4x headroom for bursts, retries, streaming overhead:
- allocate **8 vCPU total**
- e.g. **4 pods x 2 vCPU / 2–4 GB RAM**

This is modest.

---

## Storage sizing

### Cloud metadata only
If we store compact metadata only:
- ~150 KB/task average

Then:

`30,000 * 150 KB = 4.5 GB/day`

30-day retention:

`4.5 * 30 = 135 GB`

This easily fits in object storage + moderate Postgres metadata.

### Local artifacts
If raw logs and diffs are kept locally:
- assume 0.5–2 MB/task depending on test output
- per power user doing 10 tasks/day: 5–20 MB/day
- manageable on developer machines, with GC after N days

---

## Cost estimate
Assume effective blended pricing after routing/caching:
- **$1.00 / 1M input tokens**
- **$5.00 / 1M output tokens**

Then daily model cost is approximately:

- input: `4,536M * $1.00 = $4,536/day`
- output: `604.8M * $5.00 = $3,024/day`

Total:
- **~$7,560/day**
- **~$0.25/task**

If everything ran on a premium model at:
- `$3/M input`
- `$15/M output`

then cost becomes:

- input: `4,536M * 3 = $13,608/day`
- output: `604.8M * 15 = $9,072/day`

Total:
- **~$22,680/day**
- **~$0.76/task**

This strongly justifies:
- model routing
- prompt compression
- retrieval
- caching summaries

---

# 9) Security design

## 9.1 Trust boundaries
Treat the following as untrusted:
- repository code/comments/docs
- tool output
- pasted issue text
- plugin responses

Why? Prompt injection can live inside code comments or README files.

### Mitigation
System prompt and policy engine must enforce:
- repo content is data, not instructions
- only system policy and explicit user commands can authorize risky actions

---

## 9.2 Secret protection
Before sending snippets to cloud models:
- redact `.env`-like values
- detect PEM blocks
- detect common API keys/tokens
- respect `.agentignore` / policy exclusions
- let enterprises define “never leave machine” paths

### Tradeoff
Redaction can occasionally remove useful context, but safety wins.

---

## 9.3 Approval model
Require explicit approval for:
- network access
- writes outside repo
- package install/update
- git push / PR creation
- destructive file ops
- long-running background processes

### Good default
Auto-approve:
- file reads
- search
- git status/diff
- local diagnostics

Ask before:
- writes
- tests/builds if expensive
- arbitrary shell
- external side effects

---

## 9.4 Audit logging
Every session should have an auditable trail:
- prompt hashes
- tool calls
- diffs applied
- approvals granted/denied
- model/provider used
- cost/tokens

Store code snippets only locally unless user/org explicitly opts in.

---

# 10) Failure modes and mitigations

| Failure mode | What happens | Mitigation |
|---|---|---|
| Model suggests dangerous command | Data loss or system damage | Tool policy, shell AST parsing, approval gates, denylist/allowlist |
| Agent loops without progress | Cost/time blowup | Step budget, cost budget, “no progress for 3 turns” detector |
| Context overflow | Model loses important info | Context packer, ranking, summarization, snippet dedupe |
| User edits same file concurrently | Patch conflict or overwritten work | File hash precondition, re-read/rebase, ask user on conflict |
| Test command hangs | Session stalls | Timeouts, kill process tree, output cap |
| Huge logs overwhelm prompt | Token explosion | Structured summaries, stdout/stderr tails, repeated-line compression |
| Provider outage | No model responses | Retry, provider failover, local read-only fallback |
| Prompt injection in repo/docs | Unsafe actions or hidden exfil | Treat repo as untrusted data, strict policy separation |
| Secret leakage in snippets | Security incident | Local redaction, path exclusions, secret scanning |
| Patch applies incorrectly | Broken code | Atomic apply, run parser/linter after edits, reverse patch |
| Non-deterministic/flaky tests | False negatives and loops | Rerun-once policy, flaky detection, human escalation |
| Multiple agent sessions on same repo | Conflicting changes | Optional per-session worktree, file locks, branch isolation |

---

# 11) Important tradeoffs

## Local-first vs cloud-first

| Option | Pros | Cons | Choice |
|---|---|---|---|
| Local-first | privacy, speed, safe tool access | more client complexity | **Chosen** |
| Cloud-first thin client | simpler client, centralized control | poor privacy, awkward local tool access | Not chosen |

## Structured tools vs raw shell

| Option | Pros | Cons | Choice |
|---|---|---|---|
| Structured tools | safe, deterministic, easier prompting | narrower flexibility | **Default** |
| Raw shell | maximum power | risky, hard to parse/approve | Escape hatch only |

## Single agent vs multi-agent

| Option | Pros | Cons | Choice |
|---|---|---|---|
| Single orchestrator | simple, cheaper, easier to debug | less parallelism | **V1** |
| Multi-agent swarm | parallel search/planning | expensive, coordination complexity | V2+ for large tasks |

## Full-file rewrite vs patching

| Option | Pros | Cons | Choice |
|---|---|---|---|
| Patching | minimal diffs, safer review | can fail on conflicts | **Default** |
| Full rewrite | simple to generate | easy to clobber content | Fallback only |

## Always-run full tests vs targeted tests first

| Option | Pros | Cons | Choice |
|---|---|---|---|
| Full suite | stronger confidence | too slow for iteration | Final/optional |
| Targeted first | fast feedback | may miss broad regressions | **Default** |

---

# 12) Deployment architecture

## Client
- single binary, preferably Rust or Go
- auto-update with signed releases
- local SQLite + artifact directory
- platform-specific shell adapters

## Cloud
- API gateway / router service
- Postgres
- Redis
- object storage
- optional telemetry worker

This can start as a **modular monolith**:
- one control-plane service
- one DB
- one cache
- object storage

No need for dozens of microservices initially.

---

# 13) Evaluation and observability

## Product metrics
- task completion rate
- diff acceptance rate
- first-pass validation success rate
- time to first useful action
- median turns per successful task
- token cost per task
- approval rejection rate

## System metrics
- model latency
- tool call latency/failure rate
- context pack size
- retries/fallback usage
- sandbox timeout rate

## Evaluation harness
Use a benchmark suite of real repos and tasks:
- bug fix tasks
- refactors
- feature additions
- test repair
- doc/config updates

A SWE-bench-like internal suite is ideal.

---

# 14) A practical v1 / v2 roadmap

## V1
- local CLI
- repo search/read
- patch application
- test/lint execution
- approval gates
- cloud model gateway
- session resume

## V1.5
- LSP integration
- targeted test selection
- better repo summaries
- org policies
- git branch helpers

## V2
- plugin ecosystem / MCP-style tool adapters
- PR creation
- CI log ingestion
- read-only subagents for parallel search
- stronger sandbox options

---

# 15) Final recommendation

The best design is a **local-first, verification-driven terminal coding agent** with:

- a **thick local runtime**
- **structured tools**
- **repo-aware retrieval**
- **patch-based editing**
- **sandboxed execution**
- **approval gates**
- **cloud model routing**
- **strong checkpointing and validation**

If I were building this, I would optimize for these three principles:

1. **Grounding over raw generation**  
   Better repo retrieval and tool feedback matter more than fancier prose.

2. **Safety over autonomy**  
   The agent should be powerful, but never silently dangerous.

3. **Verification over confidence**  
   Passing tests and clean diffs are more valuable than a convincing explanation.

If you want, I can turn this into:
- a **component-by-component API spec**,
- a **database schema**,
- or a **step-by-step implementation plan for an MVP**.