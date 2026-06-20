---

Design a terminal-based AI coding agent that can complete software engineering tasks.


---

# System Design: Terminal-Based AI Coding Agent

This document outlines the system architecture for a terminal-based AI coding agent (similar to Aider, OpenDevin, or SWE-agent) capable of autonomously navigating a codebase, writing code, executing tests, and debugging. 

Because this is a locally-run terminal application interacting with remote LLMs, the primary system design challenges are **context window optimization**, **cost control**, **deterministic file editing**, and **safeguarding the local execution environment**.

---

## 1. High-Level Architecture

The system operates on an **Agentic Loop (ReAct/Plan-and-Execute)** pattern. It sits between the user's terminal, the local file system, and a remote LLM API.

### Core Subsystems
1.  **CLI / UI Layer:** Handles user prompts, renders Markdown/syntax-highlighting, and manages user interruptions (Ctrl+C).
2.  **Orchestration Engine:** The core state machine governing the Agent loop.
3.  **Context Manager:** Manages the LLM context window using a dynamic Repo Map, short-term conversational memory, and token counting.
4.  **Tool Execution Environment:** A suite of deterministic tools the LLM can invoke (Shell execution, File read/write, Semantic Search).
5.  **LLM Gateway:** Handles API requests, rate limits, retries, and payload formatting.

---

## 2. Architecture Diagram

```mermaid
architecture-beta
    group local(Local Developer Machine)
    group cloud(Cloud Infrastructure)

    %% Local Components
    service user(User Terminal) in local
    service orchestrator(Agent Orchestrator) in local
    service context(Context Manager) in local
    service tools(Tool Engine) in local
    
    service fs(File System) in local
    service shell(Shell / Subprocess) in local
    service ast(AST / Parser) in local
    service repomap(Repo Map Generator) in local

    %% Cloud Components
    service llmGateway(LLM Gateway) in cloud
    service llm(Foundation Model\ne.g. Claude 3.5 Sonnet) in cloud

    %% Connections
    user:right:orchestrator
    orchestrator:bottom:context
    orchestrator:bottom:tools
    
    context:bottom:repomap
    repomap:right:fs
    
    tools:bottom:fs
    tools:bottom:shell
    tools:bottom:ast

    orchestrator:right:llmGateway
    llmGateway:right:llm
```

---

## 3. Component Deep Dive

### 3.1. Context Manager & Repo Map
LLMs have finite context windows (e.g., 128k - 200k tokens). Dumping a medium-sized repository into the prompt is both computationally expensive and degrades the LLM's reasoning ("lost in the middle" effect). 

*   **Repo Map Generator:** Instead of full files, the agent generates a compressed "Map" of the repository using `tree-sitter`. It extracts class names, function signatures, and docstrings, omitting implementation details. 
*   **Dynamic Context Loading:** As the LLM identifies files of interest, it uses a `read_file` tool. The Context Manager swaps these full files into the prompt and evicts stale files to stay strictly under the token limit.

### 3.2. Tool Execution Engine
The LLM interacts with the local machine via rigidly defined JSON Schema tools.
*   **`edit_file(path, diff)`:** Replaces whole-file generation with Unified Diff or Search/Replace block generation. 
*   **`run_shell(command)`:** Executes bash commands (e.g., `pytest`, `npm run lint`).
*   **`search_code(regex)`:** Wraps `ripgrep` for blazing-fast local codebase searches.
*   **`get_linter_errors(path)`:** Runs AST-level syntax checks *before* saving files to prevent the LLM from destroying working code.

### 3.3. Orchestration Engine (The Agent Loop)
Operates a strictly bounded `while` loop:
1.  **System Prompt + Context:** "You are an expert engineer. Your task is X. Here is the Repo Map and active files."
2.  **LLM Generation:** LLM outputs thought process + Tool call.
3.  **Tool Execution:** The engine intercepts the tool call, executes it locally, captures `stdout`/`stderr`.
4.  **Observation Feedback:** Appends the result to the conversation history.
5.  **Termination:** Halts when the LLM emits a `task_complete` signal or hits a safety loop limit.

---

## 4. Capacity & Cost Math

Let's do the math for a standard coding task in a medium-sized enterprise repository.

### Assumptions:
*   **Repository Size:** 1,000 files. Average 200 lines of code (LOC) per file.
*   **Token Conversion:** ~10 tokens per LOC $\rightarrow$ 2,000 tokens/file $\rightarrow$ **2,000,000 total repo tokens**.
*   **LLM Choice:** Claude 3.5 Sonnet ($3.00 / 1M input tokens, $15.00 / 1M output tokens). Context window: 200k tokens.

### Memory & Context Math:
1.  **Repo Map Compression:** Extracting just signatures reduces token count by ~90%.
    *   2M tokens $\rightarrow$ **200k tokens**. Still too large for efficient reasoning.
    *   *Optimization:* We apply a PageRank-style algorithm based on the user's prompt to only include the Repo Map for the 50 most relevant files $\rightarrow$ **~10k tokens**.
2.  **Active Files:** The LLM requests to read 3 files fully $\rightarrow$ **6k tokens**.
3.  **System Prompt & History:** **4k tokens**.
*   **Total Payload per Turn:** ~20k tokens.

### Cost per Task Math:
An average task (e.g., "Add a caching layer to the user service and update tests") takes **15 iterations (turns)** of the Agent Loop.
*   **Input Tokens:** 15 turns * 20k tokens = 300k tokens = **$0.90**.
*   **Output Tokens:** 15 turns * ~500 tokens (diffs + thoughts) = 7.5k tokens = **$0.11**.
*   **Total Cost per Task:** **~$1.01**.
*   *Conclusion:* Highly economically viable compared to human developer time, but requires aggressive context pruning to prevent runaway costs.

---

## 5. Explicit Engineering Tradeoffs

### 1. File Editing: Unified Diffs vs. Whole File Replacements
*   **Whole File:** The LLM outputs the entire file content. 
    *   *Pros:* Extremely reliable, no formatting/indentation matching errors. 
    *   *Cons:* Very slow (output tokens are generated linearly), expensive (output tokens cost 5x input tokens).
*   **Unified Diffs / Search & Replace:** The LLM outputs `<<<< SEARCH ... >>>> REPLACE`.
    *   *Pros:* Fast, cheap.
    *   *Cons:* LLMs frequently hallucinate exact string matches (e.g., missing a leading space), causing the patch to fail.
*   **Decision:** Use **Search & Replace blocks with a fuzzy-matching fallback**. If exact match fails, calculate Levenshtein distance on lines to find the intended block. If it still fails, fall back to requesting Whole File generation.

### 2. Execution Environment: Native Local Shell vs. Docker Sandbox
*   **Docker Sandbox:**
    *   *Pros:* Secure. The LLM cannot accidentally run `rm -rf ~/Documents` or leak AWS keys.
    *   *Cons:* Severe UX friction. Requires replicating the user's local dev environment, Node/Python versions, and private package registries inside a container.
*   **Native Local Shell:**
    *   *Pros:* Zero configuration. It "just works" with whatever the developer uses.
    *   *Cons:* Dangerous. The agent has the same permissions as the user.
*   **Decision:** **Native Local Execution with strict Permission Boundaries.** Read-only commands (`cat`, `ls`, `pytest`) auto-execute. Mutating shell commands (`npm install`, `rm`) or file deletions trigger an interactive `[Y/n]` prompt in the terminal for user approval.

### 3. Agentic Framework: ReAct vs. Plan-and-Solve
*   **Decision:** Implement a hybrid. ReAct (Reason -> Act -> Observe) is great for micro-steps but loses sight of the big picture. We implement an outer **Planner** step that generates a Markdown checklist, and an inner **Executor** (ReAct) loop that completes one checklist item at a time.

---

## 6. Failure Modes & Mitigations

| Failure Mode | Cause | Mitigation Strategy |
| :--- | :--- | :--- |
| **Infinite Loops** | LLM writes a syntax error, runs linter, sees error, tries to fix it but makes the exact same mistake. | **State Hashing & Limits:** Hash the last 3 LLM actions. If `hash(N) == hash(N-2)`, intervene. Impose a hard limit of `MAX_ITERATIONS=20` per task. |
| **Context Window Overflow** | LLM runs `npm install` or `cat bundle.js`. The resulting `stdout` is 300,000 tokens, blowing up the prompt. | **Stream Truncation:** Cap `stdout`/`stderr` captures to the last 100 lines (head + tail). Use a local lightweight regex to strip ANSI color codes before sending to LLM. |
| **Code Destruction** | LLM generates a malformed diff that deletes half a file. | **AST Validation & Git Guardrails:** 1. Parse modified files with `tree-sitter` before saving. Reject if AST is invalid. 2. Auto-create a temporary Git branch (`agent-task-X`) before the loop starts. Revert files if AST breaks. |
| **Rate Limiting (429s)** | High-speed looping hits Anthropic/OpenAI Tier limits. | **Exponential Backoff & Token Bucketing:** Implement robust interceptors on the LLM Gateway. If 429 is hit, pause execution, render a progress bar in CLI (`Waiting on API limit... 5s`), and retry. |
| **Task Drift** | Over a long session, the LLM forgets the original objective and starts refactoring unrelated code. | **System Prompt Injection:** Prepend the *original* user objective to the system prompt on *every single turn*, along with the current Plan Checklist. |