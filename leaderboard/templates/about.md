---
title: Methodology
---

## How Evaluation Works

Each model is given a **cold prompt** (generic system design instructions with no problem-specific hints) followed by a **question prompt** describing the system to design. Every model runs with thinking enabled at **reasoning effort = high**. Max output is set to **16K tokens** (including reasoning). The raw transcripts are then scored by independent LLM judges across 5 dimensions using evidence-based reasoning. Judges run with the same **reasoning effort = high** setting and are required to output valid JSON — invalid responses are retried with corrective feedback up to 3 times. Interview responses record the API finish_reason to detect truncation, and failed API calls are retried automatically.

> You are an expert system design engineer.
> Produce a thorough, self-contained system design.
> Be precise: do capacity math with real numbers, discuss explicit tradeoffs, and think about what could fail.
> Include a diagram using mermaid syntax inside ```mermaid blocks.

The evaluation pipeline and analysis scripts were developed with assistance from DeepSeek V4 Pro and Claude Sonnet/Opus 4.6.

## Model Selection

I picked 3 top-tier models (gpt-5.4, claude-sonnet-4.6, kimi-k2.6), 3 mid-tier, and 3 bottom-tier models based on general popularity and availability. Opus is too expensive to run through this pipeline, but I plan to add it later.

## Problem Selection

The 9 problems were chosen based on popularity — these are the system design questions you see most often in interviews and discussions online, from URL shorteners to global object stores.

## Scoring Rubric

Each transcript is scored on 5 dimensions, 0–5 scale:

| Score | Meaning |
|-------|---------|
| 0 | Response is missing, refused, truncated, or off-topic |
| 1 | Below expectations — barely attempts the dimension |
| 2 | Partial — touches on the dimension but shallow or generic |
| 3 | Adequate — meets expectations for a solid design |
| 4 | Impressive — well above average with concrete specifics |
| 5 | Exceptional — near-unreachable in single-shot; surfaces non-obvious edge cases |

## Dimensions

| Dimension | Description |
|-----------|-------------|
| Requirements & Scoping | Did the design start from a clear, scoped problem statement with explicit assumptions and quantified SLAs? |
| Capacity Estimation | Was math actually done, and do the numbers drive design choices (QPS, storage, sharding)? |
| Architecture Coherence | Does every component serve a stated requirement? No hallucinations or dead weight? |
| Deep-Dive Depth | Did the design go deep on at least two subsystems with concrete mechanisms and tradeoffs? |
| Tradeoffs & Failure Modes | Did it acknowledge engineering choices, quantify tradeoffs, and anticipate failure modes? |

## Caveats

- I have not manually reviewed all 81 transcripts as a final quality check
- Judges are not calibrated against human feedback
- All models run through a LiteLLM gateway — results may differ from direct API access
- Only single run per model and per judge, so results may not capture variance across runs
- Judge models are also in the benchmark (a judge scoring its own transcript may bias results)

## Contribution

If you have an idea, feedback, or would like to add more problems/models for evaluation, please [submit an issue](https://github.com/nqbao/llm-system-design/issues). Feel free to send a merge request.
