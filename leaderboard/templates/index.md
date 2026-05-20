---
title: LLM System Design Benchmark
---

## What This Is

This benchmark evaluates how well different LLMs perform on **system design** tasks. Each model receives the same cold system design prompt — no examples, no hints — and produces a complete design with architecture, capacity estimation, tradeoffs, and failure analysis. Independent LLM judges then score every transcript on 5 dimensions.

I evaluated **{{ MODELS_COUNT }} models** on **{{ QUESTIONS_COUNT }} problems** with **{{ JUDGE_COUNT }} judges** — {{ TOTAL_TRANSCRIPTS }} transcripts scored in total. See the [methodology](about/).

Any feedback or request? Please <a href="https://github.com/nqbao/llm-system-design/issues" target="_blank">submit an issue</a>.

## Leaderboard

| Rank | Model | Mean Score | ±CI | Runs |
|------|-------|-----------|-----|------|
{{ LEADERBOARD_ROWS }}

---

<div style="text-align: center; margin-top: 2rem;">

<p style="margin: 0 0 0.75rem; font-style: italic; color: var(--sl-color-gray-3);">
Buy me a coffee — or 10M tokens worth ☕
</p>

<script type="text/javascript" src="https://cdnjs.buymeacoffee.com/1.0.0/button.prod.min.js" data-name="bmc-button" data-slug="nqbao" data-color="#5F7FFF" data-emoji=""  data-font="Cookie" data-text="Buy me a coffee" data-outline-color="#000000" data-font-color="#ffffff" data-coffee-color="#FFDD00" ></script>

</div>
