---
name: add-question
description: Develop and add a new system design question to questions.yaml for this benchmark. Use when the user wants to brainstorm, evaluate, or add a new interview question to the benchmark.
user-invocable: true
---

# add-question

Helps develop a well-scoped system design question and adds it to `questions.yaml`.

## Workflow

1. **Understand the topic.** The user may name a system, a product, or a vague area. If the topic is vague, ask one clarifying question: what kind of system is it (web infra, AI/ML, storage, etc.)?

2. **Search the web.** Use WebSearch to find how the system (or similar ones) is described publicly — architecture blogs, interview guides, product docs. Look for:
   - What the system actually does (core responsibilities)
   - What makes it hard to design (scale, consistency, latency, safety)
   - Whether named products exist (brand bias risk)

3. **Check existing questions.** Read `questions.yaml` and compare:
   - Is this topic already covered?
   - What tier would it fit (easy / medium / hard / chaos)?
   - Does the complexity feel comparable to existing questions at that tier?

4. **Assess fairness.** If the question is based on a specific named product (e.g. "Claude Code", "Codex"), consider whether naming it creates training-data bias. If so, reframe as an abstract system description.

5. **Draft the prompt.** Follow the existing style — one concise sentence, no sub-questions, no hints:
   - With a named reference: *"Design X like [real product]."*
   - Abstract with one constraint: *"Design X that [does Y]."*
   - Keep it comparable in length to existing entries (10–15 words max).

6. **Confirm with the user.** Show the draft prompt and proposed tier. Ask if they want to adjust before adding.

7. **Add to questions.yaml.** Append the new entry at the end of the file in this format:

```yaml
- id: <kebab-case-id>
  tier: <easy|medium|hard|chaos>
  title: <Title Case Title>
  prompt_cold: |
    <The one-sentence prompt.>
```

## Tier guide

| Tier | Complexity | Examples |
|---|---|---|
| easy | Single service, straightforward CRUD/lookup | URL shortener, RAG assistant |
| medium | Real-time or social features, moderate fanout | Chat system, news feed, Twitter |
| hard | Distributed coordination, scale, or novel domain | Rate limiter, job scheduler, YouTube, AI coding agent |
| chaos | Extreme scale or adversarial conditions | 100B events/day metrics pipeline, global object store |

## Style rules

- One sentence only — no bullet points, no sub-questions in the prompt.
- Don't give away design decisions (e.g. "multi-step", "distributed", "real-time" only if truly non-obvious).
- Prefer abstract framing over brand names when training-data bias is a concern.
- Match the length and register of existing prompts.
