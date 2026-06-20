"""Run judges on system design transcripts."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

import yaml
from openai import OpenAI

from lib import (
    HERE,
    chat_json,
    get_client,
    model_name,
    save_json,
    strip_preamble,
)

JUDGE_RUBRICS = {
    "system_design": {
        "name": "System Design Judge",
        "prompt": (
            "You are evaluating a system design response.\n\n"
            "Score 5 dimensions on a 0–5 scale. For EACH dimension, write evidence-based "
            "reasoning BEFORE assigning the score. Then output the reasoning and score "
            "together.\n\n"
            "SCALE: 3 = adequate, meets expectations. 4 = impressive, well above average. "
            "5 = exceptional (near-unreachable in single-shot — most strong responses will score "
            "4). 2 = below expectations. 1 = unacceptable. 0 = response is missing, refused, "
            "truncated, or off-topic (use only as the sentinel; do not score partial responses).\n\n"
            "Aim for a mean score of ~3.0 per dimension across a balanced sample. "
            "Out of 20 responses per dimension, expect roughly: 2 fives, 5 fours, 10 threes, "
            "2 twos, 1 one. If your scores skew higher, you are over-rewarding.\n\n"
            "When a response is between two levels, score the lower level unless it clearly "
            "exceeds it. Round down on ties.\n\n"
            "RULES:\n"
            "- Score only against the rubric. Do NOT penalize for missing topics the rubric "
            "does not require.\n"
            "- Length is NOT a quality signal. A concise response that hits all rubric "
            "criteria scores the same as a long one. If you find yourself rewarding a response "
            "for being thorough, check whether the rubric criteria are met or the response is "
            "just long.\n"
            "- If the response is missing entirely, refuses the task, is truncated mid-sentence, "
            "or addresses a different problem than the prompt, return all scores as 0 with "
            "reasoning. Do NOT score partial or off-topic responses on the rubric.\n"
            "- Naming a technology (Cassandra, Kafka, Redis) is not a deep-dive. The deep-dive "
            "must explain HOW the technology is used and WHY.\n\n"
            "DIMENSION 1: Requirements & Scoping\n"
            "Did the design start from a clear, scoped problem statement with explicit assumptions?\n"
            "1 — No requirements section, or just buzzwords (\"scalable, available, reliable\"). Jumps straight to architecture.\n"
            "2 — Lists functional requirements but no non-functional ones (or vice versa). No assumptions stated.\n"
            "3 — Both functional and non-functional stated. States explicit assumptions (scale, read/write ratio). No quantified SLAs.\n"
            "4 — Non-functionals quantified with specific targets (p99 latency, availability %, durability guarantee). Assumptions stated AND justified.\n"
            "5 — All of the above, plus sets explicit non-goals (\"we are NOT building X\") AND resolves an ambiguity in the prompt (\"the prompt doesn't specify scale; I'll pick 100M users and note where the design changes at 1B\").\n\n"
            "DIMENSION 2: Capacity Estimation\n"
            "Did the math actually happen, and do the numbers drive the design?\n"
            "1 — No numbers at all, or numbers pulled from thin air.\n"
            "2 — Some numbers present but not derived (just stated) or disconnected from each other.\n"
            "3 — Derives QPS/storage/bandwidth from assumptions. Numbers are roughly right but don't propagate into design choices.\n"
            "4 — Full estimation chain: assumptions → QPS → storage → bandwidth → component sizing. Numbers demonstrably drive later decisions (\"at 50K QPS we need N shards because each handles 5K QPS\").\n"
            "5 — All of the above, plus flags which assumptions are fragile and sanity-checks at least one result (\"that would be 10PB/year — does that make sense?\").\n\n"
            "DIMENSION 3: Architecture Coherence\n"
            "Do the components connect to specific requirements? Are there hallucinations or dead weight?\n"
            "1 — Components listed without connections. Contradictions. Hallucinated services.\n"
            "2 — Several components are decorative or buzzword-driven (Kafka where a queue would do, with no justification).\n"
            "3 — Every component serves a stated requirement. No hallucinations. Connections are sensible. Diagram or clear textual architecture description is present.\n"
            "4 — Every component justified against a specific requirement or capacity number. Data flow traceable end-to-end. Architecture description is genuinely informative.\n"
            "5 — All of the above, plus either: (a) picked the less obvious but more correct choice and explained why, OR (b) identified and avoided a specific anti-pattern (\"we do NOT use a cache here because writes dominate and invalidation cost exceeds benefit\").\n\n"
            "DIMENSION 4: Deep-Dive Depth\n"
            "When the response zoomed in on a component, did it go deep — or just restate that it exists?\n"
            "1 — No deep-dives. Components are named but never explored.\n"
            "2 — One shallow deep-dive with some specifics (schema sketch or algorithm name) but no mechanism.\n"
            "3 — One deep-dive with concrete mechanism (specific sharding key, index design, replication strategy, or queue semantics). Mostly correct.\n"
            "4 — Two or more deep-dives, each with concrete mechanism AND a named tradeoff with reasoning (\"range-sharding by user_id → hotspot for power users, so we use composite key with hash prefix\"). Demonstrates knowing WHY, not just WHAT.\n"
            "5 — All of the above, plus at least one deep-dive surfaces a non-obvious edge case or failure mode and addresses it specifically (thundering herd, hot key, write amplification, GC tail latency, partial network partition).\n\n"
            "DIMENSION 5: Tradeoffs & Failure Modes\n"
            "Did the design acknowledge that engineering is about choices? Does it think about what breaks?\n"
            "1 — No tradeoffs. Single architecture presented as obviously correct. No failure discussion.\n"
            "2 — One generic tradeoff (CAP theorem, consistency vs availability) but not tied to a specific decision.\n"
            "3 — At least two tradeoffs tied to specific decisions (\"we chose X over Y because Z\"). Some failure modes mentioned but shallow.\n"
            "4 — Tradeoffs are quantified or concretely scoped (\"strong consistency adds ~20ms p99, acceptable for writes but not reads → eventual consistency for read path\"). Failure modes have specific mitigations.\n"
            "5 — All of the above, plus the design acknowledges its own limits explicitly (\"this design cannot handle X; if that becomes a requirement we'd need Y\") AND surfaces a non-obvious failure mode (thundering herd, write amplification, GC tail latency, time-of-check-to-time-of-use, partial partition).\n\n"
            "Respond ONLY with a JSON object. For each dimension, provide reasoning FIRST, then the score:\n"
            '{"requirements_scoping": {"reasoning": "<2-3 sentences citing specific evidence from the response>", "score": <0-5>}, '
            '"capacity_estimation": {"reasoning": "<...>", "score": <0-5>}, '
            '"architecture_coherence": {"reasoning": "<...>", "score": <0-5>}, '
            '"deep_dive_depth": {"reasoning": "<...>", "score": <0-5>}, '
            '"tradeoffs_failure_modes": {"reasoning": "<...>", "score": <0-5>}}'
        ),
    },
}

PAIRWISE_PROMPT = (
    "You are judging two anonymized system design transcripts (A and B) "
    "for the SAME question. Pick the better one.\n\n"
    "Consider: technical depth, clarity, numerical grounding, tradeoff reasoning, "
    "and whether they identified and addressed real bottlenecks.\n\n"
    "Respond ONLY with a JSON object:\n"
    '{"winner": "A" or "B", "reason": "<1-3 sentences explaining why>"}'
)


def load_transcript(path: str | Path) -> dict:
    path = Path(path)
    raw_text = path.read_text()

    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError:
        tmp = Path(tempfile.mktemp(suffix=".json", prefix=f"{path.stem}_"))
        tmp.write_text(raw_text)
        print(f"  [error] JSON decode failed for {path} — raw content saved to {tmp}", file=sys.stderr)
        raise

    content = data.get("content", "")
    if not content:
        return data

    prefix = strip_preamble(content)
    data["content"] = prefix
    data["_source_path"] = str(path)

    return data


def build_transcript_for_judge(transcript: dict) -> str:
    return transcript["content"]


def run_absolute(
    client: OpenAI,
    model: str,
    transcript_path: str | Path,
    force: bool = False,
) -> dict:
    t = load_transcript(transcript_path)
    text = build_transcript_for_judge(t)
    qid = t["question_id"]
    qvariant = t["variant"]
    qmodel = t["model"]
    ts = t.get("timestamp", "")

    out_dir = HERE / "judgments" / model / qmodel / qid
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{qvariant}_absolute.json"

    if out_path.exists() and not force:
        print(f"  [skip] judgment for {qid}/{qvariant} — already exists")
        return json.loads(out_path.read_text())

    results = {}
    for judge_id, rubric in JUDGE_RUBRICS.items():
        print(f"  [judge:{judge_id}] scoring...")
        messages = [
            {"role": "system", "content": rubric["prompt"]},
            {"role": "user", "content": f"Evaluate this transcript:\n\n'''\n{text}\n'''"},
        ]
        result = chat_json(client, model, messages, max_tokens=8192)
        results[judge_id] = result

    judgment = {
        "transcript_path": str(transcript_path),
        "question_id": qid,
        "variant": qvariant,
        "model_under_test": qmodel,
        "judge_model": model,
        "mode": "absolute",
        "timestamp": ts,
        "scores": {jid: r.get("parsed") for jid, r in results.items()},
        "raw": {jid: r.get("raw") for jid, r in results.items()},
        "attempts": {jid: r.get("attempts") for jid, r in results.items()},
    }
    save_json(out_path, judgment)
    return judgment


def run_pairwise(
    client,
    model: str,
    transcript_a: str | Path,
    transcript_b: str | Path,
    force: bool = False,
) -> dict:
    ta = load_transcript(transcript_a)
    tb = load_transcript(transcript_b)
    text_a = build_transcript_for_judge(ta)
    text_b = build_transcript_for_judge(tb)

    qid = ta["question_id"]
    ma = ta["model"]
    mb = tb["model"]

    out_dir = HERE / "judgments" / "pairwise" / qid
    out_dir.mkdir(parents=True, exist_ok=True)
    pair_key = f"{ma}_vs_{mb}"
    out_path = out_dir / f"{pair_key}_pairwise.json"

    if out_path.exists() and not force:
        print(f"  [skip] pairwise {ma} vs {mb} — already exists")
        return json.loads(out_path.read_text())

    def judge(a_text, b_text, label_a, label_b):
        messages = [
            {"role": "system", "content": PAIRWISE_PROMPT},
            {"role": "user", "content": f"TRANSCRIPT {label_a}:\n'''\n{a_text}\n'''\n\nTRANSCRIPT {label_b}:\n'''\n{b_text}\n'''"},
        ]
        return chat_json(client, model, messages)

    print(f"  [pairwise] A={ma} vs B={mb}")
    r1 = judge(text_a, text_b, "A", "B")
    print(f"  [pairwise] A={mb} vs B={ma} (swapped)")
    r2 = judge(text_b, text_a, "A", "B")

    w1 = r1.get("parsed", {}).get("winner") if r1.get("parsed") else None
    w2 = r2.get("parsed", {}).get("winner") if r2.get("parsed") else None

    if w1 == "A" and w2 == "B":
        winner = ma
        consistent = True
    elif w1 == "B" and w2 == "A":
        winner = mb
        consistent = True
    else:
        winner = "tie"
        consistent = False

    judgment = {
        "question_id": qid,
        "transcript_a": str(transcript_a),
        "transcript_b": str(transcript_b),
        "model_a": ma,
        "model_b": mb,
        "judge_model": model,
        "mode": "pairwise",
        "run1_a_wins": w1 == "A",
        "run2_a_wins": w2 == "A",
        "winner": winner,
        "consistent": consistent,
        "raw_run1": r1.get("raw"),
        "raw_run2": r2.get("raw"),
        "reason_run1": r1.get("parsed", {}).get("reason") if r1.get("parsed") else None,
        "reason_run2": r2.get("parsed", {}).get("reason") if r2.get("parsed") else None,
    }
    save_json(out_path, judgment)
    return judgment


def find_transcripts_for_question(question_id: str) -> dict[str, list[Path]]:
    runs_dir = HERE / "runs"
    by_model: dict[str, list[Path]] = {}
    for run_file in runs_dir.rglob(f"{question_id}_*.json"):
        data = json.loads(run_file.read_text())
        mdl = data.get("model", "unknown")
        by_model.setdefault(mdl, []).append(run_file)
    return by_model


def load_judge_models() -> list[str]:
    path = HERE / "models.yaml"
    if not path.exists():
        raise SystemExit(f"models.yaml not found at {path}")
    cfg = yaml.safe_load(path.read_text())
    return cfg.get("judges", [])


def main():
    parser = argparse.ArgumentParser(description="Run judges on transcripts")
    parser.add_argument("--model", default=model_name())
    parser.add_argument("--all-models", action="store_true", help="run all judge models from models.yaml")
    parser.add_argument("--mode", default="absolute", choices=["absolute", "pairwise", "all"])
    parser.add_argument("--transcript", default=None, help="path to a specific transcript")
    parser.add_argument("--question", default=None, help="run all transcripts for a question_id")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.all_models:
        judge_models = load_judge_models()
        if not judge_models:
            raise SystemExit("No judges found in models.yaml")
    else:
        judge_models = [args.model]

    client = get_client()

    if args.transcript:
        for jm in judge_models:
            result = run_absolute(client, jm, args.transcript, force=args.force)
            if result:
                print(json.dumps(result["scores"], indent=2))
        return

    if args.question:
        by_model = find_transcripts_for_question(args.question)
        print(f"Found transcripts for {args.question}: {dict((k, len(v)) for k, v in by_model.items())}")

        for jm in judge_models:
            if args.mode in ("absolute", "all"):
                for model_name_str, paths in by_model.items():
                    for p in paths:
                        print(f"[absolute] judge={jm} {p}")
                        run_absolute(client, jm, p, force=args.force)

            if args.mode in ("pairwise", "all"):
                models = list(by_model.keys())
                for i in range(len(models)):
                    for j in range(i + 1, len(models)):
                        for ta in by_model[models[i]]:
                            for tb in by_model[models[j]]:
                                print(f"[pairwise] judge={jm} {models[i]} vs {models[j]} ({ta.name} vs {tb.name})")
                                run_pairwise(client, jm, ta, tb, force=args.force)
    else:
        print("Specify --transcript or --question", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
