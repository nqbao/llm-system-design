"""Run system designs with an LLM."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

import yaml
from openai import OpenAI

from bank import Bank
from lib import (
    HERE,
    TIERS,
    chat,
    get_client,
    model_name,
    save_json,
    utc_now_iso,
)

SYSTEM_PROMPT = (
    "You are an expert system design engineer.\n"
    "Produce a thorough, self-contained system design.\n"
    "Be precise: do capacity math with real numbers, discuss explicit tradeoffs, "
    "and think about what could fail.\n\n"
    "Include a diagram using mermaid syntax inside ```mermaid blocks."
)


MAX_RETRIES = 3


def _write_md(path: Path, question: str, answer: str | None, error: str | None) -> None:
    lines = ["---", question, "---"]
    if error:
        lines.append(f"**Error:** {error}")
    elif answer:
        lines.append(answer)
    path.write_text("\n\n".join(lines))
    print(f"  [md] {path.name}", file=sys.stderr)


def run_interview(
    client: OpenAI,
    model: str,
    question_id: str,
    variant: str,
    force: bool = False,
) -> dict | None:
    bank = Bank()
    q = bank.get(question_id)
    prompt = q.get_prompt(variant)
    prompt_hash = q.prompt_hash(variant)

    out_dir = _output_dir(model)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{question_id}_{variant}.json"

    if out_path.exists() and not force:
        existing = json.loads(out_path.read_text())
        if existing.get("prompt_hash") == prompt_hash:
            print(f"  [skip] {question_id}/{variant} — already run (prompt hash matches)")
            return existing
        print(f"  [rerun] {question_id}/{variant} — prompt changed")

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": prompt},
    ]

    last_error = None
    resp = None
    t0 = time.time()
    for attempt in range(MAX_RETRIES):
        try:
            resp = chat(client, model, messages)
            break
        except Exception as exc:
            last_error = exc
            if attempt < MAX_RETRIES - 1:
                delay = 2 ** attempt
                print(f"  [retry {attempt + 1}/{MAX_RETRIES}] {question_id}/{variant}: {exc} — waiting {delay}s", file=sys.stderr)
                time.sleep(delay)
            else:
                print(f"  [error] {exc}", file=sys.stderr)

    if resp is None:
        result = {
            "model": model,
            "question_id": question_id,
            "variant": variant,
            "prompt_hash": prompt_hash,
            "timestamp": utc_now_iso(),
            "error": str(last_error),
            "content": None,
            "usage": {},
            "latency_s": time.time() - t0,
            "retries": MAX_RETRIES,
        }
        save_json(out_path, result)
        _write_md(out_path.with_suffix(".md"), prompt, None, str(last_error))
        return result

    latency_s = time.time() - t0
    content = resp["content"]
    usage = resp["usage"]
    finish_reason = resp["finish_reason"]
    result = {
        "model": model,
        "question_id": question_id,
        "variant": variant,
        "question_tier": q.tier,
        "question_title": q.title,
        "prompt_hash": prompt_hash,
        "timestamp": utc_now_iso(),
        "content": content,
        "usage": usage,
        "latency_s": latency_s,
        "finish_reason": finish_reason,
        "retries": attempt + 1,
    }

    save_json(out_path, result)
    _write_md(out_path.with_suffix(".md"), prompt, content, None)
    tokens = result["usage"].get("total_tokens", "?")
    print(f"  [done] {question_id}/{variant} — {tokens} tokens, {latency_s:.1f}s")
    return result


def _output_dir(model: str) -> Path:
    from lib import HERE
    safe_model = model.replace("/", "_")
    return HERE / "runs" / safe_model


def load_models() -> dict:
    path = HERE / "models.yaml"
    if not path.exists():
        raise SystemExit(f"models.yaml not found at {path}")
    return yaml.safe_load(path.read_text())


def main():
    parser = argparse.ArgumentParser(description="Run system designs")
    parser.add_argument("--model", default=model_name())
    parser.add_argument("--all-models", action="store_true", help="run all candidate models from models.yaml")
    parser.add_argument("--question", default=None, help="question_id or 'all'")
    parser.add_argument("--variant", default="cold", choices=["cold"])
    parser.add_argument("--tier", default=None, choices=list(TIERS))
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="list what would run")
    args = parser.parse_args()

    if args.all_models:
        models_cfg = load_models()
        models = models_cfg.get("candidates", [])
        if not models:
            raise SystemExit("No candidates found in models.yaml")
    else:
        models = [args.model]

    bank = Bank()
    errors = bank.validate()
    if errors:
        print("Bank validation failed:")
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        raise SystemExit(1)

    if args.question and args.question != "all":
        questions = [bank.get(args.question)]
    else:
        questions = list(bank)
        if args.tier:
            questions = [q for q in questions if q.tier == args.tier]

    tasks = [(m, q, args.variant) for m in models for q in questions]

    if args.dry_run:
        print(f"Would run {len(tasks)} designs across {len(models)} model(s):")
        for m, q, v in tasks:
            print(f"  [{q.tier}] {m} :: {q.id}/{v}")
        return

    client = get_client()
    for m, q, v in tasks:
        print(f"[{q.tier}] {m} :: {q.id}/{v}")
        run_interview(client, m, q.id, v, force=args.force)


if __name__ == "__main__":
    main()
