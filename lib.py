"""Shared utilities for the system design benchmark."""

from __future__ import annotations

import json
import os
import re
import statistics
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI

HERE = Path(__file__).resolve().parent
load_dotenv(HERE / ".env")

TIERS = ("easy", "medium", "hard", "chaos")


def env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def base_url() -> str:
    return env("LLM_BASE_URL", "https://api.openai.com/v1")


def api_key() -> str:
    return env("LLM_API_KEY", "sk-placeholder")


def model_name() -> str:
    return env("LLM_MODEL", "gpt-4")


def get_client(timeout_s: float = 600.0) -> OpenAI:
    return OpenAI(
        base_url=base_url(),
        api_key=api_key(),
        timeout=timeout_s,
    )


def chat(
    client: OpenAI,
    model: str,
    messages: list[dict],
    max_tokens: int = 32768,
) -> dict:
    kwargs: dict = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "reasoning_effort": "high",
    }
    resp = client.chat.completions.create(**kwargs)
    choice = resp.choices[0]

    reasoning_tokens = 0
    if resp.usage and resp.usage.completion_tokens_details:
        reasoning_tokens = resp.usage.completion_tokens_details.reasoning_tokens or 0
    rc = getattr(choice.message, "reasoning_content", None)
    if not reasoning_tokens and rc:
        reasoning_tokens = len(rc) // 4

    return {
        "content": choice.message.content,
        "finish_reason": choice.finish_reason,
        "usage": {
            "prompt_tokens": resp.usage.prompt_tokens if resp.usage else 0,
            "completion_tokens": resp.usage.completion_tokens if resp.usage else 0,
            "total_tokens": resp.usage.total_tokens if resp.usage else 0,
            "reasoning_tokens": reasoning_tokens,
        },
        "model": resp.model,
    }


def chat_json(
    client: OpenAI,
    model: str,
    messages: list[dict],
    max_retries: int = 3,
    max_tokens: int = 16384,
) -> dict:
    last_error = None
    messages = list(messages)
    for attempt in range(max_retries):
        kwargs: dict = {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
            "response_format": {"type": "json_object"},
            "reasoning_effort": "high",
        }
        resp = client.chat.completions.create(**kwargs)
        raw = resp.choices[0].message.content
        try:
            parsed = json.loads(raw)
            return {
                "parsed": parsed,
                "raw": raw,
                "usage": {
                    "prompt_tokens": resp.usage.prompt_tokens if resp.usage else 0,
                    "completion_tokens": resp.usage.completion_tokens if resp.usage else 0,
                    "total_tokens": resp.usage.total_tokens if resp.usage else 0,
                },
                "attempts": attempt + 1,
            }
        except json.JSONDecodeError as exc:
            last_error = exc
            messages.append({"role": "assistant", "content": raw})
            messages.append({
                "role": "user",
                "content": f"Invalid JSON: {exc}. Please output ONLY a valid JSON object.",
            })
    return {
        "parsed": None,
        "raw": None,
        "usage": {},
        "attempts": max_retries,
        "error": str(last_error),
    }


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def save_json(path: Path | str, data: dict) -> None:
    Path(path).write_text(json.dumps(data, indent=2, ensure_ascii=False))


def load_json(path: Path | str) -> dict:
    return json.loads(Path(path).read_text())


def strip_preamble(text: str) -> str:
    text = re.sub(
        r"^(I am|As an|As a)\s+[\w\s\-.]{0,200}?\b(assistant|model|LLM|AI)\b[^.]*\.",
        "",
        text,
        count=1,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"^(Sure|Certainly|Absolutely|Of course|Great question)[!,.]?\s*",
        "",
        text,
        count=1,
        flags=re.IGNORECASE,
    )
    return text.strip()


def pearson_r(xs: list[float], ys: list[float]) -> float:
    n = len(xs)
    if n < 2:
        return 0.0
    mx = statistics.mean(xs)
    my = statistics.mean(ys)
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sx = statistics.stdev(xs)
    sy = statistics.stdev(ys)
    if sx == 0 or sy == 0:
        return 0.0
    return cov / ((n - 1) * sx * sy)
