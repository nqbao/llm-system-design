"""Tests for the generated Starlight site.

Usage:
    python3 leaderboard/test_site.py          # default (no BASE_PATH)
    BASE_PATH=/llm-system-design python3 leaderboard/test_site.py
"""
import os
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
DIST = HERE / "dist"

QUESTIONS = [
    "url-shortener", "chat-system", "distributed-rate-limiter",
    "metrics-aggregation", "news-feed", "global-object-store",
    "job-scheduler", "design-youtube", "design-twitter",
]

MODEL_SLUGS = [
    "claude-sonnet-46", "deepseek-v4-pro", "gemini-31-pro",
    "gemma-4-31b-it", "gpt-54", "gpt-oss-120b", "gpt-oss-20b",
    "kimi-k26", "minimax-m27",
]


# ── Helpers ──────────────────────────────────────────────────────────────

def build():
    result = subprocess.run(
        [sys.executable, "build_site.py"],
        cwd=HERE, capture_output=True, text=True,
    )
    assert result.returncode == 0, f"build_site.py failed:\n{result.stderr}"
    result = subprocess.run(
        ["npm", "run", "build"],
        cwd=HERE / "starlight", capture_output=True, text=True,
    )
    assert result.returncode == 0, f"npm run build failed:\n{result.stderr}"


def page(path: str) -> str:
    return (DIST / path).read_text()


def exists(path: str) -> bool:
    return (DIST / path).exists()


# ── Tests ────────────────────────────────────────────────────────────────

def all_pages_exist():
    assert exists("index.html")
    assert exists("about/index.html")
    assert exists("problems/index.html")
    assert exists("404.html")
    for mslug in MODEL_SLUGS:
        assert exists(f"models/{mslug}/index.html")
        for qid in QUESTIONS:
            assert exists(f"models/{mslug}/{qid}/index.html")
    print(f"  ✓ {len(MODEL_SLUGS)} models, {len(QUESTIONS)} questions, "
          f"{len(MODEL_SLUGS) * len(QUESTIONS)} transcripts")


def homepage_stat_is_consistent():
    html = page("index.html")
    m = re.search(
        r'I evaluated <strong>(\d+)[^<]*</strong>.*?'
        r'on <strong>(\d+)[^<]*</strong>.*?'
        r'with <strong>(\d+)[^<]*</strong>.*?'
        '\u2014' r' (\d+) transcripts scored',
        html,
    )
    assert m, "Stat line not found"
    n_models, n_questions, n_judges, n_total = map(int, m.groups())
    assert n_models > 0
    assert n_questions > 0
    assert n_judges > 0
    assert n_total == n_models * n_questions, (
        f"Stat mismatch: {n_total} != {n_models} × {n_questions}"
    )
    print(f"  ✓ {n_models} models × {n_questions} questions = {n_total} transcripts")


def dropdown_urls_are_absolute():
    html = page("problems/index.html")
    options = re.findall(r'<option value="(/[^"]+)">', html)
    model_opts = [o for o in options if "/models/" in o]
    assert len(model_opts) > 0, "No model dropdown options"
    for opt in model_opts:
        assert "//" not in opt, f"Double slash: {opt}"
        assert opt.endswith("/"), f"Missing trailing slash: {opt}"
    assert all(opt.startswith("/") for opt in model_opts), "Not absolute"
    print(f"  ✓ {len(model_opts)} problem dropdown options")


def transcript_dropdowns_have_all_models():
    for mslug in MODEL_SLUGS[:1]:  # spot-check first model
        for qid in QUESTIONS[:1]:
            path = f"models/{mslug}/{qid}/index.html"
            if not exists(path):
                continue
            html = page(path)
            options = re.findall(r'<option value="(/[^"]+)"', html)
            model_opts = [o for o in options if "/models/" in o]
            assert len(model_opts) == len(MODEL_SLUGS), (
                f"{path}: expected {len(MODEL_SLUGS)} model options, "
                f"got {len(model_opts)}"
            )
    print(f"  ✓ transcript dropdowns list all models")


def sidebar_is_generated():
    assert (HERE / "starlight" / "src" / "sidebar.gen.js").exists()
    content = (HERE / "starlight" / "src" / "sidebar.gen.js").read_text()
    assert "Home" in content
    assert "Models" in content
    assert "Questions" in content
    assert "Methodology" in content
    print("  ✓ sidebar generated")


def problems_page_lists_all_questions():
    html = page("problems/index.html")
    for q in QUESTIONS:
        assert q in html, f"Question '{q}' missing from problems page"
    print(f"  ✓ {len(QUESTIONS)} questions on problems page")


def invalid_mermaid_falls_back_to_code_view():
    minimax = (
        HERE
        / "starlight"
        / "src"
        / "content"
        / "docs"
        / "models"
        / "minimax-m2.7"
        / "rag-search-assistant.md"
    ).read_text()
    assert "```text\nflowchart TB" in minimax, "invalid Minimax Mermaid block should fall back to code"
    assert minimax.count("```mermaid") == 2, "valid Minimax Mermaid blocks should remain diagrams"

    gpt_oss = (
        HERE
        / "starlight"
        / "src"
        / "content"
        / "docs"
        / "models"
        / "gpt-oss-20b"
        / "rag-search-assistant.md"
    ).read_text()
    assert "```text\nflowchart TD" in gpt_oss, "invalid gpt-oss-20b Mermaid block should fall back to code"
    assert gpt_oss.count("```mermaid") == 1, "valid gpt-oss-20b Mermaid blocks should remain diagrams"
    print("  ✓ invalid Mermaid blocks fall back to code view")


def urls_respect_base_prefix():
    prefix = os.environ.get("BASE_PATH", "")
    if not prefix:
        print("  ⏭  no BASE_PATH set, skipping")
        return
    html = page("problems/index.html")
    model_urls = re.findall(r'<option value="(/[^"]+)"', html)
    model_urls = [u for u in model_urls if "/models/" in u]
    bad = [u for u in model_urls if not u.startswith(prefix)]
    assert not bad, f"URLs missing prefix '{prefix}': {bad[:5]}"
    print(f"  ✓ all URLs prefixed with '{prefix}'")


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    build()

    tests = [
        all_pages_exist,
        homepage_stat_is_consistent,
        dropdown_urls_are_absolute,
        transcript_dropdowns_have_all_models,
        sidebar_is_generated,
        problems_page_lists_all_questions,
        invalid_mermaid_falls_back_to_code_view,
        urls_respect_base_prefix,
    ]

    passed = 0
    failed = 0
    for t in tests:
        name = t.__name__.replace("_", " ")
        print(f"  {name}...")
        try:
            t()
            passed += 1
        except Exception as e:
            print(f"  ✗ FAIL: {e}")
            failed += 1

    print()
    if failed:
        print(f"  {passed} passed, {failed} FAILED")
        sys.exit(1)
    else:
        print(f"  All {passed} tests passed")


if __name__ == "__main__":
    main()
