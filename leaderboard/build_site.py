"""Generate Starlight site from runs and judgments."""

from __future__ import annotations

import json
import re
import statistics
from collections import defaultdict
from pathlib import Path

import sys

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from annotation_analysis import (
    compute_judge_agreement,
    compute_judge_correlations,
    compute_length_regression,
    format_judge_agreement_html,
    format_length_regression_html,
    load_reasoning_texts,
)

HERE = Path(__file__).resolve().parent
SITE = HERE / "starlight" / "src" / "content" / "docs"
RUNS = HERE.parent / "runs"
JUDGMENTS = HERE.parent / "judgments"
TEMPLATES = HERE / "templates"

DIMENSIONS = [
    "requirements_scoping",
    "capacity_estimation",
    "architecture_coherence",
    "deep_dive_depth",
    "tradeoffs_failure_modes",
]

DIM_LABELS = {
    "requirements_scoping": "Scoping",
    "capacity_estimation": "Capacity",
    "architecture_coherence": "Architecture",
    "deep_dive_depth": "Deep-Dive",
    "tradeoffs_failure_modes": "Tradeoffs",
}

TIER_ORDER = {"easy": 0, "medium": 1, "hard": 2, "chaos": 3}

TIER_BADGE = {
    "easy": '<span class="tier-badge">🟢</span>',
    "medium": '<span class="tier-badge">🟡</span>',
    "hard": '<span class="tier-badge">🟠</span>',
    "chaos": '<span class="tier-badge">🔴</span>',
}


# ── Utilities ─────────────────────────────────────────────────────────────────


def _sl(name: str) -> str:
    """Starlight slug — strip dots."""
    return name.replace(".", "")


def _badge(score: float, label: str | None = None) -> str:
    classes = _score_class(score)
    display = label if label is not None else f"{score:.2f}"
    return f'<span class="badge-score {classes}">{display}</span>'


def _score_class(score: float) -> str:
    if score >= 4.5:
        return "score-excellent"
    elif score >= 3.75:
        return "score-good"
    elif score >= 3.5:
        return "score-above"
    elif score >= 2.5:
        return "score-okay"
    elif score >= 1.5:
        return "score-weak"
    else:
        return "score-poor"


def _rank_badge(rank: int) -> str:
    extra = ""
    if rank == 1:
        extra = "top-1"
    elif rank == 2:
        extra = "top-2"
    elif rank == 3:
        extra = "top-3"
    return f'<span class="rank-num {extra}">{rank}</span>'


def _write_md(path: Path, content: str):
    path.parent.mkdir(exist_ok=True, parents=True)
    path.write_text(content.strip() + "\n")


# ── Data loading ──────────────────────────────────────────────────────────────


def load_questions():
    qpath = HERE.parent / "questions.yaml"
    with open(qpath) as f:
        return yaml.safe_load(f)


def get_judges():
    return sorted(
        p.name for p in JUDGMENTS.iterdir() if p.is_dir() and not p.name.startswith(".")
    )


def get_models():
    return sorted(
        p.name for p in RUNS.iterdir() if p.is_dir() and not p.name.startswith(".")
    )


def get_judgments(model: str, question_id: str) -> dict:
    """Collect all judge judgments for a model+question, keyed by judge name."""
    result = {}
    for judge_dir in JUDGMENTS.iterdir():
        if not judge_dir.is_dir() or judge_dir.name.startswith("."):
            continue
        jpath = judge_dir / model / question_id / "cold_absolute.json"
        if jpath.exists():
            try:
                result[judge_dir.name] = json.loads(jpath.read_text())
            except (json.JSONDecodeError, OSError):
                continue
    return result


def load_leaderboard_data() -> list[dict]:
    """Compute leaderboard from judgments on disk."""
    questions = load_questions()
    models = get_models()
    judges = get_judges()

    model_scores: dict[str, list[float]] = defaultdict(list)
    model_ci: dict[str, float] = {}

    for model in models:
        total_scores = []
        for q in questions:
            qid = q["id"]
            judgments = get_judgments(model, qid)
            if not judgments:
                continue
            all_scores = []
            for dim in DIMENSIONS:
                for jdata in judgments.values():
                    sys_scores = jdata.get("scores", {}).get("system_design", {})
                    dim_data = sys_scores.get(dim, {})
                    s = dim_data.get("score") if isinstance(dim_data, dict) else dim_data
                    if isinstance(s, (int, float)):
                        all_scores.append(s)
            if all_scores:
                overall = statistics.mean(all_scores)
                total_scores.append(overall)
        if total_scores:
            mean = statistics.mean(total_scores)
            stdev = statistics.stdev(total_scores) if len(total_scores) > 1 else 0
            ci = 1.96 * stdev / (len(total_scores) ** 0.5) if len(total_scores) > 1 else 0
            model_scores[model] = total_scores
            model_ci[model] = ci

    leaderboard = sorted(
        [
            {
                "model": m,
                "mean": round(statistics.mean(scores), 2),
                "n": len(scores),
                "ci": round(model_ci[m], 2),
            }
            for m, scores in model_scores.items()
            if scores
        ],
        key=lambda r: r["mean"],
        reverse=True,
    )
    return leaderboard


# ── Page builders ─────────────────────────────────────────────────────────────


def build_index(models, questions, leaderboard):
    """Generate docs/index.md — the homepage."""
    models_count = len(models)
    questions_count = len(questions)
    judge_count = len(get_judges())

    lb_rows = "\n".join(
        f"| {_rank_badge(i + 1)} | [{row['model']}](models/{_sl(row['model'])}/) | {_badge(row['mean'])} | ±{row['ci']} | {row['n']} |"
        for i, row in enumerate(leaderboard)
    )

    template = (TEMPLATES / "index.md").read_text()
    return (
        template
        .replace("{{ MODELS_COUNT }}", str(models_count))
        .replace("{{ QUESTIONS_COUNT }}", str(questions_count))
        .replace("{{ JUDGE_COUNT }}", str(judge_count))
        .replace("{{ TOTAL_TRANSCRIPTS }}", str(models_count * questions_count))
        .replace("{{ LEADERBOARD_ROWS }}", lb_rows)
    )


def build_about(results_html: str = ""):
    """Generate about.md — methodology page with Results section."""
    content = (TEMPLATES / "about.md").read_text()
    if results_html:
        content += "\n\n---\n\n## Results\n\n"
        content += (
            '<p><em>Model scores and rankings are on the <a href="/">leaderboard</a>. '
            "Below are some additional observations about the judges themselves.</em></p>\n\n"
        )
        content += results_html
        content += "\n"
    return content


def build_model_md(model, questions, leaderboard):
    """Generate models/<model>.md — one model's scores across all questions."""
    leaderboard_match = next((r for r in leaderboard if r["model"] == model), None)
    header_text = ""
    if leaderboard_match:
        rank = next(i + 1 for i, r in enumerate(leaderboard) if r["model"] == model)
        header_text = f"*Rank **#{rank}** · Mean score **{leaderboard_match['mean']}** (±{leaderboard_match['ci']})*"

    rows = ""
    for q in sorted(questions, key=lambda q: (TIER_ORDER.get(q["tier"], 99), q["id"])):
        qid = q["id"]
        judgments = get_judgments(model, qid)

        if judgments:
            all_scores = []
            dim_parts = []
            for dim in DIMENSIONS:
                dim_scores = []
                for jname, jdata in judgments.items():
                    sys_scores = jdata.get("scores", {}).get("system_design", {})
                    dim_data = sys_scores.get(dim, {})
                    s = dim_data.get("score") if isinstance(dim_data, dict) else dim_data
                    if isinstance(s, (int, float)):
                        dim_scores.append(s)
                if dim_scores:
                    avg = statistics.mean(dim_scores)
                    all_scores.append(avg)
                    dim_parts.append(f"{DIM_LABELS.get(dim, dim)}: {_badge(avg, f'{avg:.1f}')}")
            overall = statistics.mean(all_scores) if all_scores else 0
            score_text = _badge(overall, f"{overall:.1f}")
        else:
            overall = 0
            dim_parts = []
            score_text = '<span class="badge-score score-missing">—</span>'

        has_md = (RUNS / model / f"{qid}_cold.md").exists()
        q_link = f"[{q['title']}]({qid}/)" if has_md else q["title"]
        rows += f"| <span class=\"tier-badge\">{TIER_BADGE.get(q['tier'], '')}</span> | {q_link} | {score_text} |\n"

    return f"""---
title: {model}
---

{header_text}

| Tier | Question | Overall |
|------|----------|---------|
{rows}
"""


def build_transcript_md(model, question_id, questions):
    """Generate models/<model>/<qid>.md — transcript + judgment scores."""
    q_info = next((q for q in questions if q["id"] == question_id), None)
    title = q_info["title"] if q_info else question_id
    tier = q_info["tier"] if q_info else "unknown"
    badge = TIER_BADGE.get(tier, "")

    judgments = get_judgments(model, question_id)

    score_section = ""
    all_scores = []
    if judgments:
        judge_overalls = []
        for jname, jdata in sorted(judgments.items()):
            sys_scores = jdata.get("scores", {}).get("system_design", {})
            judge_scores = []
            judge_rows = ""
            for dim in DIMENSIONS:
                dim_data = sys_scores.get(dim, {})
                s = dim_data.get("score") if isinstance(dim_data, dict) else dim_data
                reasoning = dim_data.get("reasoning", "") if isinstance(dim_data, dict) else ""
                if isinstance(s, (int, float)):
                    judge_scores.append(s)
                    judge_rows += (
                        f"| {DIM_LABELS.get(dim, dim)} | {_badge(s)} | {reasoning} |\n"
                    )
            overall = statistics.mean(judge_scores) if judge_scores else 0
            judge_overalls.append(overall)
            all_scores.extend(judge_scores)

            score_section += f"""
<details>
<summary><strong>{jname}</strong> — overall: {_badge(overall)}</summary>

| Dimension | Score | Reasoning |
|-----------|-------|-----------|
{judge_rows}
</details>
"""

        avg_all = statistics.mean(all_scores) if all_scores else 0
        avg_judge = statistics.mean(judge_overalls) if judge_overalls else 0
    else:
        avg_all = 0
        avg_judge = 0
        score_section = "\n*No judgments yet.*\n"

    # Read transcript markdown
    md_path = RUNS / model / f"{question_id}_cold.md"
    transcript_body = ""
    if md_path.exists():
        raw = md_path.read_text()
        transcript_body = raw.strip()

    judgments_section = f"""
## Scores

Overall: {_badge(avg_all)}
{score_section}
"""

    transcript_section = ""
    if transcript_body:
        transcript_section = f"""
## Transcript

{transcript_body}
"""

    return f"""---
title: {title}
---

{badge} **{tier.upper()}** — *{q_info.get('prompt_cold', '') if q_info else ''}*

Model: **{model}**
{judgments_section}
{transcript_section}
"""


# ── Main ─────────────────────────────────────────────────────────────────────


def main():
    questions = load_questions()
    leaderboard = load_leaderboard_data()
    models = get_models()

    SITE.mkdir(parents=True, exist_ok=True)

    # Homepage
    _write_md(SITE / "index.md", build_index(models, questions, leaderboard))
    print("✓ index.md")

    # About
    reasoning_records = load_reasoning_texts(JUDGMENTS)
    correlations = compute_judge_correlations(reasoning_records) if reasoning_records else {"judges": [], "judge_correlations": [], "n_runs": 0}
    agreement = compute_judge_agreement(reasoning_records) if reasoning_records else {}
    regression = compute_length_regression(reasoning_records, RUNS) if reasoning_records else {"r2": 0, "n_points": 0}
    agreement_html = format_judge_agreement_html(correlations, agreement)
    regression_html = format_length_regression_html(regression)
    _write_md(SITE / "about.md", build_about(f"{agreement_html}\n\n{regression_html}"))
    print("✓ about.md")

    # Per-model pages
    models_dir = SITE / "models"
    models_dir.mkdir(exist_ok=True)
    for model in models:
        _write_md(models_dir / f"{model}.md", build_model_md(model, questions, leaderboard))
        print(f"  models/{model}.md")

        # Transcript pages
        for q in questions:
            qid = q["id"]
            md_path = RUNS / model / f"{qid}_cold.md"
            if not md_path.exists():
                continue
            mdir = models_dir / model
            mdir.mkdir(exist_ok=True)
            _write_md(mdir / f"{qid}.md", build_transcript_md(model, qid, questions))
        count = sum(1 for q in questions if (RUNS / model / (q["id"] + "_cold.md")).exists())
        print(f"    {count} transcripts")

    # Generate sidebar config
    sidebar_path = HERE / "starlight" / "src" / "sidebar.gen.js"
    model_links = ",\n".join(
        f"\t\t{{ label: '{model}', slug: 'models/{_sl(model)}' }}"
        for model in models
    )
    sidebar_content = f"""// Generated by build_site.py — do not edit manually
export const sidebar = [
\t{{ label: 'Home', slug: '' }},
\t{{
\t\tlabel: 'Models',
\t\tcollapsed: true,
\t\titems: [
{model_links},
\t\t],
\t}},
\t{{ label: 'Methodology', slug: 'about' }},
];
"""
    sidebar_path.write_text(sidebar_content)
    print("✓ src/sidebar.gen.js")

    print(f"\nDone! Source generated in {SITE}/")
    print("Build the site with:")
    print("  cd leaderboard/starlight && npm run build")
    print("Or serve locally with:")
    print("  cd leaderboard/starlight && npm run dev")


if __name__ == "__main__":
    main()
