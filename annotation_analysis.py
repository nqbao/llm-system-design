"""Shared annotation analysis — mines judge reasoning texts for qualitative insights.

Used by both eval.py (report.md) and build_site.py (Starlight site).
"""

from __future__ import annotations

import json
import re
import statistics
from collections import defaultdict
from pathlib import Path

DIMENSIONS = [
    "requirements_scoping",
    "capacity_estimation",
    "architecture_coherence",
    "deep_dive_depth",
    "tradeoffs_failure_modes",
]

DIM_LABELS = {
    "requirements_scoping": "Requirements Scoping",
    "capacity_estimation": "Capacity Estimation",
    "architecture_coherence": "Architecture Coherence",
    "deep_dive_depth": "Deep-Dive Depth",
    "tradeoffs_failure_modes": "Tradeoffs & Failure Modes",
}

POSITIVE_PATTERNS = {
    "explicit non-goals": r"\b(explicit\s+non[- ]goals?|non[- ]goals?\s+(are\s+)?set|out\s+of\s+scope)",
    "quantified SLAs": r"\b(quantified\s+(SLA|target|requirement)|99\.\d+%\s+(uptime|availability)|p\d{2}\s+latency)",
    "resolves ambiguity": r"\b(ambiguity|prompt\s+doesn['\u2019]t\s+specify)",
    "data flow traceable": r"\b(data\s+flow\s+(is\s+)?traceable|traceable\s+end[- ]to[- ]end)",
    "concrete mechanism": r"\b(concrete\s+mechanism|specific\s+(sharding|index|replication|strategy))",
    "non-obvious edge case": r"\b(non[- ]obvious\s+(edge\s+case|failure\s+mode)|thundering\s+herd|hot\s+key|write\s+amplification|gc\s+tail\s+latency)",
    "sanity check": r"\b(sanity[- ]?check|does\s+that\s+make\s+sense|fragile\s+assumptions)",
    "acknowledges limits": r"\b((design|cannot\s+handle|not\s+build(?:ing)?)\s+(if\s+|when\s+|yet\b)|this\s+design\s+(cannot|cannot\s+handle)|acknowledge)",
    "avoids anti-pattern": r"\b(anti[- ]?pattern|do\s+not\s+use|better\s+choice|less\s+obvious\s+but)",
    "no hallucinations": r"\bno\s+hallucinat",
}

NEGATIVE_PATTERNS = {
    "no quantified targets": r"\b(no\s+(quantified|specific)\s+(SLA|target|requirement|number)|missing\s+(quantified|SLA|target))",
    "missing non-goals": r"\b(no\s+(explicit\s+)?non[- ]goals?|missing\s+non[- ]goals?|lacks?\s+non[- ]goals?)",
    "shallow deep-dives": r"\b(no\s+deep[- ]?dive|shallow|one\s+(shallow\s+)?deep[- ]?dive)",
    "buzzword/decorative": r"\b(decorative|buzzword|just\s+(named|listed)|no\s+justification|dead\s+weight)",
    "hallucinated": r"\b(?<!no\s)(?<!without\s)hallucinat|\bcontradict",
    "no failure modes": r"\b(no\s+failure|missing\s+failure)",
    "not derived": r"\b(not\s+derive|not\s+propagat|pulled\s+from\s+(thin\s+)?air)",
    "no tradeoffs": r"\b(no\s+tradeoff|generic\s+tradeoff|not\s+tied\s+to|not\s+quantified)",
}


def load_reasoning_texts(judgments_dir: Path) -> list[dict]:
    """Load all per-dimension reasoning texts with context from judgment files."""
    records = []
    for jf in sorted(judgments_dir.glob("*/*/*/cold_absolute.json")):
        try:
            d = json.loads(jf.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        judge = jf.parent.parent.parent.name
        model = d.get("model_under_test", "")
        qid = d.get("question_id", "")
        scores = d.get("scores", {}).get("system_design") or {}
        if not isinstance(scores, dict):
            continue
        for dim, sc in scores.items():
            if not isinstance(sc, dict):
                continue
            reasoning = sc.get("reasoning", "")
            score = sc.get("score")
            if isinstance(score, (int, float)) and reasoning:
                records.append({
                    "judge": judge,
                    "model": model,
                    "question_id": qid,
                    "dimension": dim,
                    "score": score,
                    "reasoning": reasoning,
                })
    return records


def _match_patterns(text: str, patterns: dict[str, str]) -> dict[str, bool]:
    text_lower = text.lower()
    return {name: bool(re.search(pat, text_lower)) for name, pat in patterns.items()}


def compute_model_annotation_profile(records: list[dict]) -> list[dict]:
    """For each model, aggregate prevalence of praise/criticism phrases."""
    model_records: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        model_records[r["model"]].append(r)

    profiles = []
    for model, recs in sorted(model_records.items()):
        n = len(recs)
        pos_hits = defaultdict(int)
        neg_hits = defaultdict(int)
        for r in recs:
            for name, matched in _match_patterns(r["reasoning"], POSITIVE_PATTERNS).items():
                if matched:
                    pos_hits[name] += 1
            for name, matched in _match_patterns(r["reasoning"], NEGATIVE_PATTERNS).items():
                if matched:
                    neg_hits[name] += 1

        top_praise = sorted(
            [(name, count / n) for name, count in pos_hits.items()],
            key=lambda x: -x[1],
        )
        top_crit = sorted(
            [(name, count / n) for name, count in neg_hits.items()],
            key=lambda x: -x[1],
        )
        profiles.append({
            "model": model,
            "n_annotations": n,
            "praise": [(name, round(pct, 3)) for name, pct in top_praise if pct > 0],
            "criticism": [(name, round(pct, 3)) for name, pct in top_crit if pct > 0],
        })
    return profiles


def compute_dimension_score_profiles(records: list[dict]) -> dict[str, dict]:
    """For each dimension, show phrase prevalence at each score level."""
    profiles = {}
    for dim in DIMENSIONS:
        dim_recs = [r for r in records if r["dimension"] == dim]
        score_groups: dict[int, list[dict]] = defaultdict(list)
        for r in dim_recs:
            score_groups[r["score"]].append(r)

        score_profiles = {}
        for score in sorted(score_groups.keys(), reverse=True):
            recs = score_groups[score]
            n = len(recs)
            pos_hits = defaultdict(int)
            neg_hits = defaultdict(int)
            for r in recs:
                for name, matched in _match_patterns(r["reasoning"], POSITIVE_PATTERNS).items():
                    if matched:
                        pos_hits[name] += 1
                for name, matched in _match_patterns(r["reasoning"], NEGATIVE_PATTERNS).items():
                    if matched:
                        neg_hits[name] += 1
            top_pos = sorted(
                [(name, cnt / n) for name, cnt in pos_hits.items()],
                key=lambda x: -x[1],
            )[:3]
            top_neg = sorted(
                [(name, cnt / n) for name, cnt in neg_hits.items()],
                key=lambda x: -x[1],
            )[:3]
            if top_pos or top_neg:
                score_profiles[score] = {
                    "n": n,
                    "top_praise": [(n, round(p, 3)) for n, p in top_pos if p > 0],
                    "top_criticism": [(n, round(p, 3)) for n, p in top_neg if p > 0],
                }
        profiles[dim] = score_profiles
    return profiles


def format_annotation_report(
    model_profiles: list[dict],
    dim_profiles: dict[str, dict],
) -> str:
    """Format annotation analysis as markdown — for use in report.md."""
    lines = []
    lines.append("## Annotation Analysis\n")
    lines.append(
        "Qualitative analysis of judge reasoning texts. "
        "Each judge provides per-dimension reasoning justifying their score. "
        "These annotations are mined for recurring praise and criticism patterns.\n"
    )

    lines.append("### Model Strengths & Weaknesses\n")
    lines.append(
        "Prevalence of rubric phrases in judge reasoning. "
        "Shows what judges consistently praise (strengths) and criticize (weaknesses).\n"
    )
    lines.append("| Model | Top Praise | Top Criticism |")
    lines.append("|-------|------------|---------------|")
    for prof in model_profiles:
        top_p = ", ".join(
            f"\"{name}\" {pct*100:.0f}%" for name, pct in prof["praise"][:3]
        ) or "—"
        top_c = ", ".join(
            f"\"{name}\" {pct*100:.0f}%" for name, pct in prof["criticism"][:3]
        ) or "—"
        lines.append(f"| {prof['model']} | {top_p} | {top_c} |")
    lines.append("")

    lines.append("### Per-Dimension Score Profiles\n")
    lines.append(
        "For each dimension, the phrasing pattern that distinguishes "
        "score levels in judge reasoning.\n"
    )
    for dim in DIMENSIONS:
        label = DIM_LABELS.get(dim, dim)
        score_profiles = dim_profiles.get(dim, {})
        if not score_profiles:
            continue
        lines.append(f"**{label}**  ")
        for score in sorted(score_profiles.keys(), reverse=True):
            sp = score_profiles[score]
            praise_str = (
                ", ".join(f"\"{n}\" ({pct*100:.0f}%)" for n, pct in sp["top_praise"])
                or "—"
            )
            crit_str = (
                ", ".join(f"\"{n}\" ({pct*100:.0f}%)" for n, pct in sp["top_criticism"])
                or "—"
            )
            lines.append(
                f"- Score **{score}** (n={sp['n']}): "
                f"Praise: {praise_str} | "
                f"Criticism: {crit_str}"
            )
        lines.append("")

    return "\n".join(lines)


def format_annotation_html(
    model_profiles: list[dict],
    dim_profiles: dict[str, dict],
) -> str:
    """Format annotation analysis as HTML — for use in Starlight."""

    def _bar(pct: float) -> str:
        width = max(1, int(pct * 100))
        color = "#22c55e" if pct > 0.5 else "#eab308"
        return f'<div style="width:{width}%;min-width:2px;height:8px;background:{color};border-radius:4px"></div>'

    parts = []

    # ── Model Strengths & Weaknesses ──
    parts.append("<h3>Model Strengths &amp; Weaknesses</h3>")
    parts.append(
        '<p>Prevalence of rubric phrases in judge reasoning. '
        'Shows what judges consistently praise (strengths) and criticize (weaknesses).</p>'
    )

    for prof in model_profiles:
        model = prof["model"]
        parts.append(f'<details><summary><strong>{model}</strong></summary>')
        parts.append('<table style="width:100%">')
        parts.append(
            "<tr><th>Type</th><th>Phrase</th><th>Prevalence</th></tr>"
        )
        for name, pct in prof["praise"][:4]:
            parts.append(
                f"<tr><td>✅ Praise</td><td>{name}</td>"
                f"<td>{_bar(pct)} {pct*100:.0f}%</td></tr>"
            )
        for name, pct in prof["criticism"][:4]:
            parts.append(
                f"<tr><td>❌ Criticism</td><td>{name}</td>"
                f"<td>{_bar(pct)} {pct*100:.0f}%</td></tr>"
            )
        parts.append("</table></details>")

    # ── Per-Dimension Score Profiles ──
    parts.append("<h3>Per-Dimension Score Profiles</h3>")
    parts.append(
        "<p>For each dimension, the phrasing pattern that distinguishes "
        "score levels in judge reasoning.</p>"
    )

    for dim in DIMENSIONS:
        label = DIM_LABELS.get(dim, dim)
        sp = dim_profiles.get(dim, {})
        if not sp:
            continue
        parts.append(f"<details><summary><strong>{label}</strong></summary>")
        parts.append('<table style="width:100%">')
        parts.append("<tr><th>Score</th><th>n</th><th>Praise</th><th>Criticism</th></tr>")
        for score in sorted(sp.keys(), reverse=True):
            info = sp[score]
            praise_str = (
                "<br>".join(
                    f"✅ {n} ({pct*100:.0f}%)" for n, pct in info["top_praise"]
                )
                or "—"
            )
            crit_str = (
                "<br>".join(
                    f"❌ {n} ({pct*100:.0f}%)" for n, pct in info["top_criticism"]
                )
                or "—"
            )
            parts.append(
                f"<tr><td><strong>{score}</strong></td><td>{info['n']}</td>"
                f"<td>{praise_str}</td><td>{crit_str}</td></tr>"
            )
        parts.append("</table></details>")

    return "\n".join(parts)


# ── Judge Agreement ────────────────────────────────────────────────────────


def _pearson_r(xs: list[float], ys: list[float]) -> float:
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


def compute_judge_correlations(records: list[dict]) -> dict:
    """Pearson r between judge mean scores per transcript."""
    run_scores: dict[tuple, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for r in records:
        key = (r["model"], r["question_id"])
        run_scores[key][r["judge"]].append(r["score"])

    judge_labels = sorted({r["judge"] for r in records})
    judge_means: dict[str, list[float]] = {j: [] for j in judge_labels}
    for key, j_scores in run_scores.items():
        for judge_id in judge_labels:
            if judge_id in j_scores:
                judge_means[judge_id].append(statistics.mean(j_scores[judge_id]))
            else:
                judge_means[judge_id].append(None)

    pairs = []
    if len(judge_labels) >= 2:
        for i in range(len(judge_labels)):
            for j in range(i + 1, len(judge_labels)):
                xs, ys = [], []
                for a, b in zip(judge_means[judge_labels[i]], judge_means[judge_labels[j]]):
                    if a is not None and b is not None:
                        xs.append(a)
                        ys.append(b)
                if len(xs) >= 3:
                    pairs.append((judge_labels[i], judge_labels[j], round(_pearson_r(xs, ys), 3)))

    return {"judge_correlations": pairs, "judges": judge_labels, "n_runs": len(run_scores)}


def compute_judge_agreement(records: list[dict]) -> dict:
    """Per-dimension mean range across judges."""
    scores: dict[tuple, dict[str, float]] = defaultdict(dict)
    for r in records:
        scores[(r["model"], r["question_id"], r["dimension"])][r["judge"]] = r["score"]

    dim_deviations: dict[str, list[float]] = defaultdict(list)
    for key, judge_scores in scores.items():
        dim = key[2]
        if len(judge_scores) < 2:
            continue
        vals = list(judge_scores.values())
        dim_deviations[dim].append(max(vals) - min(vals))

    result = {}
    for dim, devs in dim_deviations.items():
        result[dim] = {"mean_range": round(statistics.mean(devs), 2), "n_cases": len(devs)}
    return result


def format_judge_agreement_html(correlations: dict, agreement: dict) -> str:
    """Format judge agreement data as HTML for Starlight."""
    parts = []

    parts.append("<h3>Inter-Judge Correlation</h3>")
    if len(correlations["judges"]) < 2:
        parts.append("<p>Single-judge mode — not applicable.</p>")
    elif correlations["judge_correlations"]:
        parts.append(
            "<p>Pearson r between judge mean scores per transcript.</p>"
        )
        parts.append("<table style='width:100%'><tr><th>Judge A</th><th>Judge B</th><th>r</th></tr>")
        for ja, jb, r_val in correlations["judge_correlations"]:
            label = "consistent" if r_val >= 0.7 else ("borderline" if r_val >= 0.5 else "noisy")
            parts.append(f"<tr><td>{ja}</td><td>{jb}</td><td>{r_val} ({label})</td></tr>")
        parts.append("</table>")
        parts.append(f"<p><em>Based on {correlations['n_runs']} transcripts</em></p>")

    if len(correlations["judges"]) >= 2 and agreement:
        parts.append("<h3>Per-Dimension Judge Range</h3>")
        parts.append(
            "<p>Average spread (max − min) between judges per dimension. "
            "Higher values indicate more disagreement on that dimension.</p>"
        )
        parts.append("<table style='width:100%'><tr><th>Dimension</th><th>Mean Range</th><th>Cases</th></tr>")
        for dim, info in agreement.items():
            label = DIM_LABELS.get(dim, dim)
            parts.append(f"<tr><td>{label}</td><td>{info['mean_range']}</td><td>{info['n_cases']}</td></tr>")
        parts.append("</table>")

    return "\n".join(parts)


# ── Length Bias ────────────────────────────────────────────────────────────


def _slope(xs: list[float], ys: list[float]) -> float:
    """Simple linear regression slope."""
    n = len(xs)
    if n < 2:
        return 0.0
    mx = statistics.mean(xs)
    my = statistics.mean(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = sum((x - mx) ** 2 for x in xs)
    if den == 0:
        return 0.0
    return num / den


def compute_length_regression(records: list[dict], runs_dir: Path) -> dict:
    """Linear regression of score ~ log(word_count) to check length bias."""
    import json
    import math

    run_data: dict[tuple[str, str], int] = {}
    for rf in runs_dir.glob("*/*_cold.json"):
        try:
            d = json.loads(rf.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        model = d.get("model", "")
        qid = d.get("question_id", "")
        wc = len((d.get("content", "") or "").split())
        run_data[(model, qid)] = wc

    record_scores: dict[tuple, list[float]] = defaultdict(list)
    for r in records:
        record_scores[(r["model"], r["question_id"])].append(r["score"])

    xs, ys = [], []
    for (model, qid), wc in run_data.items():
        scores = record_scores.get((model, qid))
        if not scores:
            continue
        xs.append(math.log(wc + 1))
        ys.append(statistics.mean(scores))

    if len(xs) < 3:
        return {"r2": 0, "r": 0, "slope": 0, "n_points": len(xs)}

    r = _pearson_r(xs, ys)
    r2 = r * r
    b = _slope(xs, ys)
    return {
        "r2": round(r2, 3),
        "r": round(r, 3),
        "slope": round(b, 4),
        "n_points": len(xs),
    }


def format_length_regression_html(regression: dict) -> str:
    """Format length bias analysis as HTML."""
    parts = []
    parts.append("<h3>Length Bias</h3>")
    r2 = regression["r2"]
    b = regression["slope"]
    n = regression["n_points"]
    pct = r2 * 100
    parts.append(
        f"<p>Longer transcripts tend to score higher "
        f"(slope={b}, R²={r2}, n={n}) — "
        f"length explains {pct:.1f}% of score variance, "
        f"worth monitoring for judge verbosity bias.</p>"
    )
    return "\n".join(parts)
