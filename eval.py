"""Analyze results and produce a markdown report from files on disk."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path

from annotation_analysis import (
    compute_dimension_score_profiles,
    compute_model_annotation_profile,
    format_annotation_report,
    load_reasoning_texts,
)
from lib import HERE, pearson_r


def fetch_data():
    run_data: dict[tuple[str, str, str], dict] = {}
    for rf in HERE.glob("runs/*/*_cold.json"):
        d = json.loads(rf.read_text())
        key = (d.get("model", ""), d.get("question_id", ""), d.get("variant", ""))
        run_data[key] = {
            "content_len": len(d.get("content", "") or ""),
            "word_count": len((d.get("content", "") or "").split()),
        }

    judgment_rows: list[tuple] = []
    unparseable_count = 0
    for jf in HERE.glob("judgments/*/*/*/cold_absolute.json"):
        d = json.loads(jf.read_text())
        judge_model = jf.parent.parent.parent.name
        model = d.get("model_under_test", "")
        qid = d.get("question_id", "")
        variant = d.get("variant", "")
        scores = d.get("scores", {}).get("system_design") or {}
        if not isinstance(scores, dict):
            unparseable_count += 1
            continue
        for dim, sc in scores.items():
            score = sc.get("score") if isinstance(sc, dict) else sc
            if isinstance(score, (int, float)):
                judgment_rows.append((judge_model, qid, variant, model, dim, score))

    return run_data, judgment_rows, unparseable_count


def compute_leaderboard(judgment_rows: list) -> list[dict]:
    model_dim_scores: dict[tuple[str, str], list[float]] = defaultdict(list)
    transcript_scores: dict[tuple[str, str, str], list[float]] = defaultdict(list)

    for judge_id, qid, variant, model, dim, score in judgment_rows:
        model_dim_scores[(model, dim)].append(score)
        transcript_scores[(model, qid, variant)].append(score)

    model_means: dict[str, list[float]] = defaultdict(list)
    for (model, _, _), scores in transcript_scores.items():
        model_means[model].append(statistics.mean(scores))

    rows = []
    models = sorted(model_means.keys())
    for model in models:
        transcript_means = model_means[model]
        mean = statistics.mean(transcript_means)
        n = len(transcript_means)
        std = statistics.stdev(transcript_means) if n > 1 else 0
        ci = 1.96 * std / (n ** 0.5) if n > 1 else 0

        dim_means = {}
        for dim in sorted(set(d for (m, d) in model_dim_scores if m == model)):
            ds = model_dim_scores[(model, dim)]
            dim_means[dim] = round(statistics.mean(ds), 2) if ds else 0

        rows.append({
            "model": model,
            "mean": round(mean, 2),
            "ci": round(ci, 2),
            "n": n,
            "dim_scores": dim_means,
        })
    rows.sort(key=lambda r: r["mean"], reverse=True)
    return rows


def compute_inter_judge_correlation(judgment_rows: list) -> dict:
    run_scores: dict[tuple, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for judge_id, qid, variant, model, dim, score in judgment_rows:
        run_scores[(model, qid, variant)][judge_id].append(score)

    judge_labels = sorted({row[0] for row in judgment_rows})
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
                    pairs.append((judge_labels[i], judge_labels[j], round(pearson_r(xs, ys), 3)))

    return {"judge_correlations": pairs, "judges": judge_labels, "n_runs": sum(1 for v in run_scores.values())}


def compute_inter_judge_agreement(judgment_rows: list) -> dict:
    scores: dict[tuple, dict[str, float]] = defaultdict(dict)
    for judge_id, qid, variant, model, dim, score in judgment_rows:
        scores[(model, qid, variant, dim)][judge_id] = score

    dim_deviations: dict[str, list[float]] = defaultdict(list)
    for key, judge_scores in scores.items():
        dim = key[3]
        if len(judge_scores) < 2:
            continue
        vals = list(judge_scores.values())
        dim_deviations[dim].append(max(vals) - min(vals))

    result = {}
    for dim, devs in dim_deviations.items():
        result[dim] = {"mean_range": round(statistics.mean(devs), 2), "n_cases": len(devs)}
    return result


def _slope(xs: list[float], ys: list[float]) -> float:
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


def compute_length_regression(judgment_rows: list, run_data: dict) -> dict:
    transcript_scores: dict[tuple, list[float]] = defaultdict(list)
    for judge_id, qid, variant, model, dim, score in judgment_rows:
        transcript_scores[(model, qid, variant)].append(score)

    xs, ys = [], []
    for (model, qid, variant), scores in transcript_scores.items():
        run = run_data.get((model, qid, variant))
        if not run:
            continue
        xs.append(math.log(run["word_count"] + 1))
        ys.append(statistics.mean(scores))

    if len(xs) < 3:
        return {"r2": 0, "r": 0, "slope": 0, "n_points": len(xs), "notes": "insufficient data (need ≥3 transcripts)"}

    r = pearson_r(xs, ys)
    r2 = r * r
    b = _slope(xs, ys)

    return {
        "r2": round(r2, 3),
        "r": round(r, 3),
        "slope": round(b, 4),
        "n_points": len(xs),
        "notes": f"score ~ log(words); R²={r2:.3f}, slope={b:+.4f} (length explains {r2*100:.1f}% of score variance)",
    }


def compute_disagreement_gallery(judgment_rows: list, top_n: int = 20) -> list[dict]:
    run_scores: dict[tuple, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for judge_id, qid, variant, model, dim, score in judgment_rows:
        run_scores[(model, qid, variant)][judge_id].append(score)

    disagreements = []
    for key, judge_scores in run_scores.items():
        if len(judge_scores) < 2:
            continue
        means = {j: statistics.mean(sc) for j, sc in judge_scores.items()}
        vals = list(means.values())
        spread = max(vals) - min(vals)
        if spread > 0:
            disagreements.append({
                "model": key[0],
                "question_id": key[1],
                "variant": key[2],
                "judge_means": {j: round(m, 2) for j, m in means.items()},
                "spread": round(spread, 2),
            })

    disagreements.sort(key=lambda d: d["spread"], reverse=True)
    return disagreements[:top_n]


def format_report(leaderboard, agreement, judge_corr, regression, gallery, runs, judgment_rows, unparseable_count, annotation_report="") -> str:
    lines = []
    lines.append("# System Design Benchmark Report\n")

    lines.append("## Leaderboard\n")
    lines.append("| Rank | Model | Mean Score | ±CI | Runs | Per-Dimension (avg) |")
    lines.append("|------|-------|-----------|-----|---|---------------------|")
    for i, row in enumerate(leaderboard):
        dims = ", ".join(f"{k}:{v}" for k, v in row["dim_scores"].items())
        lines.append(f"| {i+1} | {row['model']} | {row['mean']} | ±{row['ci']} | {row['n']} | {dims} |")
    lines.append("")

    lines.append("## Inter-Judge Agreement\n")
    if len(judge_corr["judges"]) < 2:
        lines.append("Single-judge mode — inter-judge agreement not applicable.\n")
    elif judge_corr["judge_correlations"]:
        lines.append("Pearson r between judge mean scores per transcript:\n")
        lines.append("| Judge A | Judge B | r |")
        lines.append("|---------|---------|---|")
        for ja, jb, corr in judge_corr["judge_correlations"]:
            label = "consistent" if corr >= 0.7 else ("borderline" if corr >= 0.5 else "noisy")
            icon = "✅" if corr >= 0.7 else ("⚠️" if corr >= 0.5 else "❌")
            lines.append(f"| {ja} | {jb} | {corr} ({icon} {label}) |")
    else:
        lines.append("Insufficient data: need multiple transcripts with all judges to compute correlation.")
    lines.append(f"  *Based on {judge_corr['n_runs']} transcripts*\n")

    if len(judge_corr["judges"]) >= 2 and agreement:
        lines.append("### Per-Dimension Range Across Judges\n")
        lines.append("| Dimension | Mean Range | Cases |")
        lines.append("|-----------|-----------|-------|")
        for dim, info in agreement.items():
            lines.append(f"| {dim} | {info['mean_range']} | {info['n_cases']} |")
    lines.append("")

    lines.append("## Length Bias Analysis\n")
    r2 = regression["r2"]
    b = regression["slope"]
    n = regression["n_points"]
    lines.append(
        f"Longer transcripts tend to score higher "
        f"(slope={b}, R²={r2}, n={n}) — "
        f"length explains {r2*100:.1f}% of score variance, "
        f"worth monitoring for judge verbosity bias.\n"
    )

    if len(judge_corr["judges"]) >= 2 and gallery:
        lines.append("## Disagreement Gallery\n")
        lines.append("Transcripts where judges disagreed most (top by spread):\n")
        lines.append("| Model | Question | Variant | Judge Means | Spread |")
        lines.append("|-------|----------|---------|-------------|--------|")
        for item in gallery[:10]:
            means_str = ", ".join(f"{j}:{m}" for j, m in item["judge_means"].items())
            lines.append(f"| {item['model']} | {item['question_id']} | {item['variant']} | {means_str} | {item['spread']} |")
        lines.append("")

    if annotation_report:
        lines.append(annotation_report)

    lines.append("---")
    n_runs = len(runs)
    n_scores = len(judgment_rows)
    lines.append(f"*Generated from {n_runs} transcripts, {n_scores} scores across {len(judge_corr['judges'])} judges*")
    if unparseable_count:
        lines.append(f"*WARNING: {unparseable_count} transcript(s) had unparseable judgments*")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Analyze and report")
    parser.add_argument("--output", default="report.md", help="Output markdown file")
    args = parser.parse_args()

    run_data, judgment_rows, unparseable_count = fetch_data()

    if not judgment_rows:
        print("No judgment data found. Run run_judges.py first.")
        return

    leaderboard = compute_leaderboard(judgment_rows)
    agreement = compute_inter_judge_agreement(judgment_rows)
    judge_corr = compute_inter_judge_correlation(judgment_rows)
    regression = compute_length_regression(judgment_rows, run_data)
    gallery = compute_disagreement_gallery(judgment_rows)

    reasoning_records = load_reasoning_texts(HERE / "judgments")
    model_profiles = compute_model_annotation_profile(reasoning_records) if reasoning_records else []
    dim_profiles = compute_dimension_score_profiles(reasoning_records) if reasoning_records else {}
    annotation_report = format_annotation_report(model_profiles, dim_profiles) if model_profiles else ""

    report = format_report(leaderboard, agreement, judge_corr, regression, gallery, run_data, judgment_rows, unparseable_count, annotation_report)
    Path(args.output).write_text(report)
    print(f"Report written to {args.output}")
    print(report)


if __name__ == "__main__":
    main()
