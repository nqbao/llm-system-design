"""Master orchestrator — run the full benchmark pipeline in one command."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from lib import HERE, model_name as default_model


STEPS = ["interview", "judge", "report"]


def step_interview(model: str, question: str, force: bool, dry: bool) -> None:
    cmd = ["python", "run_interview.py", "--model", model, "--question", question]
    if force:
        cmd.append("--force")
    if dry:
        cmd.append("--dry-run")
        subprocess.run(cmd, cwd=HERE)
        return

    print("\n\033[1m=== 1/3: Run interviews ===\033[0m")
    subprocess.run(cmd, cwd=HERE, check=True)


def step_judge(model: str, question: str, force: bool) -> None:
    print("\n\033[1m=== 2/3: Run judges ===\033[0m")
    runs_dir = HERE / "runs"
    if not runs_dir.exists():
        print("  No runs directory — skipping judges")
        return

    force_flag = ["--force"] if force else []
    found = False
    for run_file in sorted(runs_dir.rglob("*.json")):
        # filter by question if specified
        if question != "all" and question not in str(run_file.stem):
            continue
        found = True
        print(f"  Judging {run_file.name}...")
        result = subprocess.run(
            ["python", "run_judges.py", "--model", model, "--mode", "absolute",
             "--transcript", str(run_file)] + force_flag,
            cwd=HERE,
        )
        if result.returncode != 0:
            print(f"  [warn] judgment failed for {run_file.name} (continuing)")

    if not found:
        print("  No matching transcripts found")


def step_report() -> None:
    print("\n\033[1m=== 3/3: Report ===\033[0m")
    subprocess.run(["python", "eval.py"], cwd=HERE, check=True)
    report = HERE / "report.md"
    if report.exists():
        print(report.read_text())


def main():
    parser = argparse.ArgumentParser(
        description="Run the full system design benchmark pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python run.py url-shortener                     # single question\n"
            "  python run.py all                               # all questions\n"
            "  python run.py url-shortener --skip interview     # only judge/classify/report\n"
            "  python run.py url-shortener --model gpt-5.4      # different model\n"
            "  python run.py all --dry-run                      # show what would run\n"
        ),
    )
    parser.add_argument("question", nargs="?", default="all",
                        help="question_id or 'all' (default: all)")
    parser.add_argument("--model", default=default_model(),
                        help="LLM model name (default: $LLM_MODEL)")
    parser.add_argument("--force", action="store_true",
                        help="re-run already completed steps")
    parser.add_argument("--dry-run", action="store_true",
                        help="show what would run without executing")
    parser.add_argument("--start", default="interview", choices=STEPS,
                        help="start from this step (default: interview)")
    parser.add_argument("--skip", nargs="*", default=[],
                        choices=STEPS, help="skip these steps")
    args = parser.parse_args()

    question = args.question
    model = args.model
    force = args.force
    skip = set(args.skip)

    start_idx = STEPS.index(args.start)
    steps_to_run = [s for i, s in enumerate(STEPS) if i >= start_idx and s not in skip]

    if not steps_to_run:
        print("No steps to run (all skipped or start past end)")
        return

    print(f"Pipeline: {' → '.join(steps_to_run)}")
    print(f"Model: {model}")
    print(f"Question: {question}")
    if force:
        print("  (force mode: re-running completed steps)")

    step_funcs = {
        "interview": lambda: step_interview(model, question, force, args.dry_run),
        "judge": lambda: step_judge(model, question, force),
        "report": step_report,
    }

    for step in steps_to_run:
        if args.dry_run and step != "interview":
            print(f"  Would run: {step}")
            continue
        try:
            step_funcs[step]()
        except subprocess.CalledProcessError as exc:
            print(f"  [error] step '{step}' failed: {exc}", file=sys.stderr)
            sys.exit(1)
        except KeyboardInterrupt:
            print(f"\n  Stopped at step '{step}'")
            sys.exit(0)

    print("\n\033[1mDone.\033[0m")


if __name__ == "__main__":
    main()
