# Repository Guidelines

## Project Structure & Module Organization
The benchmark is a flat Python project at the repo root. Core entrypoints are `run.py` (pipeline), `run_interview.py` (generate responses), `run_judges.py` (score transcripts), and `eval.py` (aggregate results). Shared helpers live in `lib.py` and `bank.py`, and question definitions live in `questions.yaml`.

Generated artifacts are committed under `runs/<model>/` and `judgments/<judge>/`; treat them as outputs, not hand-edited source. The static leaderboard lives under `leaderboard/`: `build_site.py` generates markdown and sidebar data, while `leaderboard/starlight/` contains the Astro Starlight site.

## Build, Test, and Development Commands
Use `uv` when available because the repo already includes `uv.lock`.

- `uv run python run.py all --dry-run` previews the full benchmark pipeline.
- `uv run python run_interview.py --model gpt-5.4 --question all` generates transcripts into `runs/`.
- `uv run python run_judges.py --model claude-sonnet-4.6 --transcript runs/<model>/<question>_cold.json` writes scores into `judgments/`.
- `uv run python eval.py` rebuilds `report.md` from saved outputs.
- `make site` runs `leaderboard/build_site.py` and `npm run build` for the Starlight site.
- `uv run python leaderboard/test_site.py` verifies generated site pages and links.

## Coding Style & Naming Conventions
Follow existing Python style: 4-space indentation, standard library imports first, concise module docstrings, and `snake_case` for functions, files, and variables. Keep CLI scripts small and explicit; most scripts use `argparse` and `pathlib`. Preserve existing generated-path patterns such as `runs/<model>/<question>_cold.{md,json}`.

There is no dedicated formatter or linter configured here, so keep changes minimal and consistent with surrounding code.

## Testing Guidelines
Testing is script-based rather than framework-heavy. Run `uv run python leaderboard/test_site.py` after site or content changes, and use `uv run python test_reasoning.py` only for manual API behavior checks because it calls live model endpoints. If you add checks, prefer lightweight assertion-based scripts named `test_*.py`.

## Commit & Pull Request Guidelines
Recent history mixes plain imperative subjects (`fix broken site`, `add rag search design`) with Conventional Commit prefixes (`feat:`). Prefer short, imperative commit messages; use a prefix like `feat:` or `fix:` when it adds clarity.

Pull requests should state the benchmark or site impact, list commands run, and note any regenerated `runs/`, `judgments/`, or `leaderboard/dist/` outputs. Include screenshots when changing leaderboard UI or page structure.

## Security & Configuration Tips
Keep secrets in `.env` only; start from `.env.example` and never commit real API keys. Review large generated JSON and markdown carefully before committing because they may contain provider-specific metadata or prompt content.
