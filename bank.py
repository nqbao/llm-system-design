"""Question bank — loads and validates questions.yaml."""

import hashlib
import yaml
from pathlib import Path
from typing import Optional

from lib import TIERS, HERE


class Question:
    def __init__(self, data: dict):
        self.id: str = data["id"]
        self.tier: str = data["tier"]
        self.title: str = data["title"]
        self.prompt_cold: str = data["prompt_cold"]

    def get_prompt(self, variant: str) -> str:
        if variant == "cold":
            return self.prompt_cold
        raise ValueError(f"Unknown variant: {variant}")

    def prompt_hash(self, variant: str) -> str:
        return hashlib.sha256(self.get_prompt(variant).encode()).hexdigest()[:12]

    def __repr__(self):
        return f"Question(id={self.id!r}, tier={self.tier!r})"


class Bank:
    def __init__(self, path: Optional[Path] = None):
        self.path = path or HERE / "questions.yaml"
        self.questions: dict[str, Question] = {}
        self._load()

    def _load(self):
        raw = yaml.safe_load(self.path.read_text())
        for entry in raw:
            q = Question(entry)
            self.questions[q.id] = q

    def validate(self) -> list[str]:
        errors: list[str] = []
        ids: set[str] = set()
        for q in self.questions.values():
            if q.id in ids:
                errors.append(f"Duplicate id: {q.id}")
            ids.add(q.id)

            if q.tier not in TIERS:
                errors.append(f"{q.id}: invalid tier {q.tier!r}, must be one of {TIERS}")

            if not q.prompt_cold or not q.prompt_cold.strip():
                errors.append(f"{q.id}: missing prompt_cold")

        return errors

    def get(self, question_id: str) -> Question:
        q = self.questions.get(question_id)
        if q is None:
            raise KeyError(f"Unknown question_id: {question_id}")
        return q

    def by_tier(self, tier: str) -> list[Question]:
        return [q for q in self.questions.values() if q.tier == tier]

    def __len__(self):
        return len(self.questions)

    def __iter__(self):
        return iter(self.questions.values())


if __name__ == "__main__":
    bank = Bank()
    errors = bank.validate()
    if errors:
        print("VALIDATION ERRORS:")
        for e in errors:
            print(f"  - {e}")
        raise SystemExit(1)
    print(f"Loaded {len(bank)} questions (valid)")
    for tier in TIERS:
        qs = bank.by_tier(tier)
        if qs:
            print(f"  {tier}: {', '.join(q.id for q in qs)}")
