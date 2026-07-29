#!/usr/bin/env python3
"""Fail when executable Polymarket SQL drifts from learner copy blocks."""

from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[1]
LEARNER = ROOT / "playbook/content/docs/polymarket/learner"
CANONICAL = [
    ROOT / "polymarket/db/schema.sql",
    ROOT / "polymarket/db/queries.sql",
]


def statements(text: str) -> list[str]:
    without_comments = re.sub(r"^\s*--.*$", "", text, flags=re.MULTILINE)
    return [
        " ".join(statement.split())
        for statement in without_comments.split(";")
        if statement.strip()
    ]


learner_statements: set[str] = set()
for page in LEARNER.glob("*.mdx"):
    text = page.read_text()
    for fence in re.findall(r"```sql\s*\n(.*?)```", text, flags=re.DOTALL):
        learner_statements.update(statements(fence))

if "--print-selects" in sys.argv:
    for statement in sorted(learner_statements):
        if statement.upper().startswith(("SELECT ", "WITH ")):
            print(statement + ";")
    sys.exit(0)

missing = []
for source in CANONICAL:
    for statement in statements(source.read_text()):
        if statement not in learner_statements:
            missing.append((source.relative_to(ROOT), statement[:120]))

if missing:
    for source, preview in missing:
        print(f"ERROR: {source} SQL is missing or different in learner copy blocks: {preview}")
    sys.exit(1)

print("Polymarket canonical SQL matches learner copy blocks.")
