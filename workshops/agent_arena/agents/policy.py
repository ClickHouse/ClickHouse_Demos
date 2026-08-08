from dataclasses import dataclass
from pathlib import Path
import re

import yaml


_VERSION = re.compile(r"^policy-v[0-9]+$")


@dataclass(frozen=True)
class PolicyContext:
    version: str
    metrics: dict[str, str]
    rendered: str


def load_policy(version: str, root: Path = Path("policies")) -> PolicyContext:
    if not _VERSION.fullmatch(version):
        raise ValueError(f"unknown policy version: {version}")

    path = root / f"{version}.yaml"
    if not path.is_file():
        raise ValueError(f"unknown policy version: {version}")

    raw = yaml.safe_load(path.read_text()) or {}
    metrics = raw.get("metrics")
    if raw.get("version") != version:
        raise ValueError(f"policy version mismatch in {path}")
    if not isinstance(metrics, dict) or not metrics or not all(
        isinstance(name, str) and isinstance(definition, str) and definition.strip()
        for name, definition in metrics.items()
    ):
        raise ValueError("metrics must be a non-empty mapping of names to definitions")

    lines = [f"# Business metric policy ({version})"]
    lines.extend(
        f"- {name}: {definition.strip()}" for name, definition in metrics.items()
    )
    return PolicyContext(version, metrics, "\n".join(lines))


def with_policy(schema_context: str, policy: PolicyContext) -> str:
    return f"{schema_context.rstrip()}\n\n{policy.rendered}\n"
