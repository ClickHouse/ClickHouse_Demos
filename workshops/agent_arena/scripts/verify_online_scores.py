"""Poll and assert Langfuse trace scores without exposing trace bodies or secrets."""

import argparse
import os
import time
from urllib.parse import urlencode

from dotenv import load_dotenv

from scripts.langfuse_admin import (
    LangfuseAdmin,
    iter_cursor_pages,
    score_trace_id,
)


def normalized_score_value(score: dict) -> str:
    value = score.get("value")
    if value is None:
        value = score.get("string")
    if score.get("dataType") == "BOOLEAN":
        if isinstance(value, str):
            return value.strip().lower()
        return "true" if bool(value) else "false"
    return str(value)


def print_scores(trace_id: str, scores: list[dict]) -> None:
    print(f"trace_id={trace_id}")
    for score in scores:
        name = score.get("name") or ""
        value = normalized_score_value(score)
        comment = str(score.get("comment") or "").replace("\n", " ")
        suffix = f" comment={comment}" if comment else ""
        print(f"{name}={value}{suffix}")


def score_mismatches(scores: list[dict], expected: dict[str, str]) -> list[str]:
    actual = {
        score.get("name"): normalized_score_value(score)
        for score in scores
        if score.get("name")
    }
    mismatches = []
    for name, wanted in expected.items():
        if name not in actual:
            mismatches.append(f"{name} missing")
        elif actual[name] != wanted:
            mismatches.append(f"{name} expected {wanted} got {actual[name]}")
    return mismatches


def _parse_expected(assertions: list[str]) -> dict[str, str]:
    expected = {}
    for assertion in assertions:
        name, separator, value = assertion.partition("=")
        if not separator or not name or not value:
            raise ValueError(f"invalid score assertion {assertion!r}; expected NAME=VALUE")
        expected[name] = value.lower() if value.lower() in {"true", "false"} else value
    return expected


def _fetch_scores(api: LangfuseAdmin, trace_id: str) -> list[dict]:
    def fetch(cursor: str | None) -> dict:
        params = {"traceId": trace_id, "fields": "subject", "limit": 100}
        if cursor is not None:
            params["cursor"] = cursor
        return api.call("GET", f"/api/public/v3/scores?{urlencode(params)}")

    return [
        score
        for payload in iter_cursor_pages(fetch)
        for score in payload.get("data", []) or []
        if score_trace_id(score) == trace_id
    ]


def verify_scores(
    api: LangfuseAdmin,
    trace_id: str,
    expected: dict[str, str],
    *,
    timeout: float,
    poll: float,
) -> list[dict]:
    deadline = time.time() + timeout
    while True:
        scores = _fetch_scores(api, trace_id)
        if not score_mismatches(scores, expected):
            return scores
        if time.time() >= deadline:
            return scores
        time.sleep(poll)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("trace_id")
    parser.add_argument("assertions", nargs="+")
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--poll", type=float, default=3)
    args = parser.parse_args(argv)

    try:
        expected = _parse_expected(args.assertions)
    except ValueError as exc:
        parser.error(str(exc))

    load_dotenv()
    required = ["LANGFUSE_BASE_URL", "LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY"]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise SystemExit(
            "missing required environment variables: " + ", ".join(missing)
        )
    api = LangfuseAdmin(
        os.environ["LANGFUSE_BASE_URL"],
        os.environ["LANGFUSE_PUBLIC_KEY"],
        os.environ["LANGFUSE_SECRET_KEY"],
    )
    scores = verify_scores(
        api,
        args.trace_id,
        expected,
        timeout=args.timeout,
        poll=args.poll,
    )
    print_scores(args.trace_id, scores)
    mismatches = score_mismatches(scores, expected)
    if mismatches:
        raise SystemExit("score assertions failed: " + "; ".join(mismatches))


if __name__ == "__main__":
    main()
