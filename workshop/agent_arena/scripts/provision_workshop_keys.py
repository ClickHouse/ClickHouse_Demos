"""Provision OpenRouter API keys for an instructor-led AgentArena workshop.

Uses OpenRouter's **Management (provisioning) API** to mint capped learner keys, so a
public, instructor-led run has fair, bounded spend — you never hand out your personal
key or an uncapped one. Requires a MANAGEMENT key (not a normal inference key): create
one at https://openrouter.ai/settings/management-keys and export it as
OPENROUTER_PROVISIONING_KEY. The management key can create/inspect/delete keys and spend
on your account — treat it as an admin credential and keep it out of learner materials.

Fair-usage model:
  - Default (`--count 1`): ONE shared learner key with a hard credit `limit` (USD). The
    cap bounds total workshop spend; `--daily` resets it at 00:00 UTC for multi-day runs.
  - Fairer for large cohorts (`--count N`): N per-learner keys, each with its own small
    `limit`, so no single learner can exhaust the shared budget.

Usage:
  export OPENROUTER_PROVISIONING_KEY=sk-or-v1-<management-key>
  python -m scripts.provision_workshop_keys --name "AgentArena 2026-07" --limit 20 --daily
  python -m scripts.provision_workshop_keys --name "AgentArena 2026-07" --limit 2 --count 30
  python -m scripts.provision_workshop_keys --list
  python -m scripts.provision_workshop_keys --delete <keyHash>

The create response returns the secret key string ONCE — copy it and hand it to learners
as their OPENROUTER_API_KEY. Afterwards only the key `hash` is retrievable (to inspect,
cap-reset, or delete). Endpoints: POST/GET/PATCH/DELETE https://openrouter.ai/api/v1/keys.
"""
import argparse
import json
import os
import urllib.request

_BASE = os.environ.get("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1").rstrip("/")
_KEYS_URL = f"{_BASE}/keys"


def build_create_body(name: str, limit: float | None) -> dict:
    """Body for POST /keys. `limit` is a credit cap in USD (OpenRouter credits are $1=$1);
    omit it for an uncapped key (not recommended for a public workshop)."""
    body: dict = {"name": name}
    if limit is not None:
        body["limit"] = limit
    return body


def extract_key(resp: dict) -> str | None:
    """The create response returns the usable secret key string exactly once. OpenRouter
    has returned it top-level (`key`) and under a `data` wrapper across versions — check
    both defensively so the instructor never loses it to a shape change."""
    if not isinstance(resp, dict):
        return None
    if resp.get("key"):
        return resp["key"]
    data = resp.get("data")
    if isinstance(data, dict) and data.get("key"):
        return data["key"]
    return None


def key_hash(resp: dict) -> str | None:
    data = resp.get("data") if isinstance(resp, dict) else None
    if isinstance(data, dict) and data.get("hash"):
        return data["hash"]
    return resp.get("hash") if isinstance(resp, dict) else None


def _req(method: str, url: str, mgmt_key: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"Authorization": f"Bearer {mgmt_key}", "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        raw = r.read().decode()
    return json.loads(raw) if raw.strip() else {}


def main() -> None:
    ap = argparse.ArgumentParser(description="Provision OpenRouter workshop keys (fair usage).")
    ap.add_argument("--name", default="AgentArena workshop")
    ap.add_argument("--limit", type=float, default=20.0, help="credit cap in USD per key")
    ap.add_argument("--count", type=int, default=1,
                    help="1 = one shared key; N = N per-learner keys, each capped at --limit")
    ap.add_argument("--daily", action="store_true",
                    help="reset each key's credit cap daily at 00:00 UTC (for multi-day runs)")
    ap.add_argument("--list", action="store_true", help="list existing keys and their usage")
    ap.add_argument("--delete", default="", help="delete a key by its hash")
    args = ap.parse_args()

    mgmt = os.environ.get("OPENROUTER_PROVISIONING_KEY")
    if not mgmt:
        raise SystemExit("set OPENROUTER_PROVISIONING_KEY — a Management key from "
                         "https://openrouter.ai/settings/management-keys")

    if args.list:
        print(json.dumps(_req("GET", _KEYS_URL, mgmt), indent=2))
        return
    if args.delete:
        _req("DELETE", f"{_KEYS_URL}/{args.delete}", mgmt)
        print(f"deleted key {args.delete}")
        return

    for i in range(args.count):
        name = args.name if args.count == 1 else f"{args.name} — learner {i + 1:02d}"
        resp = _req("POST", _KEYS_URL, mgmt, build_create_body(name, args.limit))
        secret, khash = extract_key(resp), key_hash(resp)
        if args.daily and khash:
            try:
                _req("PATCH", f"{_KEYS_URL}/{khash}", mgmt, {"limit_reset": "daily"})
            except Exception as e:  # noqa: BLE001 - reset is best-effort
                print(f"  (daily reset not applied: {e})")
        print(f"created: {name}")
        print(f"  hash: {khash or '?'}")
        print(f"  key : {secret or '(copy the key field from the raw response now — shown once)'}")

    print("\nHand the KEY string(s) to learners as OPENROUTER_API_KEY. "
          "Keep OPENROUTER_PROVISIONING_KEY (the management key) private.")


if __name__ == "__main__":
    main()
