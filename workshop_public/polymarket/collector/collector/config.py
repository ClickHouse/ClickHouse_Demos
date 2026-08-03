from __future__ import annotations

import os
from dataclasses import dataclass


def _positive_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


@dataclass(frozen=True)
class Settings:
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_database: str
    clickhouse_secure: bool
    mode: str
    market_count: int
    reconcile_seconds: int
    book_fallback_seconds: int
    stall_seconds: int
    dedupe_minutes: int
    initial_lookback_minutes: int
    health_port: int
    queue_capacity: int = 10_000

    @classmethod
    def from_env(cls) -> "Settings":
        mode = os.getenv("POLYMARKET_MODE", "live").strip().lower()
        if mode not in {"live", "fixture"}:
            raise ValueError("POLYMARKET_MODE must be live or fixture")
        secure = os.getenv("CLICKHOUSE_SECURE", "true").strip().lower()
        if secure not in {"true", "false"}:
            raise ValueError("CLICKHOUSE_SECURE must be true or false")
        return cls(
            clickhouse_host=_required("CLICKHOUSE_HOST"),
            clickhouse_port=_positive_int("CLICKHOUSE_PORT", 8443),
            clickhouse_user=os.getenv("CLICKHOUSE_USER", "default").strip() or "default",
            clickhouse_password=_required("CLICKHOUSE_PASSWORD"),
            clickhouse_database=os.getenv("CLICKHOUSE_DATABASE", "polymarket").strip()
            or "polymarket",
            clickhouse_secure=secure == "true",
            mode=mode,
            market_count=_positive_int("POLYMARKET_MARKET_COUNT", 5),
            reconcile_seconds=_positive_int("POLYMARKET_RECONCILE_SECONDS", 10),
            book_fallback_seconds=_positive_int(
                "POLYMARKET_BOOK_FALLBACK_SECONDS", 30
            ),
            stall_seconds=_positive_int("POLYMARKET_STALL_SECONDS", 30),
            dedupe_minutes=_positive_int("POLYMARKET_DEDUPE_MINUTES", 15),
            initial_lookback_minutes=_positive_int(
                "POLYMARKET_INITIAL_LOOKBACK_MINUTES", 10
            ),
            health_port=_positive_int("POLYMARKET_HEALTH_PORT", 8090),
        )
