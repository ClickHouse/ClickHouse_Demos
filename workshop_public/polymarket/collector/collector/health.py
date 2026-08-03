from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

from aiohttp import web


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z") if value else None


@dataclass
class HealthState:
    status: str = "starting"
    reason: str = "initializing"
    websocket: str = "connecting"
    last_websocket_event_at: datetime | None = None
    last_trade_reconcile_at: datetime | None = None
    last_book_fallback_at: datetime | None = None
    last_clickhouse_write_at: datetime | None = None
    queue_depth: int = 0
    queue_capacity: int = 10_000
    watched_markets: int = 0
    watched_tokens: int = 0
    fresh_tokens: int = 0
    websocket_fresh_tokens: int = 0
    source_parse_errors_total: int = 0
    source_errors_total: int = 0
    clickhouse_errors_total: int = 0
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def payload(self) -> dict[str, Any]:
        data = asdict(self)
        for name in (
            "started_at",
            "last_websocket_event_at",
            "last_trade_reconcile_at",
            "last_book_fallback_at",
            "last_clickhouse_write_at",
        ):
            data[name] = _iso(getattr(self, name))
        return data


async def start_health_server(state: HealthState, port: int) -> web.AppRunner:
    async def health(_: web.Request) -> web.Response:
        status = 503 if state.status == "unhealthy" else 200
        return web.json_response(state.payload(), status=status)

    app = web.Application()
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    return runner
