from __future__ import annotations

import asyncio
import math
from collections.abc import AsyncIterator
from typing import Any

import aiohttp

from .models import MarketToken, normalize_markets


GAMMA_MARKETS = "https://gamma-api.polymarket.com/markets"
DATA_TRADES = "https://data-api.polymarket.com/trades"
CLOB_BOOK = "https://clob.polymarket.com/book"
CLOB_WEBSOCKET = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class SourceError(RuntimeError):
    pass


class RateLimited(SourceError):
    def __init__(self, retry_after: float):
        super().__init__(f"source rate limited; retry after {retry_after:g}s")
        self.retry_after = retry_after


class PolymarketAPI:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def _get_json(self, url: str, params: dict[str, Any]) -> Any:
        async with self.session.get(url, params=params) as response:
            if response.status == 429:
                raw = response.headers.get("Retry-After", "1")
                try:
                    retry_after = float(raw)
                except ValueError:
                    retry_after = 1.0
                if not math.isfinite(retry_after):
                    retry_after = 1.0
                retry_after = min(60.0, max(1.0, retry_after))
                raise RateLimited(retry_after)
            if response.status >= 400:
                body = (await response.text())[:200]
                raise SourceError(f"{url} returned HTTP {response.status}: {body}")
            try:
                return await response.json()
            except (aiohttp.ContentTypeError, ValueError) as exc:
                body = (await response.text())[:200]
                raise SourceError(f"{url} returned invalid JSON: {body}") from exc

    async def discover(self, market_count: int) -> list[MarketToken]:
        payload = await self._get_json(
            GAMMA_MARKETS,
            {
                "active": "true",
                "closed": "false",
                "limit": max(20, market_count),
                "order": "volume24hr",
                "ascending": "false",
            },
        )
        if not isinstance(payload, list):
            raise SourceError("Gamma /markets response must be an array")
        return normalize_markets(payload, market_count)

    async def trades(
        self, condition_id: str, start: int, end: int
    ) -> list[dict[str, Any]]:
        return await self._trades_window(condition_id, start, end, depth=0)

    async def _trades_window(
        self, condition_id: str, start: int, end: int, depth: int
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = 0
        limit = 10_000
        while offset <= 10_000:
            page = await self._get_json(
                DATA_TRADES,
                {
                    "market": condition_id,
                    "start": max(0, start),
                    "end": end,
                    "limit": limit,
                    "offset": offset,
                },
            )
            if not isinstance(page, list):
                raise SourceError("Data API /trades response must be an array")
            rows.extend(page)
            if len(page) < limit:
                break
            if offset == 10_000:
                if start >= end or depth >= 20:
                    raise SourceError(
                        "Data API /trades exceeds 11,000 rows inside one second; "
                        "the checkpoint was not advanced"
                    )
                midpoint = start + (end - start) // 2
                older = await self._trades_window(
                    condition_id, start, midpoint, depth + 1
                )
                newer = await self._trades_window(
                    condition_id, midpoint + 1, end, depth + 1
                )
                return [*older, *newer]
            offset += limit
        return rows

    async def book(self, token_id: int) -> dict[str, Any]:
        payload = await self._get_json(CLOB_BOOK, {"token_id": str(token_id)})
        if not isinstance(payload, dict):
            raise SourceError("CLOB /book response must be an object")
        payload.setdefault("asset_id", str(token_id))
        return payload

    async def websocket_messages(
        self,
        token_ids: list[int],
        stall_seconds: float,
        heartbeat_seconds: float = 10,
    ) -> AsyncIterator[dict[str, Any]]:
        async with self.session.ws_connect(
            CLOB_WEBSOCKET,
            heartbeat=None,
            timeout=aiohttp.ClientWSTimeout(ws_close=5),
        ) as socket:
            await socket.send_json(
                {
                    "assets_ids": [str(token_id) for token_id in token_ids],
                    "type": "market",
                    "custom_feature_enabled": True,
                }
            )

            async def heartbeat() -> None:
                while not socket.closed:
                    await asyncio.sleep(heartbeat_seconds)
                    await socket.send_str("PING")

            heartbeat_task = asyncio.create_task(heartbeat())
            loop = asyncio.get_running_loop()
            last_market_data = loop.time()
            try:
                while not socket.closed:
                    remaining = stall_seconds - (loop.time() - last_market_data)
                    if remaining <= 0:
                        raise SourceError(
                            f"WebSocket emitted no market data for {stall_seconds}s"
                        )
                    try:
                        message = await asyncio.wait_for(
                            socket.receive(), timeout=remaining
                        )
                    except TimeoutError as exc:
                        raise SourceError(
                            f"WebSocket emitted no market data for {stall_seconds}s"
                        ) from exc
                    if message.type == aiohttp.WSMsgType.TEXT:
                        if message.data == "PONG":
                            continue
                        try:
                            payload = message.json()
                        except ValueError as exc:
                            raise SourceError(
                                f"WebSocket returned invalid JSON: {message.data[:200]}"
                            ) from exc
                        items = payload if isinstance(payload, list) else [payload]
                        market_items = [item for item in items if isinstance(item, dict)]
                        if market_items:
                            last_market_data = loop.time()
                        for item in market_items:
                            yield item
                    elif message.type in {
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.CLOSE,
                        aiohttp.WSMsgType.ERROR,
                    }:
                        raise SourceError("WebSocket closed before the collector stopped")
            finally:
                heartbeat_task.cancel()
                await asyncio.gather(heartbeat_task, return_exceptions=True)
