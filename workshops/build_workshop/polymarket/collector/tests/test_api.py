import asyncio
from contextlib import asynccontextmanager

import aiohttp
import pytest
from aiohttp import web

import collector.api as api_module
from collector.api import PolymarketAPI, RateLimited, SourceError


@asynccontextmanager
async def server(routes):
    app = web.Application()
    for method, path, handler in routes:
        app.router.add_route(method, path, handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    sockets = site._server.sockets  # noqa: SLF001 - aiohttp exposes no public bound port
    port = sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_discover_sends_active_volume_query(monkeypatch):
    async def markets(request):
        assert request.query["active"] == "true"
        assert request.query["closed"] == "false"
        assert request.query["order"] == "volume24hr"
        return web.json_response(
            [
                {
                    "id": "1",
                    "conditionId": "0x" + "1" * 64,
                    "clobTokenIds": '["10", "11"]',
                    "outcomes": '["Yes", "No"]',
                        "active": True,
                        "closed": False,
                        "acceptingOrders": True,
                }
            ]
        )

    async with server([("GET", "/markets", markets)]) as base:
        monkeypatch.setattr(api_module, "GAMMA_MARKETS", f"{base}/markets")
        async with aiohttp.ClientSession() as session:
            result = await PolymarketAPI(session).discover(1)

    assert [row.token_id for row in result] == [10, 11]


@pytest.mark.asyncio
async def test_trades_pages_until_short_page(monkeypatch):
    calls = []

    async def trades(request):
        calls.append(int(request.query["offset"]))
        if request.query["offset"] == "0":
            return web.json_response([{"id": index} for index in range(10_000)])
        return web.json_response([{"id": 10_000}])

    async with server([("GET", "/trades", trades)]) as base:
        monkeypatch.setattr(api_module, "DATA_TRADES", f"{base}/trades")
        async with aiohttp.ClientSession() as session:
            result = await PolymarketAPI(session).trades("0x" + "1" * 64, 10, 20)

    assert len(result) == 10_001
    assert calls == [0, 10_000]


@pytest.mark.asyncio
async def test_trades_bisects_a_window_that_reaches_the_offset_cap(monkeypatch):
    calls = []

    async with aiohttp.ClientSession() as session:
        api = PolymarketAPI(session)

        async def get_json(_url, params):
            window = (params["start"], params["end"])
            calls.append((window, params["offset"]))
            if window == (10, 20):
                return [{"full": True}] * 10_000
            return [{"window": window}]

        monkeypatch.setattr(api, "_get_json", get_json)
        rows = await api.trades("0x" + "1" * 64, 10, 20)

    assert [row["window"] for row in rows] == [(10, 15), (16, 20)]
    assert ((10, 20), 10_000) in calls


@pytest.mark.asyncio
async def test_rate_limit_uses_retry_after(monkeypatch):
    async def limited(_request):
        return web.Response(status=429, headers={"Retry-After": "7"})

    async with server([("GET", "/book", limited)]) as base:
        monkeypatch.setattr(api_module, "CLOB_BOOK", f"{base}/book")
        async with aiohttp.ClientSession() as session:
            with pytest.raises(RateLimited) as error:
                await PolymarketAPI(session).book(10)

    assert error.value.retry_after == 7


@pytest.mark.asyncio
async def test_rate_limit_clamps_non_finite_retry_after(monkeypatch):
    async def limited(_request):
        return web.Response(status=429, headers={"Retry-After": "inf"})

    async with server([("GET", "/book", limited)]) as base:
        monkeypatch.setattr(api_module, "CLOB_BOOK", f"{base}/book")
        async with aiohttp.ClientSession() as session:
            with pytest.raises(RateLimited) as error:
                await PolymarketAPI(session).book(10)

    assert error.value.retry_after == 1


@pytest.mark.asyncio
async def test_invalid_json_is_source_error(monkeypatch):
    async def invalid(_request):
        return web.Response(text="not-json", content_type="application/json")

    async with server([("GET", "/book", invalid)]) as base:
        monkeypatch.setattr(api_module, "CLOB_BOOK", f"{base}/book")
        async with aiohttp.ClientSession() as session:
            with pytest.raises(SourceError, match="invalid JSON"):
                await PolymarketAPI(session).book(10)


@pytest.mark.asyncio
async def test_websocket_subscribes_heartbeats_and_yields_market_data(monkeypatch):
    subscription = None

    async def websocket(request):
        nonlocal subscription
        socket = web.WebSocketResponse()
        await socket.prepare(request)
        subscription = await socket.receive_json()
        ping = await socket.receive_str()
        assert ping == "PING"
        await socket.send_str("PONG")
        await socket.send_json(
            {
                "event_type": "best_bid_ask",
                "market": "0x" + "1" * 64,
                "asset_id": "10",
                "timestamp": "1782753357257",
                "best_bid": "0.4",
                "best_ask": "0.6",
            }
        )
        await socket.close()
        return socket

    async with server([("GET", "/ws", websocket)]) as base:
        monkeypatch.setattr(
            api_module, "CLOB_WEBSOCKET", base.replace("http://", "ws://") + "/ws"
        )
        async with aiohttp.ClientSession() as session:
            messages = PolymarketAPI(session).websocket_messages(
                [10, 11], stall_seconds=1, heartbeat_seconds=0.01
            )
            message = await anext(messages)
            await messages.aclose()

    assert subscription == {
        "assets_ids": ["10", "11"],
        "type": "market",
        "custom_feature_enabled": True,
    }
    assert message["event_type"] == "best_bid_ask"


@pytest.mark.asyncio
async def test_websocket_silent_stall_is_reported(monkeypatch):
    async def websocket(request):
        socket = web.WebSocketResponse()
        await socket.prepare(request)
        await socket.receive_json()
        await asyncio.sleep(0.2)
        await socket.close()
        return socket

    async with server([("GET", "/ws", websocket)]) as base:
        monkeypatch.setattr(
            api_module, "CLOB_WEBSOCKET", base.replace("http://", "ws://") + "/ws"
        )
        async with aiohttp.ClientSession() as session:
            messages = PolymarketAPI(session).websocket_messages(
                [10], stall_seconds=0.05
            )
            with pytest.raises(SourceError, match="no market data"):
                await anext(messages)


@pytest.mark.asyncio
async def test_websocket_pongs_do_not_mask_a_market_data_stall(monkeypatch):
    async def websocket(request):
        socket = web.WebSocketResponse()
        await socket.prepare(request)
        await socket.receive_json()
        while not socket.closed:
            message = await socket.receive()
            if message.type != aiohttp.WSMsgType.TEXT:
                break
            if message.data == "PING":
                await socket.send_str("PONG")
        return socket

    async with server([("GET", "/ws", websocket)]) as base:
        monkeypatch.setattr(
            api_module, "CLOB_WEBSOCKET", base.replace("http://", "ws://") + "/ws"
        )
        async with aiohttp.ClientSession() as session:
            messages = PolymarketAPI(session).websocket_messages(
                [10], stall_seconds=0.05, heartbeat_seconds=0.01
            )
            with pytest.raises(SourceError, match="no market data"):
                await anext(messages)
