from __future__ import annotations

import asyncio
import signal

import aiohttp

from .api import PolymarketAPI
from .config import Settings
from .health import HealthState, start_health_server
from .service import CollectorService, log
from .storage import ClickHouseStorage


async def main() -> None:
    settings = Settings.from_env()
    health = HealthState(queue_capacity=settings.queue_capacity)
    runner = await start_health_server(health, settings.health_port)
    storage = ClickHouseStorage(settings)
    timeout = aiohttp.ClientTimeout(total=20, connect=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        service = CollectorService(
            settings,
            PolymarketAPI(session),
            storage,
            health,
        )
        loop = asyncio.get_running_loop()
        for name in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(name, service.stop.set)
        try:
            await service.run()
        finally:
            await storage.close()
            await runner.cleanup()
            log("collector_stopped")


if __name__ == "__main__":
    asyncio.run(main())
