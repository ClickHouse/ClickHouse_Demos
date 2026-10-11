"""Keep resettable business time separate from real leases and measured latency."""
from datetime import UTC, datetime
from threading import Lock
from typing import Callable


class Clock:
    def __init__(self, scenario_time: datetime, wall: Callable[[], datetime] | None = None):
        self._lock = Lock()
        self._wall = wall or (lambda: datetime.now(UTC))
        self.restore_scenario(scenario_time)

    def scenario_now(self) -> datetime:
        with self._lock:
            return self._scenario

    def wall_now(self) -> datetime:
        return self._utc(self._wall())

    def restore_scenario(self, value: datetime) -> None:
        with self._lock:
            self._scenario = self._utc(value)

    @staticmethod
    def _utc(value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Use a timestamp with an explicit time zone.")
        return value.astimezone(UTC)
