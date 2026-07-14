from __future__ import annotations

import os
import time
from datetime import datetime, timezone

import httpx
import pytest


def _utc(dt: str) -> str:
    # ISO8601 with explicit Z so FastAPI parses it as aware datetime.
    return dt


@pytest.fixture(scope="session")
def api_base_url() -> str:
    return os.environ.get("API_BASE_URL", "http://localhost:8000").rstrip("/")


@pytest.fixture(scope="session")
def http() -> httpx.Client:
    with httpx.Client(timeout=10.0) as client:
        yield client


@pytest.fixture(scope="session", autouse=True)
def wait_for_api(api_base_url: str, http: httpx.Client) -> None:
    deadline = time.time() + 60
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            r = http.get(f"{api_base_url}/api/health")
            if r.status_code == 200 and r.json().get("clickhouse", {}).get("ok") is True:
                return
        except Exception as e:  # noqa: BLE001 - integration polling
            last_err = e
        time.sleep(1)
    raise RuntimeError(f"API did not become healthy in time. Last error: {last_err}")


@pytest.fixture(scope="session")
def sample_window() -> tuple[str, str]:
    # A window that exists in the full TLC datasets and also works for the mini seed.
    # (The dashboard defaults to a 2022 window as well.)
    start = _utc("2022-07-02T20:00:00Z")
    end = _utc("2022-07-02T22:00:00Z")
    return start, end

