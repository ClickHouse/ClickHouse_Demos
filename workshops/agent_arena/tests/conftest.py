"""Test-only environment defaults so `pytest tests -q` passes on a clean clone.

`arena/config.py`'s `load_config()` expands `${VAR}` placeholders out of
`config.yaml` and raises `KeyError` on any that are missing from the environment.
`serving/api.py` calls `load_config()` at *module import time*, so a clean clone
with no `.env` sourced and nothing exported fails before a single test runs.

This fixture never touches `arena/`, `serving/`, or `dashboard/` source — it only
supplies inert placeholder values for tests, and only for variables that are
genuinely unset:

1. `load_dotenv()` first, exactly like `load_config()` does — if a real `.env`
   is present in the working directory, its values load into `os.environ` now
   (python-dotenv does not override values already set).
2. `os.environ.setdefault(...)` for the ~10 vars `config.yaml` references, so
   anything still unset after step 1 gets a harmless placeholder.

Net effect: a real `.env`, or a real exported value, always wins over these
placeholders — they only fill the gap on a bare clone with neither.
"""

import os

from dotenv import load_dotenv

load_dotenv()

_TEST_DEFAULTS = {
    "CLICKHOUSE_CLOUD_HOST": "test.clickhouse.cloud",
    "CLICKHOUSE_CLOUD_USER": "default",
    "CLICKHOUSE_CLOUD_PASSWORD": "test-placeholder",
    "CLICKHOUSE_CLOUD_DATABASE": "arena_test",
    "ARENA_RO_PASSWORD": "test-placeholder",
    "OPENROUTER_API_KEY": "test-placeholder",
    "OPENROUTER_BASE_URL": "https://openrouter.ai/api/v1",
    "LANGFUSE_BASE_URL": "https://us.cloud.langfuse.com",
    "LANGFUSE_PUBLIC_KEY": "test-placeholder",
    "LANGFUSE_SECRET_KEY": "test-placeholder",
}

for _var, _default in _TEST_DEFAULTS.items():
    os.environ.setdefault(_var, _default)
