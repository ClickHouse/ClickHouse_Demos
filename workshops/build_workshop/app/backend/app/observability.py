"""Observability helpers: structured logging + optional OpenTelemetry tracing.

This module is intentionally self-contained and additive. Logging is always
configured (stdout, level from LOG_LEVEL). OpenTelemetry is optional: when the
process is launched under `opentelemetry-instrument` a real tracer is used and
`start_span` produces nested spans under the auto-instrumented request span;
otherwise `start_span` yields a cheap no-op so call sites never need to branch.
"""

from __future__ import annotations

import logging
import os
import sys
from contextlib import contextmanager
from typing import Any, Iterator

# Loggers that uvicorn manages. We align them with the app format/level so its
# access and error records land on stdout in the same shape the log collector
# (and the ClickStack filelog receiver) expects.
_UVICORN_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access")

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
_LOG_DATEFMT = "%Y-%m-%dT%H:%M:%S%z"


def configure_logging() -> None:
    """Configure stdout logging for the app and uvicorn.

    Idempotent: safe to call more than once (our stdout handler is replaced, not
    stacked). Level comes from the LOG_LEVEL env var (default INFO). Container log
    collectors read stdout, so everything is written there with one format.

    When the app runs under `opentelemetry-instrument` (OTEL_ENABLED=true), the
    auto-instrumentation attaches an OTLP log-export handler to the root logger
    BEFORE this module is imported. We must preserve it: a blind
    `root.handlers = [handler]` evicts it, which is why app logs reached container
    stdout but never OTLP (otel_logs stayed empty while traces worked). So we keep
    any OpenTelemetry handler and only swap in our single stdout handler.
    """
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(fmt=_LOG_FORMAT, datefmt=_LOG_DATEFMT))

    root = logging.getLogger()
    root.setLevel(level)
    otel_handlers = [h for h in root.handlers if type(h).__module__.startswith("opentelemetry")]
    root.handlers = [handler, *otel_handlers]

    # uvicorn installs its own handlers at startup; re-point them at ours so the
    # format/level stay consistent and duplicate lines are avoided.
    for name in _UVICORN_LOGGERS:
        lg = logging.getLogger(name)
        lg.handlers = [handler]
        lg.setLevel(level)
        lg.propagate = False


try:  # OpenTelemetry is optional (disabled locally, or not installed at all).
    from opentelemetry import trace as _otel_trace

    _TRACER: Any = _otel_trace.get_tracer("nyc-taxi-backend")
except Exception:  # pragma: no cover - exercised only when OTel is absent
    _TRACER = None


class _NoopSpan:
    """Stand-in span so call sites can set attributes unconditionally when
    OpenTelemetry is unavailable."""

    def set_attribute(self, *_args: Any, **_kwargs: Any) -> None:
        pass

    def record_exception(self, *_args: Any, **_kwargs: Any) -> None:
        pass


@contextmanager
def start_span(name: str) -> Iterator[Any]:
    """Yield an active span (or a no-op stand-in) for the given operation name."""
    if _TRACER is None:
        yield _NoopSpan()
        return
    with _TRACER.start_as_current_span(name) as span:
        yield span
