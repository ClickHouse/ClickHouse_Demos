#!/usr/bin/env sh
set -e

# OpenTelemetry auto-instrumentation is opt-in so local dev without a collector
# runs cleanly. When OTEL_ENABLED=true we launch uvicorn under
# `opentelemetry-instrument`, which auto-instruments FastAPI/ASGI and exports
# traces (and logs) to OTEL_EXPORTER_OTLP_ENDPOINT.
#
# CAVEAT (documented by OpenTelemetry): auto-instrumentation does not support
# uvicorn --reload or multiple workers. The command below therefore uses a
# single worker and no reload, which is what the instrumented path needs.
if [ "${OTEL_ENABLED:-false}" = "true" ]; then
  echo "[entrypoint] OTEL_ENABLED=true: starting uvicorn under opentelemetry-instrument (single worker, no reload)"
  exec opentelemetry-instrument uvicorn app.main:app --host 0.0.0.0 --port 8000
else
  echo "[entrypoint] OTEL_ENABLED not true: starting uvicorn without OpenTelemetry"
  exec uvicorn app.main:app --host 0.0.0.0 --port 8000
fi
