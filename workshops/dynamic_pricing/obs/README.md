# Optional application telemetry

Telemetry is outside the learner workshop. The backend's OpenTelemetry instrumentation and optional Compose collector remain available for application diagnostics and contributor checks.

`collector.yaml` is used by the local observability smoke check:

```bash
bash scripts/test-observability.sh
```

The check uses a disposable local receiver and does not verify Cloud delivery. The optional `telemetry` Compose profile is not required for price comparison, learner approval, or receipt-scoped observed results. Keep credentials in the private environment file.
