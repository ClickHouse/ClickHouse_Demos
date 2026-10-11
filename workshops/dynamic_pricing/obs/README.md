# Pricing diagnostics

The backend and writer initialize one vanilla OpenTelemetry provider each. Manual spans cover source transactions, analytical queries, polling, snapshots, policy, learner application, and agent tools. The backend extracts W3C trace context for incoming requests. Outbox delivery links to the initiating trace after a delay.

Only known IDs, generation, operation names, and exception types are exported. SQL, credentials, prompts, review notes, request bodies, and exception text are omitted. ClickStack traces describe this application. They do not expose ClickPipes internal spans.

Set `PRICING_OTEL_ENDPOINT=http://collector:4318` in the private environment file. Configure `PRICING_CLICKSTACK_ENDPOINT` to your verified OTLP HTTP receiver base URL and `PRICING_CLICKSTACK_API_KEY` to its ingestion key. Use HTTPS for a remote receiver. Start the telemetry profile with the workshop Compose command. Do not use a ClickHouse SQL endpoint as an OTLP URL.

Managed ClickStack can require an owned gateway collector connected to its ClickHouse service. Follow your deployment's [ClickStack collector instructions](https://clickhouse.com/docs/use-cases/observability/clickstack/ingesting-data/collector), then point this forwarding collector to that receiver. An endpoint example does not verify service availability. Check a real trace ID in your UI before proceeding.

Use the [OpenTelemetry Python exporter](https://opentelemetry.io/docs/languages/python/exporters/) arrangement in this kit. Do not combine it with automatic instrumentation or a second SDK that replaces the provider.

## Optional Langfuse observations

The application uses the same provider and trace IDs. `obs/langfuse.yaml` adds a separate, bounded collector pipeline that forwards only agent spans. Supply the verified Langfuse `/api/public/otel` base URL and private Basic authorization value. Add the file as a second collector `--config` argument and pass those two private variables to the collector. Leave this pipeline disabled when you have no Langfuse project. A failure there cannot change a business receipt.

The pipeline uses [Langfuse's native OTel ingestion](https://langfuse.com/integrations/native/opentelemetry), including its ingestion-version header. No model key, prompts, or provider response text is recorded as an observation.

## Verify delivery

Run `bash scripts/test-observability.sh` to verify that a disposable local receiver gets correlation IDs. This smoke check does not verify Cloud delivery. In your ClickStack UI, search a known snapshot or receipt ID and compare it with the ledger. Use the bounded, read-only SQL in `diagnostic-queries.sql` for deeper inspection.

## Optional live exception provider

Set `AGENT_PROVIDER=openai`, choose an available `AGENT_MODEL`, and put `AGENT_API_KEY` in the private environment file. The runtime validates the chosen model through the provider model endpoint. It requests one strict named tool per round through the [Chat Completions API](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create). It starts at most four rounds within a 30-second scheduling budget. An in-flight provider request has an eight-second timeout; a snapshot may finish its separately bounded convergence wait. Missing access, quota limits, invalid output, or timeout holds the investigation. Choose **Inspect recorded episode** to continue with labeled teaching evidence.
