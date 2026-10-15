from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pricing_lab.observability import configure, start_span, propagation_headers, trace_link


def test_correlated_spans_filter_secrets_and_exception_text():
    exporter = InMemorySpanExporter()
    runtime = configure('pricing-test', exporter=exporter)
    try:
        with start_span('pricing.snapshot', {'pricing.owner_id': 'learner-1', 'pricing.generation': 2,
                         'pricing.snapshot_id': 'snapshot-1', 'password': 'secret',
                         'db.statement': 'secret SQL', 'pricing.note': 'private note'}):
            headers = propagation_headers()
            assert 'traceparent' in headers
            link = trace_link(headers)
            assert link.context.is_valid
            try:
                with start_span('pricing.sql', {}):
                    raise RuntimeError('postgresql://user:secret@host')
            except RuntimeError:
                pass
        spans = exporter.get_finished_spans()
        parent, child = spans[1], spans[0]
        assert child.parent.span_id == parent.context.span_id
        assert parent.attributes['pricing.snapshot_id'] == 'snapshot-1'
        assert 'secret' not in str(spans) and 'secret' not in str(child.events)
        assert set(parent.attributes) == {'pricing.owner_id', 'pricing.generation', 'pricing.snapshot_id'}
    finally:
        runtime.shutdown()


def test_disabled_export_does_not_prevent_business_action():
    runtime = configure('pricing-test')
    with start_span('pricing.application', {'pricing.decision_id': 'decision-1'}):
        value = 650
    assert value == 650
    runtime.shutdown()


def test_operation_logs_reach_exporter_with_trace_ids_and_safe_attributes():
    from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter
    logs=InMemoryLogRecordExporter()
    spans=InMemorySpanExporter()
    runtime=configure('pricing-test',exporter=spans,log_exporter=logs)
    try:
        with start_span('pricing.snapshot',{'pricing.snapshot_id':'snapshot-log-1','pricing.note':'private note'}):
            pass
        try:
            with start_span('pricing.sql',{'pricing.query_id':'query-log-1'}):
                raise RuntimeError('postgresql://private:password@host')
        except RuntimeError:
            pass
        assert runtime.force_flush()
        records=[item.log_record for item in logs.get_finished_logs()]
        assert [r.body for r in records]==['pricing.snapshot completed','pricing.sql failed']
        assert [r.severity_text for r in records]==['INFO','ERROR']
        assert records[0].attributes['pricing.snapshot_id']=='snapshot-log-1'
        assert records[1].attributes['pricing.error_type']=='RuntimeError'
        for record,span in zip(records,spans.get_finished_spans()):
            assert record.trace_id==span.context.trace_id
            assert record.span_id==span.context.span_id
        assert all(item.resource.attributes['service.name']=='pricing-test' for item in logs.get_finished_logs())
        payload=str([(r.body,dict(r.attributes)) for r in records])
        assert 'private' not in payload and 'password' not in payload
    finally:
        runtime.shutdown()
