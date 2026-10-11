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
