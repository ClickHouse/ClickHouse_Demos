"""One explicit vanilla OTel provider per process; no automatic body capture."""
from contextlib import contextmanager
from functools import wraps
from dataclasses import dataclass
from time import time_ns
import inspect
from uuid import UUID
from opentelemetry import context, trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor, SpanExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.trace import Link, Status, StatusCode
from opentelemetry._logs import SeverityNumber
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, SimpleLogRecordProcessor
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

ALLOWED = frozenset('owner_id scenario_id generation sandbox_id snapshot_id candidate_id decision_id receipt_id event_id sku store_id applied_price_version policy_status error_type query_id http_status operation'.split())
_provider = None
_tracer = trace.get_tracer('pricing-workshop')
_logger = None


@dataclass
class TelemetryRuntime:
    traces: TracerProvider
    logs: LoggerProvider

    def force_flush(self, timeout_millis=30000):
        trace_ok=self.traces.force_flush(timeout_millis=timeout_millis)
        log_ok=self.logs.force_flush(timeout_millis=timeout_millis)
        return trace_ok and log_ok

    def shutdown(self):
        self.traces.shutdown()
        self.logs.shutdown()


def configure(service_name, endpoint='', *, exporter: SpanExporter | None = None, log_exporter=None):
    global _provider, _tracer, _logger
    resource=Resource.create({'service.name':service_name})
    provider = TracerProvider(resource=resource)
    logs=LoggerProvider(resource=resource)
    if exporter:
        provider.add_span_processor(SimpleSpanProcessor(exporter))
    elif endpoint:
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint.rstrip('/')+'/v1/traces',timeout=3),max_queue_size=512))
    if log_exporter:
        logs.add_log_record_processor(SimpleLogRecordProcessor(log_exporter))
    elif endpoint:
        logs.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter(endpoint=endpoint.rstrip('/')+'/v1/logs',timeout=3),max_queue_size=512))
    # Keep explicit providers so no other SDK replaces them or captures request bodies.
    _provider, _tracer = provider, provider.get_tracer('pricing-workshop')
    _logger=logs.get_logger('pricing-workshop')
    return TelemetryRuntime(provider,logs)


def safe_attributes(attributes):
    return {k:str(v) if isinstance(v,UUID) else v for k,v in attributes.items()
            if k.startswith('pricing.') and k[8:] in ALLOWED and isinstance(v,(str,int,float,bool,UUID))}


@contextmanager
def start_span(name, attributes=None, *, parent_headers=None, links=None):
    parent=TraceContextTextMapPropagator().extract(parent_headers) if parent_headers else None
    with _tracer.start_as_current_span(name,context=parent,attributes=safe_attributes(attributes or {}),
                                       links=links or [],record_exception=False,set_status_on_exception=False) as span:
        try:
            yield span
        except BaseException as error:
            span.set_attribute('pricing.error_type',type(error).__name__)
            span.set_status(Status(StatusCode.ERROR))
            if _logger:
                _logger.emit(timestamp=time_ns(),severity_number=SeverityNumber.ERROR,severity_text='ERROR',
                             body=name+' failed',attributes=safe_attributes(dict(span.attributes or {})))
            raise
        else:
            if _logger:
                failed=span.attributes.get('pricing.http_status',200)>=400
                _logger.emit(timestamp=time_ns(),severity_number=SeverityNumber.WARN if failed else SeverityNumber.INFO,
                             severity_text='WARN' if failed else 'INFO',body=name+' completed',
                             attributes=safe_attributes(dict(span.attributes or {})))


def propagation_headers():
    carrier={}
    TraceContextTextMapPropagator().inject(carrier)
    return carrier


def trace_link(carrier):
    return Link(trace.get_current_span(TraceContextTextMapPropagator().extract(carrier)).get_span_context())


def traced(name):
    """Capture known IDs, never SQL, credentials, notes, prompts, or exceptions."""
    def decorate(fn):
        signature=inspect.signature(fn)
        @wraps(fn)
        def run(*args,**kwargs):
            bound=signature.bind_partial(*args,**kwargs).arguments
            attrs={}
            scope=bound.get('scope')
            candidate=bound.get('candidate')
            if candidate is not None:
                scope=candidate.snapshot.scope
                attrs.update({'pricing.snapshot_id':str(candidate.snapshot.snapshot_id),'pricing.candidate_id':str(candidate.candidate_id)})
            if scope is not None:
                attrs.update({'pricing.owner_id':scope.owner_id,'pricing.scenario_id':scope.scenario_id,'pricing.generation':scope.generation})
            for key in ('candidate_id','decision_id','event_id','snapshot_id'):
                if key in bound: attrs['pricing.'+key]=str(bound[key])
            request=bound.get('request')
            if request is not None and hasattr(request,'event_id'): attrs['pricing.event_id']=str(request.event_id)
            with start_span(name,attrs) as span:
                result=fn(*args,**kwargs)
                for key in ('snapshot_id','receipt_id','decision_id','applied_price_version'):
                    value=result.get(key) if isinstance(result,dict) else getattr(result,key,None)
                    if value is not None: span.set_attribute('pricing.'+key,str(value) if isinstance(value,UUID) else value)
                if hasattr(result,'status'): span.set_attribute('pricing.policy_status',result.status)
                return result
        return run
    return decorate
