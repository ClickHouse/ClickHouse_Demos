"""One explicit vanilla OTel provider per process; no automatic body capture."""
from contextlib import contextmanager
from functools import wraps
import inspect
from uuid import UUID
from opentelemetry import context, trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor, SpanExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.trace import Link, Status, StatusCode

ALLOWED = frozenset('owner_id scenario_id generation sandbox_id snapshot_id candidate_id decision_id receipt_id event_id sku store_id applied_price_version policy_status error_type query_id http_status operation'.split())
_provider = None
_tracer = trace.get_tracer('pricing-workshop')


def configure(service_name, endpoint='', *, exporter: SpanExporter | None = None):
    global _provider, _tracer
    provider = TracerProvider(resource=Resource.create({'service.name':service_name}))
    if exporter:
        provider.add_span_processor(SimpleSpanProcessor(exporter))
    elif endpoint:
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint.rstrip('/')+'/v1/traces',timeout=3),max_queue_size=512))
    # Deliberately do not replace the process-global provider or auto-instrument.
    # All workshop spans use this single explicit provider, including agent spans.
    _provider, _tracer = provider, provider.get_tracer('pricing-workshop')
    return provider


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
            raise


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
