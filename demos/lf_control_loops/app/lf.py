"""Thin Langfuse (v4) helper layer, resolved per request.

This file used to build one module-level client from process-wide env keys. It now
resolves a client from the credentials bound to the current request, which is the
whole reason creds.py exists: the signatures below are unchanged, so workflow.py,
agent.py and coding_agent.py call trace()/step()/score_trace() exactly as before
and know nothing about credentials.

Multi-project resolution uses the SDK's PUBLIC surface only:
  - creds.langfuse_client() constructs Langfuse(public_key=, secret_key=, base_url=)
    once per distinct public key, which registers the per-key singleton, and resolves
    it with get_client(public_key=...).
  - llm.py pins each generation with langfuse_public_key= on the create() call.
We deliberately do not use langfuse's private _set_current_public_key contextvar.
It would work, but passing the key explicitly at both resolution points is the
documented route and does not depend on an underscore-prefixed helper staying put
across SDK versions.

The first call through here is where a visitor's Langfuse client gets built, which
is deliberately as late as possible: it is the first moment in a request at which
everything cheap and certain (headers, body shape, the host allowlist, the HMAC gate
on an apply) has already been checked, so a request that is going to end in a 4xx
never causes the SDK to spawn a thread. See creds.bound().

An unexpected failure still degrades to a no-op rather than raising, so a Langfuse
outage costs the visitor their traces and not their run. A CredentialError does NOT
degrade: it means we deliberately refused (bad keys, or this instance is at its
project ceiling), the visitor needs to be told, and a Langfuse demo that silently
runs untraced is worse than one that says why it did not.

Langfuse v4 API notes (verified against langfuse==4.14.1):
  - langfuse.start_as_current_span(...)      -> start_as_current_observation(name=, as_type=)
  - langfuse.update_current_trace(...)       -> propagate_attributes(...) + set_current_trace_io(...)
  - as_type can be: span | generation | agent | tool | chain | retriever | ...
"""

import contextlib
import hashlib
import logging

from langfuse import propagate_attributes

from app import config, creds
from app.creds import CredentialError

logger = logging.getLogger("lf_demo.lf")


class _NullSpan:
    """Stand-in for a Langfuse observation when tracing is not available."""

    def update(self, **_):
        return self

    def score(self, **_):
        return self

    def score_trace(self, **_):
        return self

    def set_trace_io(self, **_):
        return self


def _client():
    """The Langfuse client for the request in flight, or None.

    The client is never resolved without a public key. With no key the SDK resolves
    "the only client in the process", and on a multi-tenant BYOK server that is some
    other visitor's project - the exact cross-tenant write we must not make.
    """
    c = creds.current()
    if c is None or not c.langfuse_public_key:
        return None
    try:
        return creds.langfuse_client(c)
    except CredentialError:
        # Our own refusal, with a message we wrote for this visitor. Let it out.
        raise
    except Exception:
        # Never fail a run because tracing could not be set up. No exception detail
        # is logged: Langfuse SDK messages quote the base URL and the public key.
        logger.warning("could not resolve a Langfuse client for this request; running untraced")
        return None


@contextlib.contextmanager
def trace(name, as_type, session_id, tags, input=None, trace_context=None, trace_name=None):
    """Open a root observation that starts (or continues) a trace.

    Trace-level attributes (session, user, tags, name) are set via
    propagate_attributes so they attach to the whole trace. `trace_name` lets the
    trace keep a stable name even when a later request adds a differently-named
    root observation (used to continue the coding-agent trace across requests).
    """
    client = _client()
    if client is None:
        yield _NullSpan()
        return

    with propagate_attributes(
        session_id=session_id,
        user_id=config.DEMO_USER_ID,
        tags=tags,
        trace_name=trace_name or name,
    ):
        kwargs = {"name": name, "as_type": as_type}
        if input is not None:
            kwargs["input"] = input
        if trace_context is not None:
            kwargs["trace_context"] = trace_context
        with client.start_as_current_observation(**kwargs) as span:
            yield span


@contextlib.contextmanager
def step(name, as_type="span", input=None):
    """Open a nested observation (a single step inside the current trace)."""
    client = _client()
    if client is None:
        yield _NullSpan()
        return

    kwargs = {"name": name, "as_type": as_type}
    if input is not None:
        kwargs["input"] = input
    with client.start_as_current_observation(**kwargs) as span:
        yield span


def set_trace_io(input=None, output=None):
    client = _client()
    if client is None:
        return
    client.set_current_trace_io(input=input, output=output)


def score_trace(name, value, data_type="NUMERIC", comment=None):
    """Attach a score to the current trace (in-context)."""
    client = _client()
    if client is None:
        return
    client.score_current_trace(name=name, value=value, data_type=data_type, comment=comment)


def create_score(name, value, trace_id, data_type="NUMERIC", comment=None, score_id=None):
    """Attach a score to a specific trace by id (out-of-context, e.g. later feedback).

    Pass score_id for an idempotent upsert (repeated calls update one score).
    """
    client = _client()
    if client is None:
        return
    kwargs = dict(name=name, value=value, trace_id=trace_id, data_type=data_type, comment=comment)
    if score_id is not None:
        kwargs["score_id"] = score_id
    client.create_score(**kwargs)


def current_trace_id():
    client = _client()
    if client is None:
        return None
    return client.get_current_trace_id()


def new_trace_id(seed):
    """Deterministic trace id from a seed so a multi-request flow shares one trace."""
    client = _client()
    if client is not None:
        return client.create_trace_id(seed=seed)
    return hashlib.sha256(seed.encode()).hexdigest()[:32]


def trace_url(trace_id=None):
    client = _client()
    if client is None:
        return None
    try:
        return client.get_trace_url(trace_id=trace_id)
    except Exception:
        # Resolving the URL needs the project id, which costs an API round trip that
        # can fail on a bad key. A missing deep link is not worth failing the run.
        return None


def flush():
    client = _client()
    if client is None:
        return
    try:
        client.flush()
    except Exception:
        logger.warning("Langfuse flush did not complete for this request")
