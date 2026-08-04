"""Per-request bring-your-own-key credentials.

WHY A CONTEXTVAR AND NOT AN ARGUMENT
------------------------------------
workflow.py, agent.py and coding_agent.py call `timed_create(...)`, `lf.trace(...)`
and `lf.step(...)` in roughly forty places, with no credential argument anywhere.
Those three files ARE the demo: they are what the talk walks through line by line,
and they are the last place we want to be introducing plumbing bugs. So the
credentials travel out of band. One contextvar is set at the top of the request
and every helper in llm.py and lf.py reads it. The loop code stays byte-identical.

The contextvar is bound with a context manager inside the endpoint, not in HTTP
middleware, and that is deliberate. FastAPI runs a `def` (non-async) endpoint in a
threadpool, and the worker thread gets a *copy* of the context taken at dispatch
time. Setting the value inside the endpoint means the value is set in the same
context the loop code actually executes in, so there is no question about whether
it propagated; and because the copy is discarded when the request finishes, one
request's credentials cannot survive into the next.

WHAT IS NEVER DONE WITH A CREDENTIAL
------------------------------------
Not logged, not echoed in a response, not written to disk, not put back into
os.environ, and not cached under any key a later request could supply. The one
thing that IS cached across requests is a Langfuse client per public key, because
the SDK insists on owning background exporter threads; see the registry at the
bottom of this file for the admission rule and the hard ceiling. Creds.__repr__ is
overridden so an accidental f-string or a traceback frame dump cannot spill one.
"""

import hashlib
import hmac
import logging
import os
import re
import secrets
import threading
import time
import traceback
from collections import OrderedDict
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Optional
from urllib.parse import urlsplit

import httpx
from langfuse import Langfuse, get_client
from langfuse._client.resource_manager import LangfuseResourceManager

logger = logging.getLogger("lf_demo.creds")

# --- The SSRF allowlist --------------------------------------------------------
#
# X-Langfuse-Host is a client-supplied address that the SERVER then connects to,
# with the visitor's traces (which contain their full prompts and completions) in
# the request body. Unconstrained, "point it at 169.254.169.254 and read the reply"
# is the obvious attack, but the worse one is quieter: point it at a host you own
# and this server will courteously ship you the prompts of anyone you can convince
# to paste your address into the Setup tab.
#
# Two exact strings is the right shape for the allowlist. Langfuse Cloud has two
# regions and that is the entire set of destinations this demo needs. Trying to be
# clever with IP or CIDR filtering instead would be strictly worse: DNS rebinding,
# redirects, IPv6 mappings and decimal IP literals all have to be got right, and
# any one of them being wrong reopens the hole. An exact match on a normalised
# origin has no such surface.
ALLOWED_LANGFUSE_HOSTS = (
    "https://cloud.langfuse.com",
    "https://us.cloud.langfuse.com",
)

DEFAULT_LANGFUSE_HOST = ALLOWED_LANGFUSE_HOSTS[0]

# --- Header names and size caps -------------------------------------------------
#
# Credentials go in headers, never in a query string: query strings are recorded
# verbatim by App Runner and CloudFront access logs, which is the one place we can
# neither reach nor redact.
H_OPENAI_KEY = "x-openai-key"
H_LF_PUBLIC = "x-langfuse-public-key"
H_LF_SECRET = "x-langfuse-secret-key"
H_LF_HOST = "x-langfuse-host"
H_MODEL = "x-openai-model"

# Caps are per value and generous relative to real keys (an OpenAI project key is
# ~164 chars, a Langfuse key ~40). The point is not to guess the exact format, it
# is to refuse to carry a megabyte of attacker-chosen bytes through validation,
# hashing and an upstream Authorization header.
_MAX_KEY_LEN = 512
_MAX_HOST_LEN = 256
_MAX_MODEL_LEN = 64


class CredentialError(Exception):
    """A bad or missing credential header. Maps to a 4xx with this exact message.

    Every message here is written by us and contains no upstream detail, so it is
    safe to hand to an unauthenticated caller verbatim.
    """

    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class Creds:
    """One visitor's credentials, for the lifetime of one request.

    Also carries the per-request OpenAI client, so it is built once per request
    instead of once per LLM call, and closed when the request ends. Note what this
    is NOT: a process-wide client cache keyed by API key. Such a cache would let a
    later request retrieve a client authenticated as an earlier visitor merely by
    guessing or replaying a key, which is the sort of thing that is fine until the
    day it is not.
    """

    __slots__ = (
        "openai_key",
        "langfuse_public_key",
        "langfuse_secret_key",
        "langfuse_host",
        "model",
        "source",
        "_openai_client",
        "_langfuse_client",
        "langfuse_slot",
    )

    def __init__(
        self, openai_key, langfuse_public_key, langfuse_secret_key, langfuse_host, model, source=""
    ):
        self.openai_key = openai_key
        self.langfuse_public_key = langfuse_public_key
        self.langfuse_secret_key = langfuse_secret_key
        self.langfuse_host = langfuse_host
        self.model = model
        # Not a credential and not an identity: a bucket key for rationing new-project
        # admissions so one caller cannot spend everybody else's share. See _source().
        self.source = source or "unknown"
        self._openai_client = None
        # Resolved at most once per request, on first use, and only after this
        # visitor's Langfuse keys have been accepted by Langfuse itself. See
        # langfuse_client() at the bottom of this file.
        self._langfuse_client = None
        # The registry slot this request is holding, so the request can be counted
        # as in flight and the slot cannot be retired underneath it.
        self.langfuse_slot = None

    def __repr__(self) -> str:
        # Not a prefix, not a length, not a hash. Anything derived from a secret
        # invites someone to argue about how much of it is safe to print, and the
        # answer that never needs revisiting is "none of it".
        return f"<Creds model={self.model!r} langfuse_host={self.langfuse_host!r} keys=redacted>"

    __str__ = __repr__

    def openai(self):
        """The OpenAI client for this request, built on first use."""
        if self._openai_client is None:
            # Imported lazily: langfuse.openai monkey-patches the openai module on
            # import, so we want that to happen exactly once, at app import, via
            # app.llm rather than as a side effect of the first request.
            from app.llm import build_openai_client

            self._openai_client = build_openai_client(self.openai_key)
        return self._openai_client

    def close(self) -> None:
        client = self._openai_client
        self._openai_client = None
        if client is not None:
            try:
                client.close()
            except Exception:  # a failed socket close must not fail the request
                logger.debug("ignoring error while closing the per-request OpenAI client")


# --- Header parsing and validation ---------------------------------------------

def _clean(raw: Optional[str], *, max_len: int, label: str) -> str:
    """Strip, length-check and reject anything that is not printable ASCII.

    Control characters matter specifically because these values are interpolated
    into outbound HTTP headers (Authorization, x-langfuse-public-key). A CR or LF
    that got that far is a request-splitting attempt, and rejecting the whole value
    is both simpler and safer than sanitising it into something we then guess at.
    """
    value = (raw or "").strip()
    if not value:
        return ""
    if len(value) > max_len:
        raise CredentialError(
            f"The {label} you sent is too long ({len(value)} characters, limit {max_len}). "
            "Check the Setup tab: this looks like the wrong value was pasted."
        )
    if any(ch < " " or ch > "~" for ch in value):
        raise CredentialError(
            f"The {label} contains characters that are not allowed. "
            "Re-copy it in the Setup tab, without any surrounding quotes or line breaks."
        )
    return value


def normalise_langfuse_host(raw: Optional[str]) -> str:
    """Normalise then exact-match against the allowlist. Raises CredentialError.

    Normalisation is scheme+host only, lowercased. A path, query, fragment or
    userinfo is rejected outright rather than dropped, because "https://cloud.
    langfuse.com@evil.example" and "https://cloud.langfuse.com.evil.example" are
    the two shapes that beat a naive substring check, and silently stripping parts
    of a URL is how you end up matching on something you did not connect to.
    """
    value = _clean(raw, max_len=_MAX_HOST_LEN, label="Langfuse host")
    if not value:
        # Not a credential, just a destination, and the default destination is
        # already on the allowlist. Defaulting keeps the UI simple without giving
        # anything up.
        return DEFAULT_LANGFUSE_HOST

    parts = urlsplit(value)
    origin = f"{parts.scheme.lower()}://{parts.netloc.lower()}"
    rejected = (
        not parts.scheme
        or not parts.netloc
        or "@" in parts.netloc
        or parts.path not in ("", "/")
        or parts.query
        or parts.fragment
        or origin not in ALLOWED_LANGFUSE_HOSTS
    )
    if rejected:
        allowed = " or ".join(ALLOWED_LANGFUSE_HOSTS)
        raise CredentialError(
            "That Langfuse host is not allowed. This demo only sends traces to "
            f"Langfuse Cloud: use {allowed}."
        )
    return origin


def _validate_model(raw: Optional[str]) -> str:
    value = _clean(raw, max_len=_MAX_MODEL_LEN, label="model name")
    if not value:
        from app import config

        return config.DEFAULT_MODEL
    if not all(ch.isalnum() or ch in "._:-" for ch in value):
        raise CredentialError(
            "That model name contains characters that are not allowed. "
            "Pick one of the models offered in the Setup tab."
        )
    return value


_SETUP_HINT = "Open the Setup tab and paste your own keys; this demo never uses server-side keys."


def extract(headers, peer: str = "") -> Creds:
    """Build Creds from a request's headers, or raise CredentialError.

    `headers` is anything with a case-insensitive .get, i.e. Starlette's Headers.
    `peer` is the socket address of the caller, used only by _source() below.

    All three credentials are required, including the Langfuse pair. That is not
    just because this is a Langfuse demo. langfuse.openai patches the OpenAI SDK
    globally, so EVERY completion in this process goes through it, and when it is
    given no public key it falls back to "the single Langfuse client in this
    process" - which, on a multi-tenant BYOK server, would be some other visitor's
    project. Making the Langfuse keys mandatory means we always have a public key
    to pin the generation to, and the ambiguous fallback is never reached.
    """
    openai_key = _clean(headers.get(H_OPENAI_KEY), max_len=_MAX_KEY_LEN, label="OpenAI API key")
    lf_public = _clean(headers.get(H_LF_PUBLIC), max_len=_MAX_KEY_LEN, label="Langfuse public key")
    lf_secret = _clean(headers.get(H_LF_SECRET), max_len=_MAX_KEY_LEN, label="Langfuse secret key")

    missing = []
    if not openai_key:
        missing.append("an OpenAI API key")
    if not lf_public:
        missing.append("a Langfuse public key")
    if not lf_secret:
        missing.append("a Langfuse secret key")
    if missing:
        raise CredentialError(f"This request needs {', '.join(missing)}. {_SETUP_HINT}")

    return Creds(
        openai_key=openai_key,
        langfuse_public_key=lf_public,
        langfuse_secret_key=lf_secret,
        langfuse_host=normalise_langfuse_host(headers.get(H_LF_HOST)),
        model=_validate_model(headers.get(H_MODEL)),
        source=_source(headers, peer),
    )


def _source(headers, peer: str) -> str:
    """A label for who is asking, used only to ration new-project admissions fairly.

    The RIGHTMOST X-Forwarded-For entry, not the leftmost. The leftmost is whatever the
    client typed and is worthless; the rightmost is the one the nearest proxy appended,
    which on App Runner is the real client address and which the client cannot choose.
    With no proxy in front there is no X-Forwarded-For at all and the socket peer is
    used, so the only deployment where this is spoofable is one that is directly exposed
    AND behind nothing, which this is not.

    Never logged, never echoed, never used for anything but a rate limit bucket key, and
    truncated so it cannot be used to grow the bucket table with long keys.
    """
    forwarded = (headers.get("x-forwarded-for") or "").strip()
    if forwarded:
        candidate = forwarded.rsplit(",", 1)[-1].strip()[:64]
        if candidate and all(" " < ch <= "~" for ch in candidate):
            return candidate
    return (peer or "unknown")[:64]


# --- The contextvar ------------------------------------------------------------

_CREDS: ContextVar[Optional[Creds]] = ContextVar("byok_creds", default=None)


def current() -> Optional[Creds]:
    """The credentials for the request being served, or None outside a request."""
    return _CREDS.get()


def require() -> Creds:
    c = _CREDS.get()
    if c is None:
        # Reachable only if a loop helper is called outside a bound request, which
        # would be a coding error on our side, not a user error. Fail loudly rather
        # than quietly running unauthenticated.
        raise CredentialError(f"No credentials are bound to this request. {_SETUP_HINT}")
    return c


@contextmanager
def bound(c: Creds):
    """Bind credentials for the duration of one request, then unbind and clean up.

    Deliberately does NOT construct a Langfuse client. It used to, and that single
    line was an unauthenticated remote denial of service: bound() runs before the
    endpoint has verified anything it can verify, so one request per novel
    X-Langfuse-Public-Key made the process spawn four SDK background threads that
    outlive the request, measured at 2040 threads and 939 MB of RSS after 1000
    single-threaded curls that every one of which answered 400. Construction now
    happens in langfuse_client(), lazily, at the first tracing call, which is after
    header validation, after body validation, and after the HMAC gate on
    /api/coding/apply. A request that ends in a 4xx never reaches it.
    """
    token = _CREDS.set(c)
    try:
        yield c
    finally:
        _CREDS.reset(token)
        release_langfuse_slot(c)
        c.close()


# --- Langfuse client registry (admission-controlled, absolutely bounded) --------
#
# Langfuse v4 supports several projects in one process, and it is genuinely safe
# rather than merely tolerated: LangfuseSpanProcessor stamps its own public key on
# construction and drops any span whose tracer carries a different one, so one
# visitor's spans cannot be exported to another visitor's project even though all
# the processors hang off a shared tracer provider. That is checked in the
# installed SDK (langfuse/_client/span_processor.py), not assumed.
#
# WHAT THIS COSTS, MEASURED
# -------------------------
# Constructing Langfuse(public_key=...) starts FOUR background threads, verified
# against langfuse==4.14.1 by name: OtelBatchSpanRecordProcessor, MediaUploadConsumer,
# ScoreIngestionConsumer, PromptCacheRefreshConsumer. The SDK's own shutdown() stops
# only two of them (the media and score consumers); the batch span processor and the
# prompt cache refresher keep running, so "shut it down" does not mean "get the
# threads back" unless we also do the two teardowns in _teardown() below. Left
# unbounded this is ~1.16 MB of RSS and 2 permanent threads per distinct public key,
# forever: 2000 novel keys through a single-threaded curl loop reached 4040 threads
# and 1435 MB before App Runner's 2048 MB limit killed the instance.
#
# THREE SEPARATE CONTROLS, BECAUSE ONE IS NOT ENOUGH
# --------------------------------------------------
# 1. Nothing here runs until the caller has proved they own a Langfuse project.
#    _preflight() checks the public/secret pair against the allowlisted Langfuse host
#    over plain HTTPS - no SDK, no threads, no client - and only a 200 buys a client.
#    Random bytes in X-Langfuse-Public-Key therefore cost the attacker one refused
#    HTTP request and cost this process nothing that outlives it.
#
#    The alternative we weighed, and the reason we did not pick it: gate on the
#    visitor's OPENAI key being accepted by OpenAI, so thread growth needs a funded
#    key. It is marginally stronger as an anti-abuse signal (an OpenAI key costs
#    money, a Langfuse Cloud project is free to create) but it is the wrong check for
#    this resource and worse for the demo. The resource being rationed is a Langfuse
#    client, so the proof to demand is ownership of that Langfuse project; and the
#    Langfuse check also catches the single most common way this demo breaks in front
#    of an audience, a mistyped secret key or the wrong region, which under an
#    OpenAI-only gate produces a client that silently exports nothing. Both gates cost
#    one round trip, so doing both would double the setup latency to buy an anti-abuse
#    margin that control 2 already makes irrelevant.
#
# 2. Admission itself is rationed, because a preflight is an outbound request that a
#    caller can ask for, and it is rationed PER CALLER, which matters more than the
#    total. The first version of this fix used one process-wide token bucket and the
#    load test caught it immediately: 2000 novel keys from one source drained the
#    bucket, and the legitimate visitor who arrived next was told to come back later.
#    A control that turns a thread-exhaustion denial of service into a rate-limit
#    denial of service has not fixed anything.
#
#    So the limits are per source (see _source(): the address the nearest proxy
#    attributes the request to, which the caller cannot choose): _SOURCE_BURST new
#    projects then one per five seconds, and at most _SOURCE_MAX_CONCURRENT admissions
#    in flight. A global semaphore of _ADMIT_MAX_CONCURRENT caps how much of uvicorn's
#    threadpool can be parked on preflights at once, and per-source concurrency being
#    strictly smaller than the global one is what guarantees a flooding source can
#    never hold every slot. Callers already holding a registered project touch none of
#    this: they take the fast path.
#
# 3. There is a hard ceiling on clients ever constructed in this process,
#    _MAX_CLIENT_BUILDS, checked before construction and never reset. That is the
#    number that makes the thread bound ABSOLUTE rather than average:
#
#        ceiling = 4 threads * _MAX_CLIENT_BUILDS = 4 * 48 = 192
#
#    and it holds whatever happens to controls 1 and 2, whatever the SDK does with
#    its own teardown, and whatever a future version of opentelemetry does with the
#    private attributes _teardown() reaches for. Past that ceiling every new project
#    is refused with a clear message. RSS ceiling by the same arithmetic is about
#    48 * 1.16 MB = 56 MB, against App Runner's 2048 MB.
#
# WHY WE REFUSE INSTEAD OF EVICTING
# ---------------------------------
# The previous version kept an LRU of 16 and evicted the oldest. That made the cap
# itself an attack: sixteen anonymous requests with novel keys evicted the presenter's
# client and silently killed their tracing until the process restarted, which is the
# one thing the demo exists to show. So the registry never evicts to make room. A new
# project past _MAX_LANGFUSE_PROJECTS is refused, loudly, to the caller asking for it.
# The only way an entry leaves the registry is:
#   - it has been idle for _PROJECT_IDLE_SECONDS with no request in flight, which is
#     what lets one long-lived instance serve more than 16 people over an afternoon,
#     and which an attacker cannot induce for someone who is actually using the demo;
#   - the same public key comes back with a different secret key or region (a rotated
#     key), and no request is in flight on it, in which case we rebuild.
# Both paths check inflight == 0 while holding the registry lock, so a teardown can
# never touch a visitor mid-request.
_MAX_LANGFUSE_PROJECTS = 16

# 16 concurrent projects, because concurrency here is "how many people at a meetup are
# clicking Run at the same moment", which is single digits, plus headroom for the
# sessions people leave open. 48 lifetime builds is three full turnovers of that.
_MAX_CLIENT_BUILDS = 48

# 15 minutes. Long enough that a presenter who talks through a slide between clicks
# keeps their client (and their trace continuity), short enough that a workshop that
# runs all afternoon does not run out of slots.
_PROJECT_IDLE_SECONDS = 900.0

# Admission rationing. 6 concurrent preflights against ~1 s of Langfuse round trip
# means at most 6 of uvicorn's worker threads (of a default pool of 40) can be parked
# on this at once, and no single source can hold more than 2 of the 6.
#
# Per source, 6 new projects then one every five seconds. A visitor needs exactly one,
# and a visitor who is switching between two projects during a talk needs two; 2000
# novel keys from one source get 6 and then a 429 each, at about a millisecond of our
# time per refusal.
_ADMIT_MAX_CONCURRENT = 6
_SOURCE_MAX_CONCURRENT = 2
_SOURCE_BURST = 6
_SOURCE_REFILL_PER_SECOND = 0.2

# Bucket table bound, because the key comes from a request. Dropping the least recently
# used bucket hands whoever owned it a full one, which only ever helps a real visitor;
# an attacker cannot use that to refill because they cannot choose the key it is under.
_MAX_TRACKED_SOURCES = 512

# Short, because a visitor is watching a spinner and because a slow preflight holds
# one of the six admission slots.
_PREFLIGHT_TIMEOUT_S = 8.0

# Any authenticated Langfuse endpoint would do. This one is cheap, exists in both
# regions, and needs no ids we would have to guess.
_PREFLIGHT_PATH = "/api/public/projects"


class _Project:
    """One registered Langfuse public key: what it was built with, and its use."""

    __slots__ = ("fingerprint", "last_used", "inflight")

    def __init__(self, fingerprint: str):
        self.fingerprint = fingerprint
        self.last_used = time.monotonic()
        self.inflight = 0


_registry_lock = threading.Lock()
_registry: "dict[str, _Project]" = {}

# Monotonic, never decremented, not reset by teardown. This is control 3.
_builds_used = 0

_admit_slots = threading.BoundedSemaphore(_ADMIT_MAX_CONCURRENT)
_admit_lock = threading.Lock()


class _Source:
    """One caller's share of the admission budget."""

    __slots__ = ("tokens", "refilled_at", "inflight")

    def __init__(self):
        self.tokens = float(_SOURCE_BURST)
        self.refilled_at = time.monotonic()
        self.inflight = 0


_sources: "OrderedDict[str, _Source]" = OrderedDict()

# Per-process salt: the fingerprints below are only ever compared in-process, and
# salting them means the registry holds nothing derived from a secret that would
# be useful anywhere else if a heap dump escaped.
_FP_SALT = secrets.token_bytes(32)


def _fingerprint(secret_key: str, base_url: str) -> str:
    return hashlib.blake2b(
        f"{base_url}\x00{secret_key}".encode(), key=_FP_SALT, digest_size=16
    ).hexdigest()


def _acquire_source_share(source: str) -> bool:
    """One caller's token and concurrency slot for a new-project admission.

    Cheap: one dict lookup and some arithmetic under a lock held for microseconds. No
    threads, no timers, and the table is bounded.
    """
    with _admit_lock:
        entry = _sources.get(source)
        if entry is None:
            entry = _Source()
            _sources[source] = entry
            while len(_sources) > _MAX_TRACKED_SOURCES:
                oldest, victim = next(iter(_sources.items()))
                if victim.inflight > 0:
                    # Never forget a caller who is mid-admission; move it along instead.
                    _sources.move_to_end(oldest)
                    break
                _sources.pop(oldest)
        else:
            _sources.move_to_end(source)

        if entry.inflight >= _SOURCE_MAX_CONCURRENT:
            return False
        now = time.monotonic()
        entry.tokens = min(
            float(_SOURCE_BURST),
            entry.tokens + (now - entry.refilled_at) * _SOURCE_REFILL_PER_SECOND,
        )
        entry.refilled_at = now
        if entry.tokens < 1.0:
            return False
        entry.tokens -= 1.0
        entry.inflight += 1
        return True


def _release_source_share(source: str) -> None:
    with _admit_lock:
        entry = _sources.get(source)
        if entry is not None and entry.inflight > 0:
            entry.inflight -= 1


def _pop_locked(public_key: str):
    """Detach a public key from both registries and return the SDK instance.

    Caller must hold _registry_lock and must NOT tear the instance down while holding
    it: shutdown() flushes and joins threads, which can take seconds, and doing that
    under the registry lock would stall every other request's fast path.

    Lock order in this file is always _registry_lock then LangfuseResourceManager._lock,
    here and in _build_locked; nothing ever takes them the other way round.
    """
    _registry.pop(public_key, None)
    # Reaching into LangfuseResourceManager._instances is the only way to retire a
    # single key: the public surface offers reset(), which would tear down every
    # visitor's client, not just the one we are retiring.
    with LangfuseResourceManager._lock:
        return LangfuseResourceManager._instances.pop(public_key, None)


def _teardown(instances) -> None:
    """Give back all four threads a Langfuse client owns. Best effort, never raises.

    The SDK's shutdown() returns only two of the four (see the measurement above), so
    this also stops the batch span processor and the prompt cache refresher. Both are
    reached by duck typing through attributes we already hold rather than by importing
    opentelemetry (an undeclared transitive dependency): if a future version moves
    them, the getattr chain misses, we log, and the only consequence is that a retired
    project leaves two idle threads behind - which _MAX_CLIENT_BUILDS still bounds.

    Runs while its caller holds an admission slot, and shutdown() flushes through the
    SHARED tracer provider, so a Langfuse outage can park one of the admission slots here
    for as long as the OTel force_flush timeout. That is the direction to fail in: new
    visitors get a 429 they can retry, and visitors who already have a client never come
    through here at all.
    """
    for public_key, instance in instances:
        try:
            instance.shutdown()  # flushes, then stops the media and score consumers
        except Exception:
            logger.warning("a retired Langfuse client did not shut down cleanly")

        try:
            provider = getattr(instance, "tracer_provider", None)
            multi = getattr(provider, "_active_span_processor", None)
            processors = getattr(multi, "_span_processors", None)
            if processors:
                mine = [p for p in processors if getattr(p, "public_key", None) == public_key]
                keep = tuple(p for p in processors if getattr(p, "public_key", None) != public_key)
                # Detach before shutdown so no span is offered to a processor that is
                # in the middle of stopping.
                lock = getattr(multi, "_lock", None)
                if lock is not None:
                    with lock:
                        multi._span_processors = keep
                else:
                    multi._span_processors = keep
                for processor in mine:
                    processor.shutdown()
        except Exception:
            logger.warning("could not detach a retired Langfuse span processor")

        try:
            task_manager = getattr(getattr(instance, "prompt_cache", None), "_task_manager", None)
            if task_manager is not None:
                task_manager.shutdown()
        except Exception:
            logger.warning("could not stop a retired Langfuse prompt cache refresher")


def _pop_idle_locked():
    """Detach entries idle past the deadline with nothing in flight. Lock held."""
    deadline = time.monotonic() - _PROJECT_IDLE_SECONDS
    stale = [
        key
        for key, entry in _registry.items()
        if entry.inflight == 0 and entry.last_used < deadline
    ]
    return [(key, _pop_locked(key)) for key in stale]


def _hold_locked(entry: _Project, c: Creds, public_key: str) -> None:
    """Mark this request as in flight on a project, so it cannot be retired."""
    entry.inflight += 1
    entry.last_used = time.monotonic()
    c.langfuse_slot = public_key


_AT_CAPACITY = (
    "This demo instance is already tracing to the maximum number of Langfuse projects. "
    "Nothing was traced for this request. Try again in a few minutes, or ask the "
    "presenter to restart the demo."
)


def _preflight(c: Creds) -> None:
    """Confirm the visitor's Langfuse key pair with Langfuse, before spending a client.

    The URL is built from c.langfuse_host, which normalise_langfuse_host() has already
    reduced to one of two exact allowlisted origins, so this cannot be pointed anywhere
    else. The response body is never read, never logged and never echoed: only the
    status code is used, and the messages below are ours.
    """
    url = f"{c.langfuse_host}{_PREFLIGHT_PATH}"
    try:
        with httpx.Client(timeout=_PREFLIGHT_TIMEOUT_S, follow_redirects=False) as client:
            with client.stream(
                "GET", url, auth=(c.langfuse_public_key, c.langfuse_secret_key)
            ) as response:
                status = response.status_code
    except Exception:
        # Fail closed. The alternative, building the client anyway, is how you get a
        # thread leak driven by an attacker who simply blackholes the preflight.
        raise CredentialError(
            "Could not reach Langfuse to check your keys, so nothing was traced. "
            "Try again in a moment.",
            502,
        )

    if status in (401, 403):
        raise CredentialError(
            "Langfuse rejected that public/secret key pair. In the Setup tab, check "
            "that both keys come from the same Langfuse project and that the host "
            f"matches that project's region ({c.langfuse_host}).",
            401,
        )
    if status >= 400:
        raise CredentialError(
            "Langfuse could not confirm your keys just now, so nothing was traced. "
            "Try again in a moment.",
            502,
        )


def langfuse_client(c: Creds):
    """The Langfuse client for this request, built on first use. Raises CredentialError.

    Every caller is inside a request that has already passed header validation, body
    validation and (on /api/coding/apply) the HMAC gate, because this is only ever
    reached from lf.py at the first tracing call. That ordering is the fix for the
    denial of service; see bound().
    """
    if c._langfuse_client is not None:
        return c._langfuse_client

    public_key = c.langfuse_public_key
    want = _fingerprint(c.langfuse_secret_key, c.langfuse_host)

    with _registry_lock:
        entry = _registry.get(public_key)
        admitted = entry is not None and entry.fingerprint == want
        if admitted:
            _hold_locked(entry, c, public_key)

    if not admitted:
        _admit(c, public_key, want)

    client = get_client(public_key=public_key)
    c._langfuse_client = client
    return client


def _admit(c: Creds, public_key: str, want: str) -> None:
    """Ration, verify, then construct. Raises CredentialError rather than degrading.

    The caller's own share is taken first and the shared slot second, so a source that
    has spent its share is refused in microseconds without ever competing for the
    shared one.
    """
    if not _acquire_source_share(c.source):
        raise CredentialError(
            "This demo instance has set up as many new Langfuse sessions for you as it "
            "will for now. Nothing was traced for this request. Wait a few seconds and "
            "try again.",
            429,
        )
    try:
        if not _admit_slots.acquire(blocking=False):
            raise CredentialError(
                "This demo instance is setting up several new Langfuse sessions at once. "
                "Nothing was traced for this request. Try again in a few seconds.",
                429,
            )
        try:
            _admit_locked_phases(c, public_key, want)
        finally:
            _admit_slots.release()
    finally:
        _release_source_share(c.source)


def _admit_locked_phases(c: Creds, public_key: str, want: str) -> None:
    """The three phases of admission, holding an admission slot throughout."""
    # Phase one: everything that can refuse without touching the network.
    retire = []
    try:
        with _registry_lock:
            retire.extend(_pop_idle_locked())
            entry = _registry.get(public_key)
            if entry is not None and entry.fingerprint == want:
                _hold_locked(entry, c, public_key)
                return
            _check_room_locked(entry, public_key)
    finally:
        _teardown(retire)

    # Phase two: the network round trip, deliberately outside the registry lock.
    _preflight(c)

    # Phase three: build. The caps are re-checked because another request may have taken
    # the last slot or the last build while we were on the network.
    retire = []
    try:
        with _registry_lock:
            entry = _registry.get(public_key)
            if entry is not None and entry.fingerprint == want:
                _hold_locked(entry, c, public_key)
                return
            _check_room_locked(entry, public_key)
            if entry is not None:
                # Same public key, different secret or different region: a rotated key.
                # The SDK's singleton would keep serving whatever it was first built
                # with, so a visitor who rotated their secret would get silent auth
                # failures forever. Rebuild, and only because the check above proved
                # nobody is mid-request on it.
                retire.append((public_key, _pop_locked(public_key)))
            _build_locked(c, public_key, want)
    finally:
        _teardown(retire)


def _check_room_locked(entry, public_key: str) -> None:
    """Refuse rather than evict, and never disturb a request in flight. Lock held."""
    if entry is not None and entry.inflight > 0:
        # Only reachable when the same public key arrives with a different secret key
        # while an older request is still using the old one. Refusing is the point:
        # rebuilding would drop the in-flight visitor's traces.
        raise CredentialError(
            "That Langfuse public key is in use with a different secret key right now. "
            "Nothing was traced for this request. Try again in a moment.",
            409,
        )
    if entry is None and len(_registry) >= _MAX_LANGFUSE_PROJECTS:
        raise CredentialError(_AT_CAPACITY, 503)
    if _builds_used >= _MAX_CLIENT_BUILDS:
        # The absolute ceiling. Says something an operator can act on, because the only
        # cure is a restart (or an instance with a shorter life than this one has had).
        logger.warning(
            "refusing a new Langfuse client: this process has built %s, its lifetime "
            "ceiling. Restart the instance to reset it.",
            _builds_used,
        )
        raise CredentialError(_AT_CAPACITY, 503)


def _build_locked(c: Creds, public_key: str, want: str) -> None:
    """Construct the per-key SDK singleton and register it. Lock held.

    Construction is the public API: Langfuse(public_key=, secret_key=, base_url=)
    registers the per-key singleton, and lf.py then resolves it with the public
    get_client(public_key=...). We never touch langfuse's private
    _set_current_public_key contextvar, because passing the key explicitly at both
    resolution points (get_client here, langfuse_public_key= on each OpenAI create)
    does the same job through documented arguments.
    """
    global _builds_used
    Langfuse(
        public_key=public_key,
        secret_key=c.langfuse_secret_key,
        base_url=c.langfuse_host,
        # Small batches and a short flush interval: a demo audience watches the trace
        # appear in the Langfuse UI seconds after clicking Run.
        flush_at=1,
        flush_interval=0.5,
        timeout=10,
    )
    _builds_used += 1
    entry = _Project(want)
    _registry[public_key] = entry
    _hold_locked(entry, c, public_key)


def release_langfuse_slot(c: Creds) -> None:
    """Stop counting this request as in flight. Called from bound()'s finally."""
    public_key = c.langfuse_slot
    c.langfuse_slot = None
    c._langfuse_client = None
    if public_key is None:
        return
    with _registry_lock:
        entry = _registry.get(public_key)
        if entry is not None and entry.inflight > 0:
            entry.inflight -= 1
            entry.last_used = time.monotonic()


def registry_size() -> int:
    """For tests and for a sanity check that the bound is doing something."""
    with _registry_lock:
        return len(_registry)


def client_builds_used() -> int:
    """Clients constructed in this process's lifetime, against _MAX_CLIENT_BUILDS."""
    with _registry_lock:
        return _builds_used


# --- HMAC signing secret --------------------------------------------------------
#
# Used by coding_agent.py to sign proposals so propose/apply can be stateless.
# Lives here because it is the one long-lived secret this process holds.
#
# If PROPOSAL_SIGNING_SECRET is unset we generate one per process. There is no
# deterministic fallback available that is not also a forgeable one: anything derived
# from the image, the code or the hostname is either identical for every reader of
# this repo or different on every instance, and a hardcoded default is strictly worse
# than a restart-scoped random one.
#
# What we refuse to keep is the SYMPTOM being indistinguishable. A per-process secret
# recreates exactly the multi-instance failure the stateless rewrite existed to
# remove: propose lands on instance A, apply is routed to instance B, the signature
# does not verify, and the operator reads the same "could not be verified" message a
# forgery attempt produces. So every signature carries a key id: a 4 byte keyed digest
# of the signing secret, which is safe to publish (it is a 32 bit label derived through
# blake2b keyed BY the secret, so it discloses nothing about it) and which lets verify
# separate the two cases:
#
#   key id does not match this process  ->  "a different instance or a restart", said
#                                           plainly, plus a server-side warning naming
#                                           the missing env var
#   key id matches, digest does not     ->  forgery or tampering, and the caller is
#                                           told nothing beyond "not applied"
#
# Set PROPOSAL_SIGNING_SECRET to the same value on every instance to make proposals
# portable across instances and restarts.
_SIGNING_SECRET_FROM_ENV = (os.getenv("PROPOSAL_SIGNING_SECRET", "") or "").encode()
_SIGNING_SECRET = _SIGNING_SECRET_FROM_ENV or secrets.token_bytes(32)
SIGNING_SECRET_IS_EPHEMERAL = not _SIGNING_SECRET_FROM_ENV

# Popped so it cannot be read back out of the environment by anything else in the
# process, and so it never reaches a debug endpoint or a crash dump of os.environ.
os.environ.pop("PROPOSAL_SIGNING_SECRET", None)

_SIGNING_KEY_ID = hashlib.blake2b(
    b"lf-demo-proposal-signing-key-id", key=_SIGNING_SECRET, digest_size=4
).hexdigest()

# Verdicts from verify_signature. Deliberately three, not two.
SIGNATURE_OK = "ok"
SIGNATURE_FOREIGN_KEY_ID = "foreign"
SIGNATURE_INVALID = "invalid"


def signing_key_id() -> str:
    """The public label for this process's signing secret. Not a secret."""
    return _SIGNING_KEY_ID


def sign(payload: bytes) -> str:
    digest = hmac.new(_SIGNING_SECRET, payload, hashlib.sha256).hexdigest()
    return f"{_SIGNING_KEY_ID}.{digest}"


def log_signing_secret_notice() -> None:
    """Say once, at boot, which of the two failure modes this instance will have.

    The ephemeral case is a WARNING because it is the one an operator has to know about
    and because uvicorn's default log configuration leaves everything below WARNING
    invisible. The configured case is an INFO for exactly that reason: a healthy service
    should not warn at every boot, and the key id it would have printed is repeated in
    the rejection warning in coding_agent._verify(), which is the only moment anyone
    needs it.
    """
    if SIGNING_SECRET_IS_EPHEMERAL:
        logger.warning(
            "PROPOSAL_SIGNING_SECRET is not set, so proposals are signed with a "
            "per-process key (id %s). A proposal cannot be applied after a restart, "
            "and cannot be applied at all if the service runs more than one instance. "
            "Set the same PROPOSAL_SIGNING_SECRET on every instance to fix that.",
            _SIGNING_KEY_ID,
        )
    else:
        logger.info("proposal signing key id for this instance: %s", _SIGNING_KEY_ID)

# --- Log scrubber ---------------------------------------------------------------
#
# Our own code never logs a credential. That turned out not to be sufficient, and
# the discovery is worth recording rather than just fixing.
#
# With a deliberately wrong OpenAI key, langfuse/openai.py does `logger.warning(ex)`
# on the "langfuse" logger before re-raising, and OpenAI's 401 body reads:
#
#     Incorrect API key provided: sk-fake-**************-000
#
# The middle is masked; the first eight and last three characters are not. So a
# third-party log statement, in a dependency, was writing a partial credential into
# stdout, which on App Runner is CloudWatch. No amount of care in our own code
# prevents that, and reviewing every dependency's log calls on every version bump is
# not a control that survives contact with reality.
#
# WHY THE PREVIOUS BACKSTOP WAS NOT ONE
# -------------------------------------
# The claim used to be that a filter on root's handlers redacted these "regardless of
# who emitted them". Under uvicorn that is false: uvicorn's dictConfig puts handlers on
# uvicorn, uvicorn.error and uvicorn.access, and leaves root.handlers EMPTY. So the
# loop over root.handlers attached the filter to nothing, and a record from any logger
# we had not named by hand went out through logging.lastResort unredacted.
#
# Three mechanisms now, in order of how much they cover:
#
#   1. A log record factory. Every LogRecord in the process is built by the factory,
#      whatever logger it came from and whichever handler eventually emits it, so
#      scrubbing there is the only placement that is genuinely universal. It is a
#      documented API (logging.setLogRecordFactory), it chains to whatever factory was
#      installed before it, and it renders exc_info once at creation because a
#      formatter would otherwise render the raw traceback later, long after any filter
#      has run.
#   2. Filters on the loggers that handle credentials. Redundant with the factory, kept
#      because they keep working if something later replaces the record factory.
#   3. Filters on every handler that actually exists, including logging.lastResort,
#      which is the handler that emits "langfuse" records under uvicorn precisely
#      because root has none.
#
# install_log_scrubber() ends by emitting one canary line through the "langfuse"
# logger, the same third-party logger that leaked. That line is the proof, in the real
# server log of whatever environment this is deployed to, that the redaction is
# attached to the handler configuration uvicorn actually installed. If the log shows
# the canary token instead of [redacted-credential], the control is not working.
_SECRET_SHAPED = re.compile(
    r"""(?xi)
    \b
    (?: sk | pk | rk )        # OpenAI and Langfuse key prefixes
    - [A-Za-z0-9_*\-]{4,}
    """
)

_SCRUBBED_LOGGERS = ("langfuse", "openai", "httpx", "httpcore", "uvicorn", "uvicorn.error")

_REDACTED = "[redacted-credential]"

_CANARY = "sk-canary-scrubber-selftest-0000"


def _scrub(text: str) -> str:
    return _SECRET_SHAPED.sub(_REDACTED, text)


def _scrub_record(record: logging.LogRecord) -> None:
    """Redact a record in place. Safe to run twice."""
    try:
        message = record.getMessage()
    except Exception:
        # A record whose args do not match its format string. Nothing to redact that
        # we can render, and raising here would lose the log line entirely.
        return
    if _SECRET_SHAPED.search(message):
        record.msg = _scrub(message)
        record.args = ()
    if record.exc_info and not record.exc_text:
        # A handler renders exc_info long after a filter has run, so redacting
        # record.msg alone would miss it. Formatting it here and caching the scrubbed
        # text in exc_text is enough: logging.Formatter uses a pre-populated exc_text
        # verbatim instead of re-formatting. The traceback survives, which matters for
        # debugging a boot or middleware failure; only key-shaped tokens inside it do
        # not.
        try:
            rendered = "".join(traceback.format_exception(*record.exc_info))
        except Exception:
            return
        record.exc_text = _scrub(rendered)


class _RedactCredentials(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        _scrub_record(record)
        return True


_redactor = _RedactCredentials()

_installed = False


def install_log_scrubber() -> None:
    """Idempotent. Call once at app import, after the server has configured logging."""
    global _installed
    if _installed:
        return
    _installed = True

    previous_factory = logging.getLogRecordFactory()

    def _factory(*args, **kwargs):
        record = previous_factory(*args, **kwargs)
        _scrub_record(record)
        return record

    logging.setLogRecordFactory(_factory)

    targets = [logging.getLogger(name) for name in _SCRUBBED_LOGGERS]
    targets.append(logging.getLogger())
    for target in targets:
        if _redactor not in target.filters:
            target.addFilter(_redactor)

    # Every handler that exists right now, wherever it is attached, plus the one
    # logging falls back to when a record reaches root and root has no handlers.
    for handler in _live_handlers():
        if _redactor not in handler.filters:
            handler.addFilter(_redactor)

    logging.getLogger("langfuse").warning(
        "lf_demo log scrubber self-test: the token at the end of this line must be "
        "redacted, and if it is not then credentials logged by a dependency will reach "
        "this log too: %s",
        _CANARY,
    )


def _live_handlers():
    """Handlers on root, on every configured logger, and logging's last resort."""
    handlers = list(logging.getLogger().handlers)
    for logger_or_placeholder in list(logging.root.manager.loggerDict.values()):
        handlers.extend(getattr(logger_or_placeholder, "handlers", ()) or ())
    if logging.lastResort is not None:
        handlers.append(logging.lastResort)
    return handlers


def verify_signature(payload: bytes, signature: str) -> str:
    """SIGNATURE_OK, SIGNATURE_FOREIGN_KEY_ID or SIGNATURE_INVALID.

    The digest compare is constant time, so a forger cannot binary-search it by
    timing. The key id compare is not, and does not need to be: it is a public label,
    and it is the caller who chose the value being compared.
    """
    if not isinstance(signature, str):
        return SIGNATURE_INVALID
    key_id, _, digest = signature.partition(".")
    if len(key_id) != 8 or len(digest) != 64:
        return SIGNATURE_INVALID
    if key_id != _SIGNING_KEY_ID:
        return SIGNATURE_FOREIGN_KEY_ID
    if hmac.compare_digest(sign(payload), signature):
        return SIGNATURE_OK
    return SIGNATURE_INVALID
