"""FastAPI app for the Langfuse deep dive: three control loops.

Serves the UI and one endpoint per loop. This deployment is PUBLIC and has no auth
gate, which is only defensible because it is bring-your-own-key: the visitor pastes
their own OpenAI and Langfuse credentials in the Setup tab, the browser holds them
in sessionStorage, and they arrive as headers on each request. There is no
server-side key to abuse, so an open endpoint costs us nothing to have open. Every
choice in this file is there to keep that property true.

Credentials are bound to a contextvar for the duration of one request (see
creds.py) so that workflow.py, agent.py and coding_agent.py keep their existing
call sites, which is deliberate: those three files are the demo.

Run:  ./run.sh
"""

import logging
import threading
import traceback
import uuid
from pathlib import Path
from typing import Optional

import openai
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field

from app import agent, coding_agent, config, creds, lf, workflow
from app.creds import CredentialError

logger = logging.getLogger("lf_demo.api")

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"

app = FastAPI(title="Langfuse deep dive - three control loops")

# Installed at import, after uvicorn has configured logging, so that a dependency's
# log call cannot put key-shaped text in CloudWatch. See creds.install_log_scrubber
# for the specific third-party statement that made this necessary, and for the canary
# line it emits to prove the redaction is attached to uvicorn's real handlers.
creds.install_log_scrubber()

# One line at boot saying whether proposals will survive a restart or a second
# instance, so that "this proposal could not be verified" is never ambiguous later.
creds.log_signing_secret_notice()

# Free text the visitor types. Their tokens, their bill, but there is no reason for
# this service to carry a novel into a prompt.
_MAX_TEXT = 8000
_MAX_SESSION_ID = 200  # Langfuse requires session_id to be US-ASCII and <= 200 chars

# One proposal round trip is the largest legitimate body. Starlette will otherwise
# buffer whatever an unauthenticated caller feels like sending.
_MAX_BODY_BYTES = 1024 * 1024


@app.middleware("http")
async def no_store(request: Request, call_next):
    """Never serve a stale UI bundle, and harden the page that holds the keys."""
    response = await call_next(request)
    if not request.url.path.startswith("/api/"):
        # A local demo that gets edited between runs, and a deployed one where the
        # Setup tab must not be served from a cache older than the current API.
        response.headers["Cache-Control"] = "no-store, must-revalidate"

        # This page keeps the visitor's OpenAI and Langfuse credentials in
        # sessionStorage, so script injection here is credential theft rather
        # than defacement. That raises the value of headers that would be
        # boilerplate on an ordinary page.
        #
        # The CSP is deliberately strict and the app is built so it can be:
        # there is no inline script, no inline style, no third-party origin, no
        # font or analytics CDN. connect-src stays 'self' because the browser
        # never talks to OpenAI or Langfuse directly; it posts the keys to this
        # server, which makes the upstream calls. If a future change adds a CDN
        # or an inline handler, this header is what will break first, and that is
        # the intended alarm rather than an inconvenience to relax.
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self'; "
            "style-src 'self'; "
            "img-src 'self' data:; "
            "connect-src 'self'; "
            "form-action 'none'; "
            "frame-ancestors 'none'; "
            "base-uri 'none'; "
            "object-src 'none'"
        )
        # frame-ancestors covers modern browsers; X-Frame-Options is kept for the
        # older ones, since clickjacking a form that collects API keys is exactly
        # the attack worth spending one header on.
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-Content-Type-Options"] = "nosniff"
        # Trace deep links leave for cloud.langfuse.com. no-referrer keeps this
        # host's URLs out of a third party's logs; there is nothing here that
        # needs a Referer to work.
        response.headers["Referrer-Policy"] = "no-referrer"
    return response


class BodyCap:
    """Refuse a body over the cap, counted on BYTES ACTUALLY READ.

    The previous version trusted the declared Content-Length, which means it did not
    apply at all to `Transfer-Encoding: chunked`: a reviewer pushed 31.6 MB into
    /api/coding/apply unauthenticated for 190 MB of RSS, and a second measurement
    reached 1.07 GB from a single request. A header the client chooses cannot be the
    thing that limits what the client sends.

    So this is raw ASGI, not BaseHTTPMiddleware, and it drains the body itself, giving
    up the moment the running total crosses the cap. Draining it here rather than
    wrapping receive() and raising later is deliberate for two reasons. One, an
    exception raised out of a wrapped receive() surfaces from inside Starlette's own
    body reading, where ServerErrorMiddleware turns it into a 500 before we can turn
    it into a 413. Two, the bytes are buffered anyway: FastAPI parses JSON bodies from
    a full buffer, so holding at most _MAX_BODY_BYTES here costs nothing that was not
    already going to be spent, and it is the ceiling rather than a hope.

    Requests that pass are replayed downstream from the buffer, so the endpoint sees
    exactly the body it would have seen.
    """

    def __init__(self, app, max_bytes: int = _MAX_BODY_BYTES):
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        # Cheapest possible refusal for an honest client: believe a declared length
        # over the cap and never read a byte of the body.
        for name, value in scope.get("headers", ()):
            if name == b"content-length" and value.isdigit() and int(value) > self.max_bytes:
                return await _send_413(send)

        chunks = []
        total = 0
        while True:
            message = await receive()
            if message["type"] != "http.request":
                # http.disconnect while we were reading. Nothing to answer.
                return
            body = message.get("body") or b""
            total += len(body)
            if total > self.max_bytes:
                # Stop reading. Uvicorn closes the connection after the response, so a
                # sender that keeps pushing gets a TCP reset rather than more of our RSS.
                return await _send_413(send)
            if body:
                chunks.append(body)
            if not message.get("more_body"):
                break

        buffered = b"".join(chunks)
        chunks.clear()
        replayed = False

        async def replay():
            nonlocal replayed
            if not replayed:
                replayed = True
                return {"type": "http.request", "body": buffered, "more_body": False}
            # After the body, defer to the real channel so a disconnect still arrives.
            return await receive()

        await self.app(scope, replay, send)


async def _send_413(send) -> None:
    body = b'{"error": "That request body is too large."}'
    await send(
        {
            "type": "http.response.start",
            "status": 413,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
                (b"connection", b"close"),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


# Added last so it wraps everything, including the no_store middleware above: the cap
# has to run before any part of the app can ask for the body.
app.add_middleware(BodyCap)


# --- Request models ------------------------------------------------------------
#
# session_id is optional on every endpoint: the shared API contract is { ticket },
# { question }, { task }, and the UI may or may not choose to group runs into a
# Langfuse session. A missing one is generated rather than rejected.

class WorkflowIn(BaseModel):
    ticket: str = Field(min_length=1, max_length=_MAX_TEXT)
    session_id: str = Field(default="", max_length=_MAX_SESSION_ID)


class AgentIn(BaseModel):
    question: str = Field(min_length=1, max_length=_MAX_TEXT)
    session_id: str = Field(default="", max_length=_MAX_SESSION_ID)


class ProposeIn(BaseModel):
    task: str = Field(default="", max_length=_MAX_TEXT)
    session_id: str = Field(default="", max_length=_MAX_SESSION_ID)


class ProposalIn(BaseModel):
    """The signed proposal, coming back from the client on /api/coding/apply.

    This used to be `dict[str, Any]`, which is not a bound: it accepted any JSON object
    a caller felt like sending, up to the body cap, and coding_agent then re-serialised
    it with json.dumps before anything had refused it. Every field is a string with its
    own cap here, extras are forbidden, and all of that happens in Pydantic before the
    endpoint function runs, so a hostile shape is a 400 before it is ever canonicalised.

    Field names and types must stay exactly in step with coding_agent._PROPOSAL_FIELDS,
    because the HMAC is over the canonical JSON of these seven keys.
    """

    model_config = ConfigDict(extra="forbid")

    version: str = Field(max_length=16)
    file: str = Field(max_length=128)
    task: str = Field(max_length=_MAX_TEXT)
    original: str = Field(max_length=coding_agent.MAX_FILE_CHARS)
    proposed: str = Field(max_length=coding_agent.MAX_FILE_CHARS)
    explanation: str = Field(max_length=2000)
    trace_id: str = Field(max_length=64)


class ApplyIn(BaseModel):
    # The proposal comes back from the client because there is no server-side job
    # store any more; coding_agent verifies the signature before touching it.
    proposal: ProposalIn
    # "<8 hex key id>.<64 hex digest>" is 73 characters; the cap is slack, not a check.
    signature: str = Field(default="", max_length=128)
    approved: bool = False
    session_id: str = Field(default="", max_length=_MAX_SESSION_ID)


class FeedbackIn(BaseModel):
    trace_id: str = Field(min_length=1, max_length=64)
    value: int = Field(ge=0, le=1)  # 1 = up, 0 = down
    comment: str = Field(default="", max_length=2000)


def _session_id(raw: str) -> str:
    """A Langfuse-safe session id. Generated when the client did not send one."""
    cleaned = "".join(ch for ch in (raw or "").strip() if " " <= ch <= "~")[:_MAX_SESSION_ID]
    return cleaned or f"session-{uuid.uuid4().hex[:12]}"


# --- Error handling -------------------------------------------------------------
#
# This used to return HTTP 200 with {"error": "AuthenticationError: ... <upstream
# url> org-abc123"} to an unauthenticated caller. Two problems, both fixed here.
#
# One, SDK exception strings are internal detail: OpenAI and Langfuse errors quote
# upstream URLs, organisation and project identifiers, and occasionally request ids
# that are useful to nobody but someone mapping our dependencies. So the response
# carries a message we wrote plus a correlation id, and the exception goes to the
# server log where the id can be looked up.
#
# Two, 200-on-error meant every failure looked like a success to anything watching
# from outside, including the App Runner health signal and any alarm built on 5xx
# rate. Errors now use real status codes.
#
# The thing not to lose in the process is the genuinely useful half of an upstream
# error. "Your OpenAI key was rejected" is exactly what a visitor needs to hear and
# is safe to say. So the known OpenAI failure classes are translated by type into a
# message we authored, and only the unrecognised ones collapse to "something went
# wrong, here is an id".

def _fail(status: int, message: str, error_id: Optional[str] = None) -> JSONResponse:
    body = {"error": message}
    if error_id:
        body["error_id"] = error_id
    return JSONResponse(status_code=status, content=body)


@app.exception_handler(RequestValidationError)
async def malformed_body(request: Request, exc: RequestValidationError):
    """One shape of error body for the whole API.

    FastAPI's default 422 carries {"detail": [...]} with the offending input echoed
    back inside it. Echoing input is exactly what we do not want on endpoints whose
    bodies sit next to credential headers, and the UI only knows how to read
    {"error": ...} anyway.
    """
    fields = sorted({str(err.get("loc", ("body",))[-1]) for err in exc.errors()})
    return _fail(400, f"That request was not in the expected form (check: {', '.join(fields)}).")


# Ordered most specific first: APITimeoutError subclasses APIConnectionError, and
# the four auth-ish classes all subclass APIStatusError.
_OPENAI_ERRORS = (
    (
        openai.AuthenticationError,
        401,
        "OpenAI rejected the API key from the Setup tab. Check that it is current and "
        "that you copied all of it.",
    ),
    (
        openai.PermissionDeniedError,
        403,
        "OpenAI accepted the key but refused this model. The key's project may not have "
        "access to it; try another model in the Setup tab.",
    ),
    (
        openai.NotFoundError,
        400,
        "OpenAI does not recognise the selected model. Pick a different one in the Setup tab.",
    ),
    (
        openai.RateLimitError,
        429,
        "OpenAI rate limited this key, or the account has no remaining credit. Wait a "
        "moment and try again.",
    ),
    (
        openai.BadRequestError,
        400,
        "OpenAI rejected the request as malformed. If you changed the model, try the default one.",
    ),
    (openai.APITimeoutError, 504, "The request to OpenAI timed out. Try again."),
    (openai.APIConnectionError, 502, "Could not reach OpenAI from the server. Try again."),
    (openai.APIStatusError, 502, "OpenAI returned an error. Try again in a moment."),
)


def _run(request: Request, label: str, fn, *args, **kwargs):
    """Bind this request's credentials, run a loop, and translate failures.

    creds.bound() is entered here rather than in middleware on purpose. FastAPI runs
    a non-async endpoint in a worker thread with a copy of the context, so setting
    the contextvar inside the endpoint puts it in the same context the loop code
    runs in, and lets it go when the request ends.
    """
    try:
        # The peer address is passed for one purpose only: rationing new Langfuse
        # project admissions per caller, so that one flooding source cannot spend the
        # share a visitor needs. See creds._source().
        c = creds.extract(request.headers, peer=request.client.host if request.client else "")
    except CredentialError as exc:
        # Authored by us and free of upstream detail, so safe to return verbatim.
        return _fail(exc.status_code, exc.message)

    try:
        with creds.bound(c):
            result = fn(*args, **kwargs)
    except CredentialError as exc:
        return _fail(exc.status_code, exc.message)
    except Exception as exc:
        for exc_type, status, message in _OPENAI_ERRORS:
            if isinstance(exc, exc_type):
                error_id = _log_failure(label, exc)
                return _fail(status, message, error_id)
        error_id = _log_failure(label, exc)
        return _fail(500, f"Something went wrong on the server (reference {error_id}).", error_id)

    return _label_model(result, c.model)


def _log_failure(label: str, exc: BaseException) -> str:
    """Log server-side with a correlation id and return the id.

    Deliberately logs the exception TYPE and the stack frames, and never the
    exception MESSAGE. That is not paranoia, it is a measured result: with a
    deliberately wrong key, OpenAI's own 401 body comes back as

        Incorrect API key provided: sk-fake-**************-000

    so logger.exception() was writing a masked-but-real prefix and suffix of the
    visitor's key into our logs, which on App Runner means CloudWatch. The provider
    decides what it echoes and can change that at any time, so "log the message but
    only for exceptions we believe are safe" is a rule that silently stops being
    true. Type plus frames is enough to find any bug in an app this size, and it is
    built only from strings we wrote.

    Stack frames are formatted from __traceback__ alone (format_tb, not
    format_exception) so no chained exception's message rides along either. Frames
    carry file, line, function and our own source text, never local values.
    """
    error_id = uuid.uuid4().hex[:12]
    detail = ""
    if isinstance(exc, openai.APIStatusError):
        # Both provider-generated and both genuinely useful when asking OpenAI what
        # happened. Neither is derived from the credential.
        detail = f", http_status={exc.status_code}, openai_request_id={exc.request_id}"
    frames = "".join(traceback.format_tb(exc.__traceback__))
    logger.error(
        "%s failed (error_id=%s, type=%s%s)\n%s",
        label,
        error_id,
        type(exc).__name__,
        detail,
        frames,
    )
    return error_id


def _label_model(payload, model: str):
    """Stamp the model the visitor actually chose onto the response.

    workflow.py and agent.py record `"model": MODEL` in each step, where MODEL is
    the module-level default imported at startup. Those files are frozen by design
    (see creds.py), so llm.timed_create substitutes the real model on the API call
    and the labels are corrected here, in the one place every loop response passes
    through. Without this the trace would be right and the UI would be lying.
    """
    if not isinstance(payload, dict) or "steps" not in payload:
        return payload
    for step in payload.get("steps") or []:
        if isinstance(step, dict) and step.get("model"):
            step["model"] = model
    payload["model"] = model
    return payload


# --- API routes (must be declared BEFORE the static mount) ---------------------

@app.get("/api/healthz")
def healthz():
    """The App Runner health check. No credentials, no upstream calls, ever.

    If this touched OpenAI or Langfuse then a third party having a bad afternoon
    would fail our health check, App Runner would recycle healthy instances, and the
    outage would be ours instead of theirs.
    """
    return {"ok": True}


@app.get("/api/status")
def status():
    return config.status()


@app.post("/api/workflow/run")
def workflow_run(body: WorkflowIn, request: Request):
    return _run(request, "workflow", workflow.run_workflow, body.ticket, _session_id(body.session_id))


@app.post("/api/agent/run")
def agent_run(body: AgentIn, request: Request):
    return _run(request, "agent", agent.run_agent, body.question, _session_id(body.session_id))


@app.post("/api/coding/propose")
def coding_propose(body: ProposeIn, request: Request):
    return _run(request, "coding-propose", coding_agent.propose, body.task, _session_id(body.session_id))


@app.post("/api/coding/apply")
def coding_apply(body: ApplyIn, request: Request):
    return _run(
        request,
        "coding-apply",
        coding_agent.apply,
        # model_dump gives back exactly the seven string fields, so the canonical bytes
        # coding_agent signs and verifies are unchanged by the validation round trip.
        body.proposal.model_dump(),
        body.signature,
        body.approved,
        _session_id(body.session_id),
    )


@app.post("/api/coding/reset")
def coding_reset():
    """Kept for the UI's rollback button. No credentials needed and no shared state
    to clear, which is the point: this route used to wipe every concurrent visitor's
    pending proposal."""
    return coding_agent.reset()


@app.get("/api/coding/file")
def coding_file():
    return {"file": coding_agent.DEFAULT_FILE, "content": coding_agent.seed_content()}


# A thumbs-up is one score plus a flush, and the flush is a network round trip that
# was measured at 1.6 s. Held open, that is one uvicorn worker thread per click, which
# made this endpoint the cheapest way to consume the whole threadpool.
#
# Two things changed. It is no longer reachable without credentials Langfuse itself
# accepted (creds.langfuse_client refuses to build a client for keys that Langfuse
# rejects, and there is nothing else in this handler that costs anything), and the
# number of these that can be in flight at once is now capped, non-blocking, so the
# 5th concurrent click is told to try again instead of parking a worker. Six people at
# a meetup clicking thumbs-up at the same moment is not six; a flood is.
_FEEDBACK_SLOTS = threading.BoundedSemaphore(4)


@app.post("/api/feedback")
def feedback(body: FeedbackIn, request: Request):
    def _do():
        lf.create_score(
            name="user_feedback",
            value=body.value,
            trace_id=body.trace_id,
            data_type="BOOLEAN",
            comment=body.comment or ("thumbs up" if body.value else "thumbs down"),
            # Stable id so toggling up/down updates one score instead of piling up.
            score_id=f"user_feedback-{body.trace_id}",
        )
        lf.flush()
        return {"ok": True}

    if not _FEEDBACK_SLOTS.acquire(blocking=False):
        return _fail(429, "Too many feedback clicks are in flight. Try that again in a moment.")
    try:
        # Scoring writes to the visitor's own Langfuse project, so it needs their keys
        # like everything else.
        return _run(request, "feedback", _do)
    finally:
        _FEEDBACK_SLOTS.release()


# --- Static SPA (mounted last so it doesn't shadow /api routes) ----------------

app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="127.0.0.1", port=8001, reload=False)
