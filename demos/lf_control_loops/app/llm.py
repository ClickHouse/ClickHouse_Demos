"""OpenAI access, with the key taken from the request rather than the process.

We import OpenAI from `langfuse.openai` (a drop-in replacement). That import
monkey-patches the openai module process-wide, so every completion made anywhere
in this process is recorded in Langfuse as a `generation` observation (model, full
input/output, token usage, USD cost, latency) nested under whatever observation is
currently active. The only change vs. the vanilla SDK is the import line.

Two things follow from that patch being process-wide, and they are the reason this
file looks the way it does:

  1. Every create() call must carry `langfuse_public_key=`. Without it the
     integration resolves "the single Langfuse client in this process", which on a
     multi-tenant BYOK server means one visitor's prompts land in another visitor's
     project. creds.extract() makes the Langfuse keys mandatory so this key is
     always available, and _pinned_kwargs() below attaches it to every call.

  2. There is no module-level client. A client is built per request from the
     visitor's key and closed when the request ends (see creds.Creds.openai), so no
     credential outlives the request that supplied it and nothing is cached under a
     key a later request could replay.

`MODEL` remains a module-level string because workflow.py and agent.py do
`from app.llm import MODEL` and pass `model=MODEL` at every call site, and those
files are deliberately frozen. The value they see is the advertised default; the
model actually used is the visitor's choice, substituted in timed_create() and
patched into the response labels in main.py. See the note on _run() there.
"""

import logging
import threading
import time

import openai
from langfuse.openai import OpenAI

from app import config, creds

logger = logging.getLogger("lf_demo.llm")

MODEL = config.DEFAULT_MODEL

# Fail fast rather than hanging a demo: the visitor is watching a spinner, and a
# 10 minute default timeout with two silent retries is the wrong shape for that.
_REQUEST_TIMEOUT_S = 60.0
_MAX_RETRIES = 1


def build_openai_client(api_key: str) -> OpenAI:
    """One client for one request, authenticated with that request's key.

    base_url is never passed and never accepted from the caller. A client-supplied
    base URL would be the same SSRF hole as the Langfuse host, except that the
    thing shipped to the attacker's server is the visitor's own API key in an
    Authorization header.
    """
    return OpenAI(api_key=api_key, timeout=_REQUEST_TIMEOUT_S, max_retries=_MAX_RETRIES)


# Reasoning models (gpt-5*, o-series) reject a custom `temperature` and only allow
# the default. We skip temperature up front for those (so the first call does not
# error), keep it for models that support it (for determinism), and still fall back
# by dropping it if any model rejects it at runtime.
#
# The learned part is now per model rather than one process-wide flag. With a single
# flag, one visitor running gpt-5 taught the process to drop temperature for every
# later visitor on gpt-4o-mini too, quietly costing them the determinism the demo
# script relies on.
def _reasoning_model(model: str) -> bool:
    m = (model or "").lower()
    if m.startswith("gpt-5") and not m.startswith("gpt-5-chat"):
        return True
    return m.startswith(("o1", "o3", "o4"))


_temp_lock = threading.Lock()
_temp_unsupported: set[str] = set()
# Bounded because the model name comes from a request header. Validation already
# restricts it to 64 characters of a safe alphabet, but "attacker-controlled string
# used as a dict key that is never evicted" is a shape worth not having.
_MAX_LEARNED_MODELS = 64


def _temperature_supported(model: str) -> bool:
    if _reasoning_model(model):
        return False
    with _temp_lock:
        return model not in _temp_unsupported


def _remember_temperature_unsupported(model: str) -> None:
    with _temp_lock:
        if len(_temp_unsupported) < _MAX_LEARNED_MODELS:
            _temp_unsupported.add(model)


def usage_dict(response):
    """Pull token counts off an OpenAI response in a UI-friendly shape."""
    u = getattr(response, "usage", None)
    if not u:
        return None
    return {
        "prompt_tokens": getattr(u, "prompt_tokens", None),
        "completion_tokens": getattr(u, "completion_tokens", None),
        "total_tokens": getattr(u, "total_tokens", None),
    }


def timed_create(**kwargs):
    """client.chat.completions.create with wall-clock latency measured locally.

    Returns (response, latency_ms). `name=` (Langfuse generation name) and all
    normal OpenAI kwargs pass straight through. Two substitutions happen here so
    the loop call sites need no changes: the model becomes the one this visitor
    chose, and the generation is pinned to this visitor's Langfuse project.
    Transparently drops `temperature` for models that only support the default.
    """
    c = creds.require()
    client = c.openai()
    model = c.model

    kwargs["model"] = model
    # Pinning every generation to the caller's project. If this were ever absent
    # the SDK would pick a project for us; see the module docstring.
    kwargs["langfuse_public_key"] = c.langfuse_public_key

    if not _temperature_supported(model):
        kwargs.pop("temperature", None)

    start = time.perf_counter()
    try:
        response = client.chat.completions.create(**kwargs)
    except openai.BadRequestError as exc:
        msg = str(exc).lower()
        if "temperature" in kwargs and "temperature" in msg and (
            "unsupported" in msg or "does not support" in msg or "only the default" in msg
        ):
            _remember_temperature_unsupported(model)
            kwargs.pop("temperature", None)
            response = client.chat.completions.create(**kwargs)
        else:
            raise
    latency_ms = round((time.perf_counter() - start) * 1000)
    return response, latency_ms
