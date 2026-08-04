"""Non-secret configuration for the bring-your-own-key deployment.

This module used to hold the demo's credentials, read from .env at import time.
It no longer holds any, and that is the point: this app is deployed PUBLICLY with
no auth gate, and the only thing that makes that defensible is that every visitor
spends their own OpenAI and Langfuse credentials. A server-side key would turn an
open endpoint into an open tab on our account, so there must not be one to find.

Two consequences are enforced here rather than left to convention:

  1. Credential environment variables are deleted from os.environ at import.
     Both SDKs we use fall back to the environment when a key argument is None
     (openai.OpenAI() reads OPENAI_API_KEY, Langfuse() reads LANGFUSE_*). A
     leftover .env from the pre-BYOK version of this demo, or an operator who
     sets these on the App Runner service out of habit, would silently turn that
     fallback back on and nobody would notice, because everything would keep
     working. Removing the variables makes "never fall back to process env
     credentials" a property of the process, not a code review promise.

  2. OPENAI_BASE_URL is scrubbed for the same reason in the other direction: it
     would let ambient configuration redirect visitors' API keys to a host we did
     not choose. The base URL is never taken from the request either.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

_HERE = Path(__file__).resolve().parent.parent
load_dotenv(_HERE / ".env")
load_dotenv(_HERE.parent / ".env")  # shared fallback; does not override the above

# Anything that either SDK would pick up implicitly as a credential or as a
# credential destination. Popped after load_dotenv so a .env cannot reintroduce it.
_SCRUBBED_ENV = (
    "OPENAI_API_KEY",
    "OPENAI_ORG_ID",
    "OPENAI_PROJECT_ID",
    "OPENAI_BASE_URL",
    "LANGFUSE_PUBLIC_KEY",
    "LANGFUSE_SECRET_KEY",
    "LANGFUSE_HOST",
    "LANGFUSE_BASE_URL",
)

for _name in _SCRUBBED_ENV:
    os.environ.pop(_name, None)


def _csv(name: str, default: list[str]) -> list[str]:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return list(default)
    values = [v.strip() for v in raw.split(",")]
    return [v for v in values if v] or list(default)


# --- Model selection -----------------------------------------------------------
#
# The visitor picks a model in the Setup tab and sends it as X-Openai-Model. The
# choices are advertised by /api/status so the UI does not have to hardcode them,
# and so an operator can widen or narrow the list without a frontend deploy.

DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "") or "gpt-4o-mini"

MODEL_CHOICES = _csv(
    "OPENAI_MODEL_CHOICES",
    ["gpt-4o-mini", "gpt-4o", "gpt-4.1-mini", "gpt-4.1", "gpt-5-mini", "gpt-5"],
)

if DEFAULT_MODEL not in MODEL_CHOICES:
    MODEL_CHOICES = [DEFAULT_MODEL] + MODEL_CHOICES

# Every visitor traces into their OWN Langfuse project, so a fixed user id is not
# a privacy problem and keeps each visitor's traces grouped under one user.
DEMO_USER_ID = "byok-visitor"


def status() -> dict:
    """The payload behind GET /api/status.

    Deliberately says nothing about whether any key is present server-side. There
    are none, and an endpoint that reported "openai_enabled: true/false" would be
    an oracle for probing exactly that, on top of being a lie once the answer is
    permanently false.
    """
    from app import creds  # imported here to keep config free of app-level imports

    return {
        "byok": True,
        "allowed_langfuse_hosts": list(creds.ALLOWED_LANGFUSE_HOSTS),
        "default_langfuse_host": creds.DEFAULT_LANGFUSE_HOST,
        "default_model": DEFAULT_MODEL,
        "model_choices": list(MODEL_CHOICES),
    }
