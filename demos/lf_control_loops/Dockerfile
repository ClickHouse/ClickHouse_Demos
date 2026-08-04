# =============================================================================
# Langfuse three-control-loops demo -- FastAPI/uvicorn container for App Runner
# =============================================================================
# One process serves the static UI and the /api routes, so there is no separate
# web tier and nothing to reverse-proxy. Build and run locally with:
#
#   docker build -t lf-control-loops:dev demos/lf_control_loops
#   docker run --rm -p 8000:8000 lf-control-loops:dev
#   curl -i http://127.0.0.1:8000/api/healthz
#
# PORT: 8000, and that number appears in exactly three places that must agree --
# the EXPOSE and CMD below, and "Port" in apprunner-service.json. It is NOT the
# 3000 the labs App Runner service uses; App Runner takes whatever port you give
# it, and 8000 is uvicorn's own default, so a bare `uvicorn app.main:app` on a
# laptop and this container listen on the same place. Chasing parity with labs
# would have bought nothing and cost a mismatch risk on every future edit.
#
# There are deliberately NO credential ARGs or ENVs here, and nothing to add
# later. Visitors bring their own OpenAI and Langfuse keys per request in
# X-OpenAI-Key / X-Langfuse-* headers, so the image and the running task hold no
# secret at all. Adding a build-arg key would quietly destroy that property, and
# build args are recoverable from image history.
# =============================================================================

FROM python:3.12-slim

# PYTHONUNBUFFERED: App Runner ships stdout to CloudWatch, and a buffered
# interpreter makes a crash look like silence for as long as the buffer holds.
# PYTHONDONTWRITEBYTECODE: no .pyc churn in a layer that is read-only in effect.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencies before source. requirements.txt changes rarely and the install is
# the slow layer (uvicorn[standard] and fastapi[standard] pull a lot), so a
# one-line edit in app/ must not reinstall it. Keep this COPY narrow: a `COPY .`
# here would invalidate the layer on every source change and defeat the split.
COPY requirements.txt ./
RUN python -m pip install --upgrade pip \
    && python -m pip install -r requirements.txt

# Unprivileged runtime user. A public app with an LLM writing files in loop 3 is
# exactly the case where "the process could not have overwritten that even if it
# tried" is worth having. High uid/gid to stay clear of host-mapped ranges.
# Not --system: 10001 is above SYS_UID_MAX and useradd prints a warning that
# reads like a failure in build logs. The uid stays high on purpose.
RUN groupadd --gid 10001 demo \
    && useradd --uid 10001 --gid demo --no-create-home --no-log-init demo

COPY app/ ./app/
COPY static/ ./static/

# Loop 3 writes to /app/sandbox. It is excluded from the build context on
# purpose (never ship a developer's mutated copy), and app.coding_agent
# re-seeds it at import time -- but only if the directory is writable by the
# runtime user, which /app is not, since root owns everything COPYed above.
# Create it owned by demo rather than chown -R /app: the source tree staying
# root-owned and unwritable is a feature, not an oversight.
RUN mkdir -p /app/sandbox && chown demo:demo /app/sandbox

USER demo

EXPOSE 8000

# 0.0.0.0, never 127.0.0.1. A loopback bind inside a container accepts nothing
# from outside it, which presents as an App Runner health check that fails
# forever while `docker logs` cheerfully reports "Uvicorn running" -- one of the
# more expensive ways to lose an afternoon.
#
# --forwarded-allow-ips=* lets uvicorn honour the X-Forwarded-* headers App
# Runner's ingress sets, so the app sees https and a real client address instead
# of the proxy's. Safe here only because App Runner ingress is the sole route
# into this task. It does mean these headers are attacker-settable, so client IP
# is fine for logs and rate-limit buckets and must never be a security boundary.
#
# Exec form, so uvicorn is PID 1 and receives App Runner's SIGTERM directly. A
# shell wrapper would swallow it and turn every deploy into a 30s kill wait.
CMD ["python", "-m", "uvicorn", "app.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--proxy-headers", \
     "--forwarded-allow-ips", "*"]
