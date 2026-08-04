# Langfuse deep dive - three control loops

Three control loops side by side, so you can show live why more autonomy needs more
telemetry, and where Langfuse fits. Python + FastAPI + the Langfuse Python SDK (v4),
one process serving both the UI and the API, no build step for the frontend.

| # | Loop | Autonomy | What you see in the app | What Langfuse captures |
|---|------|----------|-------------------------|------------------------|
| 01 | **Workflow** | deterministic | A fixed pipeline: classify, route, draft, guardrail | key steps and outputs |
| 02 | **Agent** | model-directed | The model picks which tools to call, then answers | decisions, tools, context, scores |
| 03 | **Coding agent** | side-effecting | Proposes a diff, waits for approval, then writes a file | commands, diffs, approvals, outcomes |

Each run ends with a deep link to the exact trace in Langfuse, plus thumbs up/down
that post a `user_feedback` score back to that trace. Running the talk:
[DEMO_SCRIPT.md](DEMO_SCRIPT.md).

## This directory is canonical

It was snapshotted on 2026-08-04 from a working copy at `~/casa/projects/lf_quick_demo`
that has no git remote and no tracked files. That copy still exists and is still being
edited by hand, but it is reference material now, not a source of truth. Changes belong
here. Nothing automated syncs the two, so if you find yourself editing the old path,
you are editing a dead end.

## Bring your own key, and why this app is safe to expose publicly

There are no server-side credentials. None in the image, none in App Runner
configuration, none in Secrets Manager. A visitor pastes their own OpenAI key and their
own Langfuse project keys into the Setup tab; the browser holds them in `sessionStorage`
and sends them on each request as headers:

```
X-OpenAI-Key
X-Langfuse-Public-Key
X-Langfuse-Secret-Key
X-Langfuse-Host        (allowlisted server-side)
X-Openai-Model
```

The server uses them for that one request and never persists them: not to disk, not to
logs, not to environment, not to any cache a later request could read back. A loop
endpoint called without them returns 400 telling you to fill in the Setup tab. It never
falls back to environment credentials, because there is nothing to fall back to.

That is the entire reason the deployed app can sit on the public internet with no login.
Visitors spend their own OpenAI quota and write traces into their own Langfuse project,
so the usual worries about an open LLM endpoint (someone burning our credits, someone
extracting our keys) have no target. Every future change to this demo has to keep that
property true. If you ever find yourself adding a fallback key to make a demo smoother,
you have also just added a login requirement, a spend risk, and a rotation runbook.

What this does **not** cover is our own infrastructure bill. Nobody can spend our model
quota, but anyone can spend our App Runner compute and egress: a review saturated an
instance in about two minutes with an unauthenticated flood, and the app has no rate limit
of its own. The deploy caps App Runner autoscaling at two instances so a flood cannot
scale the bill, and the runbook recommends a WAF rate rule on the loop POSTs. "Bring your
own key" is an answer about credentials, not about capacity; see
[the abuse and cost section](../../docs/lf-control-loops-deploy.md) of the runbook.

Two consequences worth knowing before you demo:

- Traces land in the visitor's Langfuse project, not ours, so a shared screen walkthrough
  needs the presenter's own keys pasted in like anyone else's.
- Credentials are per browser session. A refresh keeps them (sessionStorage), a new tab
  or a closed browser does not.

Header names, never query strings, because query strings land in access logs verbatim.

## Run it locally

```bash
cd demos/lf_control_loops
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m uvicorn app.main:app --port 8000 --reload
# then open http://127.0.0.1:8000 and fill in the Setup tab
```

Port 8000 is what the container and App Runner use, so staying on it locally means one
number to remember. The upstream working copy's `run.sh` used 8001; there is no reason
to keep that here.

Or run exactly what production runs:

```bash
docker build -t lf-control-loops:dev demos/lf_control_loops
docker run --rm -p 8000:8000 lf-control-loops:dev
curl -i http://127.0.0.1:8000/api/healthz    # expect 200
```

No `.env` file is needed or wanted. `.dockerignore` excludes `.env` and friends on
purpose and explains why in a comment; do not delete those lines. The upstream working
copy keeps live OpenAI and Langfuse keys in a `.env` next to the source, and
`app/config.py` also searches the parent directory, so an image built from a careless
context would ship real credentials to ECR.

Every pattern in `.dockerignore` is `**/`-prefixed, and that is not cosmetic. Docker
anchors patterns at the context root, so a bare `.env` line excludes `./.env` and nothing
below it; a nested `app/.env` shipped. The same bug meant that running the local commands
above created `app/__pycache__` which the next `docker build` baked into the image, so
anyone who ran before building shipped their own bytecode. If you add a line, ask whether
the thing can appear in a subdirectory, and prefix it. CI seeds nested canaries and
asserts they are absent from the built image, so a regression fails the build.

## API surface

| Method | Path | Notes |
|---|---|---|
| GET | `/api/healthz` | 200, no credentials, no third-party calls. The App Runner health check. |
| GET | `/api/status` | `byok`, allowed Langfuse hosts, default model, model choices. Reports nothing about server-side keys, because there are none. |
| POST | `/api/workflow/run` | `{ ticket }` |
| POST | `/api/agent/run` | `{ question }` |
| POST | `/api/coding/propose` | `{ task }` returns a proposal, a signature and a diff |
| POST | `/api/coding/apply` | `{ proposal, signature, approved }` |
| POST | `/api/feedback` | `{ trace_id, value, comment }` |

`/api/healthz` is separate from `/` on purpose. `/` serves the SPA, and a health check
pointed at any loop endpoint would call OpenAI or Langfuse, so a third-party outage
would take the service down, and with no server-side keys it would fail permanently
anyway.

## Layout

```
app/
  main.py         FastAPI app: routes + static mount
  config.py       per-request credential handling, host allowlist
  lf.py           thin Langfuse v4 helper
  llm.py          Langfuse-wrapped OpenAI client
  tools.py        canned tools for the agent loop
  workflow.py     loop 1, deterministic pipeline
  agent.py        loop 2, tool-calling agent
  coding_agent.py loop 3, propose / approve / apply inside ./sandbox
static/           index.html, app.js, styles.css
sandbox/          the file loop 3 edits; auto-seeded at startup, never in the image
Dockerfile        python:3.12-slim, non-root, uvicorn on 8000
apprunner-service.json  create-service skeleton for the deploy
```

## Deploy

Runs on AWS App Runner from an ECR image, fronted by `langfuse.demohouse.cloud`, with
`dev-langfuse.demohouse.cloud` on the same service for pre-demo checks (the `dev-` prefix
matches the existing `dev-labs` pattern in this account). Ordered runbook with the exact
commands, the IAM role App Runner needs, the autoscaling ceiling, the custom domain
decision and the rollback: [../../docs/lf-control-loops-deploy.md](../../docs/lf-control-loops-deploy.md).

CI is `.github/workflows/lf-control-loops.yml`. It builds and pushes to the ECR
repository `posthouse-demo-langfuse-loops` on pushes touching this directory, and
deploys only from the `lf-control-loops-v1` branch. Auto deployments are off on the
service, so an image push never ships itself.
