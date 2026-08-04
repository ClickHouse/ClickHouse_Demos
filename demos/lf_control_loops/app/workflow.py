"""Loop 1 - WORKFLOW (deterministic).

Predefined steps, limited branching, easy to replay and test.
Scenario: triage an incoming customer support ticket through a fixed pipeline:

    classify -> route (deterministic) -> draft reply -> guardrail check

What to trace (per the slide): key steps + outputs.
The trace is one `chain` with four named child observations, in the same order
every single run.
"""

import json

from app import lf
from app.llm import MODEL, timed_create, usage_dict

# Deterministic routing table: category -> (team, SLA). No model involved here.
_ROUTING = {
    "billing": {"team": "Billing", "sla_hours": 8},
    "shipping": {"team": "Logistics", "sla_hours": 24},
    "technical": {"team": "Support Engineering", "sla_hours": 12},
    "returns": {"team": "Returns", "sla_hours": 24},
    "other": {"team": "General Support", "sla_hours": 24},
}

# Canned policy snippets injected into the draft step (a stand-in for retrieval).
_POLICY = {
    "billing": "Charges appear within 24h. Disputed charges are reviewed within 2 business days.",
    "shipping": "Standard shipping is 3-5 business days; tracking is emailed on dispatch.",
    "technical": "Ask for app version and device, then share the relevant troubleshooting steps.",
    "returns": "Returns accepted within 30 days with a prepaid label started from the order page.",
    "other": "Be helpful and concise; escalate if you are unsure.",
}


def run_workflow(ticket: str, session_id: str) -> dict:
    steps = []

    with lf.trace(
        name="workflow: triage-ticket",
        as_type="chain",
        session_id=session_id,
        tags=["meetup-demo", "workflow"],
        input={"ticket": ticket},
    ):
        # --- Step 1: classify (LLM, structured) ---
        resp, ms = timed_create(
            model=MODEL,
            name="classify",
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": "You triage support tickets. Respond with JSON only: "
                    '{"category": one of ["billing","shipping","technical","returns","other"], '
                    '"priority": one of ["low","medium","high"], '
                    '"sentiment": one of ["angry","neutral","happy"]}.',
                },
                {"role": "user", "content": ticket},
            ],
        )
        try:
            classification = json.loads(resp.choices[0].message.content)
        except (json.JSONDecodeError, TypeError):
            classification = {"category": "other", "priority": "medium", "sentiment": "neutral"}
        category = classification.get("category", "other")
        if category not in _ROUTING:
            category = "other"
        steps.append(
            {
                "name": "classify",
                "type": "generation",
                "summary": f"category={category}, priority={classification.get('priority')}, "
                f"sentiment={classification.get('sentiment')}",
                "output": classification,
                "model": MODEL,
                "tokens": usage_dict(resp),
                "latency_ms": ms,
            }
        )

        # --- Step 2: route (deterministic, no model) ---
        with lf.step(name="route", as_type="span", input={"category": category}) as span:
            routing = _ROUTING[category]
            span.update(output=routing)
        steps.append(
            {
                "name": "route",
                "type": "deterministic",
                "summary": f"team={routing['team']}, SLA={routing['sla_hours']}h",
                "output": routing,
            }
        )

        # --- Step 3: draft reply (LLM) ---
        resp, ms = timed_create(
            model=MODEL,
            name="draft-reply",
            temperature=0.4,
            messages=[
                {
                    "role": "system",
                    "content": "You are a friendly support agent. Write a short, concrete reply "
                    "(3-5 sentences). Use only the policy provided; do not invent guarantees.\n"
                    f"Relevant policy: {_POLICY[category]}",
                },
                {"role": "user", "content": ticket},
            ],
        )
        draft = resp.choices[0].message.content
        steps.append(
            {
                "name": "draft-reply",
                "type": "generation",
                "summary": draft,
                "output": draft,
                "model": MODEL,
                "tokens": usage_dict(resp),
                "latency_ms": ms,
            }
        )

        # --- Step 4: guardrail check (LLM -> score) ---
        resp, ms = timed_create(
            model=MODEL,
            name="guardrail-check",
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": "You are a compliance guardrail. Given a draft support reply, "
                    "respond with JSON only: "
                    '{"pass": true/false, "reason": "..."}. '
                    "Fail it if it promises refunds, discounts, or timelines not in the policy, "
                    "or if the tone is rude.\n"
                    f"Policy: {_POLICY[category]}",
                },
                {"role": "user", "content": draft},
            ],
        )
        try:
            guard = json.loads(resp.choices[0].message.content)
        except (json.JSONDecodeError, TypeError):
            guard = {"pass": True, "reason": "unparseable; defaulted to pass"}
        passed = bool(guard.get("pass", True))
        steps.append(
            {
                "name": "guardrail-check",
                "type": "generation",
                "summary": f"pass={passed}: {guard.get('reason', '')}",
                "output": guard,
                "model": MODEL,
                "tokens": usage_dict(resp),
                "latency_ms": ms,
            }
        )

        # Attach a score to the whole trace (the eval/quality signal).
        lf.score_trace(
            name="guardrail_pass",
            value=1 if passed else 0,
            data_type="BOOLEAN",
            comment=guard.get("reason", ""),
        )

        lf.set_trace_io(input={"ticket": ticket}, output={"draft": draft, "routing": routing})
        trace_id = lf.current_trace_id()

    lf.flush()
    return {
        "loop": "workflow",
        "steps": steps,
        "output": draft,
        "trace_id": trace_id,
        "trace_url": lf.trace_url(trace_id),
        "scores": [{"name": "guardrail_pass", "value": passed}],
    }
