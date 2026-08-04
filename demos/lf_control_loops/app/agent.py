"""Loop 2 - AGENT (model-directed).

The model chooses the next action: it decides which tools to call, in what order,
and when it has enough to answer. Harder to evaluate than a workflow.

What to trace (per the slide): decisions, tools, context, scores.
The trace is one `agent` observation containing an alternating sequence of
`generation` (model turns / decisions) and `tool` (tool executions) children,
plus a final judge score on the trace.
"""

import json

from app import lf
from app.llm import MODEL, timed_create, usage_dict
from app.tools import TOOL_IMPL, TOOL_SCHEMAS

_SYSTEM = (
    "You are a customer support agent for an online store. "
    "Use the available tools to look up real information before answering. "
    "Prefer get_order_status for order questions, search_knowledge_base for policies/how-tos, "
    "and get_refund_policy for refund questions. When you have enough information, give a short, "
    "friendly final answer. Do not make up order details or policies."
)

MAX_ITERS = 6


def run_agent(question: str, session_id: str) -> dict:
    steps = []
    messages = [
        {"role": "system", "content": _SYSTEM},
        {"role": "user", "content": question},
    ]
    final_answer = ""

    with lf.trace(
        name="agent: customer-support",
        as_type="agent",
        session_id=session_id,
        tags=["meetup-demo", "agent"],
        input={"question": question},
    ):
        for turn in range(1, MAX_ITERS + 1):
            resp, ms = timed_create(
                model=MODEL,
                name=f"agent-turn-{turn}",
                temperature=0,
                tools=TOOL_SCHEMAS,
                parallel_tool_calls=False,  # one action per turn -> a clean, sequential trace
                messages=messages,
            )
            msg = resp.choices[0].message
            messages.append(msg)  # append the object (carries tool_calls)

            if not msg.tool_calls:
                final_answer = msg.content or ""
                steps.append(
                    {
                        "name": f"decision (turn {turn})",
                        "type": "generation",
                        "summary": "Model decided it has enough context -> final answer",
                        "output": final_answer,
                        "model": MODEL,
                        "tokens": usage_dict(resp),
                        "latency_ms": ms,
                    }
                )
                break

            # Record the decision to call tools.
            called = [tc.function.name for tc in msg.tool_calls]
            steps.append(
                {
                    "name": f"decision (turn {turn})",
                    "type": "generation",
                    "summary": "Model chose to call: " + ", ".join(called),
                    "output": {"tool_calls": called},
                    "model": MODEL,
                    "tokens": usage_dict(resp),
                    "latency_ms": ms,
                }
            )

            # Execute every tool call and feed results back.
            for tc in msg.tool_calls:
                name = tc.function.name
                try:
                    args = json.loads(tc.function.arguments or "{}")
                except json.JSONDecodeError:
                    args = {}
                impl = TOOL_IMPL.get(name)
                with lf.step(name=name, as_type="tool", input=args) as span:
                    try:
                        result = impl(**args) if impl else {"error": f"unknown tool: {name}"}
                    except Exception as exc:
                        # Feed the error back as the tool result so the model can
                        # self-correct on the next turn instead of crashing the run.
                        result = {"error": f"{type(exc).__name__}: {exc}"}
                    span.update(output=result)
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": json.dumps(result),
                    }
                )
                steps.append(
                    {
                        "name": f"tool: {name}",
                        "type": "tool",
                        "summary": f"{args} -> {json.dumps(result)[:160]}",
                        "input": args,
                        "output": result,
                    }
                )

        if not final_answer:
            final_answer = "(agent stopped after reaching the max number of steps)"

        # --- LLM-as-a-judge: score the trace for helpfulness ---
        jresp, jms = timed_create(
            model=MODEL,
            name="judge-helpfulness",
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": "You are an evaluator. Given a customer question and an agent's answer, "
                    'respond with JSON only: {"score": a number from 0 to 1, "reason": "..."}. '
                    "Score how helpful, correct and grounded the answer is.",
                },
                {"role": "user", "content": f"Question: {question}\n\nAnswer: {final_answer}"},
            ],
        )
        try:
            judged = json.loads(jresp.choices[0].message.content)
            score = float(judged.get("score", 0))
        except (json.JSONDecodeError, TypeError, ValueError):
            judged, score = {"score": 0, "reason": "unparseable"}, 0.0
        steps.append(
            {
                "name": "judge-helpfulness",
                "type": "score",
                "summary": f"helpfulness={score:.2f}: {judged.get('reason', '')}",
                "output": judged,
                "model": MODEL,
                "tokens": usage_dict(jresp),
                "latency_ms": jms,
            }
        )
        lf.score_trace(
            name="helpfulness",
            value=score,
            data_type="NUMERIC",
            comment=judged.get("reason", ""),
        )

        lf.set_trace_io(input={"question": question}, output={"answer": final_answer})
        trace_id = lf.current_trace_id()

    lf.flush()
    return {
        "loop": "agent",
        "steps": steps,
        "output": final_answer,
        "trace_id": trace_id,
        "trace_url": lf.trace_url(trace_id),
        "scores": [{"name": "helpfulness", "value": round(score, 2)}],
    }
