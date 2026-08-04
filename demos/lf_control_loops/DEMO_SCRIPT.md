# Demo script - three control loops

A suggested flow for the live demo. Total time: about 8-10 minutes. The app screen
and the Langfuse UI are the two things on screen; alt-tab between them.

## Before you start

- `./run.sh` is up at http://127.0.0.1:8001, and the top-right pills read
  "model: gpt-4o-mini", "Langfuse: connected", and "session: demo-xxxxxx".
- The "connected" pill only means the keys are present. Click one trace link once
  before the talk to confirm the keys and `LANGFUSE_HOST` region actually resolve.
- Have Langfuse Cloud open in another tab, on your project's **Tracing** view.
- Note the `session:` id shown top-right - you will filter on it in Langfuse at the
  end to show all three runs grouped as one session.

## The arc (one sentence)

"As we move from a workflow to an agent to a coding agent, the system gains
autonomy - and every rung up adds new things we can only understand if we trace
them. Let me show the same idea three times, and each time go look at what Langfuse
captured."

---

## 01 - Workflow (deterministic)

**Say:** "A workflow is predefined steps. Same path every time, easy to replay and
test. Here it triages a support ticket."

**Do:** Workflow tab -> keep the sample ticket (or pick an example chip) -> **Run
workflow**.

**Point at the screen:** four steps appear in order every run - `classify`,
`route` (a deterministic, non-model step), `draft-reply`, `guardrail-check` - then
the drafted reply and a `guardrail_pass` score.

**Go to Langfuse:** click **Open this trace in Langfuse**. Show:
- The trace tree: one `chain` with four child observations, in a fixed order.
- Open the `classify` generation: the prompt, the JSON output, token counts, cost,
  latency - captured with no extra code because the call went through the Langfuse
  OpenAI wrapper.
- The `guardrail_pass` score on the trace.

**Land it:** "For a workflow, tracing key steps and outputs is usually enough - the
control flow is ours."

---

## 02 - Agent (model-directed)

**Say:** "Now the model chooses the next action. It decides which tools to call and
when it is done. Same support desk, but we are not writing the steps anymore."

**Do:** Agent tab -> keep the sample question (it references order A1001 and
returns) -> **Run agent**.

**Point at the screen:** the decision/tool sequence - the model decided to call
`get_order_status`, then `search_knowledge_base`, then answered (one action per
turn). That order is the model's choice for this input. It runs at temperature 0,
so it is deterministic - to show the choice actually change, edit the question
(for example, ask only "what is your refund policy?") and re-run; the tool the
model reaches for changes with the question.

**Go to Langfuse:** open the trace. Show:
- An `agent` trace with alternating `generation` (decisions) and `tool`
  observations - you can see exactly what the model decided and what each tool
  returned (the retrieved context).
- The `helpfulness` score - an LLM-as-a-judge ran automatically and scored the
  answer. This is the "was the behavior actually good?" question that traditional
  telemetry (did it run, was it fast, did it error) cannot answer.
- Click a thumbs up/down in the app, refresh the trace, and show the
  `user_feedback` score arriving on the same trace.

**Land it:** "More autonomy, more surface area. Now we need decisions, tools,
context, and quality scores - not just latency and errors."

---

## 03 - Coding agent (side-effecting)

**Say:** "Top of the ladder: real side effects. It touches files. So we add an
approval gate and a rollback, and we want the diff, the decision, and the outcome
all on the record."

**Do:** Coding tab -> keep the task -> **Propose change**.

**Point at the screen:**
- The `read-file` and `propose-edit` steps, then a **diff** of `sandbox/pricing.py`.
- Stop at the **approval gate**: "Nothing has been written yet." This is the
  human-in-the-loop moment.
- Click **Approve & apply**. The file is written (watch the current-file panel on
  the left update), and the outcome plus an `approval_decision` score appear.
- Optionally re-run and click **Reject** to show the rollback (no write), then
  "reset sandbox".

**Go to Langfuse:** open the trace. Show:
- One trace that spans two requests: `read-file`, `propose-edit`, then
  `approval-gate` -> `apply-edit`, plus the `approval_decision` score. Commands,
  diffs, approvals, outcomes - all in one place.

**Land it:** "This is why autonomy raises the bar for observability. The more the
system can do on its own, the more we need step-level telemetry, quality signals,
and governance."

---

## Close - the loop

Switch to Langfuse **Sessions** and filter by the `session:` id from the app (or the
`meetup-demo` tag). Show all three runs grouped together.

**Say:** "Every one of these lives on the same loop - launch, observe, improve,
repeat. Trace production behavior, monitor cost and failures, label and score,
build datasets from the interesting cases, run experiments, ship the winner. That
loop is the product. Langfuse is how you run it."
