"""Loop 3 - CODING AGENT (side-effecting).

Files, shell, sandbox: real side effects, with an approval gate and rollback.
Scenario: apply a small code change to a file in a per-request sandbox.

The flow is deliberately split into two steps so the *human-in-the-loop approval
gate* is real, not simulated:

    propose  ->  (show diff, wait)  ->  approve / reject  ->  apply or roll back

Both requests write to the SAME Langfuse trace (via a deterministic trace id), so
the trace shows the full story: read-file, propose-edit, the approval decision,
and the outcome.

What to trace (per the slide): commands, diffs, approvals, outcomes.

WHY THIS IS NOW STATELESS
-------------------------
This used to keep proposals in a module-level `_JOBS` dict and edit one shared file
under ./sandbox. On App Runner both assumptions are wrong: the filesystem is
ephemeral, and there can be more than one instance behind the service URL, so a
propose served by instance A and an apply routed to instance B would look up a
job_id that instance B has never heard of. The demo would fail live, intermittently,
in the way that is hardest to debug from the stage.

So the proposal now travels to the client and back, and is authenticated with an
HMAC over its canonical form. The client cannot invent a diff or edit the one it
was given, because it cannot produce the signature.

That trade has one honest edge, and _verify() below names it out loud rather than
hiding it: without a shared PROPOSAL_SIGNING_SECRET, a propose served by instance A
still cannot be applied by instance B. Each signature carries the public key id of the
secret that made it, so the "wrong instance or a restart" case gets its own message and
its own server-side warning, while an actual forgery keeps the uninformative one.

Two defects the audit found also disappear as a side effect, which is worth saying
out loud because they were both live-demo-ruining rather than theoretical:

  - `/api/coding/reset` cleared the global `_JOBS` for EVERY concurrent user. One
    unauthenticated request griefed the whole room mid-demo. There is no global
    state left to clear, so reset() is now a pure function that returns the seed
    file and touches nothing anybody else can see.
  - `apply()` never checked that a job belonged to the caller, so any visitor could
    apply any other visitor's pending proposal by guessing a job_id. There are no
    job ids now; a caller can only apply a proposal they were handed, because the
    signature is over the proposal itself.

WHAT "APPLY" MEANS WITHOUT A DURABLE SANDBOX
--------------------------------------------
It still performs a real filesystem write, into a temporary directory created for
that one request and deleted before the response is sent. The seed file is written,
then the proposal is written over it, then the result is read back off disk and
returned: the trace records a genuine path and byte count, and the UI can show
before and after. What it does not do is pretend there is a shared mutable file that
persists, because on this deployment there is not one and a demo that silently
depends on sticky routing is a demo that breaks in front of an audience.

The path is built from a module-level constant filename inside a directory from
tempfile.mkdtemp(). No client-supplied value is ever joined into a path, not even
the `file` field of a signed proposal, which is verified against the constant
before use.
"""

import difflib
import json
import logging
import shutil
import tempfile
import uuid
from pathlib import Path

from app import creds, lf
from app.creds import CredentialError
from app.llm import MODEL, timed_create, usage_dict

logger = logging.getLogger("lf_demo.coding_agent")

DEFAULT_FILE = "pricing.py"

# The seed file: a small function with obvious missing validation. This is the
# canonical "before" state, held in code rather than on disk so every request
# starts from the same place on any instance.
_ORIGINAL = '''"""Pricing helpers (sandbox file the coding agent edits)."""


def apply_discount(price, percent):
    return price - (price * percent / 100)


def format_price(amount):
    return f"${amount:.2f}"
'''

DEFAULT_TASK = (
    "Add input validation to apply_discount: raise ValueError if price is negative "
    "or percent is not between 0 and 100. Keep the rest of the file unchanged."
)

# Version tag inside the signed payload so the scheme can change without an old
# signature being accepted under new rules.
_PROPOSAL_VERSION = "v1"

# A proposal is one small Python file plus a task string. 256 KB is far more than
# that and far less than something worth hashing on an unauthenticated endpoint.
_MAX_PROPOSAL_BYTES = 256 * 1024

# The per-field cap on the two file-shaped fields. Public because main.py's request
# model enforces it in Pydantic, before this module is reached and before _canonical()
# re-serialises anything: the two limits have to be the same number or a proposal this
# module produced could be refused when it came back.
MAX_FILE_CHARS = 64 * 1024

_PROPOSAL_FIELDS = ("version", "file", "task", "original", "proposed", "explanation", "trace_id")


def seed_content() -> str:
    """The 'before' content of the sandbox file. Same on every instance, always."""
    return _ORIGINAL


def reset() -> dict:
    """The 'roll back everything' button.

    Pure: returns the seed content and mutates nothing. Previously this wiped a
    shared file and every pending job in the process, for everyone.
    """
    return {"file": DEFAULT_FILE, "content": _ORIGINAL}


def _unified_diff(before: str, after: str, file: str) -> str:
    diff = difflib.unified_diff(
        before.splitlines(keepends=True),
        after.splitlines(keepends=True),
        fromfile=f"a/{file}",
        tofile=f"b/{file}",
    )
    return "".join(diff) or "(no changes proposed)"


def _canonical(proposal: dict) -> bytes:
    """Byte-exact serialisation of a proposal, for signing and verifying.

    sort_keys plus fixed separators so the bytes do not depend on dict ordering or
    on how a JSON round trip through the browser happened to space things out. The
    signature covers the whole dict, so an added, removed or altered field - not
    just the diff - invalidates it.
    """
    return json.dumps(
        proposal, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


_MALFORMED = "That proposal is not in the expected form. Propose the change again."


def _verify(proposal, signature) -> dict:
    """Return a proposal we are willing to act on, or raise CredentialError.

    Order matters, and it is cheapest-first all the way down. Nothing here serialises
    the payload until its shape, its field names and its total length have been checked
    against constants, because json.dumps over a caller-supplied object is the one
    expensive step in this function and it used to run first.
    """
    if not isinstance(proposal, dict):
        raise CredentialError(_MALFORMED)

    if sorted(proposal) != sorted(_PROPOSAL_FIELDS):
        raise CredentialError(_MALFORMED)

    # Every field is a string, and the strings together are small. main.py's request
    # model already guarantees both, per field; repeated here because this function is
    # the gate and must not depend on the caller having a model in front of it.
    total = 0
    for name in _PROPOSAL_FIELDS:
        value = proposal[name]
        if not isinstance(value, str):
            raise CredentialError(_MALFORMED)
        total += len(value)
    if total > _MAX_PROPOSAL_BYTES:
        raise CredentialError("That proposal is too large. Propose the change again.")

    payload = _canonical(proposal)
    if len(payload) > _MAX_PROPOSAL_BYTES:
        # Reachable where the length check above is not: multi-byte characters, and the
        # JSON escaping of them.
        raise CredentialError("That proposal is too large. Propose the change again.")

    verdict = creds.verify_signature(payload, signature)
    if verdict == creds.SIGNATURE_FOREIGN_KEY_ID:
        # A signature this process could not have produced, because it names a
        # different signing key. That is the multi-instance and post-restart case, and
        # saying so is the point: an operator reading the logs after a live demo went
        # wrong needs to be able to tell it from an attack, and the visitor needs to
        # know that proposing again will work. It gives a forger nothing: the key id is
        # a public label, and the one in the request is the value they chose themselves.
        logger.warning(
            "rejected a proposal signed with key id %r; this instance signs with %r. "
            "If the service runs more than one instance, set the same "
            "PROPOSAL_SIGNING_SECRET on all of them.",
            str(signature).split(".", 1)[0][:16],
            creds.signing_key_id(),
        )
        raise CredentialError(
            "This proposal was signed by a different copy of this demo, so it was not "
            "applied. That happens after a restart, or when the service is running more "
            "than one instance without a shared signing secret. Propose the change again."
        )
    if verdict != creds.SIGNATURE_OK:
        # Tampered or forged. One message, no detail, nothing that says which part of
        # the guess was closer.
        raise CredentialError(
            "This proposal could not be verified, so it was not applied. "
            "Propose the change again and approve the new diff."
        )

    # Belt and braces after a valid signature: the only filename this endpoint will
    # ever write is the server-side constant. If a future bug let a signature be
    # produced over an attacker-chosen `file`, it still would not reach a path join.
    if proposal.get("file") != DEFAULT_FILE:
        raise CredentialError("That proposal refers to a file this demo does not manage.")
    if proposal.get("version") != _PROPOSAL_VERSION:
        raise CredentialError("That proposal was made by an older version of this demo. Propose it again.")

    return proposal


def propose(task: str, session_id: str) -> dict:
    task = (task or "").strip() or DEFAULT_TASK
    file = DEFAULT_FILE
    # Seeds the deterministic trace id only. Not an identifier anything is looked
    # up by, because there is no longer anything to look up.
    seed = uuid.uuid4().hex
    trace_id = lf.new_trace_id(seed=seed)

    steps = []
    with lf.trace(
        name="coding-agent: apply-change",
        as_type="agent",
        session_id=session_id,
        tags=["byok-demo", "coding-agent"],
        input={"task": task, "file": file},
        trace_context={"trace_id": trace_id},
    ):
        # Step: read the file (a real read side effect).
        with lf.step(name="read-file", as_type="tool", input={"path": f"sandbox/{file}"}) as span:
            original = seed_content()
            span.update(output={"chars": len(original)})
        steps.append(
            {
                "name": "read-file",
                "type": "tool",
                "summary": f"read sandbox/{file} ({len(original)} chars)",
                "output": {"path": f"sandbox/{file}"},
            }
        )

        # Step: propose the edit (LLM, returns the full new file).
        resp, ms = timed_create(
            model=MODEL,
            name="propose-edit",
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": "You are a coding agent. You are given a file and a task. "
                    "Respond with JSON only: "
                    '{"explanation": "one sentence on what you changed", '
                    '"new_content": "the FULL updated file content"}. '
                    "Return valid Python. Do not add unrelated changes.",
                },
                {
                    "role": "user",
                    "content": f"Task: {task}\n\nFile sandbox/{file}:\n```python\n{original}\n```",
                },
            ],
        )
        try:
            parsed = json.loads(resp.choices[0].message.content)
            proposed = parsed.get("new_content", original)
            explanation = parsed.get("explanation", "")
        except (json.JSONDecodeError, TypeError):
            proposed, explanation = original, "(could not parse proposal)"

        # Never let an empty/blank proposal wipe the file on approve.
        if not (proposed or "").strip():
            proposed = original
            explanation = (explanation + " (empty proposal; kept original)").strip()

        # The model's output is round-tripped through the client, so bound it here
        # rather than discovering on apply that it will not fit under the cap. Same
        # constant the request model on apply enforces, for exactly that reason.
        if len(proposed) > MAX_FILE_CHARS:
            proposed = original
            explanation = "(proposal was too large to apply; kept original)"

        diff = _unified_diff(original, proposed, file)
        steps.append(
            {
                "name": "propose-edit",
                "type": "generation",
                "summary": explanation,
                "output": {"explanation": explanation},
                "model": MODEL,
                "tokens": usage_dict(resp),
                "latency_ms": ms,
            }
        )

        lf.set_trace_io(input={"task": task, "file": file}, output={"diff": diff})

    lf.flush()

    proposal = {
        "version": _PROPOSAL_VERSION,
        "file": file,
        "task": task,
        "original": original,
        "proposed": proposed,
        "explanation": explanation,
        "trace_id": trace_id,
    }
    return {
        "loop": "coding-agent",
        "file": file,
        "task": task,
        "explanation": explanation,
        "diff": diff,
        "steps": steps,
        "awaiting_approval": True,
        "trace_id": trace_id,
        "trace_url": lf.trace_url(trace_id),
        # The client holds the state. It cannot alter any of it without breaking the
        # signature, and it must send both fields back to /api/coding/apply.
        "proposal": proposal,
        "signature": creds.sign(_canonical(proposal)),
    }


def apply(proposal, signature, approved: bool, session_id: str) -> dict:
    job = _verify(proposal, signature)

    file = DEFAULT_FILE  # never `job["file"]`, even though it was just checked
    trace_id = job["trace_id"]
    original = job["original"]
    proposed = job["proposed"]
    diff = _unified_diff(original, proposed, file)
    decision = "approved" if approved else "rejected"
    steps = []
    current_content = original

    # Continue the SAME trace (add a second root observation to it).
    with lf.trace(
        name="approval-and-apply",
        as_type="span",
        session_id=session_id,
        tags=["byok-demo", "coding-agent"],
        trace_context={"trace_id": trace_id},
        trace_name="coding-agent: apply-change",
    ):
        with lf.step(name="approval-gate", as_type="span", input={"diff": diff}) as gate:
            if approved:
                # The real side effect: write the file. In a sandbox that exists for
                # the duration of this request only, so the write is genuine but does
                # not depend on this instance still being here next time.
                with lf.step(name="apply-edit", as_type="tool", input={"path": f"sandbox/{file}"}) as span:
                    current_content = _write_in_temp_sandbox(original, proposed)
                    span.update(output={"written": True, "chars": len(current_content)})
                outcome = f"Applied. Wrote {len(current_content)} chars to sandbox/{file}."
                steps.append(
                    {
                        "name": "apply-edit",
                        "type": "tool",
                        "summary": outcome,
                        "output": {"written": True},
                    }
                )
            else:
                outcome = "Rejected at the gate. No file was written (rolled back)."
            gate.update(output={"decision": decision, "outcome": outcome})

        steps.insert(
            0,
            {
                "name": "approval-gate",
                "type": "approval",
                "summary": f"decision={decision}: {outcome}",
                "output": {"decision": decision},
            },
        )

        lf.score_trace(
            name="approval_decision",
            value=decision,
            data_type="CATEGORICAL",
            comment=outcome,
        )

    lf.flush()

    return {
        "loop": "coding-agent",
        "applied": approved,
        "decision": decision,
        "outcome": outcome,
        "file": file,
        "current_content": current_content,
        "steps": steps,
        "trace_id": trace_id,
        "trace_url": lf.trace_url(trace_id),
        "scores": [{"name": "approval_decision", "value": decision}],
    }


def _write_in_temp_sandbox(original: str, proposed: str) -> str:
    """Really write the edit, in a directory that exists only for this request.

    Reads the file back afterwards so the returned "after" content is what landed on
    disk rather than what we intended to put there, which is the honest version of a
    demo about observing side effects. The directory is removed before returning, so
    nothing accumulates on an instance that may be recycled at any moment.
    """
    sandbox = Path(tempfile.mkdtemp(prefix="lf-coding-agent-"))
    try:
        target = sandbox / DEFAULT_FILE  # constant filename, no client input
        target.write_text(original, encoding="utf-8")
        target.write_text(proposed, encoding="utf-8")
        return target.read_text(encoding="utf-8")
    finally:
        shutil.rmtree(sandbox, ignore_errors=True)
