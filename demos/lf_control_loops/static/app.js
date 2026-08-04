// Miniature Langfuse demo - three control loops, on the visitor's own keys.
//
// The site is public and unauthenticated, which is only defensible because it
// holds no credentials of ours: the visitor pastes their own OpenAI and Langfuse
// keys, we keep them in sessionStorage for this tab, and every request carries
// them as headers. Two rules follow and both are load bearing:
//   1. sessionStorage only. Never localStorage, never a cookie, so closing the
//      tab really does discard the keys.
//   2. Headers only. A key in a query string would be copied into every access
//      log and proxy hop on the way to the server.

// ---- credential state -------------------------------------------------------

const STORE = {
  openai: "lfdemo.openai_key",
  lfPublic: "lfdemo.lf_public_key",
  lfSecret: "lfdemo.lf_secret_key",
  lfHost: "lfdemo.lf_host",
  model: "lfdemo.model",
  modelCustom: "lfdemo.model_custom",
  session: "lfdemo.session",
};

const creds = {
  openai: "",
  lfPublic: "",
  lfSecret: "",
  lfHost: "",
  model: "",
  modelCustom: "",
};

// Session id lives in sessionStorage too so a refresh keeps grouping runs under
// one Langfuse session instead of scattering them across ids.
let SESSION_ID = "";

// From GET /api/status. Defaults keep the Setup form usable if the status call
// fails, which otherwise leaves the visitor with two empty selects.
let ALLOWED_HOSTS = ["https://cloud.langfuse.com", "https://us.cloud.langfuse.com"];
let MODEL_CHOICES = ["gpt-4o-mini"];
let DEFAULT_MODEL = "gpt-4o-mini";
let DEFAULT_HOST = "";

function hasOpenAI() { return creds.openai.trim().length > 0; }

/**
 * All three credentials are required, and this page must say so, because the
 * server refuses anything less (app/creds.py extract()). The reason is not "it is
 * a Langfuse demo": langfuse.openai patches the OpenAI SDK process-wide, so every
 * completion in the process goes through it, and a create() carrying no
 * langfuse_public_key falls back to get_client(), which returns the single
 * Langfuse client in the process when there is exactly one. On a multi-tenant BYOK
 * server that would export visitor A's full prompts into visitor B's Langfuse
 * project. Requiring all three means every generation is pinned to a public key
 * and the ambiguous fallback is unreachable. Optional Langfuse is not an option,
 * so there is no untraced-run state in this UI to describe.
 *
 * The Langfuse pair is validated as a unit: half a pair traces nothing and the
 * server rejects it, so the browser says so instead of spending a round trip.
 */
function missingCreds() {
  const missing = [];
  if (!hasOpenAI()) missing.push("an OpenAI API key");
  if (!creds.lfPublic.trim()) missing.push("a Langfuse public key");
  if (!creds.lfSecret.trim()) missing.push("a Langfuse secret key");
  return missing;
}
function hasAllCreds() { return missingCreds().length === 0; }
function halfLangfusePair() {
  const pub = creds.lfPublic.trim().length > 0;
  const sec = creds.lfSecret.trim().length > 0;
  return pub !== sec;
}

/** "a, b and c" - a list a visitor can read, not a JSON array. */
function listWords(items) {
  if (items.length <= 1) return items.join("");
  return items.slice(0, -1).join(", ") + " and " + items[items.length - 1];
}

// ---- model id ---------------------------------------------------------------
//
// The freetext override stays: a model on the server's list may not be enabled on
// the visitor's account, so they need a way past the list. But this value is
// interpolated into an outbound header, so it is bounded here rather than passed
// through. Both bounds mirror app/creds.py _validate_model / _clean exactly, so
// the browser refuses what the server would refuse and explains it in place. The
// leading-alphanumeric rule is ours: it rejects "-foo" and "..", which the server
// would accept but which are not plausible model ids.
const MODEL_MAX_LEN = 64;
const MODEL_RE = /^[A-Za-z0-9][A-Za-z0-9._:-]*$/;

/** The reason this model id is unusable, or "" when it is fine. */
function modelIdError(value) {
  const v = String(value == null ? "" : value).trim();
  if (!v) return "";  // empty just means "use the list"
  if (v.length > MODEL_MAX_LEN) {
    return "That model id is too long (" + v.length + " characters, limit " + MODEL_MAX_LEN +
      "). Check what was pasted: a model id is not an API key.";
  }
  if (!MODEL_RE.test(v)) {
    return "That is not a model id. Use letters, digits and . _ : - only, starting with a " +
      "letter or digit, for example gpt-4.1-mini.";
  }
  return "";
}

/**
 * The model actually sent. A stored value that fails the check is dropped rather
 * than sent, so a hand-edited sessionStorage entry cannot reach the header; the
 * server then applies its own default.
 */
function activeModel() {
  const custom = creds.modelCustom.trim();
  if (custom && !modelIdError(custom)) return custom;
  return (creds.model || DEFAULT_MODEL).trim();
}

const EXAMPLES = {
  workflow: [
    "My order A1002 still has not shipped and I paid for express. When will it arrive?",
    "I was charged twice for order A1003. Please refund the extra charge.",
    "The app keeps crashing when I open my order history. Help!",
  ],
  agent: [
    "Where is my order A1001, and can I still return it if it does not fit?",
    "What is your refund policy, and how long does shipping take?",
    "Has order A1003 been delivered? If so, how do I return it?",
  ],
};

// ---- helpers ----------------------------------------------------------------

function $(sel, root = document) { return root.querySelector(sel); }
function $all(sel, root = document) { return Array.from(root.querySelectorAll(sel)); }

// Escapes quotes as well as the angle brackets, because several call sites
// interpolate the result into an ATTRIBUTE value (href="...", data-trace="...")
// rather than into text. Escaping only & < > is safe for text nodes and unsafe
// in an attribute: one unescaped double quote closes the attribute early and the
// rest of the string is parsed as markup. Nothing reaching here is attacker
// controlled today (the values are server- or Langfuse-generated), so this is
// not a live hole, but this page holds the visitor's OpenAI and Langfuse keys in
// sessionStorage, which makes an XSS here a credential theft rather than a
// defacement. That asymmetry is why the cheap fix is worth taking now instead of
// relying on every future caller choosing a text context.
function esc(s) {
  return String(s == null ? "" : s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function ss(key) {
  try { return sessionStorage.getItem(key) || ""; } catch (e) { return ""; }
}

function ssSet(key, value) {
  try {
    if (value) sessionStorage.setItem(key, value);
    else sessionStorage.removeItem(key);
  } catch (e) { /* private mode: the tab still works, just not across a refresh */ }
}

function loadCreds() {
  creds.openai = ss(STORE.openai);
  creds.lfPublic = ss(STORE.lfPublic);
  creds.lfSecret = ss(STORE.lfSecret);
  creds.lfHost = ss(STORE.lfHost);
  creds.model = ss(STORE.model);
  creds.modelCustom = ss(STORE.modelCustom);
  SESSION_ID = ss(STORE.session);
  if (!SESSION_ID) {
    SESSION_ID = "demo-" + Math.random().toString(36).slice(2, 8);
    ssSet(STORE.session, SESSION_ID);
  }
}

function persistCreds() {
  ssSet(STORE.openai, creds.openai);
  ssSet(STORE.lfPublic, creds.lfPublic);
  ssSet(STORE.lfSecret, creds.lfSecret);
  ssSet(STORE.lfHost, creds.lfHost);
  ssSet(STORE.model, creds.model);
  ssSet(STORE.modelCustom, creds.modelCustom);
}

// ---- transport --------------------------------------------------------------

/** Credentials travel as headers on every call, never in the path or query. */
function credHeaders() {
  const h = {};
  if (creds.openai.trim()) h["X-OpenAI-Key"] = creds.openai.trim();
  if (creds.lfPublic.trim()) h["X-Langfuse-Public-Key"] = creds.lfPublic.trim();
  if (creds.lfSecret.trim()) h["X-Langfuse-Secret-Key"] = creds.lfSecret.trim();
  if (creds.lfHost.trim()) h["X-Langfuse-Host"] = creds.lfHost.trim();
  // Last check before the value becomes a header. Omitting a bad one is safe: the
  // server falls back to its own default model, which is a working run.
  const model = activeModel();
  if (model && !modelIdError(model)) h["X-Openai-Model"] = model;
  return h;
}

class ApiError extends Error {
  constructor(message, correlationId) {
    super(message);
    this.correlationId = correlationId || "";
  }
}

/**
 * The backend answers failures with a real 4xx or 5xx carrying a generic message
 * and a correlation id. Pull those two fields out of whichever envelope shows up
 * and never hand a raw body to the UI: a JSON blob or an HTML error page pasted
 * at a visitor tells them nothing and looks broken.
 */
async function readError(res) {
  let body = null;
  try { body = await res.json(); } catch (e) { body = null; }
  let message = "";
  let cid = "";
  if (body && typeof body === "object") {
    const d = body.detail && typeof body.detail === "object" ? body.detail : body;
    message = [d.message, d.error, typeof body.detail === "string" ? body.detail : ""]
      .find((v) => typeof v === "string" && v.trim()) || "";
    cid = [d.correlation_id, d.correlationId, d.request_id, d.error_id, body.correlation_id]
      .find((v) => typeof v === "string" && v.trim()) || "";
  }
  if (!message) {
    message = res.status >= 500
      ? "The server could not complete that run."
      : "That request was rejected (HTTP " + res.status + ").";
  }
  return new ApiError(message, cid);
}

async function api(path, body) {
  let res;
  try {
    res = await fetch(path, {
      method: "POST",
      headers: Object.assign({ "Content-Type": "application/json" }, credHeaders()),
      body: JSON.stringify(body || {}),
    });
  } catch (e) {
    throw new ApiError("Could not reach the server. Check your connection and try again.");
  }
  if (!res.ok) throw await readError(res);
  try {
    return await res.json();
  } catch (e) {
    throw new ApiError("The server sent a response this page could not read.");
  }
}

// ---- status -----------------------------------------------------------------

function renderPills() {
  const keys = $("#status-keys");
  const missing = missingCreds();
  if (missing.length === 3) {
    keys.textContent = "keys: none in this browser";
    keys.className = "pill pill-off";
  } else if (missing.length) {
    // Partial is a locked state, not a working one, so it must not read as neutral.
    keys.textContent = "keys: incomplete, loops locked";
    keys.className = "pill pill-off";
  } else {
    keys.textContent = "keys: OpenAI + Langfuse, this tab";
    keys.className = "pill pill-ok";
  }
  $("#status-model").textContent = "model: " + activeModel();
  $("#status-session").textContent = "session: " + SESSION_ID;
}

function fillSelect(sel, values, chosen) {
  sel.innerHTML = "";
  values.forEach((v) => {
    const opt = document.createElement("option");
    opt.value = v;
    opt.textContent = v;
    if (v === chosen) opt.selected = true;
    sel.appendChild(opt);
  });
}

async function loadStatus() {
  const note = $("#su-note");
  try {
    const res = await fetch("/api/status");
    if (!res.ok) throw new Error("HTTP " + res.status);
    const s = await res.json();
    if (Array.isArray(s.allowed_langfuse_hosts) && s.allowed_langfuse_hosts.length) {
      ALLOWED_HOSTS = s.allowed_langfuse_hosts;
    }
    if (Array.isArray(s.model_choices) && s.model_choices.length) MODEL_CHOICES = s.model_choices;
    if (s.default_model) DEFAULT_MODEL = s.default_model;
    if (s.default_langfuse_host) DEFAULT_HOST = s.default_langfuse_host;
    note.hidden = true;
  } catch (e) {
    note.textContent = "Could not reach the server, so these lists are defaults. Saving still works.";
    note.hidden = false;
  }
  // A stored host that is no longer allowlisted would be refused server-side, so
  // fall back to the server's own default rather than keeping a dead selection.
  if (!ALLOWED_HOSTS.includes(creds.lfHost)) {
    creds.lfHost = ALLOWED_HOSTS.includes(DEFAULT_HOST) ? DEFAULT_HOST : ALLOWED_HOSTS[0];
  }
  if (!MODEL_CHOICES.includes(creds.model)) {
    creds.model = MODEL_CHOICES.includes(DEFAULT_MODEL) ? DEFAULT_MODEL : MODEL_CHOICES[0];
  }
  fillSelect($("#su-lf-host"), ALLOWED_HOSTS, creds.lfHost);
  fillSelect($("#su-model"), MODEL_CHOICES, creds.model);
  renderPills();
}

// ---- tabs -------------------------------------------------------------------

function selectTab(name) {
  $all(".tab").forEach((t) => t.classList.toggle("is-active", t.dataset.tab === name));
  $all(".panel").forEach((p) => p.classList.toggle("is-active", p.dataset.panel === name));
}

$all(".tab").forEach((tab) => {
  // Locked tabs stay clickable: the panel is where we explain what is missing.
  tab.addEventListener("click", () => selectTab(tab.dataset.tab));
});

$all("[data-goto-setup]").forEach((btn) => {
  btn.addEventListener("click", () => selectTab("setup"));
});

// ---- gating -----------------------------------------------------------------

const LOOPS = ["workflow", "agent", "coding"];

/**
 * One gate for all three credentials, matching what the server enforces. An
 * unlocked tab, an enabled Run button and a green save message all mean the same
 * thing here, and it has to be the thing the server will actually do: a Run button
 * that only produces a credential error is worse than a locked one.
 *
 * There is no tracing-off notice to toggle any more. That state does not exist.
 */
function applyGate() {
  const unlocked = hasAllCreds();
  LOOPS.forEach((loop) => {
    const tab = $(`.tab[data-tab="${loop}"]`);
    const lock = $(".tab-lock", tab);
    tab.classList.toggle("is-locked", !unlocked);
    if (lock) lock.hidden = unlocked;

    const panel = $(`.panel[data-panel="${loop}"]`);
    panel.classList.toggle("is-gated", !unlocked);
    $(`[data-gate="${loop}"]`).hidden = unlocked;

    const runBtn = $(`.run[data-run="${loop}"]`);
    if (runBtn) runBtn.disabled = !unlocked;
  });
  renderPills();
}

// ---- setup form -------------------------------------------------------------

function fillSetupForm() {
  $("#su-openai").value = creds.openai;
  $("#su-lf-public").value = creds.lfPublic;
  $("#su-lf-secret").value = creds.lfSecret;
  $("#su-model-custom").value = creds.modelCustom;
}

function setupMessage(text, kind) {
  const el = $("#su-msg");
  el.textContent = text;
  el.className = "setup-msg" + (kind ? " is-" + kind : "");
}

$("#su-save").addEventListener("click", () => {
  // The model id is checked before anything is written, so a rejected value is
  // never stored and never becomes a header on the next run.
  const customModel = $("#su-model-custom").value.trim();
  const modelErr = modelIdError(customModel);
  if (modelErr) {
    setupMessage("Nothing saved. " + modelErr, "warn");
    return;
  }

  creds.openai = $("#su-openai").value.trim();
  creds.lfPublic = $("#su-lf-public").value.trim();
  creds.lfSecret = $("#su-lf-secret").value.trim();
  creds.lfHost = $("#su-lf-host").value;
  creds.model = $("#su-model").value;
  creds.modelCustom = customModel;
  persistCreds();
  applyGate();

  const missing = missingCreds();
  if (!missing.length) {
    setupMessage("Saved in this tab. All three loops unlocked, and every run traces into your own Langfuse project.", "ok");
    return;
  }
  // Keys typed so far are kept, so a visitor does not lose a pasted secret while
  // fetching the next one, but the message never implies anything is runnable.
  let msg = "Saved in this tab, but the loops stay locked: still need " + listWords(missing) + ".";
  if (halfLangfusePair()) {
    msg += " A Langfuse key works only as a pair, and a run that cannot be traced is not run at all.";
  }
  setupMessage(msg, "warn");
});

$("#su-clear").addEventListener("click", () => {
  creds.openai = "";
  creds.lfPublic = "";
  creds.lfSecret = "";
  creds.modelCustom = "";
  persistCreds();
  fillSetupForm();
  applyGate();
  setupMessage("Cleared from this browser tab. The loops are locked again.", "ok");
});

// ---- example chips ----------------------------------------------------------

function mountExamples(id, inputSel, list) {
  const box = $("#" + id);
  if (!box) return;
  list.forEach((ex) => {
    const chip = document.createElement("button");
    chip.className = "chip";
    chip.textContent = ex.length > 46 ? ex.slice(0, 44) + "..." : ex;
    chip.title = ex;
    chip.addEventListener("click", () => { $(inputSel).value = ex; });
    box.appendChild(chip);
  });
}
mountExamples("wf-examples", "#wf-input", EXAMPLES.workflow);
mountExamples("ag-examples", "#ag-input", EXAMPLES.agent);

// ---- rendering --------------------------------------------------------------

function badge(type) {
  const label = { generation: "generation", tool: "tool", deterministic: "deterministic", approval: "approval", score: "score" }[type] || type;
  return `<span class="badge badge-${type}">${label}</span>`;
}

function stepHtml(step, i, revealedCount) {
  let meta = "";
  if (step.latency_ms != null) meta += `${step.latency_ms} ms`;
  if (step.tokens && step.tokens.total_tokens != null) meta += (meta ? " &middot; " : "") + `${step.tokens.total_tokens} tok`;
  const body = step.summary ? `<div class="step-body">${esc(step.summary)}</div>` : "";
  // Steps already shown before a re-render (revealedCount) skip the fade-in so
  // only the newly-appended steps animate.
  const isShown = i < revealedCount;
  const cls = isShown ? "step is-shown" : "step";
  const delay = isShown ? 0 : (i - revealedCount) * 90;
  return `<li class="${cls}" style="animation-delay:${delay}ms">
    <div class="step-top">
      <span class="step-name">${esc(step.name)}</span>
      ${badge(step.type)}
      ${meta ? `<span class="step-meta">${meta}</span>` : ""}
    </div>
    ${body}
  </li>`;
}

function stepsListHtml(steps, revealedCount = 0) {
  return `<ul class="steps">${steps.map((s, i) => stepHtml(s, i, revealedCount)).join("")}</ul>`;
}

function scoresHtml(scores) {
  if (!scores || !scores.length) return "";
  return `<div class="scores">${scores.map((s) => {
    const bad = s.value === 0 || s.value === false || s.value === "rejected" || (typeof s.value === "number" && s.value < 0.5);
    return `<span class="score ${bad ? "bad" : ""}">${esc(s.name)}: ${esc(s.value)}</span>`;
  }).join("")}</div>`;
}

function traceCta(data) {
  // A run only happens with Langfuse keys, so "not traced" is not a case to show.
  // A missing url here means the server could not build the link, not that tracing
  // was off, and saying otherwise would send the visitor back to Setup for nothing.
  const btn = data.trace_url
    ? `<a class="trace-btn" href="${esc(data.trace_url)}" target="_blank" rel="noopener">Open this trace in Langfuse &rarr;</a>`
    : `<span class="trace-btn disabled">trace link unavailable</span>`;
  const fb = data.trace_id
    ? `<span class="feedback" data-trace="${esc(data.trace_id)}">
         <span class="eyebrow">FEEDBACK</span>
         <button class="fb" data-val="1">up</button>
         <button class="fb" data-val="0">down</button>
       </span>`
    : "";
  return `<div class="trace-cta">${btn}${fb}</div>`;
}

function wireFeedback(root) {
  const fbBox = $(".feedback", root);
  if (!fbBox) return;
  $all(".fb", fbBox).forEach((btn) => {
    const label = btn.dataset.val === "1" ? "up" : "down";
    btn.addEventListener("click", async () => {
      try {
        await api("/api/feedback", { trace_id: fbBox.dataset.trace, value: Number(btn.dataset.val), comment: "" });
        $all(".fb", fbBox).forEach((b) => b.classList.remove("chosen"));
        btn.classList.add("chosen");  // only mark selected once the score actually posted
      } catch (e) {
        btn.textContent = "failed";
        setTimeout(() => { btn.textContent = label; }, 1500);
      }
    });
  });
}

function runHeadHtml(title) {
  return `<div class="run-head"><span class="eyebrow">${title}</span></div>`;
}

function renderResult(box, data, answerLabel) {
  let html = runHeadHtml("CONTROL LOOP");
  html += stepsListHtml(data.steps || []);
  html += `<div class="result">`;
  if (answerLabel && data.output) {
    html += `<span class="eyebrow">${answerLabel}</span><div class="answer">${esc(data.output)}</div>`;
  }
  html += scoresHtml(data.scores);
  html += traceCta(data);
  html += `</div>`;
  box.innerHTML = html;
  wireFeedback(box);
}

/** One error surface: the message, then the correlation id small enough to quote. */
function showError(box, err) {
  const e = err instanceof ApiError ? err : new ApiError("Something went wrong on this page.");
  let html = runHeadHtml("ERROR") + `<div class="err">${esc(e.message)}`;
  if (e.correlationId) html += `<span class="err-id">reference ${esc(e.correlationId)}</span>`;
  html += `</div>`;
  box.innerHTML = html;
}

function showLoading(box, label) {
  box.innerHTML = `<div class="run-head"><span class="eyebrow"><span class="spinner"></span> ${label}</span></div>`;
}

// ---- diff rendering ---------------------------------------------------------

function diffHtml(diff) {
  const lines = String(diff).split("\n").map((ln) => {
    let cls = "";
    if (ln.startsWith("+++") || ln.startsWith("---")) cls = "hunk";
    else if (ln.startsWith("@@")) cls = "hunk";
    else if (ln.startsWith("+")) cls = "add";
    else if (ln.startsWith("-")) cls = "del";
    return `<span class="ln ${cls}">${esc(ln) || "&nbsp;"}</span>`;
  });
  return `<div class="diff">${lines.join("")}</div>`;
}

// ---- run: workflow & agent --------------------------------------------------

/** Last line of defence: the gate disables the buttons, this stops a stale click. */
function blockedByGate(box) {
  const missing = missingCreds();
  if (!missing.length) return false;
  showError(box, new ApiError(
    "This run needs " + listWords(missing) + ". Add them in the Setup tab: all three are " +
    "required, so that every run traces into your own Langfuse project."));
  return true;
}

async function runSimple(loop, inputSel, endpoint, payloadKey, answerLabel) {
  const box = $(`.runbox[data-runbox="${loop}"]`);
  const btn = $(`.run[data-run="${loop}"]`);
  const value = $(inputSel).value.trim();
  if (!value || blockedByGate(box)) return;
  btn.disabled = true;
  showLoading(box, "running " + loop + "...");
  const payload = { session_id: SESSION_ID };
  payload[payloadKey] = value;
  try {
    renderResult(box, await api(endpoint, payload), answerLabel);
  } catch (e) {
    showError(box, e);
  } finally {
    btn.disabled = false;
  }
}

// ---- run: coding agent ------------------------------------------------------

async function runCodingPropose() {
  const box = $(`.runbox[data-runbox="coding"]`);
  const btn = $(`.run[data-run="coding"]`);
  const task = $("#cd-input").value.trim();
  if (blockedByGate(box)) return;
  btn.disabled = true;
  showLoading(box, "proposing change...");
  try {
    renderPropose(box, await api("/api/coding/propose", { task, session_id: SESSION_ID }));
  } catch (e) {
    showError(box, e);
  } finally {
    btn.disabled = false;
  }
}

function renderPropose(box, data) {
  let html = runHeadHtml("CONTROL LOOP");
  html += stepsListHtml(data.steps || []);
  html += `<div class="result"><span class="eyebrow">PROPOSED DIFF &middot; ${esc(data.file)}</span>`;
  html += diffHtml(data.diff);
  html += `<div class="gate">
      <h3>Approval gate</h3>
      <div class="eyebrow">This change is not written yet. Approve to apply the side effect, or reject to roll back.</div>
      <div class="row">
        <button class="btn-approve">Approve &amp; apply</button>
        <button class="btn-reject">Reject</button>
      </div>
    </div>`;
  html += traceCta(data);
  html += `</div>`;
  box.innerHTML = html;
  wireFeedback(box);

  $(".btn-approve", box).addEventListener("click", () => decide(box, data, true));
  $(".btn-reject", box).addEventListener("click", () => decide(box, data, false));
}

async function decide(box, proposeData, approved) {
  const gate = $(".gate", box);
  if (gate) gate.innerHTML = `<div class="eyebrow"><span class="spinner"></span> ${approved ? "applying" : "rolling back"}...</div>`;
  // The proposal and its signature go back untouched: the server signed that
  // exact payload, so anything we reshape here would fail verification.
  const payload = {
    proposal: proposeData.proposal,
    signature: proposeData.signature,
    approved,
    session_id: SESSION_ID,
  };
  try {
    const data = await api("/api/coding/apply", payload);
    // Combine propose + apply steps into one continuous view.
    const merged = {
      steps: (proposeData.steps || []).concat(data.steps || []),
      output: data.outcome,
      scores: data.scores,
      trace_id: data.trace_id,
      trace_url: data.trace_url,
    };
    let html = runHeadHtml("CONTROL LOOP");
    html += stepsListHtml(merged.steps, (proposeData.steps || []).length);
    html += `<div class="result"><span class="eyebrow">PROPOSED DIFF &middot; ${esc(proposeData.file)}</span>`;
    html += diffHtml(proposeData.diff);
    html += `<span class="eyebrow" style="display:block;margin-top:12px">OUTCOME</span><div class="answer">${esc(data.outcome)}</div>`;
    html += scoresHtml(merged.scores);
    html += traceCta(merged);
    html += `</div>`;
    box.innerHTML = html;
    wireFeedback(box);
    loadCodingFile();
  } catch (e) {
    showError(box, e);
  }
}

// The sandbox file view is optional: it only appears if the backend still keeps
// a file to show. Failure hides it rather than leaving an empty black panel.
async function loadCodingFile() {
  const view = $("#cd-file-view");
  try {
    const res = await fetch("/api/coding/file");
    if (!res.ok) throw new Error("HTTP " + res.status);
    const data = await res.json();
    if (typeof data.content !== "string") throw new Error("no content");
    $("#cd-file").textContent = data.content;
    view.hidden = false;
  } catch (e) {
    view.hidden = true;
  }
}

// ---- wire buttons -----------------------------------------------------------

$(`.run[data-run="workflow"]`).addEventListener("click", () =>
  runSimple("workflow", "#wf-input", "/api/workflow/run", "ticket", "DRAFT REPLY"));
$(`.run[data-run="agent"]`).addEventListener("click", () =>
  runSimple("agent", "#ag-input", "/api/agent/run", "question", "FINAL ANSWER"));
$(`.run[data-run="coding"]`).addEventListener("click", runCodingPropose);

$("#cd-reset").addEventListener("click", async () => {
  const box = $(`.runbox[data-runbox="coding"]`);
  try {
    await api("/api/coding/reset", {});
    loadCodingFile();
    box.innerHTML =
      `<div class="runbox-empty">Sandbox reset. Propose a change to see the diff and the approval gate.</div>`;
  } catch (e) {
    showError(box, e);
  }
});

// ---- init -------------------------------------------------------------------

loadCreds();
fillSetupForm();
applyGate();
// A refresh inside the same tab keeps the keys, so drop the visitor back at the
// loops. Setup is only the landing tab when there is nothing to run with, and a
// partial set of keys counts as nothing.
if (hasAllCreds()) selectTab("workflow");
loadStatus();
loadCodingFile();
