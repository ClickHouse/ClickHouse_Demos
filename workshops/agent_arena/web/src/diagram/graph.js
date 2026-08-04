// The AgentArena architecture as React Flow nodes + animated edges.
// Positions are computed by dagre (layout.js); nodes only declare a size + env.

export const EDGE_COLORS = {
  data: '#3fb950',       // seed writes
  ai: '#a371f7',         // OpenRouter inference
  read: '#f5d90a',       // read-only analytic queries
  control: '#58a6ff',    // orchestration / requests
  trace: '#ec6cb9',      // LangFuse traces + scores
}

// Where each component runs -> card accent color (legend, not a bounding box).
export const ENV = {
  local: { label: 'Local / Docker', color: '#58a6ff' },
  ch: { label: 'ClickHouse Cloud', color: '#f5d90a' },
  saas: { label: 'External SaaS (LangFuse · OpenRouter)', color: '#a371f7' },
}

const card = (id, env, title, subtitle, kind, w = 215) => ({
  id, type: 'card', position: { x: 0, y: 0 }, width: w, height: 66,
  data: { title, subtitle, kind, env, accent: ENV[env].color },
})

export const componentNodes = [
  card('user', 'local', 'Analyst', 'asks a question', 'user', 170),
  card('datagen', 'local', 'Data generator', 'Faker · seed + mutations', 'job'),
  card('golden', 'local', 'Golden set', '18 Qs × 5 tiers + golden SQL', 'data'),
  card('serving', 'local', 'Serving API', 'FastAPI · POST /ask', 'api'),
  card('harness', 'local', 'Benchmark harness', 'grid runner · emits experiment items', 'job'),
  card('agent', 'local', 'Agent core', 'loop · P1–P3 · SQL guard', 'agent', 230),
  card('dashboard', 'local', 'Leaderboard dashboard', 'FastAPI · LangFuse Public API', 'api'),

  card('openrouter', 'saas', 'OpenRouter', 'OpenAI-compatible · Claude/GPT-4o/Gemini/DeepSeek/Qwen/Llama', 'ai', 300),

  card('views', 'ch', 'v_* analytic views', 'FINAL · dedup current state', 'view', 300),
  card('langfuse', 'saas', 'LangFuse Cloud', 'experiments · results · scores · traces', 'trace', 300),
]

// All edges flow forward in the dagre LR ranking, so source=right, target=left.
// label = short (always shown); detail = full text (shown on hover).
const e = (id, source, target, label, detail, color) => ({
  id, source, target, label, type: 'flow',
  sourceHandle: 's-right', targetHandle: 't-left',
  data: { color: EDGE_COLORS[color], detail },
})

export const edges = [
  e('datagen-views', 'datagen', 'views', 'seed', 'direct INSERT into ClickHouse seed tables → v_* views', 'data'),

  e('agent-openrouter', 'agent', 'openrouter', 'chat/completions', 'OpenRouter: NL→SQL + token usage', 'ai'),
  e('agent-views', 'agent', 'views', 'SELECT', 'read-only SELECT (sandboxed)', 'read'),
  e('golden-harness', 'golden', 'harness', 'Qs', 'questions + golden SQL', 'control'),
  e('harness-agent', 'harness', 'agent', 'run', 'run grid: model × prompt', 'control'),
  e('user-serving', 'user', 'serving', '/ask', 'POST /ask', 'control'),
  e('serving-agent', 'serving', 'agent', 'reuse', 'reuses the agent core', 'control'),

  e('harness-langfuse', 'harness', 'langfuse', 'store', 'Experiment Items with result, cost and latency; evaluators attach scores', 'trace'),
  e('langfuse-dashboard', 'langfuse', 'dashboard', 'Public API', 'experiments, item outputs, scores and conversations', 'trace'),
]
