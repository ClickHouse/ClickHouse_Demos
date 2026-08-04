import { useEffect, useMemo, useRef, useState } from 'react'
import { Tooltip } from 'react-tooltip'
import 'react-tooltip/dist/react-tooltip.css'
import './leaderboard.css'
import { api, API_BASE } from '../api.js'
import { famOf, promptMeta, outcomeMeta } from '../meta.js'
import { Icon, ConfigName, FamDot, OutcomeTag, WinPill, Bar, heatColor } from '../ui.jsx'

const pct = (v) => (v == null ? '—' : (Number(v) * 100).toFixed(1) + '%')
const pct0 = (v) => (v == null ? '—' : (Number(v) * 100).toFixed(0) + '%')
const money = (v, d = 5) => (v == null ? '—' : '$' + Number(v).toFixed(d))

function toggleSet(set, v) { const n = new Set(set); n.has(v) ? n.delete(v) : n.add(v); return n }

const COL_DESC = {
  config_id: 'model__prompt-strategy',
  n_questions: 'questions graded',
  accuracy: 'Execution accuracy — does the SQL return the exact correct result set? This is ground truth.',
  n_correct: 'questions answered correctly',
  avg_judge_score: 'Average LLM-judge score — plausibility, NOT correctness. A fallible secondary signal.',
  total_cost_usd: 'total OpenRouter token cost across all questions',
  avg_latency_ms: 'mean wall-clock per question',
  cost_per_correct_answer: 'THE headline metric: total cost ÷ correct answers. A cheap-but-wrong model scores badly.',
}
const COLS = [
  ['config_id', 'Config'], ['n_questions', '#Q'], ['accuracy', 'Accuracy'],
  ['n_correct', 'Correct'], ['avg_judge_score', 'LLM judge'], ['total_cost_usd', 'Total $'],
  ['avg_latency_ms', 'Avg latency'], ['cost_per_correct_answer', '$/correct'],
]

export default function Leaderboard() {
  const [runs, setRuns] = useState([])
  const [run, setRun] = useState(null)
  const [board, setBoard] = useState([])
  const [tiers, setTiers] = useState([])
  const [outcomes, setOutcomes] = useState([])
  const [meta, setMeta] = useState({})
  const [sort, setSort] = useState({ key: 'accuracy', dir: -1 })
  const [promptFilter, setPromptFilter] = useState('All')
  const [analysisTab, setAnalysisTab] = useState('tiers')
  const [error, setError] = useState(null)
  const [runsLoading, setRunsLoading] = useState(true)
  const [dashboardLoading, setDashboardLoading] = useState(false)

  const [expanded, setExpanded] = useState(() => new Set())
  const [details, setDetails] = useState({})
  const [conv, setConv] = useState(null)
  const [turns, setTurns] = useState({})

  const [prompts, setPrompts] = useState([])
  const detailsRef = useRef(details)
  useEffect(() => { detailsRef.current = details }, [details])

  function loadRuns() {
    setRunsLoading(true)
    return api('/api/runs')
      .then((r) => {
        setRuns(r)
        if (r.length) setRun((current) => current || r[0])
      })
      .finally(() => setRunsLoading(false))
  }

  useEffect(() => {
    loadRuns().catch((e) => setError(String(e)))
    api('/api/meta').then(setMeta).catch(() => {})
    api('/api/grid-options').then((o) => setPrompts(o.prompts)).catch(() => {})
  }, [])

  function loadBoard(rid) {
    if (!rid) return
    setDashboardLoading(true)
    const q = `?run_id=${encodeURIComponent(rid)}`
    return Promise.all([api('/api/leaderboard' + q), api('/api/tiers' + q), api('/api/outcomes' + q)])
      .then(([b, t, o]) => { setBoard(b); setTiers(t); setOutcomes(o); setError(null) })
      .catch((e) => setError(String(e)))
      .finally(() => setDashboardLoading(false))
  }

  useEffect(() => {
    if (!run) return
    setExpanded(new Set()); setConv(null); setDetails({}); setPromptFilter('All')
    loadBoard(run)
  }, [run])

  const promptDesc = useMemo(() => Object.fromEntries(prompts.map((p) => [p.name, p.desc])), [prompts])
  const promptsInRun = useMemo(() => [...new Set(board.map((r) => r.prompt_name))].sort(), [board])
  const matchP = (cid) => promptFilter === 'All' || (cid || '').split('__').pop() === promptFilter
  const fboard = useMemo(
    () => (promptFilter === 'All' ? board : board.filter((r) => r.prompt_name === promptFilter)),
    [board, promptFilter])

  const sortedBoard = useMemo(() => {
    const rows = [...fboard]
    rows.sort((a, b) => (a[sort.key] > b[sort.key] ? 1 : -1) * sort.dir)
    return rows
  }, [fboard, sort])

  const best = fboard.length ? Math.max(...fboard.map((r) => Number(r.accuracy))) : 0
  const winners = fboard.filter((r) => Number(r.accuracy) === best)
    .sort((a, b) => Number(a.cost_per_correct_answer) - Number(b.cost_per_correct_answer))
  const winner = winners[0] || {}
  const totalGraded = fboard.reduce((s, r) => s + Number(r.n_questions), 0)
  const lfBase = meta.langfuse_base
  const sessionUrl = (cfg) => lfBase && `${lfBase}/sessions/${encodeURIComponent(run + '__' + cfg)}`
  const toggleSort = (key) => setSort((s) => ({ key, dir: s.key === key ? -s.dir : -1 }))

  async function loadDetails(cfg) {
    if (detailsRef.current[cfg] !== undefined) return
    try {
      const rows = await api(`/api/questions?run_id=${encodeURIComponent(run)}&config_id=${encodeURIComponent(cfg)}`)
      setDetails((d) => ({ ...d, [cfg]: rows }))
    } catch (e) {
      setDetails((d) => ({ ...d, [cfg]: { error: String(e) } }))
    }
  }
  function toggleExpand(cfg) {
    setExpanded((s) => toggleSet(s, cfg))
    loadDetails(cfg)
  }
  async function openConversation(cfg) {
    setConv({ config_id: cfg, loading: true, exchanges: [] })
    try {
      const r = await api(`/api/lf/session?session_id=${encodeURIComponent(run + '__' + cfg)}`)
      setConv({ config_id: cfg, loading: false, exchanges: r.exchanges || [] })
    } catch (e) { setConv({ config_id: cfg, loading: false, error: String(e), exchanges: [] }) }
  }
  async function loadTurns(traceId) {
    if (!traceId || turns[traceId]) return
    try {
      const r = await api(`/api/lf/trace?trace_id=${encodeURIComponent(traceId)}`)
      setTurns((t) => ({ ...t, [traceId]: r.turns || [] }))
    } catch { /* ignore */ }
  }

  if (error) {
    return (
      <div className="lb-error">
        <b>Can't reach the dashboard API at {API_BASE}.</b>
        <div className="lb-hint">{error}</div>
        <div className="lb-hint">Start it: <code>scripts/arena.sh serve --api-only</code> (or <code>uvicorn dashboard.app:app --port 8000</code>).</div>
      </div>
    )
  }

  return (
    <div className="lb">
      <Tooltip id="tip" className="arena-tt" delayShow={0} delayHide={0} />
      <div className="lb-wrap">
        {/* ---------- controls ---------- */}
        <div className="lb-controls">
          <div className="run-select" aria-busy={runsLoading}>
            <label>Run</label>
            <select value={run || ''} disabled={runsLoading || dashboardLoading || !runs.length} onChange={(e) => setRun(e.target.value)}>
              {runsLoading && <option value="">Loading runs…</option>}
              {!runsLoading && !runs.length && <option value="">No runs found</option>}
              {runs.map((r) => <option key={r}>{r}</option>)}
            </select>
          </div>
          <div className="grow" />
          {meta.datasets_url && (
            <a className="trace-link" style={{ fontSize: 13 }} href={meta.datasets_url} target="_blank" rel="noreferrer">
              Open experiment in LangFuse <Icon name="ext" size={13} />
            </a>
          )}
        </div>

        {dashboardLoading ? (
          <div className="dashboard-loading" role="status" aria-live="polite">
            <span className="loading-spinner" aria-hidden="true" />
            <div>
              <b>Loading dashboard data</b>
              <span>Fetching leaderboard, tiers, and outcomes for <code>{run}</code>.</span>
            </div>
          </div>
        ) : !run && !runsLoading ? (
          <div className="dashboard-empty">No benchmark runs found.</div>
        ) : (
          <>

        {/* ---------- KPI strip ---------- */}
        <SummaryCards rows={fboard} winner={winner} best={best} totalGraded={totalGraded} promptDesc={promptDesc} />

        {/* ---------- hero: cost × accuracy + best value ---------- */}
        <div className="lb-hero" style={{ marginTop: 16 }}>
          <CostScatter rows={fboard} promptDesc={promptDesc} />
          <div className="side-card">
            <h4>Best value · cost per correct</h4>
            <CostPerCorrectRank rows={fboard} />
          </div>
        </div>

        {/* ---------- leaderboard table ---------- */}
        <div className="lb-section">
          <div className="lt-head">
            <span className="lt-title">Leaderboard — click a row to drill into per-question LangFuse traces</span>
          </div>
          {promptsInRun.length > 1 && (
            <div className="prompt-filter">
              <span className="lab">Filter by prompt</span>
              <button className="chip" data-on={promptFilter === 'All'} onClick={() => setPromptFilter('All')}>All</button>
              {promptsInRun.map((p) => {
                const pm = promptMeta(p, promptDesc)
                return (
                  <button key={p} className="chip" data-on={promptFilter === p} onClick={() => setPromptFilter(p)}
                    data-tooltip-id="tip" data-tooltip-content={pm.desc || pm.name}>
                    <span className="mono" style={{ fontSize: 10, opacity: .7 }}>{pm.short}</span> {pm.name}
                  </button>
                )
              })}
            </div>
          )}
          <table className="lt">
            <thead>
              <tr>
                <th className="l" style={{ width: 30 }} />
                {COLS.map(([k, label]) => (
                  <th key={k} className={k === 'config_id' ? 'l' : ''} onClick={() => toggleSort(k)}
                    data-tooltip-id="tip" data-tooltip-content={COL_DESC[k]}>
                    <span>{label}{sort.key === k && <span className="arr">{sort.dir < 0 ? '↓' : '↑'}</span>}</span>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sortedBoard.map((r, i) => {
                const isWin = Number(r.accuracy) === best
                const open = expanded.has(r.config_id)
                return (
                  <FragmentRow key={r.config_id}>
                    <tr className={`row${open ? ' open' : ''}${isWin ? ' win-row' : ''}`} onClick={() => toggleExpand(r.config_id)}>
                      <td className="l rank">{i + 1}</td>
                      <td className="l">
                        <span style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
                          <span className="caret"><Icon name="chev" size={13} /></span>
                          <ConfigName config_id={r.config_id} promptDesc={promptDesc} />
                          {isWin && <WinPill />}
                        </span>
                      </td>
                      <td className="tnum">{r.n_questions}</td>
                      <td>
                        <span className="acc-cell">
                          <span className="acc-bar"><Bar value={Number(r.accuracy) * 100} color={isWin ? 'var(--accent)' : 'var(--oc-correct)'} h={6} /></span>
                          <b className="num-strong tnum" style={{ width: 52, textAlign: 'right' }}>{pct(r.accuracy)}</b>
                        </span>
                      </td>
                      <td className="tnum">{r.n_correct}</td>
                      <td><span className="judge-chip">{pct0(r.avg_judge_score)}</span></td>
                      <td className="tnum">{money(r.total_cost_usd)}</td>
                      <td className="tnum" style={{ color: r.avg_latency_ms > 5000 ? 'var(--oc-exec)' : 'inherit' }}>{Math.round(r.avg_latency_ms).toLocaleString()} ms</td>
                      <td className="cpc tnum">{money(r.cost_per_correct_answer)}</td>
                    </tr>
                    {open && (
                      <tr className="dd">
                        <td className="l" colSpan={COLS.length + 1} style={{ height: 'auto', padding: 0 }}>
                          <DrillDown
                            config_id={r.config_id} rows={details[r.config_id]}
                            sessionUrl={sessionUrl(r.config_id)} onConv={() => openConversation(r.config_id)}
                            conv={conv && conv.config_id === r.config_id ? conv : null}
                            turns={turns} loadTurns={loadTurns}
                          />
                        </td>
                      </tr>
                    )}
                  </FragmentRow>
                )
              })}
            </tbody>
          </table>
        </div>

        {/* ---------- analysis tabs ---------- */}
        <div className="lb-section">
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12, flexWrap: 'wrap', gap: 10 }}>
            <h3 style={{ margin: 0 }}>Analysis</h3>
            <div className="seg">
              <button data-on={analysisTab === 'tiers'} onClick={() => setAnalysisTab('tiers')}><Icon name="grid" size={14} color={analysisTab === 'tiers' ? 'var(--ink)' : 'var(--ink-3)'} />Difficulty tiers</button>
              <button data-on={analysisTab === 'outcomes'} onClick={() => setAnalysisTab('outcomes')}><Icon name="chart" size={14} color={analysisTab === 'outcomes' ? 'var(--ink)' : 'var(--ink-3)'} />Outcome mix</button>
            </div>
          </div>
          <div className="panel" style={{ padding: 16 }}>
            {analysisTab === 'tiers'
              ? <Heatmap tiers={tiers.filter((t) => matchP(t.config_id))} promptDesc={promptDesc} />
              : <OutcomeBreakdown outcomes={outcomes.filter((o) => matchP(o.config_id))} promptDesc={promptDesc} />}
          </div>
        </div>
          </>
        )}
      </div>
    </div>
  )
}

function FragmentRow({ children }) { return <>{children}</> }

/* ===================== KPI cards ===================== */
function SummaryCards({ rows, winner, best, totalGraded, promptDesc }) {
  if (!rows.length) return null
  const wp = winner.prompt_name ? promptMeta(winner.prompt_name, promptDesc) : null
  return (
    <div className="kpis" style={{ gridTemplateColumns: 'repeat(4, 1fr)' }}>
      <div className="kpi winner">
        <div className="k-label">🏆 Winner</div>
        <div className="k-value">{winner.config_id ? <ConfigName config_id={winner.config_id} size={15} showPrompt={false} promptDesc={promptDesc} /> : '—'}</div>
        <div className="k-sub">{wp ? `${wp.name} · best value at top accuracy` : ''}</div>
      </div>
      <div className="kpi">
        <div className="k-label">Best accuracy</div>
        <div className="k-value">{pct(best)}</div>
        <div className="k-sub">execution accuracy · ground truth</div>
      </div>
      <div className="kpi headline">
        <div className="k-label">Cost / correct · headline</div>
        <div className="k-value">{money(winner.cost_per_correct_answer)}</div>
        <div className="k-sub">winner · cheap-but-wrong scores badly</div>
      </div>
      <div className="kpi">
        <div className="k-label">Configs × answers</div>
        <div className="k-value">{rows.length} <span style={{ color: 'var(--ink-3)', fontSize: 15 }}>× {totalGraded}</span></div>
        <div className="k-sub">{rows.length} configs this run</div>
      </div>
    </div>
  )
}

/* ===================== drill-down ===================== */
function DrillDown({ config_id, rows, sessionUrl, onConv, conv, turns, loadTurns }) {
  const loading = rows === undefined
  const err = rows && rows.error
  const list = Array.isArray(rows) ? rows : []
  return (
    <div className="dd-inner rise">
      <div className="dd-bar">
        <div className="dd-note">per-question results — <span className="trace-link">trace ↗</span> opens the LangFuse trace</div>
        <div style={{ display: 'flex', gap: 8 }}>
          {sessionUrl && <a className="trace-link" href={sessionUrl} target="_blank" rel="noreferrer">session ↗</a>}
          <button className="btn sm" style={{ background: 'color-mix(in oklab, var(--langfuse) 22%, var(--bg-3))', borderColor: 'color-mix(in oklab, var(--langfuse) 45%, transparent)' }} onClick={onConv}>
            <Icon name="ext" size={13} /> View conversation (LangFuse)
          </button>
        </div>
      </div>

      {loading && <div style={{ color: 'var(--ink-3)', padding: '14px 0', fontSize: 13 }}>Loading per-question results from LangFuse…</div>}
      {err && (
        <div className="lb-hint">Couldn't load per-question results: {rows.error}. Is the dashboard API up to date? Restart it (<code>scripts/arena.sh serve</code>).</div>
      )}
      {!loading && !err && (
        <table className="qt">
          <thead><tr>
            <th className="l">Q</th>
            <th className="l" data-tooltip-id="tip" data-tooltip-content="Difficulty tier: 1 = simple aggregation … 5 = funnel / window / retention.">tier</th>
            <th data-tooltip-id="tip" data-tooltip-content="✓ = SQL executed to the exact correct result set (execution accuracy); ✕ = wrong/failed.">✓</th>
            <th data-tooltip-id="tip" data-tooltip-content="LLM-judge score for this answer — SQL-quality rating, separate from correctness.">judge</th>
            <th>latency</th><th className="l" style={{ paddingLeft: 18 }}>outcome</th><th>trace</th>
          </tr></thead>
          <tbody>
            {list.map((q) => (
              <tr key={q.question_id} data-q={q.question_id} data-qtext={q.question || ''}>
                <td className="l"><span className="qid" data-tooltip-id="tip" data-tooltip-content={q.question || q.question_id}>{q.question_id}</span></td>
                <td className="l"><span className="tier-pip">{[1, 2, 3, 4, 5].map((t) => <i key={t} className={t <= q.tier ? 'on' : ''} />)}</span></td>
                <td className={q.correctness ? 'ok' : 'no'}>{q.correctness ? '✓' : '✕'}</td>
                <td className="tnum">{pct0(q.judge_score)}</td>
                <td className="tnum">{q.latency_ms} ms</td>
                <td className="l" style={{ paddingLeft: 18 }}><OutcomeTag outcome={q.outcome} /></td>
                <td>{q.trace_url ? <a className="trace-link" href={q.trace_url} target="_blank" rel="noreferrer">trace ↗</a> : '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {conv && (
        <div className="cv">
          <div className="dd-note" style={{ marginBottom: 10 }}>Conversation history — fetched live from the LangFuse API{conv.loading ? ' …loading' : ''}</div>
          {conv.error && <div className="lb-hint">{conv.error}</div>}
          {conv.exchanges.map((ex) => (
            <div className="cv-ex" key={ex.question_id}>
              <details onToggle={(e) => e.target.open && loadTurns(ex.trace_id)}>
                <summary className="cv-ex-head" style={{ listStyle: 'none' }}>
                  <span className="caret" style={{ color: 'var(--ink-3)' }}><Icon name="chev" size={13} /></span>
                  <span className="mono" style={{ color: 'var(--ink-3)', fontSize: 11 }}>{ex.question_id}</span>
                  <span className="q">{ex.question}</span>
                  <OutcomeTag outcome={ex.outcome} />
                </summary>
                <div className="cv-turns">
                  {!turns[ex.trace_id] && <div style={{ color: 'var(--ink-3)', fontSize: 12.5 }}>Fetching conversation from LangFuse…</div>}
                  {(turns[ex.trace_id] || []).map((t, i) => (
                    <div key={i} className={`bubble ${t.role}`}>
                      <div className="who">{t.role}</div>
                      <span>{t.content}</span>
                    </div>
                  ))}
                </div>
              </details>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

/* ===================== cost × accuracy scatter ===================== */
function CostScatter({ rows, promptDesc }) {
  const W = 720, H = 420, pad = 54
  const [hover, setHover] = useState(null)
  if (!rows.length) return <div className="scatter-card" style={{ color: 'var(--ink-3)' }}>No data for this run.</div>
  const accs = rows.map((r) => Number(r.accuracy) * 100)
  const cpcs = rows.map((r) => Number(r.cost_per_correct_answer) || 0)
  const accMin = Math.max(0, Math.floor((Math.min(...accs) - 8) / 5) * 5)
  const accMax = Math.min(100, Math.ceil((Math.max(...accs) + 5) / 5) * 5)
  const maxCost = (Math.max(...cpcs) || 0.001) * 1.08
  const x = (a) => pad + ((a - accMin) / (accMax - accMin || 1)) * (W - pad - 18)
  const y = (c) => (H - pad) - (c / maxCost) * (H - pad - 18)
  const rad = (r) => 7 + Math.sqrt(Number(r.total_cost_usd) || 0) * 28
  const ticks = []
  for (let t = accMin; t <= accMax; t += Math.max(5, Math.round((accMax - accMin) / 5 / 5) * 5)) ticks.push(t)
  // distinct families present, in first-seen order, for the legend
  const legend = []
  const seenFam = new Set()
  rows.forEach((r) => { const f = famOf(r.model_name); if (!seenFam.has(f.key)) { seenFam.add(f.key); legend.push(f) } })
  return (
    <div className="scatter-card">
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 6 }}>
        <h3 style={{ fontFamily: 'var(--f-display)', fontSize: 17 }}>Cost <span className="hl">×</span> accuracy</h3>
        <span style={{ fontSize: 11.5, color: 'var(--ink-3)' }}>bubble size = total spend</span>
      </div>
      <div className="sc-legend">
        {legend.map((f) => (
          <span key={f.key}><span className="sw" style={{ background: f.color, color: f.color }} />{f.label}</span>
        ))}
      </div>
      <div className="scatter">
        <svg viewBox={`0 0 ${W} ${H}`} onMouseLeave={() => setHover(null)}>
          <rect className="sc-quadrant" x={x((accMin + accMax) / 2)} y={y(maxCost * 0.34)} width={W - 18 - x((accMin + accMax) / 2)} height={(H - pad) - y(maxCost * 0.34)} rx="8" />
          <text className="sc-quad-lab" x={W - 22} y={H - pad - 8} textAnchor="end">cheap &amp; accurate ✦</text>
          <line className="sc-grid" x1={pad} y1={H - pad} x2={W - 12} y2={H - pad} />
          <line className="sc-grid" x1={pad} y1={18} x2={pad} y2={H - pad} />
          {ticks.map((t) => (
            <g key={t}>
              <line className="sc-grid" x1={x(t)} y1={H - pad} x2={x(t)} y2={H - pad + 5} />
              <text className="sc-axis-lab" x={x(t)} y={H - pad + 18} textAnchor="middle">{t}%</text>
            </g>
          ))}
          <text className="sc-axis-lab" x={(W + pad) / 2} y={H - 8} textAnchor="middle">execution accuracy →</text>
          <text className="sc-axis-lab" x={16} y={28}>$$ costly</text>
          <text className="sc-axis-lab" x={16} y={H - pad}>¢ cheap</text>
          {rows.map((r) => {
            const fam = famOf(r.model_name)
            const isH = hover && hover.config_id === r.config_id
            return (
              <circle key={r.config_id} className="sc-bubble" cx={x(Number(r.accuracy) * 100)} cy={y(Number(r.cost_per_correct_answer) || 0)} r={rad(r)}
                fill={`color-mix(in oklab, ${fam.color} ${isH ? 60 : 38}%, transparent)`} stroke={fam.color} strokeWidth={isH ? 2.4 : 1.4}
                onMouseEnter={() => setHover(r)} />
            )
          })}
        </svg>
        {hover && (
          <div style={{ position: 'absolute', left: `${(x(Number(hover.accuracy) * 100) / W) * 100}%`, top: `${(y(Number(hover.cost_per_correct_answer) || 0) / H) * 100}%`, transform: 'translate(-50%, -130%)', background: '#05070a', border: '1px solid var(--line-strong)', borderRadius: 9, padding: '7px 10px', pointerEvents: 'none', whiteSpace: 'nowrap', boxShadow: 'var(--sh-2)' }}>
            <div style={{ marginBottom: 3 }}><ConfigName config_id={hover.config_id} size={12.5} promptDesc={promptDesc} /></div>
            <div className="mono" style={{ fontSize: 11, color: 'var(--ink-2)' }}>{pct(hover.accuracy)} acc · <span style={{ color: 'var(--accent)' }}>{money(hover.cost_per_correct_answer)}/correct</span></div>
          </div>
        )}
      </div>
    </div>
  )
}

/* ===================== cost-per-correct ranked bars ===================== */
function CostPerCorrectRank({ rows }) {
  if (!rows.length) return <div style={{ color: 'var(--ink-3)', fontSize: 12.5 }}>No data.</div>
  const data = [...rows].filter((r) => r.cost_per_correct_answer != null)
    .sort((a, b) => Number(a.cost_per_correct_answer) - Number(b.cost_per_correct_answer)).slice(0, 8)
  const max = Math.max(...data.map((r) => Number(r.cost_per_correct_answer)), 0.0001)
  return (
    <div>
      {data.map((r, i) => {
        const fam = famOf(r.model_name)
        return (
          <div className="cpc-row" key={r.config_id}>
            <div className="label"><span className="mono" style={{ color: 'var(--ink-4)', width: 14 }}>{i + 1}</span><FamDot model={r.model_name} /><span className="nm">{r.model_name}</span></div>
            <Bar value={Number(r.cost_per_correct_answer)} max={max} color={i === 0 ? 'var(--accent)' : fam.color} h={9} />
            <div className="val">{money(r.cost_per_correct_answer)}</div>
          </div>
        )
      })}
      <div style={{ fontSize: 11.5, color: 'var(--ink-3)', marginTop: 8 }}>Lower is better. The cheapest <i>correct</i> answer wins — accuracy and price together.</div>
    </div>
  )
}

/* ===================== tier heatmap ===================== */
function Heatmap({ tiers, promptDesc }) {
  const keys = [...new Set(tiers.map((t) => t.tier))].sort((a, b) => a - b)
  const cids = [...new Set(tiers.map((t) => t.config_id))].sort()
  const map = Object.fromEntries(tiers.map((t) => [t.config_id + '|' + t.tier, t.accuracy]))
  if (!cids.length) return <div style={{ color: 'var(--ink-3)', fontSize: 12.5 }}>No data.</div>
  return (
    <div style={{ overflowX: 'auto' }}>
      <table className="hm">
        <thead><tr><th className="l">Config</th>{keys.map((t) => <th key={t}>Tier {t}</th>)}</tr></thead>
        <tbody>
          {cids.map((cid) => (
            <tr key={cid}>
              <td className="l"><ConfigName config_id={cid} size={12.5} promptDesc={promptDesc} /></td>
              {keys.map((t) => {
                const a = map[cid + '|' + t]
                const v = a == null ? null : Number(a) * 100
                return <td key={t}><div className={`cell${v === 0 ? ' dim' : ''}`} style={{ background: heatColor(v) }}>{v == null ? '—' : `${v.toFixed(0)}%`}</div></td>
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

/* ===================== outcome breakdown ===================== */
function OutcomeBreakdown({ outcomes, promptDesc }) {
  const byCfg = {}
  outcomes.forEach((o) => { (byCfg[o.config_id] = byCfg[o.config_id] || {})[o.outcome] = Number(o.n) })
  const cids = Object.keys(byCfg).sort()
  const kinds = [...new Set(outcomes.map((o) => o.outcome))]
  if (!cids.length) return <div style={{ color: 'var(--ink-3)', fontSize: 12.5 }}>No data.</div>
  return (
    <div>
      {cids.map((cid) => {
        const m = byCfg[cid]
        const total = Object.values(m).reduce((a, b) => a + b, 0) || 1
        return (
          <div className="ob-row" key={cid}>
            <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}><ConfigName config_id={cid} size={12.5} promptDesc={promptDesc} /></div>
            <div className="ob-bar">
              {Object.entries(m).map(([k, n]) => n ? (
                <div key={k} className="ob-seg" style={{ width: `${(n / total) * 100}%`, background: outcomeMeta(k).color }}
                  data-tooltip-id="tip" data-tooltip-content={`${outcomeMeta(k).label}: ${n}`} />
              ) : null)}
            </div>
          </div>
        )
      })}
      <div className="ob-legend">
        {kinds.map((k) => <span key={k}><span className="dot" style={{ background: outcomeMeta(k).color }} />{outcomeMeta(k).label}</span>)}
      </div>
    </div>
  )
}
