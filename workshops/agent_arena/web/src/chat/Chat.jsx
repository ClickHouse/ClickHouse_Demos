import { useEffect, useState } from 'react'
import { getServing, postServing } from '../api.js'
import './chat.css'

// stable per-mount conversation id so a chat's turns group into one LangFuse session
const SESSION_ID = 'chat-' + Math.random().toString(36).slice(2, 12)

export default function Chat() {
  const [configs, setConfigs] = useState([])
  const [config, setConfig] = useState('')
  const [question, setQuestion] = useState('')
  const [turns, setTurns] = useState([])      // {question, resp, feedback, feedbackPending}
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState('')

  useEffect(() => {
    getServing('/configs')
      .then((d) => { setConfigs(d.config_ids || []); setConfig((d.config_ids || [])[0] || '') })
      .catch((e) => setErr(String(e)))
  }, [])

  async function ask() {
    if (!question.trim() || !config) return
    setLoading(true); setErr('')
    try {
      const resp = await postServing('/ask', { question, config_id: config, session_id: SESSION_ID })
      setTurns((t) => [...t, { question, resp, feedback: null, feedbackPending: false }])
      setQuestion('')
    } catch (e) { setErr(String(e)) } finally { setLoading(false) }
  }

  async function rate(i, value) {
    const t = turns[i]
    if (!t.resp.trace_id) return
    setErr('')
    setTurns((ts) => ts.map((x, j) => (j === i ? { ...x, feedbackPending: true } : x)))
    try {
      await postServing('/feedback', { trace_id: t.resp.trace_id, value })
      setTurns((ts) => ts.map((x, j) => (j === i ? { ...x, feedback: value } : x)))
    } catch (e) { setErr(String(e)) } finally {
      setTurns((ts) => ts.map((x, j) => (j === i ? { ...x, feedbackPending: false } : x)))
    }
  }

  return (
    <div className="chat">
      <div className="chat-head">
        <select value={config} onChange={(e) => setConfig(e.target.value)}>
          {configs.map((c) => <option key={c} value={c}>{c}</option>)}
        </select>
        <span className="chat-sub mono">production /ask · traced to LangFuse</span>
      </div>
      {err && <div className="chat-err" role="alert">{err}</div>}
      <div className="chat-log">
        {turns.map((t, i) => (
          <div className="turn" key={i}>
            <div className="q">{t.question}</div>
            <pre className="sql">{t.resp.sql || '(no SQL)'}</pre>
            {t.resp.error && <div className="turn-err">{t.resp.error}</div>}
            {t.resp.rows && (
              <div className="tbl-wrap">
                <table className="tbl">
                  <thead><tr>{(t.resp.columns || []).map((c) => <th key={c}>{c}</th>)}</tr></thead>
                  <tbody>
                    {t.resp.rows.slice(0, 20).map((r, ri) => (
                      <tr key={ri}>{r.map((c, ci) => <td key={ci}>{String(c)}</td>)}</tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
            <div className="meta mono">
              ${Number(t.resp.cost_usd).toFixed(5)} · {t.resp.latency_ms}ms · {t.resp.outcome}
              <span className="fb">
                <button aria-label="Thumbs up" data-on={t.feedback === true}
                        disabled={t.feedbackPending} onClick={() => rate(i, true)}>👍</button>
                <button aria-label="Thumbs down" data-on={t.feedback === false}
                        disabled={t.feedbackPending} onClick={() => rate(i, false)}>👎</button>
                {t.feedbackPending
                  ? <em role="status" data-state="pending">sending…</em>
                  : t.feedback !== null && <em role="status">feedback sent</em>}
              </span>
            </div>
          </div>
        ))}
      </div>
      <div className="chat-input">
        <input value={question} placeholder="Ask a question about the data…"
               onChange={(e) => setQuestion(e.target.value)}
               onKeyDown={(e) => e.key === 'Enter' && ask()} disabled={loading} />
        <button onClick={ask} disabled={loading || !config}>{loading ? '…' : 'Ask'}</button>
      </div>
    </div>
  )
}
