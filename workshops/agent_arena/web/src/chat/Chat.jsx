import { useEffect, useState } from 'react'
import { getServing, postServing } from '../api.js'
import './chat.css'

// stable per-mount conversation id so a chat's turns group into one LangFuse session
const SESSION_ID = 'chat-' + Math.random().toString(36).slice(2, 12)

export default function Chat() {
  const [configs, setConfigs] = useState([])
  const [config, setConfig] = useState('')
  const [question, setQuestion] = useState('')
  const [turns, setTurns] = useState([])      // {question, resp, feedback}
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
      setTurns((t) => [...t, { question, resp, feedback: null }])
      setQuestion('')
    } catch (e) { setErr(String(e)) } finally { setLoading(false) }
  }

  async function rate(i, value) {
    const t = turns[i]
    if (!t.resp.trace_id) return
    try {
      await postServing('/feedback', { trace_id: t.resp.trace_id, value })
      setTurns((ts) => ts.map((x, j) => (j === i ? { ...x, feedback: value } : x)))
    } catch (e) { setErr(String(e)) }
  }

  return (
    <div className="chat">
      <div className="chat-head">
        <select value={config} onChange={(e) => setConfig(e.target.value)}>
          {configs.map((c) => <option key={c} value={c}>{c}</option>)}
        </select>
        <span className="chat-sub mono">production /ask · traced to LangFuse</span>
      </div>
      {err && <div className="chat-err">{err}</div>}
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
                <button data-on={t.feedback === 1} onClick={() => rate(i, 1)}>👍</button>
                <button data-on={t.feedback === 0} onClick={() => rate(i, 0)}>👎</button>
                {t.feedback !== null && <em>feedback sent</em>}
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
