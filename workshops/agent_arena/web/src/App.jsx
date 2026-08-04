import { useEffect, useState } from 'react'
import FlowDiagram from './diagram/FlowDiagram.jsx'
import Leaderboard from './leaderboard/Leaderboard.jsx'
import Countdown from './countdown/Countdown.jsx'
import Chat from './chat/Chat.jsx'
import { BrandLock, Icon } from './ui.jsx'

const NAV = [
  { id: 'countdown', label: 'Countdown', icon: 'clock' },
  { id: 'architecture', label: 'Architecture', icon: 'flow' },
  { id: 'leaderboard', label: 'Leaderboard', icon: 'trophy' },
  { id: 'chat', label: 'Chat', icon: 'bolt' },
]

function fmtClock(s) {
  s = Math.max(0, s)
  return `${String(Math.floor(s / 60)).padStart(2, '0')}:${String(s % 60).padStart(2, '0')}`
}

export default function App() {
  const [tab, setTab] = useState('countdown')
  // mount each view the first time its tab is shown (so React Flow's fitView
  // measures a real container); keep them mounted after to preserve state.
  const [visited, setVisited] = useState({ countdown: true })
  const goTab = (id) => { setTab(id); setVisited((v) => ({ ...v, [id]: true })) }

  // ---- timer (owned here so it keeps running across tab switches) ----
  const [duration, setDuration] = useState(600)
  const [remaining, setRemaining] = useState(600)
  const [running, setRunning] = useState(false)
  useEffect(() => {
    if (!running) return
    const t = setInterval(() => setRemaining((r) => {
      if (r <= 1) { clearInterval(t); setRunning(false); return 0 }
      return r - 1
    }), 1000)
    return () => clearInterval(t)
  }, [running])
  const onPreset = (sec) => { setRunning(false); setDuration(sec); setRemaining(sec) }
  const onToggle = () => { if (remaining <= 0) { setRemaining(duration); setRunning(false) } else setRunning((r) => !r) }
  const onReset = () => { setRunning(false); setRemaining(duration) }
  const cdProps = { duration, remaining, running, onPreset, onToggle, onReset }

  return (
    <div className="app" data-direction="a">
      <header className="appbar">
        <div className="brand"><BrandLock /></div>
        <nav className="tabs">
          {NAV.map((n) => (
            <button key={n.id} className="tab" data-on={tab === n.id} onClick={() => goTab(n.id)}>
              <Icon name={n.icon} size={15} color={tab === n.id ? 'var(--accent-ink)' : 'currentColor'} />{n.label}
            </button>
          ))}
        </nav>
        <div className="spacer" />
        {tab !== 'countdown' && (
          <div className={`cd-mini${running ? ' run' : ''}`}>
            <Icon name="clock" size={13} color={running ? 'var(--accent)' : 'var(--ink-3)'} />
            <span className="d">{fmtClock(remaining)}</span>
          </div>
        )}
        <span className="app-sub mono">Model contest for NL→SQL · LangFuse × ClickHouse</span>
      </header>

      <main className="app-body">
        <div className="view-fill" style={{ display: tab === 'countdown' ? 'block' : 'none' }}>
          <Countdown {...cdProps} />
        </div>
        {visited.architecture && (
          <div className="view-fill" style={{ display: tab === 'architecture' ? 'block' : 'none' }}>
            <FlowDiagram />
          </div>
        )}
        {visited.leaderboard && (
          <div style={{ display: tab === 'leaderboard' ? 'block' : 'none' }}>
            <Leaderboard />
          </div>
        )}
        {visited.chat && (
          <div style={{ display: tab === 'chat' ? 'block' : 'none', height: '100%' }}>
            <Chat />
          </div>
        )}
      </main>
    </div>
  )
}
