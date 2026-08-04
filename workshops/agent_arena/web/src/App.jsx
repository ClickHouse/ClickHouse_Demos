import { useState } from 'react'
import Leaderboard from './leaderboard/Leaderboard.jsx'
import Chat from './chat/Chat.jsx'
import { BrandLock, Icon } from './ui.jsx'

const NAV = [
  { id: 'leaderboard', label: 'Leaderboard', icon: 'trophy' },
  { id: 'chat', label: 'Chat', icon: 'bolt' },
]

export default function App() {
  const [tab, setTab] = useState('leaderboard')
  // Keep a view mounted after its first visit so learner interactions persist.
  const [visited, setVisited] = useState({ leaderboard: true })
  const goTab = (id) => { setTab(id); setVisited((v) => ({ ...v, [id]: true })) }

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
        <span className="app-sub mono">Model contest for NL→SQL · LangFuse × ClickHouse</span>
      </header>

      <main className="app-body">
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
