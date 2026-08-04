import { useEffect, useMemo, useState } from 'react'
import './countdown.css'
import arenaImg from '../assets/arena.jpg'
import { api } from '../api.js'
import { FAMILIES, FIGHTER_FAMILIES, famKeyOf } from '../meta.js'
import { Icon } from '../ui.jsx'

function Embers() {
  const els = useMemo(() => Array.from({ length: 26 }, () => ({
    left: Math.random() * 100, dur: 7 + Math.random() * 9, delay: -Math.random() * 12, size: 2 + Math.random() * 2.5,
  })), [])
  return (
    <div className="cd-embers" aria-hidden="true">
      {els.map((e, i) => (
        <span key={i} className="cd-ember" style={{ left: `${e.left}%`, width: e.size, height: e.size, animationDuration: `${e.dur}s`, animationDelay: `${e.delay}s` }} />
      ))}
    </div>
  )
}

function Sparks() {
  const els = useMemo(() => Array.from({ length: 16 }, () => {
    const a = Math.random() * Math.PI * 2, d = 40 + Math.random() * 120
    return { dx: Math.cos(a) * d, dy: Math.sin(a) * d, delay: -Math.random() * 1.6 }
  }), [])
  return <>{els.map((s, i) => <span key={i} className="cd-spark" style={{ '--dx': `${s.dx}px`, '--dy': `${s.dy}px`, animationDelay: `${s.delay}s` }} />)}</>
}

function Confetti() {
  const els = useMemo(() => Array.from({ length: 90 }, () => ({
    left: Math.random() * 100, dur: 2.4 + Math.random() * 2.4, delay: Math.random() * 2,
    c: ['var(--accent)', 'var(--fam-claude)', 'var(--fam-qwen)', 'var(--fam-deepseek)', 'var(--fam-kimi)', '#fff'][Math.floor(Math.random() * 6)],
  })), [])
  return (
    <div className="cd-confetti" aria-hidden="true">
      {els.map((e, i) => <span key={i} className="cd-conf" style={{ left: `${e.left}%`, background: e.c, animationDuration: `${e.dur}s`, animationDelay: `${e.delay}s` }} />)}
    </div>
  )
}

export default function Countdown({ duration, remaining, running, onPreset, onToggle, onReset }) {
  const [bets, setBets] = useState(() => new Set())
  // best execution accuracy per family, from the most recent run (optional flair)
  const [bestByFam, setBestByFam] = useState({})
  useEffect(() => {
    let alive = true
    api('/api/runs')
      .then((runs) => (runs && runs.length ? api(`/api/leaderboard?run_id=${encodeURIComponent(runs[0])}`) : []))
      .then((board) => {
        if (!alive) return
        const best = {}
        board.forEach((r) => {
          const k = famKeyOf(r.model_name)
          const acc = Number(r.accuracy) * 100
          if (!(k in best) || acc > best[k]) best[k] = acc
        })
        setBestByFam(best)
      })
      .catch(() => {})
    return () => { alive = false }
  }, [])

  const glints = useMemo(() => Array.from({ length: 10 }, () => ({
    left: 15 + Math.random() * 70, top: 30 + Math.random() * 50, dur: 1.4 + Math.random() * 1.8, delay: -Math.random() * 2.5,
  })), [])

  const live = remaining <= 0
  const mm = String(Math.floor(Math.max(0, remaining) / 60)).padStart(2, '0')
  const ss = String(Math.max(0, remaining) % 60).padStart(2, '0')
  const digits = [mm[0], mm[1], ':', ss[0], ss[1]]
  const toggleBet = (k) => setBets((prev) => { const n = new Set(prev); n.has(k) ? n.delete(k) : n.add(k); return n })

  return (
    <div className={`cd${live ? ' live' : ''}`}>
      <Embers />
      {live && <Confetti />}

      {/* LEFT (≈70%): title + the arena image */}
      <div className="cd-left">
        <div className="cd-titleblock">
          <div className="cd-kicker"><span className="dmd">◆</span> AgentArena · Live NL→SQL Contest <span className="dmd">◆</span></div>
          <h1 className="cd-head">
            {live ? <>⚔ The <span className="glow">Arena</span> is Live ⚔</> : <>Guess who will <span className="glow">win</span> the Arena?</>}
          </h1>
        </div>

        {/* stage — the original AI-generated arena, framed, with sword/beam VFX */}
        <div className="cd-stage">
        <img className="cd-arena-img" src={arenaImg} alt="AgentArena — models battle for NL→SQL supremacy" />
        <div className="cd-vfx" aria-hidden="true">
          <div className="beam b1" /><div className="beam b2" /><div className="beam b3" /><div className="beam b4" />
          <div className="clash" />
          <Sparks />
          {glints.map((g, i) => (
            <span key={i} className="cd-glint" style={{ left: `${g.left}%`, top: `${g.top}%`, animationDuration: `${g.dur}s`, animationDelay: `${g.delay}s` }} />
          ))}
        </div>
        </div>
      </div>

      {/* RIGHT (≈30%): the countdown timer + presenter controls */}
      <div className="cd-right">
      {/* timer */}
      <div className="cd-timerwrap">
        <div className="cd-timerlabel">{live ? 'Fight!' : 'Demo starts in'}</div>
        <div className="cd-timer">
          {digits.map((d, i) => d === ':' ? <span key={i} className="cd-colon">:</span> : <span key={i} className="cd-digit">{d}</span>)}
        </div>
      </div>

      {/* bets */}
      <div className="cd-badges">
        {FIGHTER_FAMILIES.map((f) => {
          const fam = FAMILIES[f]
          return <button key={f} className="cd-badge" data-bet={bets.has(f)} style={{ '--c': fam.color }} onClick={() => toggleBet(f)}>{fam.label}</button>
        })}
      </div>
      <div className="cd-bets">
        {bets.size
          ? <>Your pick{bets.size > 1 ? 's' : ''}: <b style={{ color: 'var(--accent)' }}>{[...bets].map((b) => FAMILIES[b].label).join(', ')}</b> 👑</>
          : <>Place your bets — who takes the crown? 👑</>}
      </div>
      {Object.keys(bestByFam).length > 0 && (
        <div className="cd-leaders">
          Top so far —{' '}
          {FIGHTER_FAMILIES.filter((f) => bestByFam[f] != null).map((f, i, arr) => (
            <span key={f}><b style={{ color: FAMILIES[f].color }}>{FAMILIES[f].label}</b> {bestByFam[f].toFixed(0)}%{i < arr.length - 1 ? '  ·  ' : ''}</span>
          ))}
        </div>
      )}

      {/* presenter controls */}
      <div className="cd-controls">
        {[5, 10, 15].map((m) => (
          <button key={m} className="chip" data-on={!running && duration === m * 60} onClick={() => onPreset(m * 60)}>{m}m</button>
        ))}
        <div className="cd-sep" />
        <button className="btn primary" onClick={onToggle}>
          <Icon name={running ? 'clock' : 'play'} size={15} color="var(--accent-ink)" />
          {live ? 'Reset' : running ? 'Pause' : 'Start'}
        </button>
        <button className="btn ghost" onClick={onReset}>Reset</button>
      </div>
      </div>{/* /cd-right */}
    </div>
  )
}
