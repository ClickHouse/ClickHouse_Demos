// Shared presentational atoms for AgentArena (Direction A · Broadcast).
import { famOf, splitConfig, promptMeta, outcomeMeta } from './meta.js'

/* ---------------- ClickHouse logomark (inline SVG, colorable) ---------------- */
export function CHLogoMark({ size = 24, color = 'var(--accent)' }) {
  return (
    <svg width={size} height={size} viewBox="0 0 100 100" aria-hidden="true">
      <g fill={color}>
        <rect x="6"  y="12" width="12" height="76" rx="3" />
        <rect x="27" y="12" width="12" height="76" rx="3" />
        <rect x="48" y="12" width="12" height="76" rx="3" />
        <rect x="69" y="12" width="12" height="76" rx="3" />
        <rect x="86" y="44" width="9"  height="20" rx="2.5" />
      </g>
    </svg>
  )
}

export function BrandLock({ compact }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
      <CHLogoMark size={compact ? 20 : 23} />
      <div style={{ fontFamily: 'var(--f-display)', fontWeight: 700, fontSize: compact ? 15 : 16.5, letterSpacing: '-0.01em', whiteSpace: 'nowrap' }}>
        Agent<span className="hl">Arena</span>
      </div>
    </div>
  )
}

/* ---------------- icon set ---------------- */
export function Icon({ name, size = 18, color = 'currentColor', strokeWidth = 1.7 }) {
  const p = { width: size, height: size, viewBox: '0 0 24 24', fill: 'none', stroke: color, strokeWidth, strokeLinecap: 'round', strokeLinejoin: 'round' }
  switch (name) {
    case 'trophy': return <svg {...p}><path d="M7 4h10v4a5 5 0 0 1-10 0V4Z"/><path d="M7 6H4v1a3 3 0 0 0 3 3M17 6h3v1a3 3 0 0 1-3 3M9 18h6M10 18v-3M14 18v-3M8 21h8"/></svg>
    case 'ext': return <svg {...p}><path d="M14 5h5v5M19 5l-8 8M18 13v5a1 1 0 0 1-1 1H6a1 1 0 0 1-1-1V7a1 1 0 0 1 1-1h5"/></svg>
    case 'chev': return <svg {...p}><path d="M9 6l6 6-6 6"/></svg>
    case 'bolt': return <svg {...p}><path d="M13 2 4 14h7l-1 8 9-12h-7l1-8Z"/></svg>
    case 'dollar': return <svg {...p}><path d="M12 3v18M16 7c0-1.7-1.8-3-4-3s-4 1.3-4 3 1.8 3 4 3 4 1.3 4 3-1.8 3-4 3-4-1.3-4-3"/></svg>
    case 'scatter': return <svg {...p}><path d="M4 4v16h16"/><circle cx="9" cy="14" r="1.4"/><circle cx="13" cy="9" r="1.4"/><circle cx="17" cy="11" r="1.4"/><circle cx="8" cy="9" r="1.4"/></svg>
    case 'grid': return <svg {...p}><rect x="3" y="3" width="7" height="7" rx="1.4"/><rect x="14" y="3" width="7" height="7" rx="1.4"/><rect x="3" y="14" width="7" height="7" rx="1.4"/><rect x="14" y="14" width="7" height="7" rx="1.4"/></svg>
    case 'chart': return <svg {...p}><path d="M4 20V4M4 20h16M8 17v-5M12 17V8M16 17v-8"/></svg>
    default: return <svg {...p}><circle cx="12" cy="12" r="8"/></svg>
  }
}

/* ---------------- model / prompt / outcome chips ---------------- */
export function FamDot({ model, size = 9 }) {
  const fam = famOf(model)
  return <span style={{ width: size, height: size, borderRadius: 3, background: fam.color, boxShadow: `0 0 10px -2px ${fam.color}`, flex: 'none', display: 'inline-block' }} />
}

// config_id → family dot + model label + prompt tag (with tooltip via react-tooltip id="tip")
export function ConfigName({ config_id, showPrompt = true, size = 14, dot = true, promptDesc = {} }) {
  const { model, prompt } = splitConfig(config_id)
  const pm = promptMeta(prompt, promptDesc)
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 9, minWidth: 0 }}>
      {dot && <FamDot model={model} />}
      <span style={{ fontWeight: 600, fontSize: size, whiteSpace: 'nowrap' }}>{model}</span>
      {showPrompt && prompt && (
        <span className="tag mono" style={{ background: 'var(--bg-3)', color: 'var(--ink-2)', border: '1px solid var(--line-2)', cursor: 'help' }}
          data-tooltip-id="tip" data-tooltip-content={`${pm.name} · ${prompt}${pm.desc ? ' — ' + pm.desc : ''}`}>{pm.short}</span>
      )}
    </span>
  )
}

export function OutcomeTag({ outcome }) {
  const m = outcomeMeta(outcome)
  return <span className="tag" style={{ background: `color-mix(in oklab, ${m.color} 20%, transparent)`, color: m.color, border: `1px solid color-mix(in oklab, ${m.color} 45%, transparent)` }}>{m.label}</span>
}

export function WinPill() {
  return <span className="pill-win"><Icon name="trophy" size={11} color="var(--accent-ink)" /> WIN</span>
}

export function Bar({ value, max = 100, color = 'var(--accent)', h = 7, track = 'var(--bg-inset)' }) {
  return (
    <div style={{ background: track, borderRadius: 6, height: h, width: '100%', overflow: 'hidden' }}>
      <div style={{ width: `${Math.max(0, Math.min(100, (value / max) * 100))}%`, height: '100%', background: color, borderRadius: 6, transition: 'width .5s cubic-bezier(.2,.7,.2,1)' }} />
    </div>
  )
}

// heatmap cell color from accuracy 0..100 (green scale, brand-tinted)
export function heatColor(v) {
  if (v == null) return 'transparent'
  const t = v / 100
  const a = 0.1 + t * 0.85
  return `color-mix(in oklab, var(--oc-correct) ${Math.round(a * 100)}%, #0c0f14)`
}
