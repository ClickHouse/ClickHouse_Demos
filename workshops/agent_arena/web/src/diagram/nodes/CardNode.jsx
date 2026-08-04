import { Handle, Position } from '@xyflow/react'

const ICONS = {
  user: '🧑‍💻', api: '🛰️', job: '⚙️', agent: '🤖', data: '📄',
  otel: '📡', db: '🗄️', ai: '🧠', view: '🔎', trace: '📊',
}

const SIDES = [
  ['s-left', 'source', Position.Left], ['t-left', 'target', Position.Left],
  ['s-right', 'source', Position.Right], ['t-right', 'target', Position.Right],
  ['s-top', 'source', Position.Top], ['t-top', 'target', Position.Top],
  ['s-bottom', 'source', Position.Bottom], ['t-bottom', 'target', Position.Bottom],
]

export default function CardNode({ data }) {
  return (
    <div className="card-node" style={{ '--accent': data.accent }}>
      {SIDES.map(([id, type, position]) => (
        <Handle key={id} id={id} type={type} position={position} className="card-handle" />
      ))}
      <div className="card-icon">{ICONS[data.kind] || '⬡'}</div>
      <div className="card-text">
        <div className="card-title">{data.title}</div>
        <div className="card-sub">{data.subtitle}</div>
      </div>
    </div>
  )
}
