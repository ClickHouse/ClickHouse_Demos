import { BaseEdge, EdgeLabelRenderer, getSmoothStepPath } from '@xyflow/react'

// A data-flow edge: a faint orthogonal base path, a moving dashed overlay, and a
// packet (circle) traveling along it to convey direction. Smoothstep routing
// keeps edges in clean horizontal/vertical lanes (far fewer visual crossings
// than beziers).
export default function AnimatedFlowEdge({
  id, sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition,
  label, data, markerEnd,
}) {
  const [path, labelX, labelY] = getSmoothStepPath({
    sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition,
    borderRadius: 12,
  })
  const color = data?.color || '#8b96a8'

  return (
    <>
      <BaseEdge id={id} path={path} markerEnd={markerEnd}
        style={{ stroke: color, strokeOpacity: 0.3, strokeWidth: 2 }} />
      <path d={path} fill="none" stroke={color} strokeWidth={2}
        strokeDasharray="5 9" strokeLinecap="round" className="flow-dash" />
      <circle r={4.5} fill={color} className="flow-packet">
        <animateMotion dur="2.8s" repeatCount="indefinite" path={path} rotate="auto" />
      </circle>
      {label && (
        <EdgeLabelRenderer>
          <div
            className="edge-label nodrag nopan"
            style={{
              transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`,
              '--edge-color': color,
            }}
          >
            {label}
            {data?.detail && <span className="edge-tip">{data.detail}</span>}
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  )
}
