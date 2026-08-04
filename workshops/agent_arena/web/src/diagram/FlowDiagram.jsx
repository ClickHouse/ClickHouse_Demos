import { useMemo } from 'react'
import {
  ReactFlow, Background, Controls, MiniMap, Panel, MarkerType,
  useNodesState, useEdgesState,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'

import { componentNodes, edges as rawEdges, EDGE_COLORS, ENV } from './graph.js'
import { layoutLR } from './layout.js'
import CardNode from './nodes/CardNode.jsx'
import AnimatedFlowEdge from './edges/AnimatedFlowEdge.jsx'

const nodeTypes = { card: CardNode }
const edgeTypes = { flow: AnimatedFlowEdge }

const FLOW_LEGEND = [
  ['OLTP + CDC data', EDGE_COLORS.data],
  ['OpenRouter inference', EDGE_COLORS.ai],
  ['read-only query', EDGE_COLORS.read],
  ['orchestration', EDGE_COLORS.control],
  ['LangFuse traces', EDGE_COLORS.trace],
]

export default function FlowDiagram() {
  const initialNodes = useMemo(() => layoutLR(componentNodes, rawEdges), [])
  const initialEdges = useMemo(
    () => rawEdges.map((e) => ({
      ...e,
      markerEnd: { type: MarkerType.ArrowClosed, color: e.data.color, width: 15, height: 15 },
    })),
    [],
  )

  const [nodes, , onNodesChange] = useNodesState(initialNodes)
  const [edges, , onEdgesChange] = useEdgesState(initialEdges)

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      nodeTypes={nodeTypes}
      edgeTypes={edgeTypes}
      fitView
      fitViewOptions={{ padding: 0.1 }}
      minZoom={0.2}
      proOptions={{ hideAttribution: true }}
      nodesDraggable
      elevateEdgesOnSelect
    >
      <Background color="#1c2230" gap={26} size={1.5} />
      <Controls showInteractive={false} />
      <MiniMap pannable zoomable nodeColor={(n) => n.data?.accent || '#888'}
        maskColor="rgba(14,17,22,0.7)" style={{ background: '#161b22' }} />

      <Panel position="top-right" className="legend-panel">
        <div className="legend-head">Runs on</div>
        {Object.values(ENV).map((envv) => (
          <div key={envv.label} className="legend-row">
            <span className="legend-dot" style={{ background: envv.color }} />
            {envv.label}
          </div>
        ))}
        <div className="legend-head" style={{ marginTop: 8 }}>Flow type</div>
        {FLOW_LEGEND.map(([label, color]) => (
          <div key={label} className="legend-row">
            <span className="legend-swatch" style={{ background: color }} />
            {label}
          </div>
        ))}
      </Panel>
    </ReactFlow>
  )
}
