import dagre from '@dagrejs/dagre'

// Layered left->right layout with crossing minimization (dagre's barycenter
// ordering). Returns nodes with computed positions.
export function layoutLR(nodes, edges) {
  const g = new dagre.graphlib.Graph()
  g.setGraph({ rankdir: 'LR', nodesep: 46, ranksep: 130, marginx: 50, marginy: 50, ranker: 'tight-tree' })
  g.setDefaultEdgeLabel(() => ({}))

  nodes.forEach((n) => g.setNode(n.id, { width: n.width, height: n.height }))
  edges.forEach((e) => g.setEdge(e.source, e.target))

  dagre.layout(g)

  return nodes.map((n) => {
    const p = g.node(n.id)
    return { ...n, position: { x: p.x - n.width / 2, y: p.y - n.height / 2 } }
  })
}
