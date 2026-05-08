import Dagre from '@dagrejs/dagre'
import type { Node, Edge } from '@xyflow/react'
import type { PetriNet, Place, Transition } from '../parser/types'

const PLACE_W = 140
const PLACE_H = 80
const TRANS_W = 32
const TRANS_H = 100

export function buildReactFlowGraph(petriNet: PetriNet): {
  nodes: Node[]
  edges: Edge[]
} {
  const g = new Dagre.graphlib.Graph()
  g.setDefaultEdgeLabel(() => ({}))
  g.setGraph({ rankdir: 'LR', nodesep: 60, ranksep: 100 })

  for (const place of petriNet.places) {
    g.setNode(place.id, { width: PLACE_W, height: PLACE_H })
  }
  for (const trans of petriNet.transitions) {
    g.setNode(trans.id, { width: TRANS_W, height: TRANS_H })
  }
  for (const arc of petriNet.arcs) {
    g.setEdge(arc.source, arc.target, { id: arc.id })
  }

  Dagre.layout(g)

  const placeNodes: Node[] = petriNet.places.map(place => {
    const pos = g.node(place.id)
    return {
      id: place.id,
      type: place.placeType === 'unknown' ? 'result' : place.placeType,
      position: { x: pos.x - PLACE_W / 2, y: pos.y - PLACE_H / 2 },
      data: place as unknown as Record<string, unknown>,
    }
  })

  const transNodes: Node[] = petriNet.transitions.map(trans => {
    const pos = g.node(trans.id)
    return {
      id: trans.id,
      type: 'transition',
      position: { x: pos.x - TRANS_W / 2, y: pos.y - TRANS_H / 2 },
      data: trans as unknown as Record<string, unknown>,
    }
  })

  const edges: Edge[] = petriNet.arcs.map(arc => ({
    id: arc.id,
    source: arc.source,
    target: arc.target,
    animated: true,
    markerEnd: { type: 'arrowclosed' as const },
  }))

  return { nodes: [...placeNodes, ...transNodes], edges }
}
