import { useCallback, useMemo, useRef, useState } from 'react'
import {
  ReactFlow,
  Controls,
  Background,
  BackgroundVariant,
  type Node,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'

import semXml from '../../parser/__fixtures__/sem.maiml?raw'
import { parseMaiml } from '../../parser/parseMaiml'
import { buildReactFlowGraph } from '../../graph/layout'
import type { Place, Transition } from '../../parser/types'
import MaterialPlaceNode from '../../components/nodes/MaterialPlaceNode'
import ConditionPlaceNode from '../../components/nodes/ConditionPlaceNode'
import ResultPlaceNode from '../../components/nodes/ResultPlaceNode'
import TransitionNode from '../../components/nodes/TransitionNode'
import SidePanel from '../../components/SidePanel'
import DocumentHeader from '../../components/DocumentHeader'

const nodeTypes = {
  material: MaterialPlaceNode,
  condition: ConditionPlaceNode,
  result: ResultPlaceNode,
  transition: TransitionNode,
}

const maimlDoc = parseMaiml(semXml)
const MIN_PANEL_W = 240
const MAX_PANEL_W = 640

export default function GraphCanvas() {
  const [activeMethodIdx, setActiveMethodIdx] = useState(0)
  const [selectedNode, setSelectedNode] = useState<Place | Transition | null>(null)
  const [panelWidth, setPanelWidth] = useState(320)
  const panelDrag = useRef({ active: false, startX: 0, startW: 0 })

  const petriNet = maimlDoc.petriNets[activeMethodIdx]

  const { nodes, edges } = useMemo(
    () => (petriNet ? buildReactFlowGraph(petriNet) : { nodes: [], edges: [] }),
    [petriNet],
  )

  const handleNodeClick = useCallback((_e: React.MouseEvent, node: Node) => {
    setSelectedNode(node.data as unknown as Place | Transition)
  }, [])

  const handlePaneClick = useCallback(() => setSelectedNode(null), [])

  // Panel width drag — pointer capture means no document listeners needed
  const onPanelResizeDown = useCallback((e: React.PointerEvent<HTMLDivElement>) => {
    e.currentTarget.setPointerCapture(e.pointerId)
    panelDrag.current = { active: true, startX: e.clientX, startW: panelWidth }
  }, [panelWidth])

  const onPanelResizeMove = useCallback((e: React.PointerEvent<HTMLDivElement>) => {
    if (!panelDrag.current.active) return
    const delta = panelDrag.current.startX - e.clientX
    setPanelWidth(Math.max(MIN_PANEL_W, Math.min(MAX_PANEL_W, panelDrag.current.startW + delta)))
  }, [])

  const onPanelResizeUp = useCallback(() => {
    panelDrag.current.active = false
  }, [])

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100vh', background: '#080a0c' }}>
      <DocumentHeader doc={maimlDoc} />

      {maimlDoc.petriNets.length > 1 && (
        <div style={{ display: 'flex', gap: 2, padding: '4px 12px', background: '#0d1219', borderBottom: '1px solid #192534' }}>
          {maimlDoc.petriNets.map((net, i) => (
            <button
              key={net.id}
              onClick={() => { setActiveMethodIdx(i); setSelectedNode(null) }}
              style={{
                fontSize: 12, padding: '3px 12px', borderRadius: 4, border: 'none', cursor: 'pointer',
                background: i === activeMethodIdx ? '#22c9c9' : 'transparent',
                color: i === activeMethodIdx ? '#080a0c' : '#7090a8',
                fontWeight: i === activeMethodIdx ? 700 : 400,
              }}
            >
              {net.methodName}
            </button>
          ))}
        </div>
      )}

      <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
        <div style={{ flex: 1, height: '100%', minWidth: 0 }}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            onNodeClick={handleNodeClick}
            onPaneClick={handlePaneClick}
            fitView
            fitViewOptions={{ padding: 0.2 }}
            proOptions={{ hideAttribution: true }}
          >
            <Controls />
            <Background variant={BackgroundVariant.Dots} color="#192534" gap={20} />
          </ReactFlow>
        </div>

        {/* Resizable panel container */}
        <div style={{ position: 'relative', width: panelWidth, flexShrink: 0, height: '100%' }}>
          {/* Drag handle — sits on the left border */}
          <div
            onPointerDown={onPanelResizeDown}
            onPointerMove={onPanelResizeMove}
            onPointerUp={onPanelResizeUp}
            title="Drag to resize panel"
            style={{
              position: 'absolute',
              left: 0,
              top: 0,
              bottom: 0,
              width: 6,
              cursor: 'col-resize',
              zIndex: 20,
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              justifyContent: 'center',
              gap: 3,
              background: 'transparent',
              transition: 'background 0.12s',
            }}
            onMouseEnter={e => (e.currentTarget.style.background = 'rgba(34,201,201,0.25)')}
            onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}
          >
            {[0, 1, 2].map(i => (
              <span key={i} style={{ width: 2, height: 2, borderRadius: '50%', background: '#405060', flexShrink: 0 }} />
            ))}
          </div>

          {selectedNode ? (
            <SidePanel node={selectedNode} onClose={() => setSelectedNode(null)} />
          ) : (
            <div style={{
              width: '100%', height: '100%', background: '#111b27',
              borderLeft: '1px solid #192534',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
            }}>
              <p style={{ fontSize: 13, color: '#405060', fontStyle: 'italic', textAlign: 'center', padding: '0 24px' }}>
                Select a node to inspect its properties
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
