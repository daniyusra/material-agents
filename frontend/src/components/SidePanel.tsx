import { useCallback, useRef, useState } from 'react'
import type { Place, Transition } from '../parser/types'

interface Props {
  node: Place | Transition
  onClose: () => void
}

function isPlace(n: Place | Transition): n is Place {
  return 'placeType' in n
}

const TYPE_STYLES: Record<string, { bg: string; text: string; label: string }> = {
  material: { bg: '#E1F5EE', text: '#0F6E56', label: 'material' },
  condition: { bg: '#FEF3C7', text: '#92400E', label: 'condition' },
  result: { bg: '#FAECE7', text: '#993C1D', label: 'result' },
  unknown: { bg: '#F3F4F6', text: '#374151', label: 'unknown' },
  transition: { bg: '#E5E7EB', text: '#374151', label: 'transition' },
}

const IMAGE_EXTS = /\.(bmp|jpg|jpeg|png|gif|tif|tiff|webp)$/i
const TEXT_EXTS = /\.(txt|csv|tsv|log)$/i

// ─── PropTable with resizable columns ────────────────────────────────────────

interface PropTableProps {
  rows: { key: string; value: string; units?: string }[]
  maxHeight?: number
}

function PropTable({ rows, maxHeight = 300 }: PropTableProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [keyWidth, setKeyWidth] = useState(110)
  const [unitsWidth, setUnitsWidth] = useState(50)

  // Mirror widths in refs so callbacks always read fresh values without re-creating
  const keyWidthRef = useRef(keyWidth)
  const unitsWidthRef = useRef(unitsWidth)
  keyWidthRef.current = keyWidth
  unitsWidthRef.current = unitsWidth

  const keyDrag = useRef({ active: false, startX: 0, startW: 0 })
  const unitsDrag = useRef({ active: false, startX: 0, startW: 0 })

  // Key column right-edge handle
  const onKeyDown = useCallback((e: React.PointerEvent<HTMLDivElement>) => {
    e.preventDefault()
    e.currentTarget.setPointerCapture(e.pointerId)
    keyDrag.current = { active: true, startX: e.clientX, startW: keyWidthRef.current }
  }, [])

  const onKeyMove = useCallback((e: React.PointerEvent<HTMLDivElement>) => {
    if (!keyDrag.current.active) return
    const containerW = containerRef.current?.clientWidth ?? 400
    const delta = e.clientX - keyDrag.current.startX
    setKeyWidth(Math.max(50, Math.min(containerW - unitsWidthRef.current - 50, keyDrag.current.startW + delta)))
  }, [])

  const onKeyUp = useCallback(() => { keyDrag.current.active = false }, [])

  // Value column right-edge handle (= Units column left border)
  // Dragging right → Value widens, Units narrows (unitsWidth -= delta)
  const onUnitsDown = useCallback((e: React.PointerEvent<HTMLDivElement>) => {
    e.preventDefault()
    e.currentTarget.setPointerCapture(e.pointerId)
    unitsDrag.current = { active: true, startX: e.clientX, startW: unitsWidthRef.current }
  }, [])

  const onUnitsMove = useCallback((e: React.PointerEvent<HTMLDivElement>) => {
    if (!unitsDrag.current.active) return
    const containerW = containerRef.current?.clientWidth ?? 400
    const delta = e.clientX - unitsDrag.current.startX
    setUnitsWidth(Math.max(30, Math.min(containerW - keyWidthRef.current - 50, unitsDrag.current.startW - delta)))
  }, [])

  const onUnitsUp = useCallback(() => { unitsDrag.current.active = false }, [])

  if (rows.length === 0) {
    return <p style={{ fontSize: 12, color: '#405060', fontStyle: 'italic', margin: 0 }}>No values recorded.</p>
  }

  const resizeHandle = (
    onDown: (e: React.PointerEvent<HTMLDivElement>) => void,
    onMove: (e: React.PointerEvent<HTMLDivElement>) => void,
    onUp: (e: React.PointerEvent<HTMLDivElement>) => void,
  ) => (
    <div
      onPointerDown={onDown}
      onPointerMove={onMove}
      onPointerUp={onUp}
      title="Drag to resize column"
      style={{
        position: 'absolute', right: -3, top: 0, bottom: 0, width: 6,
        cursor: 'col-resize', zIndex: 10, userSelect: 'none',
        background: 'transparent', transition: 'background 0.1s',
      }}
      onMouseEnter={e => (e.currentTarget.style.background = 'rgba(34,201,201,0.4)')}
      onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}
    />
  )

  return (
    <div ref={containerRef} style={{ maxHeight, overflowY: 'auto', overflowX: 'hidden', borderRadius: 4, border: '1px solid #192534' }}>
      <table style={{ width: '100%', tableLayout: 'fixed', borderCollapse: 'collapse', fontSize: 12 }}>
        <colgroup>
          <col style={{ width: keyWidth }} />
          <col />
          <col style={{ width: unitsWidth }} />
        </colgroup>
        <thead style={{ position: 'sticky', top: 0, background: '#0d1219', zIndex: 1 }}>
          <tr>
            <th style={{
              textAlign: 'left', padding: '5px 8px', color: '#7090a8', fontWeight: 600,
              borderBottom: '1px solid #192534', borderRight: '1px solid #192534',
              position: 'relative', overflow: 'visible', userSelect: 'none',
            }}>
              Key
              {resizeHandle(onKeyDown, onKeyMove, onKeyUp)}
            </th>
            <th style={{
              textAlign: 'left', padding: '5px 8px', color: '#7090a8', fontWeight: 600,
              borderBottom: '1px solid #192534', borderRight: '1px solid #192534',
              position: 'relative', overflow: 'visible', userSelect: 'none',
            }}>
              Value
              {resizeHandle(onUnitsDown, onUnitsMove, onUnitsUp)}
            </th>
            <th style={{
              textAlign: 'left', padding: '5px 8px', color: '#7090a8', fontWeight: 600,
              borderBottom: '1px solid #192534', userSelect: 'none',
            }}>
              Units
            </th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} style={{ background: i % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.02)' }}>
              <td style={{
                padding: '4px 8px', color: '#c8d8e8',
                fontFamily: 'IBM Plex Mono, monospace', fontSize: 11,
                overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                borderRight: '1px solid #192534',
              }}>
                {r.key}
              </td>
              <td style={{
                padding: '4px 8px', color: r.value ? '#9ee4e4' : '#405060',
                overflow: 'hidden', textOverflow: 'ellipsis', wordBreak: 'break-all',
                borderRight: '1px solid #192534',
              }}>
                {r.value || '—'}
              </td>
              <td style={{ padding: '4px 8px', color: '#7090a8', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {r.units ?? '—'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function SectionLabel({ label, count }: { label: string; count?: number }) {
  return (
    <div style={{ fontSize: 11, fontWeight: 600, color: '#7090a8', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 6, display: 'flex', alignItems: 'center', gap: 6 }}>
      {label}
      {count !== undefined && (
        <span style={{ fontSize: 10, background: '#192534', color: '#7090a8', borderRadius: 999, padding: '1px 6px' }}>{count}</span>
      )}
    </div>
  )
}

function CollapseToggle({ open, onToggle, label, count }: { open: boolean; onToggle: () => void; label: string; count?: number }) {
  return (
    <button
      onClick={onToggle}
      style={{
        display: 'flex', alignItems: 'center', gap: 6,
        background: 'none', border: 'none', cursor: 'pointer', padding: '4px 0',
        color: '#7090a8', fontSize: 11, fontWeight: 600,
        textTransform: 'uppercase', letterSpacing: '0.06em', width: '100%', textAlign: 'left',
      }}
    >
      <span style={{ fontSize: 10, display: 'inline-block', transition: 'transform 0.15s', transform: open ? 'rotate(90deg)' : 'rotate(0deg)' }}>▶</span>
      {label}
      {count !== undefined && (
        <span style={{ fontSize: 10, background: '#192534', color: '#7090a8', borderRadius: 999, padding: '1px 6px' }}>{count}</span>
      )}
    </button>
  )
}

// ─── SidePanel ───────────────────────────────────────────────────────────────

export default function SidePanel({ node, onClose }: Props) {
  const [templateOpen, setTemplateOpen] = useState(false)

  const place = isPlace(node) ? node : null
  const trans = !isPlace(node) ? node : null

  const typeKey = place ? place.placeType : 'transition'
  const style = TYPE_STYLES[typeKey] ?? TYPE_STYLES.unknown

  const templateProps = place?.properties ?? []
  const instanceProps = place?.instanceValues ?? []
  const insertionUri = place?.insertionUri

  const isImage = insertionUri && IMAGE_EXTS.test(insertionUri)
  const isText = insertionUri && TEXT_EXTS.test(insertionUri)
  const filename = insertionUri ? insertionUri.split('/').pop() : undefined

  return (
    <div style={{ width: '100%', height: '100%', background: '#111b27', borderLeft: '1px solid #192534', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
      {/* Header */}
      <div style={{ padding: '12px 16px 8px', borderBottom: '1px solid #192534', display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: 8, flexShrink: 0 }}>
        <div style={{ minWidth: 0 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', marginBottom: 4 }}>
            <h2 style={{ fontSize: 14, fontWeight: 700, color: '#c8d8e8', margin: 0, wordBreak: 'break-all' }}>{node.name}</h2>
            <span style={{ fontSize: 10, fontWeight: 600, padding: '2px 8px', borderRadius: 999, background: style.bg, color: style.text, flexShrink: 0 }}>
              {style.label}
            </span>
          </div>
          {node.description && <p style={{ fontSize: 12, color: '#7090a8', margin: 0 }}>{node.description}</p>}
        </div>
        <button onClick={onClose} style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#7090a8', fontSize: 18, lineHeight: 1, padding: '0 2px', flexShrink: 0 }} aria-label="Close">×</button>
      </div>

      <div style={{ flex: 1, overflowY: 'auto', padding: '12px 16px', display: 'flex', flexDirection: 'column', gap: 16 }}>

        {/* Transition event */}
        {trans && trans.firedAt && (
          <div>
            <SectionLabel label="Event" />
            <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
              <div style={{ fontSize: 12, color: '#c8d8e8' }}>
                <span style={{ color: '#7090a8' }}>timestamp: </span>{trans.firedAt}
              </div>
              {trans.lifecycle && (
                <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12 }}>
                  <span style={{ color: '#7090a8' }}>lifecycle: </span>
                  <span style={{
                    display: 'inline-flex', alignItems: 'center', gap: 5, fontWeight: 600,
                    color: trans.lifecycle === 'complete' ? '#10B981' : trans.lifecycle === 'start' ? '#F59E0B' : '#9CA3AF',
                  }}>
                    <span style={{
                      width: 8, height: 8, borderRadius: '50%',
                      background: trans.lifecycle === 'complete' ? '#10B981' : trans.lifecycle === 'start' ? '#F59E0B' : '#9CA3AF',
                    }} />
                    {trans.lifecycle}
                  </span>
                </div>
              )}
            </div>
          </div>
        )}

        {/* SEM image */}
        {isImage && insertionUri && (
          <div>
            <SectionLabel label="SEM Image" />
            <a href={insertionUri} target="_blank" rel="noreferrer" title="Open full size in new tab">
              <img src={insertionUri} alt="SEM" style={{ width: '100%', borderRadius: 4, border: '1px solid #192534', display: 'block', cursor: 'zoom-in' }} />
            </a>
            <p style={{ fontSize: 11, color: '#405060', margin: '4px 0 0', fontStyle: 'italic' }}>{filename} · click to open full size</p>
          </div>
        )}

        {/* Text file link */}
        {isText && !isImage && insertionUri && (
          <div>
            <SectionLabel label="Attached File" />
            <a href={insertionUri} target="_blank" rel="noreferrer" style={{ fontSize: 12, color: '#22c9c9', display: 'inline-flex', alignItems: 'center', gap: 6, textDecoration: 'none' }}>
              <span>📄</span>{filename}
            </a>
          </div>
        )}

        {/* Actual values — columns are resizable */}
        {place && (
          <div>
            <SectionLabel label="Actual values" count={instanceProps.length || undefined} />
            {instanceProps.length > 0
              ? <PropTable rows={instanceProps.map(p => ({ key: p.key, value: p.value, units: p.units }))} maxHeight={300} />
              : <p style={{ fontSize: 12, color: '#405060', fontStyle: 'italic', margin: 0 }}>No measured values in this run.</p>
            }
          </div>
        )}

        {/* Template schema — collapsible */}
        {place && templateProps.length > 0 && (
          <div>
            <CollapseToggle open={templateOpen} onToggle={() => setTemplateOpen(v => !v)} label="Template schema" count={templateProps.length} />
            {templateOpen && (
              <div style={{ marginTop: 6 }}>
                <PropTable rows={templateProps.map(p => ({ key: p.key, value: p.value, units: p.units }))} maxHeight={Math.min(templateProps.length * 29 + 38, 340)} />
              </div>
            )}
          </div>
        )}

      </div>
    </div>
  )
}
