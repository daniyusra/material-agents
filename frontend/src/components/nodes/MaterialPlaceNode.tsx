import { Handle, Position, type NodeProps } from '@xyflow/react'
import type { Place } from '../../parser/types'

export default function MaterialPlaceNode({ data, selected }: NodeProps) {
  const place = data as unknown as Place
  const hasToken = place.instanceValues.length > 0

  return (
    <div
      style={{
        width: 140,
        height: 80,
        borderRadius: '50%',
        background: '#E1F5EE',
        border: `2px solid ${selected ? '#0F6E56' : '#0F6E56'}`,
        outline: selected ? '3px solid rgba(15,110,86,0.4)' : 'none',
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        padding: '0 12px',
        position: 'relative',
        boxSizing: 'border-box',
      }}
    >
      <Handle type="target" position={Position.Left} style={{ background: '#0F6E56' }} />
      <span
        style={{
          fontSize: 13,
          fontWeight: 700,
          color: '#0F6E56',
          maxWidth: 120,
          overflow: 'hidden',
          whiteSpace: 'nowrap',
          textOverflow: 'ellipsis',
          textAlign: 'center',
        }}
        title={place.name}
      >
        {place.name}
      </span>
      <span style={{ fontSize: 10, color: '#3a8a6e', marginTop: 2 }}>material</span>
      {hasToken && (
        <span
          style={{
            position: 'absolute',
            top: 6,
            right: 14,
            width: 8,
            height: 8,
            borderRadius: '50%',
            background: '#0F6E56',
          }}
        />
      )}
      <Handle type="source" position={Position.Right} style={{ background: '#0F6E56' }} />
    </div>
  )
}
