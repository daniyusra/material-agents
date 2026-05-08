import { Handle, Position, type NodeProps } from '@xyflow/react'
import type { Place } from '../../parser/types'

export default function ResultPlaceNode({ data, selected }: NodeProps) {
  const place = data as unknown as Place
  const hasToken = place.instanceValues.length > 0

  return (
    <div
      style={{
        width: 140,
        height: 80,
        borderRadius: 10,
        background: '#FAECE7',
        border: `2px solid ${selected ? '#993C1D' : '#993C1D'}`,
        outline: selected ? '3px solid rgba(153,60,29,0.35)' : 'none',
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        padding: '0 12px',
        position: 'relative',
        boxSizing: 'border-box',
      }}
    >
      <Handle type="target" position={Position.Left} style={{ background: '#993C1D' }} />
      <span
        style={{
          fontSize: 13,
          fontWeight: 700,
          color: '#993C1D',
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
      <span style={{ fontSize: 10, color: '#b84e2b', marginTop: 2 }}>result</span>
      {hasToken && (
        <span
          style={{
            position: 'absolute',
            top: 6,
            right: 10,
            width: 8,
            height: 8,
            borderRadius: '50%',
            background: '#993C1D',
          }}
        />
      )}
      <Handle type="source" position={Position.Right} style={{ background: '#993C1D' }} />
    </div>
  )
}
