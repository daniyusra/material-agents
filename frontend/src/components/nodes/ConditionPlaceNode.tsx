import { Handle, Position, type NodeProps } from '@xyflow/react'
import type { Place } from '../../parser/types'

export default function ConditionPlaceNode({ data, selected }: NodeProps) {
  const place = data as unknown as Place
  const hasToken = place.instanceValues.length > 0

  return (
    <div
      style={{
        width: 140,
        height: 80,
        clipPath: 'polygon(50% 0%, 100% 38%, 82% 100%, 18% 100%, 0% 38%)',
        background: '#FEF3C7',
        outline: selected ? '3px solid rgba(217,119,6,0.4)' : 'none',
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        padding: '0 16px',
        position: 'relative',
        boxSizing: 'border-box',
      }}
    >
      <Handle
        type="target"
        position={Position.Left}
        style={{ background: '#D97706', left: 8 }}
      />
      <span
        style={{
          fontSize: 13,
          fontWeight: 700,
          color: '#92400E',
          maxWidth: 100,
          overflow: 'hidden',
          whiteSpace: 'nowrap',
          textOverflow: 'ellipsis',
          textAlign: 'center',
          marginTop: 8,
        }}
        title={place.name}
      >
        {place.name}
      </span>
      <span style={{ fontSize: 10, color: '#B45309', marginTop: 2 }}>condition</span>
      {hasToken && (
        <span
          style={{
            position: 'absolute',
            top: 10,
            right: 24,
            width: 8,
            height: 8,
            borderRadius: '50%',
            background: '#D97706',
          }}
        />
      )}
      <Handle
        type="source"
        position={Position.Right}
        style={{ background: '#D97706', right: 8 }}
      />
    </div>
  )
}
