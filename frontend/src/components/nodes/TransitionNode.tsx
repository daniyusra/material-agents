import { Handle, Position, type NodeProps } from '@xyflow/react'
import type { Transition } from '../../parser/types'

const LIFECYCLE_COLOR: Record<string, string> = {
  schedule: '#9CA3AF',
  start: '#F59E0B',
  complete: '#10B981',
}

export default function TransitionNode({ data, selected }: NodeProps) {
  const trans = data as unknown as Transition
  const lcColor = trans.lifecycle ? LIFECYCLE_COLOR[trans.lifecycle] : undefined

  return (
    <div
      style={{
        width: 32,
        height: 100,
        background: '#374151',
        border: `2px solid ${selected ? '#9CA3AF' : '#4B5563'}`,
        outline: selected ? '3px solid rgba(156,163,175,0.4)' : 'none',
        borderRadius: 4,
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        position: 'relative',
        boxSizing: 'border-box',
        overflow: 'visible',
      }}
    >
      <Handle type="target" position={Position.Left} style={{ background: '#9CA3AF' }} />
      <span
        style={{
          fontSize: 9,
          fontWeight: 700,
          color: '#D1D5DB',
          writingMode: 'vertical-rl',
          textOrientation: 'mixed',
          transform: 'rotate(180deg)',
          maxHeight: 88,
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
          cursor: 'default',
        }}
        title={trans.name}
      >
        {trans.name}
      </span>
      {lcColor && (
        <span
          style={{
            position: 'absolute',
            top: 4,
            right: 4,
            width: 7,
            height: 7,
            borderRadius: '50%',
            background: lcColor,
          }}
          title={trans.lifecycle}
        />
      )}
      <Handle type="source" position={Position.Right} style={{ background: '#9CA3AF' }} />
    </div>
  )
}
