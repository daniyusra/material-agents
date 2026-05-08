import type { MaimlDocument } from '../parser/types'

interface Props {
  doc: MaimlDocument
}

export default function DocumentHeader({ doc }: Props) {
  const parts = [
    doc.instrument && { label: 'Instrument', value: doc.instrument },
    doc.vendor && { label: 'Vendor', value: doc.vendor },
    doc.owner && { label: 'Owner', value: doc.owner },
    doc.date && { label: 'Date', value: doc.date },
  ].filter(Boolean) as { label: string; value: string }[]

  return (
    <div
      style={{
        height: 40,
        background: '#0d1219',
        borderBottom: '1px solid #192534',
        display: 'flex',
        alignItems: 'center',
        padding: '0 16px',
        gap: 20,
        flexShrink: 0,
        overflow: 'hidden',
      }}
    >
      <span style={{ fontSize: 12, fontWeight: 700, color: '#22c9c9', marginRight: 4 }}>
        {doc.name}
      </span>
      {parts.map(({ label, value }) => (
        <span key={label} style={{ fontSize: 11, color: '#7090a8', whiteSpace: 'nowrap' }}>
          <span style={{ color: '#405060', marginRight: 3 }}>{label}:</span>
          {value}
        </span>
      ))}
    </div>
  )
}
