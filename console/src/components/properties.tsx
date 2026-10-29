import type { ComponentChildren } from 'preact'

import './properties.css'

interface Property {
  label: string
  value: ComponentChildren
}

export function Properties({ items }: { items: Property[] }) {
  return (
    <dl class="properties">
      {items.map(({ label, value }) => (
        <div key={label} class="properties-item">
          <dt class="properties-label">{label}</dt>
          <dd class="properties-value">{value}</dd>
        </div>
      ))}
    </dl>
  )
}
