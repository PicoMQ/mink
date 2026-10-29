import type { ComponentChildren } from 'preact'

import './label.css'

export function Label({ text, children }: { text: string; children: ComponentChildren }) {
  return (
    <label class="label">
      <small>{text}</small>
      {children}
    </label>
  )
}
