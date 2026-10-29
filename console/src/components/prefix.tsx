import type { ComponentChildren } from 'preact'

import './prefix.css'

export function Prefix({ text, id, children }: { text: string | undefined; id: string; children: ComponentChildren }) {
  if (text === undefined) {
    return <>{children}</>
  }

  return (
    <div class="prefix">
      <label class="prefix-label" for={id}>
        {text}
      </label>
      {children}
    </div>
  )
}
