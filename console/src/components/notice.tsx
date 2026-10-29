import type { ComponentChildren } from 'preact'

import './notice.css'

interface Props {
  kind?: 'plain' | 'danger'
  children: ComponentChildren
}

export function Notice({ kind = 'plain', children }: Props) {
  return (
    <p class="notice" data-kind={kind} role={kind === 'danger' ? 'alert' : undefined}>
      {children}
    </p>
  )
}
