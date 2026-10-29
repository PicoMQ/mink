import * as format from '../format'
import { Tag } from './tag'
import type { Language } from './editor'

import './field.css'

const HEX_PREVIEW = 64

export interface Value {
  data: string
  encoding: 'utf8' | 'base64'
  bytes: number
  truncated: boolean
}

interface Preview {
  kind: 'text' | 'binary'
  text: string
}

export function preview(field: Value): Preview {
  if (field.encoding === 'base64') {
    return { kind: 'binary', text: hex(field, HEX_PREVIEW) }
  }
  return { kind: 'text', text: field.data }
}

export function pretty(field: Value): { language: Language; text: string } {
  if (field.encoding === 'base64') {
    return { language: 'text', text: hex(field, field.bytes) }
  }

  const json = !field.truncated && /^\s*[[{]/.test(field.data) ? indent(field.data) : null
  return json === null ? { language: 'text', text: field.data } : { language: 'json', text: json }
}

export function hex(field: Value, limit: number): string {
  const decoded = atob(field.data.slice(0, Math.ceil(limit / 3) * 4))
  const shown = Math.min(decoded.length, limit)
  const pairs = Array.from({ length: shown }, (_, index) =>
    decoded.charCodeAt(index).toString(16).padStart(2, '0'),
  )

  return field.bytes > shown ? `${pairs.join(' ')} …` : pairs.join(' ')
}

export function Field({ field }: { field: Value | null }) {
  if (field === null) {
    return <Tag>null</Tag>
  }

  const shown = preview(field)
  return (
    <span class="field" data-kind={shown.kind}>
      <span class="field-text">{shown.text}</span>
      {shown.kind === 'binary' && <Tag>binary</Tag>}
      {field.truncated && <span class="field-size">{format.bytes(field.bytes)}</span>}
    </span>
  )
}

export function indent(text: string): string | null {
  try {
    return JSON.stringify(JSON.parse(text), null, 2)
  } catch {
    return null
  }
}
