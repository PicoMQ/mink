import { Check, Copy } from 'lucide-preact'
import { useRef, useState } from 'preact/hooks'

import { Button } from './button'

import './code.css'

const SHOWN_MS = 1500

export function Code({ label, text }: { label: string; text: string }) {
  const [done, setDone] = useState<'copied' | 'selected' | null>(null)
  const shown = useRef<HTMLElement>(null)

  const copy = async () => {
    const written = window.isSecureContext && 'clipboard' in navigator && (await navigator.clipboard.writeText(text).then(() => true, () => false))

    if (!written) {
      select(shown.current)
    }

    setDone(written || document.execCommand('copy') ? 'copied' : 'selected')
    setTimeout(() => setDone(null), SHOWN_MS)
  }

  return (
    <figure class="code">
      <figcaption class="code-bar">
        <span class="code-label">{label}</span>
        <Button onClick={() => void copy()} aria-label={`Copy ${label}`}>
          {done ? <Check size={16} /> : <Copy size={16} />}
          {done === 'copied' ? 'Copied' : done === 'selected' ? 'Selected' : 'Copy'}
        </Button>
      </figcaption>
      <pre class="code-text">
        <code ref={shown}>{text}</code>
      </pre>
    </figure>
  )
}

function select(element: HTMLElement | null) {
  const selection = window.getSelection()
  if (!element || !selection) {
    return
  }

  const range = document.createRange()
  range.selectNodeContents(element)
  selection.removeAllRanges()
  selection.addRange(range)
}
