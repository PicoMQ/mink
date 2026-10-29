import { ChevronDown } from 'lucide-preact'
import type { SelectHTMLAttributes } from 'preact'
import { useId } from 'preact/hooks'

import { Prefix } from './prefix'

import './select.css'

export function Select({ prefix, ...props }: SelectHTMLAttributes<HTMLSelectElement> & { prefix?: string }) {
  const id = useId()

  return (
    <Prefix text={prefix} id={id}>
      <span class="select">
        <select {...props} id={id} />
        <ChevronDown size={14} />
      </span>
    </Prefix>
  )
}
