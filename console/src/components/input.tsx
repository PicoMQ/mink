import type { InputHTMLAttributes } from 'preact'
import { useId } from 'preact/hooks'

import { Prefix } from './prefix'

export function Input({ prefix, ...props }: InputHTMLAttributes<HTMLInputElement> & { prefix?: string }) {
  const id = useId()

  return (
    <Prefix text={prefix} id={id}>
      <input {...props} id={id} />
    </Prefix>
  )
}
