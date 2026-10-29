import type { ComponentChildren } from 'preact'

import { Actions } from './actions'
import { Button } from './button'
import { Notice } from './notice'

import './form.css'

interface Props {
  submit: string
  kind?: 'primary' | 'danger'
  busy?: boolean
  disabled?: boolean
  error?: Error | null
  onSubmit: () => void
  onCancel?: () => void
  children: ComponentChildren
}

export function Form({ submit, kind = 'primary', busy, disabled, error, onSubmit, onCancel, children }: Props) {
  const send = (event: Event) => {
    event.preventDefault()
    onSubmit()
  }

  return (
    <form class="form" onSubmit={send}>
      {children}
      {error && <Notice kind="danger">{error.message}</Notice>}
      <Actions>
        {onCancel && <Button onClick={onCancel}>Cancel</Button>}
        <Button kind={kind} type="submit" disabled={busy || disabled}>
          {submit}
        </Button>
      </Actions>
    </form>
  )
}
