import type { ComponentChildren } from 'preact'
import { useState } from 'preact/hooks'

import { Dialog } from './dialog'
import { Form } from './form'
import { Input } from './input'
import { Label } from './label'

interface Props {
  title: string
  name: string
  action: string
  open: boolean
  onClose: () => void
  onConfirm: () => Promise<unknown>
  children: ComponentChildren
}

export function Confirm({ title, name, action, open, onClose, onConfirm, children }: Props) {
  return (
    <Dialog title={title} open={open} onClose={onClose}>
      <Check name={name} action={action} onClose={onClose} onConfirm={onConfirm}>
        {children}
      </Check>
    </Dialog>
  )
}

function Check({ name, action, onClose, onConfirm, children }: Omit<Props, 'title' | 'open'>) {
  const [typed, setTyped] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<Error | null>(null)

  const confirm = async () => {
    setBusy(true)
    setError(null)

    try {
      await onConfirm()
      onClose()
    } catch (failure) {
      setError(failure instanceof Error ? failure : new Error(String(failure)))
      setBusy(false)
    }
  }

  return (
    <Form
      submit={action}
      kind="danger"
      busy={busy}
      disabled={typed !== name}
      error={error}
      onSubmit={() => void confirm()}
      onCancel={onClose}
    >
      <p>{children}</p>
      <Label text={`Type ${name} to confirm`}>
        <Input value={typed} autocomplete="off" onInput={(event) => setTyped(event.currentTarget.value)} />
      </Label>
    </Form>
  )
}
