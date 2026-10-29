import { useState } from 'preact/hooks'

export function useDraft<T extends Record<string, string>>(initial: T) {
  const [draft, setDraft] = useState(initial)

  const edit = (field: keyof T) => (event: { currentTarget: { value: string } }) =>
    setDraft((current) => ({ ...current, [field]: event.currentTarget.value }))

  return { draft, edit }
}
