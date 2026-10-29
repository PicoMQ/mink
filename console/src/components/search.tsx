import { useEffect, useRef, useState } from 'preact/hooks'

import { Input } from './input'

const DELAY_MS = 250

interface Props {
  value: string
  hint: string
  onChange: (value: string) => void
}

export function Search({ value, hint, onChange }: Props) {
  const [typed, setTyped] = useState(value)
  const shown = useRef(value)

  useEffect(() => {
    if (value !== shown.current) {
      shown.current = value
      setTyped(value)
    }
  }, [value])

  useEffect(() => {
    if (typed === value) {
      return
    }

    const timer = setTimeout(() => onChange(typed), DELAY_MS)
    return () => clearTimeout(timer)
  }, [typed])

  return (
    <Input
      type="search"
      aria-label={hint}
      placeholder={hint}
      value={typed}
      onInput={(event) => setTyped(event.currentTarget.value)}
    />
  )
}
