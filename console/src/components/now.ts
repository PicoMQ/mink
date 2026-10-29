import { useEffect, useState } from 'preact/hooks'

const listeners = new Set<(nowMs: number) => void>()

let timer: ReturnType<typeof setInterval> | undefined

export function useNow(): number {
  const [nowMs, setNowMs] = useState(Date.now)

  useEffect(() => subscribe(setNowMs), [])

  return nowMs
}

function subscribe(listener: (nowMs: number) => void): () => void {
  listeners.add(listener)

  timer ??= setInterval(() => {
    const nowMs = Date.now()
    listeners.forEach((each) => each(nowMs))
  }, 1000)

  return () => {
    listeners.delete(listener)

    if (listeners.size === 0) {
      clearInterval(timer)
      timer = undefined
    }
  }
}
