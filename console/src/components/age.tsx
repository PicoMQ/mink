import * as format from '../format'
import { useNow } from './now'

export function Age({ atMs }: { atMs: number }) {
  const nowMs = useNow()

  return (
    <time dateTime={new Date(atMs).toISOString()} title={format.time(atMs)}>
      {format.ago(atMs, nowMs)} ago
    </time>
  )
}
