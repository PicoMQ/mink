import * as format from '../format'
import { useNow } from './now'

export function Until({ atMs }: { atMs: number }) {
  const nowMs = useNow()

  return (
    <time dateTime={new Date(atMs).toISOString()} title={format.time(atMs)}>
      {atMs <= nowMs ? 'expired' : `in ${format.ago(nowMs, atMs)}`}
    </time>
  )
}
