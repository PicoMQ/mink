import * as format from '../format'

import './range.css'

interface Props {
  earliest: number
  stable: number
  start: number | null
  end: number | null
}

export function Range({ earliest, stable, start, end }: Props) {
  const span = Math.max(stable - earliest, 1)
  const at = (offset: number) => `${(Math.min(Math.max(offset - earliest, 0), span) / span) * 100}%`
  const title =
    start === null || end === null
      ? `Kafka ${format.count(earliest)} to ${format.count(stable)}, not indexed yet`
      : `indexed ${format.count(start)} to ${format.count(end)} of Kafka ${format.count(earliest)} to ${format.count(stable)}`

  return (
    <div class="range" role="img" aria-label={title} title={title}>
      {start !== null && end !== null && (
        <div class="range-indexed" style={{ left: at(start), width: `calc(${at(end)} - ${at(start)})` }} />
      )}
    </div>
  )
}
