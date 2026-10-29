import { read } from './query'

export interface Overview {
  computedAtMs: number
  tables: number
  nodes: number
  records: number
  bytes: number
  collectable: number
  mostLagging: { table: string; lag: number }[]
  largest: { table: string; bytes: number }[]
  stalest: { table: string; snapshotAtMs: number | null }[]
}

export const overview = () => read<Overview>(['overview'])
