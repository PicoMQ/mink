import { read } from './query'

export interface Field {
  data: string
  encoding: 'utf8' | 'base64'
  bytes: number
  truncated: boolean
}

export interface Header {
  key: string
  value: Field | null
}

export interface TableRecord {
  offset: number
  timestampAtMs: number
  key: Field | null
  value: Field | null
  headers: Header[]
  row: Record<string, unknown> | null
  rowError?: string
}

interface RecordPage {
  items: TableRecord[]
  next: number | null
}

interface RecordStart {
  offset?: number
  timestamp?: number
  limit?: number
}

export const records = (name: string, partition: number, start: RecordStart) =>
  read<RecordPage>(['tables', name, 'partitions', partition, 'records'], { ...start })
