import { admin, request } from './client'
import { listing, read } from './query'

export type State = 'indexing' | 'removing' | 'replaced' | 'missing'

export interface Table {
  name: string
  uuid: string
  partitions: number
  granularity: string
  state: State
  snapshotId: number | null
  snapshotAtMs: number | null
  records: number
  bytes: number
  schemaId: number
}

export interface Lease {
  table: string
  partition: number | null
  holder: string
  epoch: number
  expiresAtMs: number
}

export interface TableDetail extends Table {
  location: string
  metadataLocation: string | null
  specs: { id: number; granularity: string }[]
  writer: Lease | null
  statisticsAtMs: number | null
  lag: number | null
}

export interface Partition {
  index: number
  earliestOffset: number
  lastStableOffset: number
  startOffset: number | null
  endOffset: number | null
  lag: number | null
  backfillRemaining: number
  lease: Lease | null
}

export interface DataFile {
  name: string
  partition: number
  startOffset: number
  endOffset: number
  bucket: number | null
  specId: number
  schemaId: number
  version: number
  parts: number
  records: number
  bytes: number
  firstTimestampAtMs: number
  addedSnapshotId: number | null
}

export interface Column {
  fieldId: number
  name: string | null
  lower: unknown
  upper: unknown
  nulls: number | null
  nans: number | null
}

interface DataFileDetail extends DataFile {
  path: string
  replacedSnapshotId: number | null
  partsRanges: { startOffset: number; endOffset: number }[]
  columns: Column[]
}

export interface Snapshot {
  id: number
  timestampAtMs: number
  epoch: number
  records: number | null
  addedFiles: number | null
  replacedFiles: number | null
}

interface SnapshotDetail extends Snapshot {
  offsets: { partition: number; startOffset: number; endOffset: number }[]
  metadataLocation: string
}

interface SchemaField {
  id: number
  name: string
  type: string
  required: boolean
  fields?: SchemaField[]
}

export interface Merge {
  part: 'key' | 'value'
  schemaId: string
  registryKey: string | null
  tableSchemaId: number | null
  rejection: string | null
}

interface Schemas {
  current: number
  schemas: { id: number; fields: SchemaField[] }[]
  merges: Merge[]
}

export const tables = (prefix?: string) => listing<Table>(['tables'], { prefix })

export const table = (name: string) => read<TableDetail>(['tables', name])

export const partitions = (name: string) => listing<Partition>(['tables', name, 'partitions'])

export const files = (name: string, partition?: number, bucket?: number) =>
  listing<DataFile>(['tables', name, 'files'], { partition, bucket })

export const file = (name: string, file: string) =>
  read<DataFileDetail>(['tables', name, 'files', ...file.split('/')])

export const snapshots = (name: string) => listing<Snapshot>(['tables', name, 'snapshots'])

export const snapshot = (name: string, id: number) => read<SnapshotDetail>(['tables', name, 'snapshots', id])

export const schemas = (name: string) => read<Schemas>(['tables', name, 'schemas'])

export const addTable = (topic: string, granularity?: string) =>
  request<Table>('POST', admin('tables'), { body: { topic, granularity } })

export const changeGranularity = (name: string, granularity: string) =>
  request<Table>('PATCH', admin('tables', name), { body: { granularity } })

export const removeTable = (name: string) => request<void>('DELETE', admin('tables', name))
