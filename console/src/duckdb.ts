import * as duckdb from '@duckdb/duckdb-wasm'
import { DataType, TimeUnit, type RecordBatch } from 'apache-arrow'

import type { Cluster, Sql } from './api/cluster'
import { hex, type Value } from './components/field'
import * as format from './format'

declare const DUCKDB_FILES: Record<string, string>

const ROWS = 1000

const CATALOG = 'mink'

const INTERVAL_NOTE = 'interval, cast it to VARCHAR to show it'

export interface Result {
  columns: string[]
  rows: (Value | null)[][]
  more: boolean
  ms: number
}

export interface Profile {
  scans: Scan[]
  tables: TableFiles[]
  rows: number
  ms: number
}

export interface Scan {
  table: string | null
  filters: string[]
  columns: string[]
  files: number
  total: number | null
  rows: number
  ms: number
}

interface TableFiles {
  table: string
  files: number | null
}

interface TableName {
  schema: string
  table: string
}

interface Operator {
  operator_type?: string
  operator_cardinality?: number
  operator_timing?: number
  extra_info?: Record<string, string | string[]>
  children?: Operator[]
}

interface Session {
  run: (statement: string) => Promise<Result>
  profile: (statement: string) => Promise<Profile>
}

type Origin = Pick<Location, 'protocol' | 'host' | 'origin'>

let opened: Promise<Session> | null = null

export function session(cluster: Cluster, sql: Sql): Promise<Session> {
  opened ??= open(cluster, sql).catch((error: unknown) => {
    opened = null
    throw error
  })

  return opened
}

async function open(cluster: Cluster, sql: Sql): Promise<Session> {
  const bundle = await duckdb.selectBundle(engine(sql.duckdbEngine))
  const script = await checked(bundle.mainWorker!, 'text/javascript')
  const module = await checked(bundle.mainModule, 'application/wasm').catch((error: unknown) => {
    URL.revokeObjectURL(script)
    throw error
  })

  const worker = new Worker(script)
  const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker)
  const stopped = new Promise<never>((_, reject) =>
    worker.addEventListener('error', (event) => reject(new Error(`DuckDB stopped: ${event.message}`))),
  )
  stopped.catch(() => {
    opened = null
    void db.terminate()
  })

  try {
    await Promise.race([db.instantiate(module), stopped])
    const connection = await Promise.race([db.connect(), stopped])
    for (const statement of setup(cluster, sql, location)) {
      await Promise.race([connection.query(statement), stopped])
    }

    return {
      run: (statement) => Promise.race([run(connection, statement), stopped]),
      profile: (statement) => Promise.race([profile(connection, cluster, statement), stopped]),
    }
  } catch (error) {
    await db.terminate()
    throw new Error(failure(error))
  } finally {
    URL.revokeObjectURL(script)
    URL.revokeObjectURL(module)
  }
}

async function checked(url: string, type: string): Promise<string> {
  const integrity = DUCKDB_FILES[url.slice(url.lastIndexOf('/') + 1)]
  const refused = (reason: string) =>
    new Error(`DuckDB did not load from ${url}: ${reason}. It must be reachable and hold the files of DuckDB-Wasm ${duckdb.PACKAGE_VERSION}`)

  const response = await fetch(url, { integrity }).catch((error: unknown) => {
    throw refused(message(error))
  })
  if (!response.ok) {
    throw refused(`${response.status} ${response.statusText}`)
  }

  return URL.createObjectURL(new Blob([await response.arrayBuffer()], { type }))
}

function engine(base: string | null): duckdb.DuckDBBundles {
  if (base === null) {
    return duckdb.getJsDelivrBundles()
  }

  const dist = base.endsWith('/') ? base : `${base}/`
  return {
    mvp: { mainModule: `${dist}duckdb-mvp.wasm`, mainWorker: `${dist}duckdb-browser-mvp.worker.js` },
    eh: { mainModule: `${dist}duckdb-eh.wasm`, mainWorker: `${dist}duckdb-browser-eh.worker.js` },
  }
}

export function setup(cluster: Cluster, sql: Sql, at: Origin): string[] {
  const secure = at.protocol === 'https:'
  const repository =
    sql.duckdbExtensions === null ? [] : [`SET custom_extension_repository = ${literal(sql.duckdbExtensions)}`]

  return [
    ...repository,
    'LOAD iceberg',
    'LOAD httpfs',
    `CREATE SECRET mink (TYPE S3, KEY_ID 'mink', SECRET 'mink', ENDPOINT ${literal(`${at.host}/s3`)}, URL_STYLE 'path', USE_SSL ${secure})`,
    `ATTACH ${literal(cluster.warehouse)} AS ${CATALOG} (TYPE iceberg, ENDPOINT ${literal(`${at.origin}/catalog`)}, AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')`,
    `USE ${CATALOG}.${identifier(cluster.namespace)}`,
  ]
}

async function profile(connection: duckdb.AsyncDuckDBConnection, cluster: Cluster, statement: string): Promise<Profile> {
  try {
    return await measure(connection, cluster, statement)
  } catch (error) {
    throw new Error(failure(error))
  }
}

async function measure(connection: duckdb.AsyncDuckDBConnection, cluster: Cluster, statement: string): Promise<Profile> {
  const serialized = await connection.query(`SELECT json_serialize_sql(${literal(statement)}) AS tree`)
  const names = tables(String(serialized.getChildAt(0)?.get(0)), cluster.namespace)

  const started = performance.now()
  const explained = await connection.query(`EXPLAIN (ANALYZE, FORMAT JSON) ${statement}`)
  const ms = Math.round(performance.now() - started)

  const files: TableFiles[] = []
  for (const name of names) {
    const table = name.schema === cluster.namespace ? name.table : `${name.schema}.${name.table}`
    files.push({ table, files: await dataFiles(connection, name) })
  }

  const plan = JSON.parse(String(explained.getChildAt(1)?.get(0))) as Operator
  return scanned(plan, files, ms)
}

async function dataFiles(connection: duckdb.AsyncDuckDBConnection, name: TableName): Promise<number | null> {
  const qualified = `${CATALOG}.${identifier(name.schema)}.${identifier(name.table)}`

  try {
    const counted = await connection.query(
      `SELECT count(*) AS files FROM iceberg_metadata(${qualified}) WHERE manifest_content = 'DATA' AND status <> 'DELETED'`,
    )
    return Number(counted.getChildAt(0)?.get(0))
  } catch {
    return null
  }
}

export function tables(serialized: string, namespace: string): TableName[] {
  const tree = JSON.parse(serialized) as { error?: boolean; error_type?: string; error_message?: string; statements?: unknown[] }
  if (tree.error) {
    const kind = tree.error_type ?? 'parser'
    throw new Error(`${kind.charAt(0).toUpperCase()}${kind.slice(1)} Error: ${tree.error_message ?? ''}`)
  }
  if (tree.statements?.length !== 1) {
    throw new Error('Profile takes one query')
  }

  const defined = new Set(named(tree.statements))
  const found = references(tree.statements)
    .filter((reference) => reference.catalog === '' || reference.catalog === CATALOG)
    .filter((reference) => reference.catalog !== '' || reference.schema !== '' || !defined.has(reference.table))
    .map((reference) => ({ schema: reference.schema || namespace, table: reference.table }))

  return [...new Map(found.map((name) => [`${name.schema}.${name.table}`, name])).values()]
}

function references(node: unknown): { catalog: string; schema: string; table: string }[] {
  return children(node).flatMap((fields) =>
    fields.type === 'BASE_TABLE' && typeof fields.table_name === 'string'
      ? [{ catalog: String(fields.catalog_name ?? ''), schema: String(fields.schema_name ?? ''), table: fields.table_name }]
      : [],
  )
}

function named(node: unknown): string[] {
  return children(node).flatMap((fields) => {
    const map = (fields.cte_map as { map?: { key?: unknown }[] } | undefined)?.map ?? []
    return map.flatMap((entry) => (typeof entry.key === 'string' ? [entry.key] : []))
  })
}

function children(node: unknown): Record<string, unknown>[] {
  if (Array.isArray(node)) {
    return node.flatMap(children)
  }
  if (node === null || typeof node !== 'object') {
    return []
  }

  const fields = node as Record<string, unknown>
  return [fields, ...Object.values(fields).flatMap(children)]
}

export function scanned(plan: Operator, tables: TableFiles[], ms: number): Profile {
  const only = tables.length === 1 ? tables[0]! : null
  const top = plan.children?.[0]?.children?.[0]

  return {
    scans: icebergScans(plan).map((scan) => {
      const info = scan.extra_info ?? {}

      return {
        table: only?.table ?? null,
        filters: list(info.Filters),
        columns: list(info.Projections),
        files: Number(info['Total Files Read'] ?? 0),
        total: only?.files ?? null,
        rows: scan.operator_cardinality ?? 0,
        ms: Math.round((scan.operator_timing ?? 0) * 1000),
      }
    }),
    tables,
    rows: top?.operator_cardinality ?? 0,
    ms,
  }
}

function icebergScans(operator: Operator): Operator[] {
  const own = operator.operator_type === 'TABLE_SCAN' && operator.extra_info?.Function === 'ICEBERG_SCAN' ? [operator] : []

  return [...own, ...(operator.children ?? []).flatMap(icebergScans)]
}

function list(value: string | string[] | undefined): string[] {
  if (value === undefined || value === '') {
    return []
  }
  return Array.isArray(value) ? value : [value]
}

async function run(connection: duckdb.AsyncDuckDBConnection, statement: string): Promise<Result> {
  try {
    return await stream(connection, statement)
  } catch (error) {
    throw new Error(failure(error))
  }
}

async function stream(connection: duckdb.AsyncDuckDBConnection, statement: string): Promise<Result> {
  const started = performance.now()
  const reader = await connection.send(statement, true)

  let columns: string[] = []
  const rows: (Value | null)[][] = []
  let more = false
  for await (const batch of reader) {
    columns = batch.schema.fields.map((field) => field.name)
    more = read(batch, rows)
    if (more) {
      break
    }
  }

  if (more) {
    await connection.cancelSent()
  }
  return { columns, rows, more, ms: Math.round(performance.now() - started) }
}

function read(batch: RecordBatch, rows: (Value | null)[][]): boolean {
  const fields = batch.schema.fields

  for (let index = 0; index < batch.numRows; index++) {
    if (rows.length === ROWS) {
      return true
    }
    rows.push(fields.map((field, column) => cell(field.type, () => batch.getChildAt(column)?.get(index))))
  }
  return false
}

export function cell(type: DataType, read: () => unknown): Value | null {
  try {
    return value(type, read())
  } catch (error) {
    return text(`cannot show: ${message(error)}`)
  }
}

export function value(type: DataType, data: unknown): Value | null {
  if (data === null || data === undefined) {
    return null
  }
  if (data instanceof Uint8Array) {
    return binary(data)
  }

  const shown = plain(type, data)
  return text(shown !== null && typeof shown === 'object' ? JSON.stringify(shown) : String(shown))
}

function plain(type: DataType, data: unknown): unknown {
  if (data === null || data === undefined) {
    return null
  }
  if (data instanceof Uint8Array) {
    return hex(binary(data), data.length)
  }

  if (DataType.isDecimal(type)) {
    return decimal(String(data), type.scale)
  }
  if (DataType.isTimestamp(type)) {
    return type.timezone ? format.time(Number(data)) : withoutZone(Number(data))
  }
  if (DataType.isDate(type)) {
    return withoutZone(Number(data)).slice(0, 10)
  }
  if (DataType.isTime(type)) {
    return timeOfDay(BigInt(data as number | bigint), type.unit)
  }
  if (DataType.isInterval(type)) {
    return INTERVAL_NOTE
  }

  if (DataType.isStruct(type)) {
    const row = data as Record<string, unknown>
    return Object.fromEntries(type.children.map((child) => [child.name, plain(child.type, row[child.name])]))
  }
  if (DataType.isList(type) || DataType.isFixedSizeList(type)) {
    const item = type.children[0]!.type
    return Array.from(data as Iterable<unknown>, (element) => plain(item, element))
  }
  if (DataType.isMap(type)) {
    const [key, entry] = type.children[0]!.type.children.map((child) => child.type)
    return Object.fromEntries(
      Array.from(data as Iterable<[unknown, unknown]>, ([name, element]) => [String(plain(key!, name)), plain(entry!, element)]),
    )
  }

  return typeof data === 'bigint' ? data.toString() : data
}

function decimal(digits: string, scale: number): string {
  const negative = digits.startsWith('-')
  const unsigned = (negative ? digits.slice(1) : digits).padStart(scale + 1, '0')
  const split = unsigned.length - scale
  const scaled = scale === 0 ? unsigned : `${unsigned.slice(0, split)}.${unsigned.slice(split)}`

  return negative ? `-${scaled}` : scaled
}

function withoutZone(ms: number): string {
  return new Date(ms).toISOString().replace('T', ' ').replace('Z', '')
}

function timeOfDay(ticks: bigint, unit: TimeUnit): string {
  const digits = { [TimeUnit.SECOND]: 0, [TimeUnit.MILLISECOND]: 3, [TimeUnit.MICROSECOND]: 6, [TimeUnit.NANOSECOND]: 9 }[unit]
  const perSecond = 10n ** BigInt(digits)
  const seconds = Number(ticks / perSecond)
  const pad = (part: number) => String(part).padStart(2, '0')
  const whole = `${pad(Math.floor(seconds / 3600))}:${pad(Math.floor(seconds / 60) % 60)}:${pad(seconds % 60)}`

  return digits === 0 ? whole : `${whole}.${String(ticks % perSecond).padStart(digits, '0')}`
}

export function failure(error: unknown): string {
  const reason = message(error)
  const parsed = json(reason)

  return parsed?.exception_type && parsed.exception_message
    ? `${parsed.exception_type} Error: ${parsed.exception_message}`
    : reason
}

function json(reason: string): { exception_type?: string; exception_message?: string } | null {
  try {
    return JSON.parse(reason) as { exception_type?: string; exception_message?: string }
  } catch {
    return null
  }
}

function text(data: string): Value {
  return { data, encoding: 'utf8', bytes: data.length, truncated: false }
}

function binary(bytes: Uint8Array): Value {
  return {
    data: btoa(Array.from(bytes, (byte) => String.fromCharCode(byte)).join('')),
    encoding: 'base64',
    bytes: bytes.length,
    truncated: false,
  }
}

function literal(text: string): string {
  return `'${text.replaceAll("'", "''")}'`
}

function identifier(text: string): string {
  return `"${text.replaceAll('"', '""')}"`
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
