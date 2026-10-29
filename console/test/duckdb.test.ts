import { readFileSync } from 'node:fs'
import { createRequire } from 'node:module'

import * as duckdb from '@duckdb/duckdb-wasm/blocking'
import { beforeAll, describe, expect, it } from 'vitest'

import type { Cluster } from '../src/api/cluster'
import { cell, failure, scanned, setup, tables, value } from '../src/duckdb'
import * as format from '../src/format'

let connection: duckdb.DuckDBConnection

beforeAll(async () => {
  const dist = createRequire(import.meta.url).resolve('@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm').replace(/duckdb-mvp\.wasm$/, '')
  const db = await duckdb.createDuckDB(
    { mvp: { mainModule: `${dist}duckdb-mvp.wasm`, mainWorker: '' }, eh: { mainModule: `${dist}duckdb-eh.wasm`, mainWorker: '' } },
    new duckdb.VoidLogger(),
    duckdb.NODE_RUNTIME,
  )
  await db.instantiate()

  connection = db.connect()
})

function shown(expression: string): string | null {
  const table = connection.query(`SELECT ${expression} AS shown`)
  const field = table.schema.fields[0]!

  return value(field.type, table.getChildAt(0)!.get(0))?.data ?? null
}

describe('value', () => {
  it('shows decimals with their scale', () => {
    expect(shown('1.25::DECIMAL(10,2)')).toBe('1.25')
    expect(shown('-0.05::DECIMAL(10,2)')).toBe('-0.05')
    expect(shown('123456789012345::DECIMAL(38,4)')).toBe('123456789012345.0000')
    expect(shown('12345678901234567890::HUGEINT')).toBe('12345678901234567890')
  })

  it('shows instants in local time and wall clock times as written', () => {
    const instant = Date.UTC(2026, 9, 29, 12, 0, 0, 123)

    expect(shown("TIMESTAMPTZ '2026-10-29 12:00:00.123+00'")).toBe(format.time(instant))
    expect(shown("TIMESTAMP '2026-10-29 12:00:00.123'")).toBe('2026-10-29 12:00:00.123')
    expect(shown("DATE '2026-10-29'")).toBe('2026-10-29')
    expect(shown("TIME '12:34:56.789'")).toBe('12:34:56.789000')
  })

  it('shows whole 64 bit integers and keeps other scalars', () => {
    expect(shown('9007199254740993::BIGINT')).toBe('9007199254740993')
    expect(shown('1.5::DOUBLE')).toBe('1.5')
    expect(shown("'NaN'::DOUBLE")).toBe('NaN')
    expect(shown("'-infinity'::DOUBLE")).toBe('-Infinity')
    expect(shown('true')).toBe('true')
    expect(shown("'text'")).toBe('text')
    expect(shown('NULL::INTEGER')).toBe(null)
  })

  it('keeps binary as bytes and shows nested values as JSON', () => {
    const table = connection.query(`SELECT '\\xAA\\xBB'::BLOB AS shown`)

    expect(value(table.schema.fields[0]!.type, table.getChildAt(0)!.get(0))).toEqual({
      data: btoa('\xaa\xbb'),
      encoding: 'base64',
      bytes: 2,
      truncated: false,
    })
    expect(shown("{'price': 1.25::DECIMAL(10,2), 'key': '\\xAA'::BLOB, 'at': DATE '2026-10-29'}")).toBe(
      '{"price":"1.25","key":"aa","at":"2026-10-29"}',
    )
    expect(shown('[1::BIGINT, NULL]')).toBe('["1",null]')
    expect(shown("MAP {'k': 2.50::DECIMAL(4,2)}")).toBe('{"k":"2.50"}')
  })

  it('says how to show an interval rather than showing it wrong', () => {
    expect(shown('INTERVAL 2 HOUR')).toBe('interval, cast it to VARCHAR to show it')
  })

  it('shows a value it cannot read in its own cell', () => {
    const table = connection.query("SELECT 'infinity'::TIMESTAMP AS shown")
    const read = () => table.getChildAt(0)!.get(0)

    expect(cell(table.schema.fields[0]!.type, read)?.data).toMatch(/^cannot show: /)
  })
})

describe('failure', () => {
  it('shows JSON errors from the browser build as DuckDB writes them in text', () => {
    const json =
      '{"exception_type":"Parser","exception_message":"syntax error at or near \\"-\\"\\n\\nLINE 1: select * from e2e-orders-mvss7tcf6c5u\\n ^","error_subtype":"SYNTAX_ERROR","position":"17"}'

    expect(failure(new Error(json))).toBe('Parser Error: syntax error at or near "-"\n\nLINE 1: select * from e2e-orders-mvss7tcf6c5u\n ^')
    expect(failure(new Error('Catalog Error: Table with name missing does not exist!'))).toBe(
      'Catalog Error: Table with name missing does not exist!',
    )
  })
})

describe('setup', () => {
  const cluster = { namespace: 'kafka', warehouse: 's3://mink/warehouse' } as Cluster
  const sql = { duckdbEngine: null, duckdbExtensions: null }

  it('reads the catalog and S3 through the page origin, over TLS when the page is', () => {
    const statements = setup(cluster, sql, { protocol: 'https:', host: 'mink.example.com', origin: 'https://mink.example.com' })

    expect(statements).toContain(
      "CREATE SECRET mink (TYPE S3, KEY_ID 'mink', SECRET 'mink', ENDPOINT 'mink.example.com/s3', URL_STYLE 'path', USE_SSL true)",
    )
    expect(statements).toContain(
      "ATTACH 's3://mink/warehouse' AS mink (TYPE iceberg, ENDPOINT 'https://mink.example.com/catalog', AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')",
    )
    expect(statements.at(-1)).toBe('USE mink."kafka"')
    expect(statements.some((statement) => statement.includes('custom_extension_repository'))).toBe(false)
  })

  it('loads extensions from a mirror when one is set', () => {
    const mirrored = setup(cluster, { ...sql, duckdbExtensions: "https://mirror/it's" }, { protocol: 'http:', host: 'h', origin: 'http://h' })

    expect(mirrored[0]).toBe("SET custom_extension_repository = 'https://mirror/it''s'")
    expect(mirrored).toContain("CREATE SECRET mink (TYPE S3, KEY_ID 'mink', SECRET 'mink', ENDPOINT 'h/s3', URL_STYLE 'path', USE_SSL false)")
  })
})

describe('scanned', () => {
  const fixture = (name: string) =>
    JSON.parse(readFileSync(new URL(`profile/${name}.json`, import.meta.url), 'utf8'))

  it('reads each Iceberg scan of a one table query and names its table', () => {
    const profile = scanned(fixture('scan'), [{ table: 'events', files: 12 }], 15)

    expect(profile).toEqual({
      scans: [{ table: 'events', filters: ['id<5'], columns: ['id', 'item'], files: 1, total: 12, rows: 5, ms: 6 }],
      tables: [{ table: 'events', files: 12 }],
      rows: 5,
      ms: 15,
    })
  })

  it('reads filters on several columns as a list', () => {
    const profile = scanned(fixture('filters'), [{ table: 'events', files: 12 }], 10)

    expect(profile.scans.map(({ filters, files, total, rows }) => ({ filters, files, total, rows }))).toEqual([
      { filters: ['_kafka_partition=1', 'id<600'], files: 2, total: 12, rows: 200 },
    ])
  })

  it('leaves the scans of a join unnamed, as DuckDB 1.4 does not name them', () => {
    const files = [
      { table: 'events', files: 12 },
      { table: 'refunds', files: 1 },
    ]
    const profile = scanned(fixture('join'), files, 20)

    expect(profile.scans.map(({ table, filters, files, total, rows }) => ({ table, filters, files, total, rows }))).toEqual([
      { table: null, filters: ['id<5'], files: 1, total: null, rows: 5 },
      { table: null, filters: [], files: 1, total: null, rows: 30 },
    ])
    expect(profile.rows).toBe(1)
  })
})

describe('tables', () => {
  const parsed = (statement: string) =>
    tables(String(connection.query(`SELECT json_serialize_sql('${statement.replaceAll("'", "''")}') AS tree`).getChildAt(0)!.get(0)), 'kafka')

  it('names each table once in the namespace unless the query names another', () => {
    expect(parsed('SELECT * FROM orders a JOIN orders b USING (_offset) JOIN mink.other.t USING (_offset)')).toEqual([
      { schema: 'kafka', table: 'orders' },
      { schema: 'other', table: 't' },
    ])
  })

  it('leaves out the query\'s own WITH names and tables of other catalogs', () => {
    expect(parsed('WITH recent AS (SELECT * FROM orders) SELECT * FROM recent JOIN memory.main.x USING (_offset)')).toEqual([
      { schema: 'kafka', table: 'orders' },
    ])
  })

  it('takes one query and shows parser errors as they are', () => {
    expect(() => parsed('SELECT 1; SELECT 1, 2')).toThrow('Profile takes one query')
    expect(() => parsed('SELEC 1')).toThrow('Parser Error: syntax error at or near "SELEC"')
  })
})
