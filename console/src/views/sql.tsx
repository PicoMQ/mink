import { useQuery } from '@tanstack/preact-query'
import { lazy, useLocation } from 'preact-iso'
import { useMemo, useState } from 'preact/hooks'

import { cluster, NO_SQL, type Cluster, type Sql } from '../api/cluster'
import { Button } from '../components/button'
import { Field, type Value } from '../components/field'
import { Loading } from '../components/loading'
import { Notice } from '../components/notice'
import { Table, type Column } from '../components/table'
import { Tag } from '../components/tag'
import { Toolbar } from '../components/toolbar'
import { View } from '../components/view'
import type { Profile, Result, Scan } from '../duckdb'
import * as format from '../format'
import { withQuery } from '../location'

import './sql.css'

const Editor = lazy(() => import('../components/editor'))

const START = 'SHOW TABLES'

const SCAN_COLUMNS: Column<Numbered>[] = [
  { name: 'table', header: 'Table', cell: ({ scan }) => scan.table ?? <Tag>not named</Tag> },
  { name: 'filters', header: 'Filters', cell: ({ scan }) => (scan.filters.length ? scan.filters.join(' AND ') : <Tag>none</Tag>) },
  { name: 'columns', header: 'Columns', cell: ({ scan }) => scan.columns.join(', ') },
  {
    name: 'files',
    header: 'Files read',
    width: '9rem',
    kind: 'number',
    cell: ({ scan }) => (scan.total === null ? format.count(scan.files) : `${format.count(scan.files)} of ${format.count(scan.total)}`),
  },
  { name: 'rows', header: 'Rows out', width: '9rem', kind: 'number', cell: ({ scan }) => format.count(scan.rows) },
  { name: 'time', header: 'Time', width: '7rem', kind: 'number', cell: ({ scan }) => format.duration(scan.ms) },
]

interface Row {
  index: number
  cells: (Value | null)[]
}

interface Outcome {
  result?: Result
  profile?: Profile
  error?: string
  running: boolean
}

interface Numbered {
  index: number
  scan: Scan
}

export default function SqlView() {
  const state = useQuery(cluster())
  const trail = [{ label: 'SQL' }]

  if (!state.data) {
    return (
      <View trail={trail}>
        <Loading error={state.error} />
      </View>
    )
  }

  const sql = state.data.sql
  return (
    <View trail={trail}>
      {sql === null ? (
        <Notice>{NO_SQL}</Notice>
      ) : (
        <Query cluster={state.data} sql={sql} />
      )}
    </View>
  )
}

function Query({ cluster, sql }: { cluster: Cluster; sql: Sql }) {
  const { path, query, route } = useLocation()
  const [text, setText] = useState(() => query.q ?? START)
  const [outcome, setOutcome] = useState<Outcome>({ running: false })

  const edit = (changed: string) => {
    setText(changed)
    route(withQuery(path, query, { q: changed === START ? '' : changed }), true)
  }

  const start = async (work: () => Promise<Omit<Outcome, 'running'>>) => {
    if (outcome.running) {
      return
    }
    setOutcome({ running: true })

    try {
      setOutcome({ ...(await work()), running: false })
    } catch (error) {
      setOutcome({ error: error instanceof Error ? error.message : String(error), running: false })
    }
  }

  const opened = async () => {
    const { session } = await import('../duckdb')
    return session(cluster, sql)
  }

  const run = (statement: string) => start(async () => ({ result: await (await opened()).run(statement) }))
  const profile = (statement: string) => start(async () => ({ profile: await (await opened()).profile(statement) }))

  return (
    <>
      <div class="sql-editor">
        <Editor language="sql" text={text} onChange={edit} onSubmit={run} />
      </div>
      <Toolbar
        actions={
          <>
            <Button onClick={() => profile(text)} disabled={outcome.running}>
              Profile
            </Button>
            <Button kind="primary" onClick={() => run(text)} disabled={outcome.running}>
              {outcome.running ? 'Running' : 'Run'}
            </Button>
          </>
        }
      />
      {outcome.error && (
        <div class="sql-error">
          <Notice kind="danger">{outcome.error}</Notice>
        </div>
      )}
      {outcome.result && <Results result={outcome.result} />}
      {outcome.profile && <Scans profile={outcome.profile} />}
    </>
  )
}

function Results({ result }: { result: Result }) {
  const columns = useMemo(
    (): Column<Row>[] =>
      result.columns.map((name, column) => ({
        name: String(column),
        header: name,
        cell: (row) => <Field field={row.cells[column] ?? null} />,
      })),
    [result],
  )
  const rows = useMemo(() => result.rows.map((cells, index) => ({ index, cells })), [result])
  const counted = format.counted(rows.length, 'row')

  return (
    <>
      <Table columns={columns} rows={rows} rowKey={(row) => String(row.index)} empty="No rows" />
      <Notice>
        {result.more ? `First ${counted}` : counted} in {format.duration(result.ms)}
      </Notice>
    </>
  )
}

function Scans({ profile }: { profile: Profile }) {
  const rows = useMemo(() => profile.scans.map((scan, index) => ({ index, scan })), [profile])
  const unnamed = profile.scans.some((scan) => scan.table === null)

  return (
    <>
      <Table columns={SCAN_COLUMNS} rows={rows} rowKey={(row) => String(row.index)} empty="No Iceberg scans" />
      {unnamed && profile.tables.length > 0 && (
        <Notice>
          Data files:{' '}
          {profile.tables.map((table) => `${table.table} ${table.files === null ? 'unknown' : format.count(table.files)}`).join(', ')}
        </Notice>
      )}
      <Notice>
        {format.counted(profile.rows, 'row')} returned in {format.duration(profile.ms)}
      </Notice>
    </>
  )
}
