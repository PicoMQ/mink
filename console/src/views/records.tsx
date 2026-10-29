import { useQuery } from '@tanstack/preact-query'
import { ErrorBoundary, lazy, useLocation } from 'preact-iso'

import { records, type Field as Bytes, type TableRecord } from '../api/records'
import type { TableDetail } from '../api/tables'
import { Button } from '../components/button'
import { DateTime } from '../components/datetime'
import { Field, pretty } from '../components/field'
import { Input } from '../components/input'
import { Loading } from '../components/loading'
import { Notice } from '../components/notice'
import { Select } from '../components/select'
import { Table, type Column } from '../components/table'
import { Tag } from '../components/tag'
import { Toolbar } from '../components/toolbar'
import * as format from '../format'
import { numberOf, withQuery } from '../location'

import './records.css'

const Editor = lazy(() => import('../components/editor'))

const LIMITS = ['50', '100', '500']

const COLUMNS: Column<TableRecord>[] = [
  { name: 'offset', header: 'Offset', width: '9rem', kind: 'number', cell: (record) => format.count(record.offset) },
  { name: 'time', header: 'Timestamp', width: '14rem', cell: (record) => format.time(record.timestampAtMs) },
  { name: 'key', header: 'Key', width: '14rem', cell: (record) => <Field field={record.key} /> },
  { name: 'value', header: 'Value', cell: (record) => <Field field={record.value} /> },
  { name: 'row', header: 'Row', width: '8rem', cell: (record) => (record.row === null ? <Tag kind="danger">not decoded</Tag> : <Tag kind="success">decoded</Tag>) },
  { name: 'headers', header: 'Headers', width: '7rem', kind: 'number', cell: (record) => record.headers.length },
]

export function shown(value: unknown) {
  if (value === null || value === undefined) {
    return <Tag>null</Tag>
  }
  if (typeof value === 'object' && 'encoding' in value) {
    return <Field field={value as Bytes} />
  }

  return typeof value === 'string' ? value : JSON.stringify(value)
}

export function Records({ name, table }: { name: string; table: TableDetail }) {
  const { path, query, route } = useLocation()
  const partition = Math.min(Math.max(numberOf(query.partition ?? '') ?? 0, 0), table.partitions - 1)
  const limit = numberOf(query.limit ?? '') ?? Number(LIMITS[0])
  const start = {
    offset: numberOf(query.offset ?? ''),
    timestamp: query.offset === undefined ? numberOf(query.time ?? '') : undefined,
    limit,
  }
  const page = useQuery(records(name, partition, start))
  const opened = query.record
  const go = (changes: Record<string, string | number | undefined>) => route(withQuery(path, query, changes), true)

  return (
    <>
      <Toolbar actions={<Button onClick={() => void page.refetch()}>Refresh</Button>}>
        <Input
          type="number"
          min={0}
          max={table.partitions - 1}
          prefix="Partition"
          value={String(partition)}
          onChange={(event) => go({ partition: event.currentTarget.value, offset: undefined, record: undefined })}
        />
        <Input
          type="number"
          min={0}
          prefix="From offset"
          value={query.offset ?? ''}
          onChange={(event) => go({ offset: event.currentTarget.value, time: undefined, record: undefined })}
        />
        <DateTime
          prefix="From time"
          clearable
          value={numberOf(query.time ?? '') ?? null}
          onChange={(ms) => go({ time: ms ?? undefined, offset: undefined, record: undefined })}
        />
        <Select prefix="Show" value={String(limit)} onChange={(event) => go({ limit: event.currentTarget.value })}>
          {LIMITS.map((each) => (
            <option key={each} value={each}>
              {each}
            </option>
          ))}
        </Select>
      </Toolbar>
      {page.data && page.error && <Notice kind="danger">{page.error.message}</Notice>}
      {page.data ? (
        <>
          <Table
            columns={COLUMNS}
            rows={page.data.items}
            rowKey={(record) => String(record.offset)}
            onSelect={(record) => go({ record: opened === String(record.offset) ? undefined : record.offset })}
            expanded={
              opened === undefined
                ? undefined
                : { key: opened, content: <Full record={page.data.items.find((record) => String(record.offset) === opened)} /> }
            }
            empty="No committed records here"
          />
          <Toolbar>
            <Notice>{format.counted(page.data.items.length, 'record')}</Notice>
            {page.data.next !== null && (
              <Button onClick={() => go({ offset: page.data.next ?? undefined, time: undefined, record: undefined })}>
                Next from offset {format.count(page.data.next)}
              </Button>
            )}
          </Toolbar>
        </>
      ) : (
        <Loading error={page.error} />
      )}
    </>
  )
}

function Full({ record }: { record: TableRecord | undefined }) {
  if (!record) {
    return null
  }

  return (
    <div class="records-detail">
      <section class="records-section">
        <h3>Row</h3>
        {record.row === null ? (
          <Notice kind="danger">{record.rowError ?? 'not decoded'}</Notice>
        ) : (
          <ErrorBoundary>
            <Editor language="json" text={JSON.stringify(record.row, null, 2)} />
          </ErrorBoundary>
        )}
      </section>
      {(['key', 'value'] as const).map((part) => {
        const field = record[part]

        return (
          <section key={part} class="records-section">
            <h3>{part === 'key' ? 'Key' : 'Value'}</h3>
            {field === null ? <Tag>null</Tag> : <ErrorBoundary><Editor {...pretty(field)} /></ErrorBoundary>}
          </section>
        )
      })}
      <section class="records-section">
        <h3>Headers</h3>
        {record.headers.length === 0 ? (
          <Tag>none</Tag>
        ) : (
          <dl class="records-headers">
            {record.headers.map((header, index) => (
              <div key={index} class="records-header">
                <dt>{header.key}</dt>
                <dd>
                  <Field field={header.value} />
                </dd>
              </div>
            ))}
          </dl>
        )}
      </section>
    </div>
  )
}
