import { useMutation } from '@tanstack/preact-query'
import { Plus } from 'lucide-preact'
import { useLocation } from 'preact-iso'
import { useState } from 'preact/hooks'

import { usePages } from '../api/query'
import { addTable, tables, type State, type Table } from '../api/tables'
import { Age } from '../components/age'
import { Button } from '../components/button'
import { Dialog } from '../components/dialog'
import { useDraft } from '../components/draft'
import { Form } from '../components/form'
import { Input } from '../components/input'
import { Label } from '../components/label'
import { List } from '../components/list'
import { Search } from '../components/search'
import { Select } from '../components/select'
import type { Column } from '../components/table'
import { Tag } from '../components/tag'
import { Toolbar } from '../components/toolbar'
import { View } from '../components/view'
import * as format from '../format'
import { tablePage, withQuery } from '../location'

export const GRANULARITIES = ['hour', 'day', 'month', 'none']

const COLUMNS: Column<Table>[] = [
  { name: 'name', header: 'Name', cell: (table) => <a href={tablePage(table.name)}>{table.name}</a> },
  { name: 'state', header: 'State', width: '9rem', cell: (table) => state(table.state) },
  { name: 'partitions', header: 'Partitions', width: '8rem', kind: 'number', cell: (table) => format.count(table.partitions) },
  { name: 'granularity', header: 'Granularity', width: '9rem', cell: (table) => table.granularity },
  { name: 'snapshot', header: 'Snapshot', width: '12rem', cell: snapshot },
  { name: 'records', header: 'Records', width: '10rem', kind: 'number', cell: (table) => format.count(table.records) },
  { name: 'bytes', header: 'Indexed', width: '9rem', kind: 'number', cell: (table) => format.bytes(table.bytes) },
]

export function state(shown: State) {
  const kind = shown === 'indexing' ? 'success' : shown === 'removing' ? 'plain' : 'danger'

  return <Tag kind={kind}>{shown}</Tag>
}

function snapshot(table: Table) {
  if (table.snapshotId === null || table.snapshotAtMs === null) {
    return 'none yet'
  }

  return (
    <>
      {table.snapshotId}, <Age atMs={table.snapshotAtMs} />
    </>
  )
}

export default function Tables() {
  const { path, query, route } = useLocation()
  const pages = usePages(tables(query.prefix))
  const [adding, setAdding] = useState(false)

  return (
    <View trail={[{ label: 'Tables' }]}>
      <Toolbar
        actions={
          <>
            <Button onClick={pages.refresh}>Refresh</Button>
            <Button kind="primary" onClick={() => setAdding(true)}>
              <Plus size={16} />
              Add table
            </Button>
          </>
        }
      >
        <Search
          value={query.prefix ?? ''}
          hint="Search tables by name prefix"
          onChange={(prefix) => route(withQuery(path, query, { prefix }), true)}
        />
      </Toolbar>
      <List pages={pages} columns={COLUMNS} rowKey={(table) => table.name} noun="table" />
      <Dialog title="Add table" open={adding} onClose={() => setAdding(false)}>
        <AddTable onCancel={() => setAdding(false)} onAdded={(table) => route(tablePage(table.name))} />
      </Dialog>
    </View>
  )
}

function AddTable({ onCancel, onAdded }: { onCancel: () => void; onAdded: (table: Table) => void }) {
  const { draft, edit } = useDraft({ topic: '', granularity: '' })
  const mutation = useMutation({
    mutationFn: () => addTable(draft.topic.trim(), draft.granularity || undefined),
    onSuccess: onAdded,
  })

  return (
    <Form
      submit="Add"
      busy={mutation.isPending}
      disabled={draft.topic.trim() === ''}
      error={mutation.error}
      onSubmit={() => mutation.mutate()}
      onCancel={onCancel}
    >
      <Label text="Kafka topic">
        <Input value={draft.topic} required placeholder="orders" onInput={edit('topic')} />
      </Label>
      <Label text="Granularity">
        <Select value={draft.granularity} onChange={edit('granularity')}>
          <option value="">The node's default</option>
          {GRANULARITIES.map((granularity) => (
            <option key={granularity} value={granularity}>
              {granularity}
            </option>
          ))}
        </Select>
      </Label>
    </Form>
  )
}
