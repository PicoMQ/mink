import { usePages } from '../api/query'
import { snapshots, type Snapshot, type TableDetail } from '../api/tables'
import { Age } from '../components/age'
import { Button } from '../components/button'
import { List } from '../components/list'
import type { Column } from '../components/table'
import { Toolbar } from '../components/toolbar'
import * as format from '../format'
import { page } from '../location'

export function Snapshots({ name }: { name: string; table: TableDetail }) {
  const pages = usePages(snapshots(name))

  const columns: Column<Snapshot>[] = [
    { name: 'id', header: 'Snapshot', width: '9rem', cell: (snapshot) => <a href={page('tables', name, 'snapshots', snapshot.id)}>{snapshot.id}</a> },
    { name: 'time', header: 'Committed', cell: (snapshot) => <Age atMs={snapshot.timestampAtMs} /> },
    { name: 'epoch', header: 'Epoch', width: '7rem', kind: 'number', cell: (snapshot) => snapshot.epoch },
    { name: 'records', header: 'Records', width: '10rem', kind: 'number', cell: (snapshot) => format.count(snapshot.records) },
    { name: 'added', header: 'Files added', width: '9rem', kind: 'number', cell: (snapshot) => format.count(snapshot.addedFiles) },
    { name: 'replaced', header: 'Files replaced', width: '9rem', kind: 'number', cell: (snapshot) => format.count(snapshot.replacedFiles) },
  ]

  return (
    <>
      <Toolbar actions={<Button onClick={pages.refresh}>Refresh</Button>} />
      <List pages={pages} columns={columns} rowKey={(snapshot) => String(snapshot.id)} noun="snapshot" />
    </>
  )
}
