import { usePages } from '../api/query'
import { partitions, type Partition, type TableDetail } from '../api/tables'
import { Button } from '../components/button'
import { List } from '../components/list'
import { Range } from '../components/range'
import type { Column } from '../components/table'
import { Toolbar } from '../components/toolbar'
import * as format from '../format'
import { page, tablePage, withQuery } from '../location'

export function Partitions({ name }: { name: string; table: TableDetail }) {
  const pages = usePages(partitions(name))

  const columns: Column<Partition>[] = [
    {
      name: 'index',
      header: 'Partition',
      width: '8rem',
      cell: (partition) => <a href={withQuery(tablePage(name, 'records'), {}, { partition: partition.index })}>{partition.index}</a>,
    },
    {
      name: 'range',
      header: 'Indexed within Kafka',
      cell: (partition) => (
        <Range
          earliest={partition.earliestOffset}
          stable={partition.lastStableOffset}
          start={partition.startOffset}
          end={partition.endOffset}
        />
      ),
    },
    { name: 'earliest', header: 'Earliest', width: '9rem', kind: 'number', cell: (partition) => format.count(partition.earliestOffset) },
    { name: 'start', header: 'Indexed from', width: '9rem', kind: 'number', cell: (partition) => format.count(partition.startOffset) },
    { name: 'end', header: 'Indexed to', width: '9rem', kind: 'number', cell: (partition) => format.count(partition.endOffset) },
    { name: 'stable', header: 'Last stable', width: '9rem', kind: 'number', cell: (partition) => format.count(partition.lastStableOffset) },
    { name: 'lag', header: 'Lag', width: '7rem', kind: 'number', cell: (partition) => format.count(partition.lag) },
    { name: 'backfill', header: 'Backfill left', width: '9rem', kind: 'number', cell: (partition) => format.count(partition.backfillRemaining) },
    {
      name: 'lease',
      header: 'Indexer',
      width: '9rem',
      cell: (partition) =>
        partition.lease ? <a href={page('nodes', partition.lease.holder)}>{partition.lease.holder.slice(0, 8)}</a> : 'none',
    },
  ]

  return (
    <>
      <Toolbar actions={<Button onClick={pages.refresh}>Refresh</Button>} />
      <List pages={pages} columns={columns} rowKey={(partition) => String(partition.index)} noun="partition" />
    </>
  )
}
