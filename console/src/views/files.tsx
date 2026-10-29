import { useLocation } from 'preact-iso'

import { usePages } from '../api/query'
import { files, type DataFile, type TableDetail } from '../api/tables'
import { Button } from '../components/button'
import { Input } from '../components/input'
import { List } from '../components/list'
import type { Column } from '../components/table'
import { Toolbar } from '../components/toolbar'
import * as format from '../format'
import { filePage, numberOf, withQuery } from '../location'

export function Files({ name }: { name: string; table: TableDetail }) {
  const { path, query, route } = useLocation()
  const partition = numberOf(query.partition ?? '')
  const bucket = numberOf(query.bucket ?? '')
  const pages = usePages(files(name, partition, bucket))
  const filter = (field: string) => (event: { currentTarget: { value: string } }) =>
    route(withQuery(path, query, { [field]: event.currentTarget.value }), true)

  const columns: Column<DataFile>[] = [
    { name: 'name', header: 'File', cell: (file) => <a href={filePage(name, file.name)}>{file.name}</a> },
    { name: 'partition', header: 'Partition', width: '8rem', cell: (file) => file.partition },
    { name: 'offsets', header: 'Offsets', width: '14rem', cell: (file) => `${format.count(file.startOffset)} - ${format.count(file.endOffset)}` },
    { name: 'bucket', header: 'Bucket', width: '8rem', kind: 'number', cell: (file) => file.bucket ?? 'none' },
    { name: 'schema', header: 'Schema', width: '7rem', cell: (file) => file.schemaId },
    { name: 'parts', header: 'Parts', width: '6rem', kind: 'number', cell: (file) => file.parts || 'none' },
    { name: 'records', header: 'Records', width: '9rem', kind: 'number', cell: (file) => format.count(file.records) },
    { name: 'bytes', header: 'Size', width: '8rem', kind: 'number', cell: (file) => format.bytes(file.bytes) },
  ]

  return (
    <>
      <Toolbar actions={<Button onClick={pages.refresh}>Refresh</Button>}>
        <Input type="number" min={0} prefix="Partition" value={query.partition ?? ''} onInput={filter('partition')} />
        <Input type="number" prefix="Bucket" value={query.bucket ?? ''} onInput={filter('bucket')} />
      </Toolbar>
      <List pages={pages} columns={columns} rowKey={(file) => file.name} noun="file" />
    </>
  )
}
