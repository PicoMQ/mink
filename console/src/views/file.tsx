import { useQuery } from '@tanstack/preact-query'
import { FileText } from 'lucide-preact'
import { useRoute } from 'preact-iso'

import { cluster } from '../api/cluster'
import { file, type Column as Stat } from '../api/tables'
import { Details } from '../components/details'
import { Heading } from '../components/heading'
import { Loading } from '../components/loading'
import { Properties } from '../components/properties'
import { Table } from '../components/table'
import { View } from '../components/view'
import * as format from '../format'
import { page, tablePage } from '../location'
import { shown } from './records'

export default function FileView() {
  const { params } = useRoute()
  const name = params.name ?? ''
  const path = params.file ?? ''
  const detail = useQuery(file(name, path))
  const endpoint = useQuery(cluster()).data?.s3Endpoint
  const trail = [
    { label: 'Tables', href: '/tables' },
    { label: name, href: tablePage(name, 'files') },
    { label: path },
  ]

  if (!detail.data) {
    return (
      <View trail={trail}>
        <Loading error={detail.error} />
      </View>
    )
  }

  const data = detail.data
  return (
    <View trail={trail}>
      <Heading title={data.name} icon={FileText} />
      <Details
        items={[
          { label: 'Partition', value: data.partition },
          { label: 'Offsets', value: `${format.count(data.startOffset)} - ${format.count(data.endOffset)}` },
          { label: 'Records', value: format.count(data.records) },
          { label: 'Size', value: format.bytes(data.bytes) },
          { label: 'Bucket', value: data.bucket ?? 'none' },
          { label: 'Spec', value: data.specId },
          { label: 'Schema', value: data.schemaId },
          { label: 'Version', value: data.version },
          { label: 'Added by', value: snapshotLink(name, data.addedSnapshotId) },
          { label: 'Replaced by', value: snapshotLink(name, data.replacedSnapshotId) },
        ]}
      />
      <Properties
        items={[
          { label: 'First record', value: format.time(data.firstTimestampAtMs) },
          { label: 'Path', value: endpoint ? <a href={download(endpoint, data.path)}>{data.path}</a> : data.path },
          {
            label: 'Parts',
            value: data.partsRanges.length === 0 ? 'not assembled' : data.partsRanges.map((part) => `${part.startOffset}-${part.endOffset}`).join(', '),
          },
        ]}
      />
      <Table
        columns={[
          { name: 'field', header: 'Field', cell: (column: Stat) => column.name ?? `field ${column.fieldId}` },
          { name: 'id', header: 'Id', width: '6rem', cell: (column: Stat) => column.fieldId },
          { name: 'lower', header: 'Lower bound', cell: (column: Stat) => shown(column.lower) },
          { name: 'upper', header: 'Upper bound', cell: (column: Stat) => shown(column.upper) },
          { name: 'nulls', header: 'Nulls', width: '8rem', kind: 'number', cell: (column: Stat) => format.count(column.nulls) },
          { name: 'nans', header: 'NaNs', width: '8rem', kind: 'number', cell: (column: Stat) => format.count(column.nans) },
        ]}
        rows={data.columns}
        rowKey={(column) => String(column.fieldId)}
        empty="No column statistics"
      />
    </View>
  )
}

function snapshotLink(name: string, id: number | null) {
  return id === null ? 'none' : <a href={page('tables', name, 'snapshots', id)}>snapshot {id}</a>
}

function download(endpoint: string, path: string): string {
  return `${endpoint.replace(/\/$/, '')}/${path.replace(/^s3:\/\//, '')}`
}
