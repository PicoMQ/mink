import { useQuery } from '@tanstack/preact-query'
import { Camera } from 'lucide-preact'
import { useRoute } from 'preact-iso'

import { snapshot } from '../api/tables'
import { Details } from '../components/details'
import { Heading } from '../components/heading'
import { Loading } from '../components/loading'
import { Properties } from '../components/properties'
import { Table } from '../components/table'
import { View } from '../components/view'
import * as format from '../format'
import { tablePage } from '../location'

export default function SnapshotView() {
  const { params } = useRoute()
  const name = params.name ?? ''
  const id = Number(params.id)
  const detail = useQuery(snapshot(name, id))
  const trail = [
    { label: 'Tables', href: '/tables' },
    { label: name, href: tablePage(name, 'snapshots') },
    { label: `snapshot ${params.id ?? ''}` },
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
      <Heading title={`Snapshot ${data.id}`} icon={Camera} />
      <Details
        items={[
          { label: 'Epoch', value: data.epoch },
          { label: 'Records', value: format.count(data.records) },
          { label: 'Files added', value: format.count(data.addedFiles) },
          { label: 'Files replaced', value: format.count(data.replacedFiles) },
        ]}
      />
      <Properties
        items={[
          { label: 'Committed', value: format.time(data.timestampAtMs) },
          { label: 'Metadata', value: data.metadataLocation },
        ]}
      />
      <Table
        columns={[
          { name: 'partition', header: 'Partition', width: '9rem', cell: (offsets) => offsets.partition },
          { name: 'start', header: 'From offset', kind: 'number', cell: (offsets) => format.count(offsets.startOffset) },
          { name: 'end', header: 'To offset', kind: 'number', cell: (offsets) => format.count(offsets.endOffset) },
        ]}
        rows={data.offsets}
        rowKey={(offsets) => String(offsets.partition)}
      />
    </View>
  )
}
