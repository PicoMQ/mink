import { useMutation, useQuery, useQueryClient } from '@tanstack/preact-query'
import { Database } from 'lucide-preact'
import { useLocation, useRoute } from 'preact-iso'
import { useState } from 'preact/hooks'

import { REFRESH_MS } from '../api/query'
import { changeGranularity, removeTable, table, type TableDetail } from '../api/tables'
import { Age } from '../components/age'
import { Button } from '../components/button'
import { Confirm } from '../components/confirm'
import { Details } from '../components/details'
import { Dialog } from '../components/dialog'
import { useDraft } from '../components/draft'
import { Form } from '../components/form'
import { Heading } from '../components/heading'
import { Label } from '../components/label'
import { Loading } from '../components/loading'
import { Notice } from '../components/notice'
import { Properties } from '../components/properties'
import { Select } from '../components/select'
import { Tabs } from '../components/tabs'
import { View } from '../components/view'
import * as format from '../format'
import { page, tablePage } from '../location'
import { Files } from './files'
import Missing from './missing'
import { Partitions } from './partitions'
import { Records } from './records'
import { Schema } from './schema'
import { Snapshots } from './snapshots'
import { GRANULARITIES, state } from './tables'

const TABS = [
  { path: undefined, label: 'Overview', component: Summary },
  { path: 'partitions', label: 'Partitions', component: Partitions },
  { path: 'files', label: 'Files', component: Files },
  { path: 'snapshots', label: 'Snapshots', component: Snapshots },
  { path: 'schema', label: 'Schema', component: Schema },
  { path: 'records', label: 'Records', component: Records },
]

export default function TableView() {
  const { params } = useRoute()
  const { route } = useLocation()
  const client = useQueryClient()
  const name = params.name ?? ''
  const tab = TABS.find((each) => each.path === params.tab)
  const detail = useQuery({ ...table(name), refetchInterval: REFRESH_MS })
  const [removing, setRemoving] = useState(false)
  const [changing, setChanging] = useState(false)
  const trail = [{ label: 'Tables', href: '/tables' }, { label: name }]

  const remove = async () => {
    await removeTable(name)

    client.removeQueries({ queryKey: table(name).queryKey, exact: true })
    route('/tables')
  }

  if (!tab) {
    return <Missing />
  }
  if (!detail.data) {
    return (
      <View trail={trail}>
        <Loading error={detail.error} />
      </View>
    )
  }

  const shown = detail.data
  const Content = tab.component
  return (
    <View trail={trail}>
      <Heading
        title={shown.name}
        icon={Database}
        tags={state(shown.state)}
        actions={
          <>
            <Button onClick={() => setChanging(true)} disabled={shown.state === 'removing'}>
              Change granularity
            </Button>
            <Button kind="danger" onClick={() => setRemoving(true)} disabled={shown.state === 'removing'}>
              Remove table
            </Button>
          </>
        }
      />
      {detail.error && <Notice kind="danger">{detail.error.message}</Notice>}
      <Tabs label="Table" tabs={TABS} href={(path) => tablePage(name, path)} />
      <Content name={name} table={shown} />
      <Dialog title="Change granularity" open={changing} onClose={() => setChanging(false)}>
        <Granularity shown={shown} onDone={() => setChanging(false)} />
      </Dialog>
      <Confirm
        title="Remove table"
        name={shown.name}
        action="Remove table"
        open={removing}
        onClose={() => setRemoving(false)}
        onConfirm={remove}
      >
        Mink stops indexing {shown.name} and deletes its metadata objects and rows once its leases end. The Kafka topic
        is not touched.
      </Confirm>
    </View>
  )
}

function Summary({ table }: { name: string; table: TableDetail }) {
  return (
    <>
      <Details
      items={[
        { label: 'Partitions', value: format.count(table.partitions) },
        { label: 'Records', value: format.count(table.records) },
        { label: 'Indexed', value: format.bytes(table.bytes) },
        { label: 'Lag', value: table.lag === null ? 'not indexing' : format.count(table.lag) },
        { label: 'Granularity', value: table.granularity },
        { label: 'Schema', value: table.schemaId === 0 ? 'binary' : `schema ${table.schemaId}` },
        {
          label: 'Snapshot',
          value:
            table.snapshotId === null || table.snapshotAtMs === null ? (
              'none yet'
            ) : (
              <a href={page('tables', table.name, 'snapshots', table.snapshotId)}>
                {table.snapshotId}, <Age atMs={table.snapshotAtMs} />
              </a>
            ),
        },
        {
          label: 'Writer',
          value: table.writer ? <a href={page('nodes', table.writer.holder)}>{table.writer.holder.slice(0, 8)}</a> : 'none',
        },
        {
          label: 'Statistics',
          value: table.statisticsAtMs === null ? 'none yet' : <Age atMs={table.statisticsAtMs} />,
        },
      ]}
      />
      <Properties
        items={[
          { label: 'uuid', value: table.uuid },
          { label: 'Location', value: table.location },
          { label: 'Metadata', value: table.metadataLocation ?? 'none yet' },
          { label: 'Specs', value: table.specs.map((spec) => `${spec.id}: ${spec.granularity}`).join(', ') },
        ]}
      />
    </>
  )
}

function Granularity({ shown, onDone }: { shown: TableDetail; onDone: () => void }) {
  const client = useQueryClient()
  const { draft, edit } = useDraft({ granularity: shown.granularity })
  const mutation = useMutation({
    mutationFn: () => changeGranularity(shown.name, draft.granularity),
    onSuccess: () => {
      void client.invalidateQueries({ queryKey: table(shown.name).queryKey, exact: true })
      onDone()
    },
  })

  return (
    <Form submit="Save" busy={mutation.isPending} error={mutation.error} onSubmit={() => mutation.mutate()} onCancel={onDone}>
      <Label text="Granularity of the files written from now on">
        <Select value={draft.granularity} onChange={edit('granularity')}>
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
