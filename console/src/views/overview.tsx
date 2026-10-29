import { useQuery } from '@tanstack/preact-query'
import { Archive, Database, HardDrive, Layers, Network, Recycle, Server } from 'lucide-preact'

import { cluster } from '../api/cluster'
import { nodes } from '../api/nodes'
import { overview } from '../api/overview'
import { REFRESH_MS, usePages } from '../api/query'
import { Age } from '../components/age'
import { Details } from '../components/details'
import { Heading } from '../components/heading'
import { List } from '../components/list'
import { Loading } from '../components/loading'
import { Notice } from '../components/notice'
import { Table } from '../components/table'
import { Tag } from '../components/tag'
import { View } from '../components/view'
import * as format from '../format'
import { tablePage } from '../location'
import { NODE_COLUMNS } from './nodes'

import './overview.css'

const RANKING_REFRESH_MS = 60_000

export default function Overview() {
  const state = useQuery({ ...cluster(), refetchInterval: REFRESH_MS })
  const ranking = useQuery({ ...overview(), refetchInterval: RANKING_REFRESH_MS })
  const pages = usePages(nodes())

  const registry = state.data?.registry
  const reachable = !state.data
    ? ''
    : registry
      ? <Tag kind={registry.reachable ? 'success' : 'danger'}>{registry.reachable ? 'reachable' : 'unreachable'}</Tag>
      : 'none'

  return (
    <View trail={[{ label: 'Overview' }]}>
      {state.error && <Notice kind="danger">{state.error.message}</Notice>}
      {ranking.data ? (
        <>
          <Heading title={`Namespace ${state.data?.namespace ?? ''}`} icon={Network} />
          {ranking.error && <Notice kind="danger">{ranking.error.message}</Notice>}
          <Details
            items={[
              { label: 'Live nodes', icon: Server, value: format.count(ranking.data.nodes) },
              { label: 'Tables', icon: Layers, value: format.count(ranking.data.tables) },
              { label: 'Records', icon: Database, value: format.count(ranking.data.records) },
              { label: 'Indexed', icon: HardDrive, value: format.bytes(ranking.data.bytes) },
              { label: 'Collectable objects', icon: Recycle, value: format.count(ranking.data.collectable) },
              { label: 'Schema registry', icon: Archive, value: reachable },
            ]}
          />
        </>
      ) : (
        <Loading error={ranking.error} />
      )}
      <section class="overview-section">
        <div class="overview-title">
          <h2>Tables</h2>
          {ranking.data && (
            <small>
              ranked <Age atMs={ranking.data.computedAtMs} />
            </small>
          )}
        </div>
        {ranking.data && (
          <div class="overview-rankings">
            <Table
              columns={[
                { name: 'table', header: 'Most lagging', cell: (entry) => <a href={tablePage(entry.table)}>{entry.table}</a> },
                { name: 'lag', header: 'Lag', width: '9rem', kind: 'number', cell: (entry) => format.count(entry.lag) },
              ]}
              rows={ranking.data.mostLagging}
              rowKey={(entry) => entry.table}
              empty="No table is behind Kafka"
            />
            <Table
              columns={[
                { name: 'table', header: 'Largest', cell: (entry) => <a href={tablePage(entry.table)}>{entry.table}</a> },
                { name: 'bytes', header: 'Indexed', width: '9rem', kind: 'number', cell: (entry) => format.bytes(entry.bytes) },
              ]}
              rows={ranking.data.largest}
              rowKey={(entry) => entry.table}
              empty="No tables yet"
            />
            <Table
              columns={[
                { name: 'table', header: 'Stalest', cell: (entry) => <a href={tablePage(entry.table)}>{entry.table}</a> },
                {
                  name: 'snapshot',
                  header: 'Last snapshot',
                  width: '9rem',
                  cell: (entry) => (entry.snapshotAtMs === null ? 'none yet' : <Age atMs={entry.snapshotAtMs} />),
                },
              ]}
              rows={ranking.data.stalest}
              rowKey={(entry) => entry.table}
              empty="No tables yet"
            />
          </div>
        )}
      </section>
      <section class="overview-section">
        <h2>Nodes</h2>
        <List pages={pages} columns={NODE_COLUMNS} rowKey={(node) => node.id} noun="node" />
      </section>
    </View>
  )
}
