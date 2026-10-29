import { useQuery } from '@tanstack/preact-query'
import { Server } from 'lucide-preact'
import { ErrorBoundary, lazy, useLocation, useRoute } from 'preact-iso'

import * as metrics from '../api/metrics'
import { leases, node } from '../api/nodes'
import { REFRESH_MS, usePages } from '../api/query'
import type { Lease } from '../api/tables'
import { Details } from '../components/details'
import { Heading } from '../components/heading'
import { List } from '../components/list'
import { Loading } from '../components/loading'
import { Notice } from '../components/notice'
import { Properties } from '../components/properties'
import { Table, type Column } from '../components/table'
import { Until } from '../components/until'
import { View } from '../components/view'
import * as format from '../format'
import { tablePage, withQuery } from '../location'
import { roles } from './nodes'

import './node.css'

const Chart = lazy(() => import('../components/chart'))

const SCRAPE_MS = 2000
const SCRAPES = 300

interface Series {
  label: string
  color: string
  name: string
  labels?: Record<string, string>
}

interface Metric {
  key: string
  label: string
  kind: 'rate' | 'gauge' | 'mean'
  show: (value: number) => string
  series: Series[]
}

const seconds = (value: number) => `${(value * 1000).toFixed(1)} ms`
const perSecond = (value: number) => `${format.bytes(value)}/s`

const METRICS: Metric[] = [
  {
    key: 'indexing',
    label: 'Indexing',
    kind: 'rate',
    show: format.rate,
    series: [
      { label: 'live', color: '--color-series-1', name: 'mink_indexed_records_total', labels: { source: 'live' } },
      { label: 'backfill', color: '--color-series-2', name: 'mink_indexed_records_total', labels: { source: 'backfill' } },
    ],
  },
  {
    key: 'kafka',
    label: 'Kafka',
    kind: 'rate',
    show: format.rate,
    series: [
      { label: 'fetch', color: '--color-series-1', name: 'mink_kafka_requests_total', labels: { request: 'fetch', result: 'ok' } },
      { label: 'offsets', color: '--color-series-2', name: 'mink_kafka_requests_total', labels: { request: 'offsets', result: 'ok' } },
      { label: 'metadata', color: '--color-series-3', name: 'mink_kafka_requests_total', labels: { request: 'metadata', result: 'ok' } },
      { label: 'errors', color: '--color-danger', name: 'mink_kafka_requests_total', labels: { result: 'error' } },
    ],
  },
  {
    key: 's3',
    label: 'S3 requests',
    kind: 'rate',
    show: format.rate,
    series: [
      { label: '2xx', color: '--color-success', name: 'mink_s3_requests_total', labels: { status: '2xx' } },
      { label: '4xx', color: '--color-series-2', name: 'mink_s3_requests_total', labels: { status: '4xx' } },
      { label: '5xx', color: '--color-danger', name: 'mink_s3_requests_total', labels: { status: '5xx' } },
    ],
  },
  {
    key: 'sent',
    label: 'S3 bytes',
    kind: 'rate',
    show: perSecond,
    series: [{ label: 'sent', color: '--color-series-1', name: 'mink_s3_sent_bytes_total' }],
  },
  {
    key: 'cache',
    label: 'Cache',
    kind: 'rate',
    show: format.rate,
    series: [
      { label: 'hit', color: '--color-success', name: 'nestor_blocks_hit_total', labels: { namespace: 'files' } },
      { label: 'miss', color: '--color-series-2', name: 'nestor_blocks_miss_total', labels: { namespace: 'files' } },
    ],
  },
  {
    key: 'cached',
    label: 'Cache size',
    kind: 'gauge',
    show: format.bytes,
    series: [{ label: 'bytes', color: '--color-series-1', name: 'mink_cache_bytes' }],
  },
  {
    key: 'builds',
    label: 'Builds',
    kind: 'rate',
    show: format.rate,
    series: [
      { label: 'ok', color: '--color-success', name: 'mink_builds_total', labels: { result: 'ok' } },
      { label: 'error', color: '--color-danger', name: 'mink_builds_total', labels: { result: 'error' } },
    ],
  },
  {
    key: 'build-time',
    label: 'Build time',
    kind: 'mean',
    show: seconds,
    series: [{ label: 'mean', color: '--color-series-1', name: 'mink_build_duration_seconds' }],
  },
  {
    key: 'catalog',
    label: 'Catalog',
    kind: 'rate',
    show: format.rate,
    series: [
      { label: '2xx', color: '--color-success', name: 'mink_catalog_requests_total', labels: { status: '2xx' } },
      { label: '4xx', color: '--color-series-2', name: 'mink_catalog_requests_total', labels: { status: '4xx' } },
      { label: '5xx', color: '--color-danger', name: 'mink_catalog_requests_total', labels: { status: '5xx' } },
      { label: 'tails', color: '--color-series-3', name: 'mink_tails_total' },
    ],
  },
  {
    key: 'commits',
    label: 'Commits',
    kind: 'rate',
    show: format.rate,
    series: [
      { label: 'ok', color: '--color-success', name: 'mink_commits_total', labels: { result: 'ok' } },
      { label: 'error', color: '--color-danger', name: 'mink_commits_total', labels: { result: 'error' } },
    ],
  },
  {
    key: 'commit-time',
    label: 'Commit time',
    kind: 'mean',
    show: seconds,
    series: [{ label: 'mean', color: '--color-series-1', name: 'mink_commit_duration_seconds' }],
  },
]

const LEASE_COLUMNS: Column<Lease>[] = [
  { name: 'table', header: 'Table', cell: (lease) => <a href={tablePage(lease.table)}>{lease.table}</a> },
  { name: 'partition', header: 'Partition', width: '9rem', cell: (lease) => (lease.partition === null ? 'writer' : lease.partition) },
  { name: 'epoch', header: 'Epoch', width: '7rem', kind: 'number', cell: (lease) => format.count(lease.epoch) },
]

export default function NodeView() {
  const { params } = useRoute()
  const id = params.id ?? ''
  const detail = useQuery({ ...node(id), refetchInterval: REFRESH_MS })
  const held = usePages(leases(id))
  const trail = [{ label: 'Nodes', href: '/nodes' }, { label: id.slice(0, 8) }]

  if (!detail.data) {
    return (
      <View trail={trail}>
        <Loading error={detail.error} />
      </View>
    )
  }

  const shown = detail.data
  const cache = shown.cache
  return (
    <View trail={trail}>
      <Heading title={shown.id.slice(0, 8)} icon={Server} tags={roles(shown)} />
      {detail.error && <Notice kind="danger">{detail.error.message}</Notice>}
      <Details
        items={[
          { label: 'Partition leases', value: format.count(shown.partitionLeases) },
          { label: 'Writer leases', value: format.count(shown.topicLeases) },
          { label: 'RAM cache', value: cache ? `${format.bytes(cache.memoryBytes)} of ${format.bytes(cache.memoryMaxBytes)}` : 'no s3 role' },
          { label: 'Disk cache', value: cache?.diskMaxBytes ? format.bytes(cache.diskMaxBytes) : 'none' },
          { label: 'Building', value: cache ? format.count(cache.building) : 'none' },
          { label: 'Row expires', value: <Until atMs={shown.expiresAtMs} /> },
        ]}
      />
      <Properties
        items={[
          { label: 'Id', value: shown.id },
          { label: 'S3 address', value: shown.s3Address ?? 'none' },
          { label: 'Admin address', value: shown.adminAddress ?? 'none' },
        ]}
      />
      <Live id={shown.id} />
      <section class="node-section">
        <h2>Leases</h2>
        <List pages={held} columns={LEASE_COLUMNS} rowKey={(lease) => `${lease.table}/${lease.partition ?? 'writer'}`} noun="lease" />
      </section>
      <section class="node-section">
        <h2>Settings</h2>
        <Table
          columns={[
            { name: 'name', header: 'Setting', width: '16rem', cell: ([name]) => name },
            { name: 'value', header: 'Value', cell: ([, value]) => value },
          ]}
          rows={Object.entries(shown.settings)}
          rowKey={([name]) => name}
        />
      </section>
    </View>
  )
}

function Live({ id }: { id: string }) {
  const { path, query } = useLocation()
  const { scrapes, error } = metrics.useScrapes(id, SCRAPE_MS, SCRAPES)
  const selected = METRICS.find((metric) => metric.key === query.metric) ?? METRICS[0]!

  const plotted = (metric: Metric) => ({
    times: (metric.kind === 'gauge' ? scrapes : scrapes.slice(1)).map((scrape) => scrape.atMs),
    series: metric.series.map((each) => ({
      label: each.label,
      color: each.color,
      values: values(metric, each, scrapes),
    })),
  })
  const chart = plotted(selected)

  return (
    <section class="node-live">
      <nav class="node-metrics">
        {METRICS.map((metric) => (
          <a
            key={metric.key}
            class="node-metric"
            href={withQuery(path, query, { metric: metric.key })}
            aria-current={metric === selected ? 'page' : undefined}
          >
            <span>{metric.label}</span>
            <small>{range(plotted(metric).series.flatMap((each) => each.values), metric.show)}</small>
          </a>
        ))}
      </nav>
      <div class="node-chart">
        {error && <Loading error={error} />}
        <ErrorBoundary>
          <Chart key={selected.key} times={chart.times} series={chart.series} show={selected.show} />
        </ErrorBoundary>
        <small class="node-note">
          every {SCRAPE_MS / 1000}s since this page opened, last {format.duration(SCRAPE_MS * SCRAPES)}
        </small>
      </div>
    </section>
  )
}

function values(metric: Metric, series: Series, scrapes: metrics.Scrape[]): (number | null)[] {
  switch (metric.kind) {
    case 'rate':
      return metrics.series(scrapes, series.name, series.labels)
    case 'gauge':
      return metrics.gauge(scrapes, series.name, series.labels)
    case 'mean':
      return metrics.means(scrapes, series.name)
  }
}

function range(values: (number | null)[], show: (value: number) => string): string {
  const known = values.filter((value) => value !== null)

  return known.length === 0 ? 'waiting' : `${show(Math.min(...known))} - ${show(Math.max(...known))}`
}
