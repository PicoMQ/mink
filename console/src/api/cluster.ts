import { read } from './query'

export interface Cluster {
  nodeId: string
  roles: string[]
  version: string
  namespace: string
  catalogUrl: string
  s3Endpoint: string
  warehouse: string
  registry: { location: string; reachable: boolean } | null
  sql: Sql | null
}

export interface Sql {
  duckdbEngine: string | null
  duckdbExtensions: string | null
}

export const NO_SQL = 'This node does not run the catalog and s3 roles, so it cannot run SQL'

export const cluster = () => read<Cluster>(['cluster'])
