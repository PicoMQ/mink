import { admin, requestText } from './client'
import { listing, read } from './query'
import type { Lease } from './tables'

export interface ClusterNode {
  id: string
  roles: string[]
  s3Address: string | null
  adminAddress: string | null
  expiresAtMs: number
  partitionLeases: number
  topicLeases: number
}

interface NodeDetail extends ClusterNode {
  settings: Record<string, string>
  cache: { memoryBytes: number; memoryMaxBytes: number; diskMaxBytes: number | null; building: number } | null
}

export const nodes = () => listing<ClusterNode>(['nodes'])

export const node = (id: string) => read<NodeDetail>(['nodes', id])

export const leases = (node: string) => listing<Lease>(['leases'], { node })

export const nodeMetrics = (id: string, signal: AbortSignal) => requestText(admin('nodes', id, 'metrics'), { signal })
