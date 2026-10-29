import { nodes, type ClusterNode } from '../api/nodes'
import { usePages } from '../api/query'
import { Button } from '../components/button'
import { List } from '../components/list'
import type { Column } from '../components/table'
import { Tag } from '../components/tag'
import { Toolbar } from '../components/toolbar'
import { Until } from '../components/until'
import { View } from '../components/view'
import * as format from '../format'
import { page } from '../location'

export const NODE_COLUMNS: Column<ClusterNode>[] = [
  { name: 'node', header: 'Node', width: '10rem', cell: (node) => nodeLink(node.id) },
  { name: 'roles', header: 'Roles', cell: roles },
  { name: 'partitions', header: 'Partition leases', width: '10rem', kind: 'number', cell: (node) => format.count(node.partitionLeases) },
  { name: 'writers', header: 'Writer leases', width: '9rem', kind: 'number', cell: (node) => format.count(node.topicLeases) },
  { name: 'expires', header: 'Row expires', width: '9rem', cell: (node) => <Until atMs={node.expiresAtMs} /> },
]

function nodeLink(id: string) {
  return <a href={page('nodes', id)}>{id.slice(0, 8)}</a>
}

export function roles(node: { roles: string[] }) {
  return (
    <>
      {node.roles.map((role) => (
        <Tag key={role}>{role}</Tag>
      ))}
    </>
  )
}

export default function Nodes() {
  const pages = usePages(nodes())

  return (
    <View trail={[{ label: 'Nodes' }]}>
      <Toolbar actions={<Button onClick={pages.refresh}>Refresh</Button>} />
      <List pages={pages} columns={NODE_COLUMNS} rowKey={(node) => node.id} noun="node" />
    </View>
  )
}
