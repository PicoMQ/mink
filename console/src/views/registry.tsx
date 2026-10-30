import { usePages } from '../api/query'
import { schemas, type RegistrySchema } from '../api/registry'
import { Button } from '../components/button'
import { List } from '../components/list'
import type { Column } from '../components/table'
import { Toolbar } from '../components/toolbar'
import { View } from '../components/view'
import { page, tablePage } from '../location'

const COLUMNS: Column<RegistrySchema>[] = [
  { name: 'key', header: 'Schema', width: '14rem', cell: (schema) => <a href={page('registry', schema.key)}>{schema.key}</a> },
  { name: 'format', header: 'Format', width: '9rem', cell: (schema) => schema.format },
  { name: 'references', header: 'References', cell: (schema) => schema.references.map((reference) => reference.name).join(', ') || 'none' },
  {
    name: 'tables',
    header: 'Tables',
    cell: (schema) =>
      schema.tables.length === 0
        ? 'none'
        : schema.tables.map((table, index) => (
            <span key={table}>
              {index > 0 && ', '}
              <a href={tablePage(table)}>{table}</a>
            </span>
          )),
  },
]

export default function Registry() {
  const pages = usePages(schemas())

  return (
    <View trail={[{ label: 'Registry' }]}>
      <Toolbar actions={<Button onClick={pages.refresh}>Refresh</Button>} />
      <List pages={pages} columns={COLUMNS} rowKey={(schema) => schema.key} noun="schema" />
    </View>
  )
}
