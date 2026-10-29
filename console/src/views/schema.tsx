import { useQuery } from '@tanstack/preact-query'
import { useLocation } from 'preact-iso'

import { schemas, type Reference, type TableDetail } from '../api/tables'
import { Fields } from '../components/fields'
import { Loading } from '../components/loading'
import { Select } from '../components/select'
import { Table, type Column } from '../components/table'
import { Tag } from '../components/tag'
import { Toolbar } from '../components/toolbar'
import { page, withQuery } from '../location'

import './schema.css'

const REFERENCE_COLUMNS: Column<Reference>[] = [
  { name: 'part', header: 'Part', width: '7rem', cell: (reference) => reference.part },
  { name: 'reference', header: 'Reference', cell: (reference) => reference.reference },
  {
    name: 'registry',
    header: 'Registry schema',
    width: '12rem',
    cell: (reference) =>
      reference.registryId === null ? 'none' : <a href={page('registry', reference.registryId)}>{reference.registryId}</a>,
  },
  {
    name: 'result',
    header: 'Result',
    width: '16rem',
    cell: (reference) =>
      reference.rejection === null ? <Tag kind="success">merged into {reference.schemaId}</Tag> : <Tag kind="danger">{reference.rejection}</Tag>,
  },
]

export function Schema({ name }: { name: string; table: TableDetail }) {
  const { path, query, route } = useLocation()
  const loaded = useQuery(schemas(name))

  if (!loaded.data) {
    return <Loading error={loaded.error} />
  }

  const data = loaded.data
  const chosen = Number(query.schema ?? data.current)
  const shown = data.schemas.find((schema) => schema.id === chosen) ?? data.schemas.at(-1)

  return (
    <div class="schema">
      <Toolbar>
        <Select
          prefix="Schema"
          value={String(shown?.id ?? '')}
          onChange={(event) => route(withQuery(path, query, { schema: event.currentTarget.value }), true)}
        >
          {data.schemas.map((schema) => (
            <option key={schema.id} value={schema.id}>
              {schema.id === 0 ? '0, binary' : schema.id}
              {schema.id === data.current ? ', current' : ''}
            </option>
          ))}
        </Select>
      </Toolbar>
      {shown && <Fields fields={shown.fields} />}
      <h2>References</h2>
      <Table
        columns={REFERENCE_COLUMNS}
        rows={data.references}
        rowKey={(reference) => `${reference.part}/${reference.reference}`}
        empty="No record has named a registry schema"
      />
    </div>
  )
}
