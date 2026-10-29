import { useQuery } from '@tanstack/preact-query'
import { FileCode } from 'lucide-preact'
import { ErrorBoundary, lazy, useRoute } from 'preact-iso'

import { schema } from '../api/registry'
import { indent } from '../components/field'
import { Heading } from '../components/heading'
import { Loading } from '../components/loading'
import { Properties } from '../components/properties'
import { View } from '../components/view'
import { page, tablePage } from '../location'

const Editor = lazy(() => import('../components/editor'))

export default function Definition() {
  const { params } = useRoute()
  const id = Number(params.id)
  const detail = useQuery(schema(id))
  const trail = [{ label: 'Registry', href: '/registry' }, { label: `schema ${params.id ?? ''}` }]

  if (!detail.data) {
    return (
      <View trail={trail}>
        <Loading error={detail.error} />
      </View>
    )
  }

  const data = detail.data
  const json = indent(data.definition)
  return (
    <View trail={trail}>
      <Heading title={`Schema ${data.id}`} icon={FileCode} />
      <Properties
        items={[
          { label: 'Format', value: data.format },
          {
            label: 'References',
            value:
              data.references.length === 0
                ? 'none'
                : data.references.map((reference, index) => (
                    <span key={reference.id}>
                      {index > 0 && ', '}
                      <a href={page('registry', reference.id)}>{reference.name}</a>
                    </span>
                  )),
          },
          {
            label: 'Tables',
            value:
              data.tables.length === 0
                ? 'none'
                : data.tables.map((table, index) => (
                    <span key={table}>
                      {index > 0 && ', '}
                      <a href={tablePage(table)}>{table}</a>
                    </span>
                  )),
          },
        ]}
      />
      <ErrorBoundary>
        <Editor language={json === null ? 'text' : 'json'} text={json ?? data.definition} />
      </ErrorBoundary>
    </View>
  )
}
