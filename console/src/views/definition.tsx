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
  const key = params.key ?? ''
  const detail = useQuery(schema(key))
  const trail = [{ label: 'Registry', href: '/registry' }, { label: `schema ${key}` }]

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
      <Heading title={`Schema ${data.key}`} icon={FileCode} />
      <Properties
        items={[
          { label: 'Format', value: data.format },
          {
            label: 'References',
            value:
              data.references.length === 0
                ? 'none'
                : data.references.map((reference, index) => (
                    <span key={reference.key}>
                      {index > 0 && ', '}
                      <a href={page('registry', reference.key)}>{reference.name}</a>
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
