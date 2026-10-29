import { Tag } from './tag'

import './fields.css'

export interface Field {
  id: number
  name: string
  type: string
  required: boolean
  fields?: Field[]
}

export function Fields({ fields }: { fields: Field[] }) {
  return (
    <ul class="fields">
      {fields.map((field) => (
        <li key={field.id} class="fields-item">
          <span class="fields-line">
            <span class="fields-name">{field.name}</span>
            <span class="fields-type">{field.type}</span>
            {field.required && <Tag>required</Tag>}
            <span class="fields-id">id {field.id}</span>
          </span>
          {field.fields && <Fields fields={field.fields} />}
        </li>
      ))}
    </ul>
  )
}
