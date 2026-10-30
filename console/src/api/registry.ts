import { listing, read } from './query'

export interface RegistrySchema {
  key: string
  format: string
  references: { name: string; key: string }[]
  tables: string[]
}

interface RegistryDetail extends RegistrySchema {
  definition: string
}

export const schemas = () => listing<RegistrySchema>(['registry'])

export const schema = (key: string) => read<RegistryDetail>(['registry', key])
