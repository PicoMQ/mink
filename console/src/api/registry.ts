import { listing, read } from './query'

export interface RegistrySchema {
  id: number
  format: string
  references: { name: string; id: number }[]
  tables: string[]
}

interface RegistryDetail extends RegistrySchema {
  definition: string
}

export const schemas = () => listing<RegistrySchema>(['registry'])

export const schema = (id: number) => read<RegistryDetail>(['registry', id])
