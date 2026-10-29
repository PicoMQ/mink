import { Notice } from './notice'

export function Loading({ error }: { error: Error | null }) {
  return error ? <Notice kind="danger">{error.message}</Notice> : <Notice>Loading</Notice>
}
