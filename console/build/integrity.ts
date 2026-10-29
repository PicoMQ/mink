import { createHash } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { createRequire } from 'node:module'

const FILES = ['duckdb-mvp.wasm', 'duckdb-eh.wasm', 'duckdb-browser-mvp.worker.js', 'duckdb-browser-eh.worker.js']

export function integrity(): Record<string, string> {
  const require = createRequire(import.meta.url)

  return Object.fromEntries(
    FILES.map((file) => {
      const bytes = readFileSync(require.resolve(`@duckdb/duckdb-wasm/dist/${file}`))
      return [file, `sha384-${createHash('sha384').update(bytes).digest('base64')}`]
    }),
  )
}
