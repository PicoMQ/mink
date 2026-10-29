import preact from '@preact/preset-vite'
import { defineConfig } from 'vitest/config'

import { gzip } from './build/gzip.ts'
import { integrity } from './build/integrity.ts'
import { size } from './build/size.ts'

export default defineConfig({
  plugins: [preact(), gzip(), size()],
  define: {
    DUCKDB_FILES: JSON.stringify(integrity()),
  },
  build: {
    outDir: '../mink/mink-server/console',
    emptyOutDir: true,
    reportCompressedSize: false,
  },
  server: {
    proxy: {
      '/admin': 'http://127.0.0.1:9180',
      '/catalog': 'http://127.0.0.1:9180',
      '/s3': 'http://127.0.0.1:9180',
    },
  },
  test: {
    include: ['test/**/*.test.ts'],
  },
})
