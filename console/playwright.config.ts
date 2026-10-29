import { defineConfig } from '@playwright/test'

import { ADMIN } from './e2e/cluster'

export default defineConfig({
  testDir: 'e2e',
  globalSetup: './e2e/ready.ts',
  workers: 1,
  timeout: 180_000,
  expect: { timeout: 10_000 },
  reporter: 'list',
  use: {
    baseURL: ADMIN,
    permissions: ['clipboard-read', 'clipboard-write'],
  },
})
