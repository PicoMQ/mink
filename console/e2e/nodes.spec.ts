import { expect, test } from '@playwright/test'

import { detail, rows } from './cluster'

test('a node opens from the list and draws its metrics as they arrive', async ({ page }) => {
  await page.goto('/nodes')
  await expect(page.getByText(/^\d+ nodes?$/)).toBeVisible()

  await rows(page).first().getByRole('link').first().click()
  await expect(detail(page, 'RAM cache')).toContainText(' of ')
  await expect(detail(page, 'Disk cache')).toHaveText(/\d/)
  await expect(page.getByRole('heading', { name: 'Leases' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Settings' })).toBeVisible()
  await expect(page.getByRole('cell', { name: 'kafka_bootstrap', exact: true })).toBeVisible()
  await expect(page.locator('.chart-plot canvas').first()).toBeVisible()

  const metrics = page.locator('.node-metric')
  await expect(metrics).toHaveCount(11)
  await expect(metrics.first().locator('small')).not.toHaveText('waiting', { timeout: 15_000 })

  await page.getByRole('link', { name: /^Catalog/ }).click()
  await expect(page).toHaveURL(/metric=catalog/)
  await expect(page.locator('.chart-legend')).toContainText('tails')
})
