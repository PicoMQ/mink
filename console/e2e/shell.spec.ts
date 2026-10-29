import { expect, test } from '@playwright/test'

import { request } from './cluster'

test('the overview loads and every section opens from the navigation', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByText('Live nodes')).toBeVisible()

  const sections = page.getByRole('navigation', { name: 'Sections' })
  for (const section of ['Tables', 'Nodes', 'Registry', 'Connect', 'Overview']) {
    await sections.getByRole('link', { name: section, exact: true }).click()
    await expect(page.getByRole('navigation', { name: 'Breadcrumb' })).toHaveText(section)
  }
})

test('a deep link survives a reload', async ({ page }) => {
  await page.goto('/tables?prefix=deep-link')
  await page.reload()

  await expect(page.getByRole('searchbox')).toHaveValue('deep-link')
})

test('an unknown address says so', async ({ page }) => {
  await page.goto('/nowhere/at/all')

  await expect(page.getByText('Nothing lives at this address')).toBeVisible()
})

test('connect fills the examples with the table and copies them', async ({ page }) => {
  const cluster = await request<{ catalogUrl: string; namespace: string }>('GET', '/admin/cluster')
  await page.goto('/connect?table=orders')

  await expect(page.getByRole('textbox', { name: 'Table' })).toHaveValue('orders')
  await expect(page.getByText(`SELECT count(*) FROM mink.${cluster.namespace}."orders";`)).toBeVisible()
  await expect(page.getByText(cluster.catalogUrl, { exact: true })).toBeVisible()

  await page.getByRole('button', { name: 'Copy Catalog URL' }).click()
  await expect(page.getByRole('button', { name: 'Copy Catalog URL' })).toHaveText('Copied')
  expect(await page.evaluate(() => navigator.clipboard.readText())).toBe(cluster.catalogUrl)
})

test('the documentation link opens the docs', async ({ page }) => {
  await page.goto('/')

  await expect(page.getByRole('link', { name: 'Documentation' })).toHaveAttribute('href', 'https://github.com/PicoMQ/mink')
})
