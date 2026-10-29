import { expect, test } from '@playwright/test'

import { createTopic, detail, indexed, request, rows, seed, unique } from './cluster'

const PARTITIONS = 3
const RECORDS = 30

test('a table is added, indexed, browsed, changed and removed', async ({ page }) => {
  const name = unique('e2e-orders')
  const schemaId = await seed(name, PARTITIONS, RECORDS)
  const tabs = page.getByRole('navigation', { name: 'Table' })

  await page.goto('/tables')
  await page.getByRole('button', { name: 'Add table' }).click()
  const dialog = page.getByRole('dialog')
  await dialog.getByLabel('Kafka topic').fill(name)
  await dialog.getByLabel('Granularity').selectOption('hour')
  await dialog.getByRole('button', { name: 'Add' }).click()
  await expect(page).toHaveURL(new RegExp(`/tables/${name}$`))
  await expect(page.getByRole('heading', { name })).toBeVisible()

  await indexed(page, RECORDS)
  await expect(detail(page, 'Granularity')).toHaveText('hour')

  await tabs.getByRole('link', { name: 'Partitions' }).click()
  await expect(page.getByText(`${PARTITIONS} partitions`, { exact: true })).toBeVisible()
  await expect(rows(page)).toHaveCount(PARTITIONS)

  await tabs.getByRole('link', { name: 'Files' }).click()
  await expect(rows(page).first()).toBeVisible()
  await page.getByRole('spinbutton', { name: 'Partition' }).fill('1')
  await expect(page).toHaveURL(/partition=1/)
  await expect(rows(page).first().getByRole('cell').nth(1)).toHaveText('1')
  await rows(page).first().getByRole('link').click()
  await expect(detail(page, 'Partition')).toHaveText('1')
  await expect(page.getByRole('cell', { name: 'item', exact: true })).toBeVisible()
  await page.goBack()
  await expect(page.getByRole('spinbutton', { name: 'Partition' })).toHaveValue('1')

  await tabs.getByRole('link', { name: 'Snapshots' }).click()
  await expect(rows(page).first()).toBeVisible()
  await rows(page).first().getByRole('link').click()
  await expect(page.getByRole('heading', { name: /^Snapshot \d+$/ })).toBeVisible()
  await expect(rows(page)).toHaveCount(PARTITIONS)
  await page.goBack()

  await tabs.getByRole('link', { name: 'Schema' }).click()
  await expect(page.getByText('item', { exact: true })).toBeVisible()
  const reference = rows(page).filter({ hasText: `id:${schemaId}` })
  await expect(reference).toContainText('merged into')
  await reference.getByRole('link', { name: String(schemaId) }).click()
  await expect(page.getByRole('heading', { name: `Schema ${schemaId}` })).toBeVisible()
  await expect(detail(page, 'Tables')).toContainText(name)
  await expect(page.locator('.cm-content')).toContainText('"item"')

  await page.goto('/registry')
  await expect(rows(page).filter({ hasText: name }).first()).toBeVisible()

  await page.goto(`/tables/${name}/records`)
  await expect(page.getByText(`${RECORDS / PARTITIONS} records`, { exact: true })).toBeVisible()
  await rows(page).first().click()
  await expect(page).toHaveURL(/record=0/)
  const opened = page.locator('.records-detail')
  const section = (title: string) =>
    opened.locator('.records-section', { has: page.getByRole('heading', { name: title, exact: true }) })
  await expect(opened.getByRole('heading', { name: 'Row' })).toBeVisible()
  await expect(section('Row').locator('.cm-content')).toContainText('item-0')
  await expect(section('Key').locator('.cm-content')).toHaveText('key-0')
  await expect(section('Value').locator('.cm-content')).toContainText('00 00 ')
  await expect(section('Headers')).toContainText('source')
  await expect(section('Headers')).toContainText('e2e')
  await page.reload()
  await expect(page.getByRole('heading', { name: 'Row' })).toBeVisible()

  await page.goto(`/tables/${name}/records?partition=1&limit=5`)
  await expect(page.getByText('5 records', { exact: true })).toBeVisible()
  await page.getByRole('button', { name: 'Next from offset 5' }).click()
  await expect(page).toHaveURL(/offset=5/)
  await expect(rows(page).first().getByRole('cell').first()).toHaveText('5')
  await expect(page.getByRole('button', { name: /^Next from offset/ })).toHaveCount(0)

  const time = page.getByRole('textbox', { name: 'From time' })
  await time.fill('2000-01-01 00:00:00')
  await time.press('Enter')
  await expect(page).toHaveURL(/time=946/)
  await expect(rows(page).first().getByRole('cell').first()).toHaveText('0')
  await time.fill('2999-01-01 00:00:00')
  await time.press('Enter')
  await expect(page.getByText('No committed records here')).toBeVisible()

  await tabs.getByRole('link', { name: 'Overview' }).click()
  await page.getByRole('button', { name: 'Change granularity' }).click()
  await page.getByRole('dialog').getByRole('combobox').selectOption('day')
  await page.getByRole('dialog').getByRole('button', { name: 'Save' }).click()
  await expect(detail(page, 'Granularity')).toHaveText('day')

  await page.getByRole('button', { name: 'Remove table' }).click()
  const confirm = page.getByRole('dialog')
  await expect(confirm.getByRole('button', { name: 'Remove table' })).toBeDisabled()
  await confirm.getByRole('textbox').fill(name)
  await confirm.getByRole('button', { name: 'Remove table' }).click()
  await expect(page).toHaveURL(/\/tables$/)
  await page.getByRole('searchbox').fill(name)
  await expect(page).toHaveURL(new RegExp(`prefix=${name}`))
  const left = rows(page).filter({ hasText: name })
  await expect(page.getByText('No tables', { exact: true }).or(left.filter({ hasText: 'removing' }))).toBeVisible()
})

test('adding a table twice shows the error', async ({ page }) => {
  const name = unique('e2e-twice')
  await createTopic(name, 1)
  await request('POST', '/admin/tables', { topic: name })

  await page.goto('/tables')
  await page.getByRole('button', { name: 'Add table' }).click()
  const dialog = page.getByRole('dialog')
  await dialog.getByLabel('Kafka topic').fill(name)
  await dialog.getByRole('button', { name: 'Add' }).click()

  await expect(dialog.getByRole('alert')).toBeVisible()
  await request('DELETE', `/admin/tables/${name}`)
})
