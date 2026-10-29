import { expect, test } from '@playwright/test'

import { indexed, request, rows, seed, unique } from './cluster'

const RECORDS = 30
const LOADED_MS = 120_000

test('SQL reads a table through DuckDB in the browser', async ({ page }) => {
  const name = unique('e2e-sql')
  await seed(name, 1, RECORDS)
  await request('POST', '/admin/tables', { topic: name })
  await page.goto(`/tables/${name}`)
  await indexed(page, RECORDS)
  const editor = page.getByRole('textbox')

  await page.goto(`/sql?q=${encodeURIComponent(`SELECT count(*) AS records FROM "${name}"`)}`)
  await page.getByRole('button', { name: 'Run' }).click()
  await expect(rows(page).first().getByRole('cell')).toHaveText(String(RECORDS), { timeout: LOADED_MS })
  await expect(page.getByText(/^1 row in /)).toBeVisible()

  await page.goto(`/sql?q=${encodeURIComponent(`SELECT id FROM "${name}" WHERE id < 5`)}`)
  await page.getByRole('button', { name: 'Profile' }).click()
  const scan = rows(page).first().getByRole('cell')
  await expect(scan.nth(0)).toHaveText(name)
  await expect(scan.nth(1)).toHaveText('id<5')
  await expect(scan.nth(3)).toHaveText(/^\d+ of \d+$/)
  await expect(page.getByText(/^5 rows returned in /)).toBeVisible()

  await editor.click()
  await page.keyboard.press('ControlOrMeta+A')
  await page.keyboard.type('SELECT * FROM range(5000)')
  await page.keyboard.press('ControlOrMeta+Enter')
  await expect(page.getByText(/^First 1,000 rows in /)).toBeVisible()
  await expect(page).toHaveURL(/q=SELECT/)

  await page.reload()
  await expect(editor).toHaveText('SELECT * FROM range(5000)')
  await request('DELETE', `/admin/tables/${name}`)
})
