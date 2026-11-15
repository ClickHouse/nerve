import { expect, test, type Page } from '@playwright/test'

test.beforeEach(async ({ page }) => {
  await page.clock.setFixedTime(new Date('2026-01-15T10:20:00Z'))
})

async function snapshot(page: Page, name: string, path: string, ready: string) {
  await page.goto(path)
  await expect(page.getByText(ready).first()).toBeVisible()
  await page.getByRole('status').filter({ hasText: 'Finish setting up Nerve' }).locator('..')
    .getByRole('button', { name: 'Dismiss' }).click()
  await expect(page).toHaveScreenshot(`${name}.png`)
}

test('chat', async ({ page }) => {
  await snapshot(page, 'chat', '/chat/snap-chat', 'Fix: pass a fixed date')
})

test('tasks', async ({ page }) => {
  await snapshot(page, 'tasks', '/tasks', 'Pin the clock in the date parser test')
})

test('plan', async ({ page }) => {
  await snapshot(page, 'plan', '/plans/plan-snap-1', 'Inject a fixed date.')
})

test('notifications', async ({ page }) => {
  await snapshot(page, 'notifications', '/notifications', 'Roll back the nightly deploy?')
})
