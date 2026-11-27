import { writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { expect, test, type Page, type Request, type Route } from '@playwright/test'

// The Preview tab against the sample deck of scripts/ui_snapshot_server.py
// (workspace/decks/demo), in a real browser. The server's state directory is
// the one playwright.config.ts gives it.
const DECKS = join(tmpdir(), 'nerve-ui-snapshots', 'workspace', 'decks')

/** Every request of the page and its frames, as `METHOD url`. */
function record(page: Page): string[] {
  const seen: string[] = []
  page.on('request', (r: Request) => seen.push(`${r.method()} ${r.url()}`))
  return seen
}

/** Open the Preview tab of the file `name` of the Write calls of snap-older. */
async function openTab(page: Page, name: string) {
  await page.goto('/chat/snap-older')
  // The Write calls of the message are one folded burst.
  await page.getByRole('button', { name: 'Edited 4 files' }).click()
  // The burst shows its last rows; the first one is behind "Show 1 more".
  await page.getByRole('button', { name: /^Show 1 more/ }).click()
  await page.getByRole('button', { name: `Preview ${name}` }).click()
  return page.frameLocator('iframe[title="Preview"]').frameLocator('iframe')
}

async function openPreview(page: Page, name = 'index.html', title = 'Quarterly review') {
  const content = await openTab(page, name)
  await expect(content.locator('#title')).toHaveText(title)
  return content
}

test('the preview shows the deck in the sandbox frame and asks no other address', async ({ page, baseURL }) => {
  const seen = record(page)
  const content = await openPreview(page)

  // The local script ran, the local image and font are in the document.
  await expect(content.locator('#ran')).toHaveText('script ran')
  await expect(content.locator('#logo')).toHaveAttribute('src', /^data:image\/png;base64,/)

  // The proxy frame has no sandbox attribute of its own; the frame inside it
  // may only run scripts. Neither has the origin of the panel.
  const outer = page.locator('iframe[title="Preview"]')
  await expect(outer).not.toHaveAttribute('sandbox', /.*/)
  const inner = page.frameLocator('iframe[title="Preview"]').locator('iframe')
  await expect(inner).toHaveAttribute('sandbox', 'allow-scripts')
  expect(await content.locator('body').evaluate(el => (el as unknown as { ownerDocument: { defaultView: { origin: string } } }).ownerDocument.defaultView.origin)).toBe('null')

  // Nothing of another host was asked for: not by the parse in the panel,
  // not by the document in the frame.
  await page.waitForTimeout(1000)
  const origin = new URL(baseURL!).origin
  expect(seen.filter(line => !line.split(' ')[1].startsWith(origin))).toEqual([])
  expect(seen.filter(line => line.includes('remote') || line.includes('prerender'))).toEqual([])
})

test('a change by a shell command shows within a few seconds', async ({ page }, testInfo) => {
  // One project only: both would write the same file.
  test.skip(testInfo.project.name !== 'desktop', 'one writer of the file')
  const content = await openPreview(page, 'live.html', 'First version')
  // What a shell command does: a write that sends no file event.
  writeFileSync(join(DECKS, 'live.html'), '<!doctype html><h1 id="title">Second version</h1>\n')
  await expect(content.locator('#title')).toHaveText('Second version', { timeout: 8000 })
})

test('a Markdown file shows as the Markdown page of the kit', async ({ page }, testInfo) => {
  test.skip(testInfo.project.name !== 'desktop', 'one layout is enough')
  const notes = await openTab(page, 'notes.md')
  await expect(notes.getByRole('heading', { name: 'Release notes' })).toBeVisible()
  // A link is its text and its address; raw HTML of the file is text.
  await expect(notes.locator('article')).toContainText('the plan (https://example.org/plan)')
  await expect(notes.locator('article')).toContainText('<b>raw html</b>')
  await expect(notes.locator('article a, article b')).toHaveCount(0)
  const frame = await page.locator('iframe[title="Preview"]').boundingBox()
  expect(frame!.height).toBeGreaterThan(300)
})

test('a CSV file shows as the table page of the kit', async ({ page }, testInfo) => {
  test.skip(testInfo.project.name !== 'desktop', 'one layout is enough')
  const cost = await openTab(page, 'cost.csv')
  await expect(cost.getByRole('table', { name: 'cost.csv' })).toBeVisible()
  await expect(cost.locator('[aria-live="polite"]')).toHaveText('2 rows')
  await expect(cost.getByRole('cell', { name: 'query' })).toBeVisible()
  // The frame sleeps under the veil of the keyboard guard until the pointer is on it.
  const box = (await page.locator('iframe[title="Preview"]').boundingBox())!
  await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2)
  await cost.getByRole('button', { name: 'Cost', exact: true }).click()
  await expect(cost.locator('tbody tr').first()).toContainText('query')
  await cost.getByRole('searchbox', { name: 'Search the table' }).fill('ingest')
  await expect(cost.locator('[aria-live="polite"]')).toHaveText('1 of 2 rows')
  expect(box.height).toBeGreaterThan(300)
})

test.describe('on a phone of 390 by 844', () => {
  test.use({ viewport: { width: 390, height: 844 } })

  test('the header, the deck controls, the frame and the footer are on screen', async ({ page }, testInfo) => {
    test.skip(testInfo.project.name !== 'phone', 'the phone layout')
    await openPreview(page)
    const region = page.getByRole('region', { name: 'Preview' })
    const inView = async (name: string, box: { x: number; y: number; width: number; height: number } | null) => {
      expect(box, name).not.toBeNull()
      expect(box!.x, name).toBeGreaterThanOrEqual(0)
      expect(box!.y, name).toBeGreaterThanOrEqual(0)
      expect(box!.x + box!.width, name).toBeLessThanOrEqual(390)
      expect(box!.y + box!.height, name).toBeLessThanOrEqual(844)
    }
    // The page said it is a deck: the slide controls are there.
    const slides = region.getByRole('group', { name: 'Slides' })
    await expect(slides).toContainText('3 / 12')

    const header = region.getByRole('group', { name: 'Preview header' })
    const path = header.getByText('decks/demo/index.html')
    await expect(path).toBeVisible()
    const pathBox = await path.boundingBox()
    await inView('path', pathBox)
    expect(pathBox!.width, 'the path keeps its room').toBeGreaterThan(120)
    for (const name of ['Previous slide', 'Next slide', 'Reload the preview', 'Show the source', 'Present', 'Close the panel']) {
      const control = region.getByRole('button', { name })
      await expect(control).toBeVisible()
      await inView(name, await control.boundingBox())
    }
    await inView('slide count', await slides.boundingBox())
    const overflow = await header.evaluate(el => {
      const box = el as unknown as { scrollWidth: number; clientWidth: number }
      return box.scrollWidth - box.clientWidth
    })
    expect(overflow, 'no horizontal overflow of the header').toBeLessThanOrEqual(0)

    await region.getByRole('button', { name: 'Next slide' }).click()
    await expect(slides).toContainText('4 / 12')

    const frame = page.locator('iframe[title="Preview"]')
    const frameBox = await frame.boundingBox()
    await inView('frame', frameBox)
    expect(frameBox!.height).toBeGreaterThan(300)
    const footer = region.getByText('Preview from the agent.')
    await inView('footer', await footer.boundingBox())

    await region.getByRole('button', { name: 'Close the panel' }).click()
    await expect(region).toBeHidden()
  })
})

/**
 * Hosted mode with the publish of artifacts on, as GET /api/auth/status and
 * GET /api/auth/me of a Nerve in external mode answer it. The rest comes
 * from the local test server.
 */
async function asHosted(page: Page) {
  // In hosted mode the page sends no token (the gateway signs it in); the
  // passwordless test server needs one, so each API request gets it here.
  const login = await page.request.post('/api/auth/login', { data: { password: '' } })
  const { token } = await login.json()
  const withToken = (route: Route) => ({ headers: { ...route.request().headers(), authorization: `Bearer ${token}` } })
  await page.route('**/api/**', route => route.continue(withToken(route)))
  await page.route('**/api/auth/status', async route => {
    const response = await route.fetch(withToken(route))
    await route.fulfill({ response, json: { ...(await response.json()), mode: 'external' } })
  })
  await page.route('**/api/auth/me', async route => {
    const response = await route.fetch(withToken(route))
    await route.fulfill({
      response,
      json: { ...(await response.json()), artifacts: { publish: true, viewer_origin: 'https://artifacts.example.test' } },
    })
  })
}

test.describe('on a phone of 390 by 844, hosted, with Publish', () => {
  test.use({ viewport: { width: 390, height: 844 } })

  test('the header has the Publish button and every control on screen', async ({ page }, testInfo) => {
    test.skip(testInfo.project.name !== 'phone', 'the phone layout')
    await asHosted(page)
    await openPreview(page)
    const region = page.getByRole('region', { name: 'Preview' })
    const header = region.getByRole('group', { name: 'Preview header' })
    await expect(region.getByRole('group', { name: 'Slides' })).toContainText('/ 12')
    for (const name of ['Previous slide', 'Next slide', 'Reload the preview', 'Show the source', 'Present', 'Publish', 'Close the panel']) {
      const control = header.getByRole('button', { name, exact: true })
      await expect(control, name).toBeVisible()
      const box = (await control.boundingBox())!
      expect(box.x, name).toBeGreaterThanOrEqual(0)
      expect(box.y, name).toBeGreaterThanOrEqual(0)
      expect(box.x + box.width, name).toBeLessThanOrEqual(390)
      expect(box.y + box.height, name).toBeLessThanOrEqual(844)
    }
    const pathBox = (await header.getByText('decks/demo/index.html').boundingBox())!
    expect(pathBox.width, 'the path keeps its room').toBeGreaterThan(120)
    const overflow = await header.evaluate(el => {
      const box = el as unknown as { scrollWidth: number; clientWidth: number }
      return box.scrollWidth - box.clientWidth
    })
    expect(overflow, 'no horizontal overflow of the header').toBeLessThanOrEqual(0)
    // The button is reachable: it opens the dialog with the title field.
    await header.getByRole('button', { name: 'Publish', exact: true }).click()
    const dialog = page.getByRole('dialog', { name: 'Publish' })
    await expect(dialog.getByLabel('Title')).toHaveValue('demo')
    const dialogBox = (await dialog.boundingBox())!
    expect(dialogBox.x).toBeGreaterThanOrEqual(0)
    expect(dialogBox.x + dialogBox.width).toBeLessThanOrEqual(390)
    await page.screenshot({ path: testInfo.outputPath('hosted-phone-publish.png') })
  })
})
