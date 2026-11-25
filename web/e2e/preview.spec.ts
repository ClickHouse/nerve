import { writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { expect, test, type Page, type Request } from '@playwright/test'

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

async function openPreview(page: Page, name = 'index.html', title = 'Quarterly review') {
  await page.goto('/chat/snap-older')
  // The two Write calls of the message are one folded burst.
  await page.getByRole('button', { name: 'Edited 2 files' }).click()
  await page.getByRole('button', { name: `Preview ${name}` }).click()
  const content = page.frameLocator('iframe[title="Preview"]').frameLocator('iframe')
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

test.describe('on a phone of 390 by 844', () => {
  test.use({ viewport: { width: 390, height: 844 } })

  test('every control, the frame and the footer are on screen', async ({ page }, testInfo) => {
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
    for (const name of ['Reload the preview', 'Show the source', 'Present', 'Close the panel']) {
      const control = region.getByRole('button', { name })
      await expect(control).toBeVisible()
      await inView(name, await control.boundingBox())
    }
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
