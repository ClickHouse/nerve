# Playwright recipes for one-off checks

Use these when a question does not fit `audit.mjs` or the flow template. Write a
short script in the working folder, run it with `node`, read its output and its
screenshots, then delete it or keep it with the evidence. Every snippet below ran
against a test page with Playwright 1.65.1 and Chromium 159.

## Skeleton

```js
// check.mjs: node check.mjs <base url>
import { createRequire } from 'node:module';
import { homedir } from 'node:os';
import { join } from 'node:path';
const toolsDir = process.env.NERVE_BROWSER_DIR || join(homedir(), '.local/share/nerve-browser');
const require = createRequire(join(toolsDir, 'package.json'));
let chromium, devices;
try { ({ chromium, devices } = require('playwright')); }
catch { console.error(`playwright is not in ${toolsDir}: see the Browser setup step of the skill`); process.exit(2); }

const BASE = process.argv[2];
const browser = await chromium.launch();
const context = await browser.newContext({ viewport: { width: 1280, height: 800 } });
const page = await context.newPage();
await page.goto(`${BASE}/`);
// ... the check ...
await page.screenshot({ path: 'check.png' });
await browser.close();
```

Always close the browser at the end. A script that throws before `close()` still
ends, because Node stops the browser with the script.

## Network

Replace an API answer before the page loads (test empty, error and edge states):

```js
await page.route('**/api/plans', (route) => route.fulfill({ json: [{ name: 'Basic' }, { name: 'Team' }] }));
await page.route('**/api/user', (route) => route.fulfill({ status: 500, body: 'boom' }));
```

Block third-party hosts that slow the page or make the result change:

```js
await context.route(/(^|\.)(analytics|ads)\.example\.com/, (route) => route.abort());
```

Wait for a response, not a fixed time:

```js
const [resp] = await Promise.all([
  page.waitForResponse((r) => r.url().includes('/api/plans')),
  page.reload(),
]);
console.log(resp.status());
```

Test the offline state:

```js
await context.setOffline(true);
// ... check the page's offline message ...
await context.setOffline(false);
```

Record all requests to a HAR file: `browser.newContext({ recordHar: { path: 'run.har', content: 'omit' } })`.
The file is written on `context.close()`. A HAR holds headers and cookies: treat it
as a secret when the run was signed in.

## Forms and controls

```js
await page.getByLabel('Email').fill('alice@example.com');
await page.getByLabel('I accept the terms').check();
await page.getByLabel('Plan').selectOption('Team');
await page.getByLabel('Avatar').setInputFiles('avatar.png');
await page.getByRole('button', { name: 'Save' }).click();
```

## Dialogs, downloads, new tabs, frames

```js
page.once('dialog', (dialog) => dialog.accept());          // before the click that opens it
await page.getByRole('button', { name: 'Delete account' }).click();

const [download] = await Promise.all([
  page.waitForEvent('download'),
  page.getByRole('link', { name: 'Download report' }).click(),
]);
await download.saveAs(download.suggestedFilename());

const [popup] = await Promise.all([
  page.waitForEvent('popup'),
  page.getByRole('link', { name: 'Open in a new tab' }).click(),
]);
await popup.waitForLoadState();

await page.frameLocator('iframe[title="Help"]').getByRole('button', { name: 'Ask a question' }).click();
```

Without `page.once('dialog', ...)`, Playwright dismisses the dialog: `confirm()`
returns false and the flow takes the "cancel" path.

## Look and measure

```js
await page.getByRole('main').screenshot({ path: 'main.png', animations: 'disabled', mask: [page.locator('.clock')] });
const color = await page.getByRole('heading', { name: 'Lab' }).evaluate((el) => getComputedStyle(el).color);
const box = await page.getByRole('button', { name: 'Save' }).boundingBox();   // { x, y, width, height }
```

`mask` paints a box over parts that change on every run (clocks, avatars), so two
screenshots stay comparable.

Keyboard focus order (each Tab press, the element that has focus):

```js
for (let i = 0; i < 6; i++) {
  await page.keyboard.press('Tab');
  console.log(await page.evaluate(() => {
    const el = document.activeElement;
    return `${el.tagName.toLowerCase()} ${(el.innerText || el.getAttribute('aria-label') || el.id || '').trim().slice(0, 30)}`;
  }));
}
```

## Device, language, time zone, motion

```js
const phone = await browser.newContext({
  ...devices['Pixel 7'], locale: 'de-DE', timezoneId: 'Europe/Berlin', reducedMotion: 'reduce',
});
```

`devices` holds the viewport, scale factor, user agent and touch support of many
phones and tablets. `colorScheme: 'dark'` emulates the dark preference.

## Sign-in state

After a sign-in through the UI, save cookies and local storage for later runs:

```js
await context.storageState({ path: 'state.json' });
```

Then `browser.newContext({ storageState: 'state.json' })`, or `--storage state.json`
for the skill's scripts. The file holds the session: keep it in the working folder,
`chmod 600`, and delete it when the work is done.

## Accessibility tree

```js
console.log(await page.ariaSnapshot());                              // YAML of roles and names
console.log(await page.getByRole('navigation').ariaSnapshot());      // one part of the page
```

Compare two snapshots (before and after a change) with `diff` to see what a screen
reader user gets.
