// Screenshot a built report page in light, dark and phone width, and check it.
//   node page_shots.mjs <page.html | https://...> <out-prefix> [--at <css selector>] [--full]
// Writes <out-prefix>-light.png and -dark.png (1440 x 1000) and -phone.png
// (390 x 844 at 2x). With a match for --at (default: the comparison table),
// it also writes -light-at.png, -dark-at.png and -phone-at.png scrolled there.
// --full takes the full page height. A public URL works too, to capture a
// product page for img/ (it only loads the page: it fills no form and clicks nothing).
// Exit 1 when an image did not load, the page has a script error, or the page
// is wider than the phone screen. Exit 2: usage or setup.
import { createRequire } from 'node:module';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const toolsDir = process.env.NERVE_BROWSER_DIR || join(homedir(), '.local/share/nerve-browser');
const require = createRequire(join(toolsDir, 'package.json'));
let chromium;
try { ({ chromium } = require('playwright')); }
catch { console.error(`playwright is not in ${toolsDir}: see the Browser setup step of the skill`); process.exit(2); }

const args = process.argv.slice(2);
const opt = name => { const i = args.indexOf(name); return i === -1 ? null : args.splice(i, 2)[1]; };
const at = opt('--at') ?? '.table-wrap:has(.edge)';
const full = args.includes('--full');
if (full) args.splice(args.indexOf('--full'), 1);
const [target, prefix] = args;
if (!target || !prefix) { console.error('usage: node page_shots.mjs <page.html|url> <out-prefix> [--at <selector>] [--full]'); process.exit(2); }
const url = /^https?:\/\//.test(target) ? target : pathToFileURL(resolve(target)).href;

const views = [
  ['light', { viewport: { width: 1440, height: 1000 }, colorScheme: 'light' }],
  ['dark', { viewport: { width: 1440, height: 1000 }, colorScheme: 'dark' }],
  ['phone', { viewport: { width: 390, height: 844 }, deviceScaleFactor: 2, isMobile: true, hasTouch: true }],
];
const problems = [];
const browser = await chromium.launch();
for (const [name, options] of views) {
  const context = await browser.newContext(options);
  const page = await context.newPage();
  page.on('pageerror', e => problems.push(`${name}: script error: ${e.message}`));
  await page.goto(url, { waitUntil: 'load', timeout: 60000 });
  await page.addStyleTag({ content: 'html{scroll-behavior:auto!important}' });
  const broken = await page.evaluate(() => [...document.images].filter(i => i.complete && !i.naturalWidth && i.src).length);
  if (broken) problems.push(`${name}: ${broken} image(s) did not load`);
  if (name === 'phone') {
    const wide = await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
    if (wide > 1) problems.push(`phone: the page is ${wide}px wider than the screen`);
  }
  await page.screenshot({ path: `${prefix}-${name}.png`, fullPage: full });
  const found = await page.evaluate(sel => { const e = document.querySelector(sel); if (!e) return false; e.scrollIntoView(); return true; }, at);
  if (found) await page.screenshot({ path: `${prefix}-${name}-at.png` });
  console.log(`wrote ${prefix}-${name}.png${found ? ` and ${prefix}-${name}-at.png` : ''}`);
  await context.close();
}
await browser.close();
if (problems.length) { console.log('CHECK:\n  ' + problems.join('\n  ')); process.exit(1); }
console.log('checks: images load, no script errors, fits the phone width');
