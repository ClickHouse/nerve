#!/usr/bin/env node
// Look at a page before you write a flow: a screenshot, the accessibility tree
// (roles and names), a locator for each control and heading, the test ids, and
// the console errors.
//
//   node snapshot.mjs --url <url> [--out <dir>] [--name <file stem>]
//        [--viewport desktop|mobile|<w>x<h>] [--scheme light|dark] [--full]
//        [--storage <state.json>] [--init <file.js>] [--wait <ms>]
//
// Writes <out>/<name>.png and <out>/<name>.aria.yml; prints the rest.
// Exit code: 0 the page loaded, 1 it did not, 2 bad usage or missing tools.
import { createRequire } from 'node:module';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { mkdirSync, writeFileSync, existsSync } from 'node:fs';

const toolsDir = process.env.NERVE_BROWSER_DIR || join(homedir(), '.local/share/nerve-browser');
const require = createRequire(join(toolsDir, 'package.json'));
let chromium;
try { ({ chromium } = require('playwright')); }
catch { console.error(`playwright is not in ${toolsDir}: see the Browser setup step of the skill`); process.exit(2); }

// Output piped to `head` closes early: keep working, drop the rest of the output.
process.stdout.on('error', () => {});
const args = process.argv.slice(2);
const arg = (name, def) => {
  const i = args.indexOf(`--${name}`);
  return i > -1 && args[i + 1] !== undefined ? args[i + 1] : def;
};
const usage = (msg) => { console.error(`snapshot: ${msg}`); process.exit(2); };
const URL_ = arg('url') || usage('--url <url> is required');
const OUT = arg('out', '.');
const NAME = arg('name', 'snapshot');
const VP = arg('viewport', 'desktop');
const viewport = VP === 'desktop' ? { width: 1280, height: 800 }
  : VP === 'mobile' ? { width: 375, height: 812 }
  : (() => { const m = VP.match(/^(\d+)x(\d+)$/); if (!m) usage(`unknown viewport "${VP}"`); return { width: +m[1], height: +m[2] }; })();
const STORAGE = arg('storage');
const INIT = arg('init');
for (const f of [STORAGE, INIT]) if (f && !existsSync(f)) usage(`file not found: ${f}`);
mkdirSync(OUT, { recursive: true });

// Roles worth a locator: things a user clicks, types into or reads as a title.
const ROLES = new Set(['button', 'link', 'textbox', 'searchbox', 'checkbox', 'radio', 'combobox',
  'listbox', 'option', 'menuitem', 'menuitemcheckbox', 'menuitemradio', 'tab', 'switch', 'slider',
  'spinbutton', 'heading', 'dialog', 'alert', 'status', 'navigation', 'form', 'table']);

const browser = await chromium.launch();
const ctx = await browser.newContext({
  viewport, isMobile: VP === 'mobile', hasTouch: VP === 'mobile',
  colorScheme: arg('scheme', 'light'),
  ...(STORAGE ? { storageState: STORAGE } : {}),
});
if (INIT) await ctx.addInitScript({ path: resolve(INIT) });
const page = await ctx.newPage();
const errors = [];
page.on('console', (m) => { if (m.type() === 'error') errors.push(`console: ${m.text()}`); });
page.on('pageerror', (e) => errors.push(`pageerror: ${e.message}`));
page.on('response', (r) => { if (r.status() >= 400) errors.push(`request: ${r.status()} ${r.url()}`); });

let res;
try {
  res = await page.goto(URL_, { waitUntil: 'load', timeout: 30000 });
  await page.waitForLoadState('networkidle', { timeout: 5000 }).catch(() => {});
  await page.waitForTimeout(Number(arg('wait', '500')));
} catch (e) {
  console.error(`snapshot: ${e.message.split('\n')[0]}`);
  await browser.close();
  process.exit(1);
}

const shot = join(OUT, `${NAME}.png`);
await page.screenshot({ path: shot, fullPage: args.includes('--full') });
console.log(`url:    ${page.url()} (HTTP ${res ? res.status() : '?'})`);
console.log(`title:  ${await page.title()}`);
console.log(`shot:   ${shot}`);

// The "ai" snapshot gives each node a ref. A ref turns into a stable locator, but
// only until the next snapshot, so resolve all refs before anything else.
// Refs need a recent Playwright (checked with 1.65.1). An older library gives
// no locators, and the rest of the snapshot still works.
let ai = '';
if (typeof page.getByRef === 'function') {
  try { ai = await page.ariaSnapshot({ mode: 'ai' }); } catch { ai = ''; }
}
console.log('\nlocators (role and name; prefer these to CSS selectors):');
if (!ai) console.log('  none: this Playwright has no refs. In the tools folder: npm install playwright@latest && npx playwright install chromium');
for (const line of ai.split('\n')) {
  const m = line.match(/^\s*- (\w+)(?: "([^"]*)")?.*\[ref=(e\d+)\]/);
  if (!m || !ROLES.has(m[1])) continue;
  try {
    const loc = await page.getByRef(m[3]).normalize();
    console.log(`  page.${loc.toString()}`);
  } catch { /* the node is gone; skip it */ }
}

const testIds = await page.evaluate(() => [...document.querySelectorAll('[data-testid]')]
  .map((el) => `${el.getAttribute('data-testid')}  (${el.tagName.toLowerCase()})`));
if (testIds.length) {
  console.log('\ntest ids (page.getByTestId(...)):');
  for (const t of testIds.slice(0, 40)) console.log(`  ${t}`);
}

const tree = await page.ariaSnapshot();
const yml = join(OUT, `${NAME}.aria.yml`);
writeFileSync(yml, `${tree}\n`);
const lines = tree.split('\n');
console.log(`\naccessibility tree (${lines.length} lines, full copy in ${yml}):`);
console.log(lines.slice(0, 60).join('\n'));
if (lines.length > 60) console.log(`... ${lines.length - 60} more lines in ${yml}`);

console.log(`\nerrors: ${errors.length}`);
for (const e of errors.slice(0, 10)) console.log(`  ${e.replace(/\s+/g, ' ').slice(0, 200)}`);
await browser.close();
