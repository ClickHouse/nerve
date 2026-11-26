#!/usr/bin/env node
// Template: one user flow in headless Chromium. Copy this file to your working
// folder, replace the example STEPS with the flow you test, then run:
//
//   node flow.mjs --base <url> [--out <dir>] [--viewport desktop|mobile|<w>x<h>]
//        [--scheme light|dark] [--storage <state.json>] [--init <file.js>]
//        [--video] [--trace always] [--strict]
//
// Each step gets a screenshot <out>/NN-<step>.png. When a step fails, the run
// stops and writes <out>/trace.zip (actions, DOM snapshots, screenshots, console).
// --trace always keeps the trace of a passing run too. --video records
// <out>/video/*.webm. --strict also fails the run on console or page errors.
// Exit code: 0 all steps passed, 1 a step failed, 2 setup problem.
import { createRequire } from 'node:module';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { mkdirSync, existsSync } from 'node:fs';

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
const BASE = (arg('base') || (console.error('flow: --base <url> is required'), process.exit(2))).replace(/\/$/, '');
const OUT = resolve(arg('out', 'flow-out'));
mkdirSync(OUT, { recursive: true });

// Small assertions, so the template needs only the playwright package.
async function expectVisible(locator, timeout = 5000) {
  await locator.first().waitFor({ state: 'visible', timeout });
}
async function expectText(locator, expected, timeout = 5000) {
  const ok = (t) => (expected instanceof RegExp ? expected.test(t) : t.includes(expected));
  const end = Date.now() + timeout;
  let text = '';
  while (Date.now() < end) {
    text = await locator.first().innerText({ timeout: 1000 }).catch(() => '');
    if (ok(text)) return;
    await new Promise((r) => setTimeout(r, 200));
  }
  throw new Error(`expected text ${expected} in ${locator}, got "${text.slice(0, 200)}"`);
}

// ---------------------------------------------------------------- edit from here
// Each step: [name, async (page) => { actions; at least one assertion }].
// Find role and label names first with scripts/snapshot.mjs. Prefer getByRole,
// getByLabel and getByTestId to CSS selectors.
const STEPS = [
  ['open the home page', async (page) => {
    await page.goto(`${BASE}/`);
    await expectVisible(page.getByRole('heading', { level: 1 }));
  }],
  ['subscribe with an email address', async (page) => {
    await page.getByLabel('Email').fill('alice@example.com');
    await page.getByRole('button', { name: 'Subscribe' }).click();
    await expectText(page.getByRole('status'), 'subscribed');
  }],
  ['open the pricing page', async (page) => {
    await page.getByRole('link', { name: 'Pricing' }).click();
    await page.waitForURL(/pricing/);
    await expectVisible(page.getByRole('heading', { name: 'Pricing' }));
  }],
];
// ---------------------------------------------------------------- to here

const VP = arg('viewport', 'desktop');
const viewport = VP === 'desktop' ? { width: 1280, height: 800 }
  : VP === 'mobile' ? { width: 375, height: 812 }
  : (() => { const m = VP.match(/^(\d+)x(\d+)$/); if (!m) { console.error(`flow: unknown viewport ${VP}`); process.exit(2); } return { width: +m[1], height: +m[2] }; })();
const STORAGE = arg('storage');
const INIT = arg('init');
for (const f of [STORAGE, INIT]) if (f && !existsSync(f)) { console.error(`flow: file not found: ${f}`); process.exit(2); }

const browser = await chromium.launch();
const ctx = await browser.newContext({
  viewport, isMobile: VP === 'mobile', hasTouch: VP === 'mobile',
  colorScheme: arg('scheme', 'light'),
  ...(STORAGE ? { storageState: STORAGE } : {}),
  ...(args.includes('--video') ? { recordVideo: { dir: join(OUT, 'video'), size: viewport } } : {}),
});
if (INIT) await ctx.addInitScript({ path: resolve(INIT) });
await ctx.tracing.start({ screenshots: true, snapshots: true });
const page = await ctx.newPage();
page.setDefaultTimeout(10000);

const problems = [];
page.on('console', (m) => { if (m.type() === 'error') problems.push(`console: ${m.text()}`); });
page.on('pageerror', (e) => problems.push(`pageerror: ${e.message}`));

const slug = (s) => s.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').slice(0, 40);
let failed = null;
for (const [i, [name, run]] of STEPS.entries()) {
  const nn = String(i + 1).padStart(2, '0');
  const t0 = Date.now();
  try {
    await run(page);
    await page.screenshot({ path: join(OUT, `${nn}-${slug(name)}.png`) });
    console.log(`PASS ${nn} ${name} (${Date.now() - t0} ms)`);
  } catch (e) {
    const msg = e.message.replace(/\x1b\[[0-9;]*m/g, '').split('\n').filter((l) => l.trim());
    failed = { step: `${nn} ${name}`, error: msg.slice(0, 5).join('\n     ') };
    await page.screenshot({ path: join(OUT, `${nn}-${slug(name)}-FAILED.png`) }).catch(() => {});
    console.log(`FAIL ${nn} ${name} (${Date.now() - t0} ms)\n     ${failed.error}`);
    break;
  }
}

if (problems.length) {
  console.log(`\nconsole and page errors: ${problems.length}`);
  for (const p of problems.slice(0, 10)) console.log(`  ${p.replace(/\s+/g, ' ').slice(0, 200)}`);
}
const strictFail = args.includes('--strict') && problems.length > 0;
if (failed || strictFail || arg('trace') === 'always') {
  await ctx.tracing.stop({ path: join(OUT, 'trace.zip') });
  console.log(`\ntrace: ${join(OUT, 'trace.zip')}`);
} else {
  await ctx.tracing.stop();
}
const video = page.video();
await ctx.close();
if (video) console.log(`video: ${await video.path()}`);
await browser.close();
const passed = failed ? Number(failed.step.slice(0, 2)) - 1 : STEPS.length;
console.log(`\n${failed || strictFail ? 'FAILED' : 'PASSED'}: ${passed} of ${STEPS.length} steps passed, screenshots in ${OUT}`);
process.exit(failed || strictFail ? 1 : 0);
