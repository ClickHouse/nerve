#!/usr/bin/env node
// check_page.mjs (shareable-html skill): open a built page from disk the way a
// person does, and report what is wrong.
//
// Usage: node check_page.mjs <page.html> [--out <dir>] [--pdf]
//
// For desktop (1280 px) and phone (390 px) widths, in light and dark:
//   - blocks and reports every request that is not file:, data: or blob:
//   - reports console errors, page errors and failed requests
//   - reports content wider than the screen (horizontal overflow)
//   - reports images that did not load, a missing viewport meta tag, and
//     template text left in ("EXAMPLE DATA", "lorem ipsum", "TODO", "TBD")
//   - writes screenshots <out>/desktop-light.png, desktop-dark.png,
//     phone-light.png, phone-dark.png: the top 1800 CSS px of the page.
//     A longer page gets more parts: desktop-light.2.png, .3.png, .4.png.
// --pdf also writes <out>/page.pdf (A4) and <out>/print.png (print styles).
// Exit 0: no findings. Exit 1: findings. Exit 2: setup problem.
import { createRequire } from 'node:module';
import { homedir } from 'node:os';
import { join, resolve, dirname } from 'node:path';
import { existsSync, mkdirSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

const toolsDir = process.env.NERVE_BROWSER_DIR || join(homedir(), '.local/share/nerve-browser');
const require = createRequire(join(toolsDir, 'package.json'));
let chromium;
try { ({ chromium } = require('playwright')); }
catch { console.error(`playwright is not in ${toolsDir}: see the Browser setup step of the skill`); process.exit(2); }

const argv = process.argv.slice(2);
const opt = (name) => { const i = argv.indexOf(name); return i >= 0 ? argv.splice(i, 2)[1] : undefined; };
const outArg = opt('--out');
const wantPdf = argv.includes('--pdf');
const files = argv.filter((a) => !a.startsWith('--'));
if (files.length !== 1) { console.error('usage: node check_page.mjs <page.html> [--out <dir>] [--pdf]'); process.exit(2); }
const file = resolve(files[0]);
if (!existsSync(file)) { console.error(`no such file: ${file}`); process.exit(2); }
const outDir = resolve(outArg || join(dirname(file), 'shots'));
mkdirSync(outDir, { recursive: true });
console.log(`screenshots in ${outDir}`);
const url = pathToFileURL(file).href;

const VIEWS = [
  { name: 'desktop-light', width: 1280, height: 800, scheme: 'light', mobile: false },
  { name: 'desktop-dark', width: 1280, height: 800, scheme: 'dark', mobile: false },
  { name: 'phone-light', width: 390, height: 844, scheme: 'light', mobile: true },
  { name: 'phone-dark', width: 390, height: 844, scheme: 'dark', mobile: true },
];
const PART = 1800;  // CSS px per screenshot part: the Read tool shows it without shrinking much
const MAX_PARTS = 4; // longer pages are cut in the screenshots only
const PLACEHOLDER = /EXAMPLE DATA|[Ll]orem ipsum|\bTODO\b|\bTBD\b|REPLACE[ _]ME|\{\{.*?\}\}/;

const findings = new Map(); // text -> set of view names
const add = (view, text) => { if (!findings.has(text)) findings.set(text, new Set()); findings.get(text).add(view); };
const isLocal = (u) => /^(file|data|blob|about):/i.test(u);

async function openPage(browser, v, media) {
  const ctx = await browser.newContext({
    viewport: { width: v.width, height: v.height }, colorScheme: v.scheme,
    isMobile: v.mobile, hasTouch: v.mobile, deviceScaleFactor: 1,
  });
  const blocked = new Set();
  await ctx.route('**/*', (route) => {
    const u = route.request().url();
    if (isLocal(u)) return route.continue();
    blocked.add(u);
    add(v.name, `blocked network request: ${u}`);
    return route.abort('blockedbyclient');
  });
  const page = await ctx.newPage();
  page.on('websocket', (ws) => add(v.name, `websocket opened: ${ws.url()}`));
  page.on('console', (m) => {
    if (m.type() !== 'error') return;
    const t = m.text();
    if (/ERR_BLOCKED_BY_CLIENT/.test(t) && blocked.size) return; // already reported as a blocked request
    add(v.name, `console error: ${t}`);
  });
  page.on('pageerror', (e) => add(v.name, `page error: ${e.message}`));
  page.on('requestfailed', (r) => {
    if (blocked.has(r.url())) return;
    add(v.name, `request failed: ${r.url().slice(0, 120)} (${r.failure()?.errorText})`);
  });
  if (media) await page.emulateMedia({ media });
  await page.goto(url, { waitUntil: 'load', timeout: 30000 });
  await page.evaluate(() => document.fonts.ready);
  await page.waitForTimeout(300);
  return { ctx, page };
}

const browser = await chromium.launch();
try {
  for (const v of VIEWS) {
    const { ctx, page } = await openPage(browser, v);
    const r = await page.evaluate(() => {
      const vw = document.documentElement.clientWidth;
      const name = (el) => el.tagName.toLowerCase() + (el.id ? '#' + el.id : '')
        + (el.classList.length ? '.' + [...el.classList].join('.') : '');
      // An element counts when its right edge passes the screen and no
      // ancestor below <body> clips or scrolls it sideways.
      const wide = new Set();
      for (const el of document.body.querySelectorAll('*')) {
        const box = el.getBoundingClientRect();
        if (box.width === 0 || box.right <= vw + 1) continue;
        let p = el.parentElement, clipped = false;
        while (p && p !== document.body) {
          if (getComputedStyle(p).overflowX !== 'visible') { clipped = true; break; }
          p = p.parentElement;
        }
        if (!clipped) wide.add(el);
      }
      const outer = [...wide].filter((el) => !wide.has(el.parentElement))
        .map((el) => `${name(el)} ends at ${Math.round(el.getBoundingClientRect().right)} px`);
      const broken = [...document.images].filter((i) => i.complete && i.naturalWidth === 0)
        .map((i) => (i.currentSrc || i.src || '').slice(0, 80));
      return {
        vw, sw: document.documentElement.scrollWidth, outer: outer.slice(0, 6), broken,
        height: document.documentElement.scrollHeight, text: document.body.innerText,
        viewport: !!document.querySelector('meta[name="viewport"]'),
      };
    });
    if (r.sw > r.vw + 1 || r.outer.length) {
      add(v.name, `horizontal overflow: page is ${r.sw} px wide on a ${r.vw} px screen`
        + (r.outer.length ? `; ${r.outer.join('; ')}` : ''));
    }
    for (const b of r.broken) add(v.name, `image did not load: ${b}`);
    const ph = r.text.match(new RegExp(`.{0,30}(?:${PLACEHOLDER.source}).{0,30}`));
    if (ph) add(v.name, `template text left in: "${ph[0].trim()}"`);
    if (v.mobile && !r.viewport) add(v.name, 'no <meta name="viewport">: a phone lays the page out at desktop width and shrinks it');
    const parts = Math.min(Math.ceil(r.height / PART), MAX_PARTS);
    const names = [];
    for (let i = 0; i < parts; i++) {
      const name = i === 0 ? `${v.name}.png` : `${v.name}.${i + 1}.png`;
      const y = i * PART;
      await page.screenshot({ path: join(outDir, name), fullPage: true,
        clip: { x: 0, y, width: v.width, height: Math.min(PART, r.height - y) } });
      names.push(name);
    }
    console.log(`shots ${v.name}: ${names.join(' ')} (page ${v.width}x${r.height} CSS px`
      + `${r.height > PART * MAX_PARTS ? `, cut at ${PART * MAX_PARTS}` : ''})`);
    await ctx.close();
  }
  if (wantPdf) {
    const v = { name: 'print', width: 794, height: 1123, scheme: 'light', mobile: false };
    const { ctx, page } = await openPage(browser, v, 'print');
    const pdf = join(outDir, 'page.pdf');
    await page.pdf({ path: pdf, format: 'A4', printBackground: true, preferCSSPageSize: true });
    await page.screenshot({ path: join(outDir, 'print.png'), fullPage: true });
    console.log(`pdf ${pdf}; print styles shot print.png`);
    await ctx.close();
  }
} finally {
  await browser.close();
}

if (!findings.size) { console.log(`findings: 0 (${file})`); process.exit(0); }
console.log(`findings: ${findings.size} (${file})`);
for (const [text, views] of findings) console.log(`  - ${text} [${[...views].join(', ')}]`);
process.exit(1);
