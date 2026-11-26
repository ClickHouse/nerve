#!/usr/bin/env node
// Audit the rendered pages of a web app in headless Chromium.
//
//   node audit.mjs --base <url> [--routes /,/about] [--viewports desktop,mobile]
//        [--schemes light,dark] [--shots <dir>] [--full] [--axe contrast|all]
//        [--storage <state.json>] [--init <file.js>] [--wait <ms>] [--json <file>]
//
// One line per route x viewport x scheme, then details for each finding.
// Checks: axe-core (color-contrast, or all rules with --axe all), visible elements
// with zero width or height that hold text, horizontal overflow, console errors and
// page errors, failed requests (HTTP status >= 400, or no response).
// Exit code: 0 clean, 1 findings or a page that did not load, 2 bad usage or missing tools.
import { createRequire } from 'node:module';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { readFileSync, mkdirSync, writeFileSync, existsSync } from 'node:fs';

const toolsDir = process.env.NERVE_BROWSER_DIR || join(homedir(), '.local/share/nerve-browser');
const require = createRequire(join(toolsDir, 'package.json'));
let chromium, AXE;
try {
  ({ chromium } = require('playwright'));
  AXE = readFileSync(require.resolve('axe-core/axe.min.js'), 'utf8');
} catch {
  console.error(`playwright or axe-core is not in ${toolsDir}: see the Browser setup step of the skill`);
  process.exit(2);
}

// Output piped to `head` closes early: keep working, drop the rest of the output.
process.stdout.on('error', () => {});
const args = process.argv.slice(2);
const arg = (name, def) => {
  const i = args.indexOf(`--${name}`);
  return i > -1 && args[i + 1] !== undefined ? args[i + 1] : def;
};
const flag = (name) => args.includes(`--${name}`);
const usage = (msg) => { console.error(`audit: ${msg}`); process.exit(2); };

const BASE = (arg('base') || usage('--base <url> is required (for example http://127.0.0.1:8123)')).replace(/\/$/, '');
const ROUTES = arg('routes', '/').split(',').map((r) => (r.startsWith('/') ? r : `/${r}`));
const VIEWPORTS = arg('viewports', 'desktop,mobile').split(',').map((v) => {
  if (v === 'desktop') return { name: v, width: 1280, height: 800 };
  if (v === 'mobile') return { name: v, width: 375, height: 812, isMobile: true, hasTouch: true };
  const m = v.match(/^(\d+)x(\d+)$/);
  if (!m) usage(`unknown viewport "${v}": use desktop, mobile or <width>x<height>`);
  return { name: v, width: Number(m[1]), height: Number(m[2]) };
});
const SCHEMES = arg('schemes', 'light,dark').split(',');
for (const s of SCHEMES) if (!['light', 'dark', 'no-preference'].includes(s)) usage(`unknown scheme "${s}"`);
const SHOTS = arg('shots');
const AXE_MODE = arg('axe', 'contrast');
if (!['contrast', 'all'].includes(AXE_MODE)) usage('--axe takes contrast or all');
const STORAGE = arg('storage');
const INIT = arg('init');
const WAIT = Number(arg('wait', '500'));
const JSON_OUT = arg('json');
for (const f of [STORAGE, INIT]) if (f && !existsSync(f)) usage(`file not found: ${f}`);
if (SHOTS) mkdirSync(SHOTS, { recursive: true });

const short = (s, n = 140) => String(s).replace(/\s+/g, ' ').trim().slice(0, n);

async function checkPage(page) {
  const axe = await page.evaluate(async ({ src, mode }) => {
    if (!window.axe) {
      const s = document.createElement('script');
      s.textContent = src;
      document.head.appendChild(s);
    }
    const opts = mode === 'all' ? {} : { runOnly: ['color-contrast'] };
    const r = await window.axe.run(document, opts);
    return r.violations.flatMap((v) => v.nodes.map((n) => ({
      rule: v.id, target: String(n.target[0]), msg: (n.any?.[0]?.message || v.help || '').split('\n')[0],
    })));
  }, { src: AXE, mode: AXE_MODE });

  // Elements that the browser renders (they have a box) but with zero width or height,
  // while they hold text. checkVisibility() skips display:none, display:contents,
  // closed <details> content, visibility:hidden and opacity:0.
  const zero = await page.evaluate(() => {
    const out = [];
    for (const el of document.body.querySelectorAll('*')) {
      if (['OPTION', 'OPTGROUP', 'SCRIPT', 'STYLE', 'TEMPLATE', 'BR', 'WBR'].includes(el.tagName)) continue;
      const r = el.getBoundingClientRect();
      if (r.width > 0 && r.height > 0) continue;
      if (!el.checkVisibility({ opacityProperty: true, visibilityProperty: true })) continue;
      const text = (el.textContent || '').replace(/\s+/g, ' ').trim();
      if (!text) continue;
      const cls = typeof el.className === 'string' && el.className ? `.${el.className.trim().split(/\s+/).join('.')}` : '';
      // "clips": overflow is not visible, so the text is cut off. Without it the text
      // can still show outside the box (for example a box that holds only floats).
      const clips = getComputedStyle(el).overflow !== 'visible';
      out.push({ el: `${el.tagName.toLowerCase()}${el.id ? '#' + el.id : ''}${cls}`, size: `${r.width}x${r.height}`, clips, text: text.slice(0, 60) });
    }
    return out;
  });

  // Sideways scroll of the document, plus the elements that stick out the most.
  const overflow = await page.evaluate(() => {
    const doc = document.documentElement;
    const px = doc.scrollWidth - doc.clientWidth;
    if (px <= 0) return { px: 0, culprits: [] };
    const clips = (el) => {
      for (let p = el.parentElement; p && p !== doc; p = p.parentElement) {
        const ox = getComputedStyle(p).overflowX;
        if (ox !== 'visible') return true;
      }
      return false;
    };
    const sticksOut = (el) => {
      const r = el.getBoundingClientRect();
      return r.right > doc.clientWidth + 1 && r.width > 0;
    };
    // Report the outermost elements that stick out: their parent is still inside.
    const culprits = [];
    for (const el of document.body.querySelectorAll('*')) {
      const r = el.getBoundingClientRect();
      if (sticksOut(el) && !(el.parentElement && el.parentElement !== document.body && sticksOut(el.parentElement)) && !clips(el)) {
        const cls = typeof el.className === 'string' && el.className ? `.${el.className.trim().split(/\s+/).join('.')}` : '';
        culprits.push({ el: `${el.tagName.toLowerCase()}${el.id ? '#' + el.id : ''}${cls}`, right: Math.round(r.right), width: Math.round(r.width) });
      }
    }
    culprits.sort((a, b) => b.right - a.right || b.width - a.width);
    return { px, culprits: culprits.slice(0, 3) };
  });
  return { axe, zero, overflow };
}

const browser = await chromium.launch();
const rows = [];
for (const scheme of SCHEMES) {
  for (const vp of VIEWPORTS) {
    const ctx = await browser.newContext({
      viewport: { width: vp.width, height: vp.height },
      isMobile: !!vp.isMobile, hasTouch: !!vp.hasTouch,
      colorScheme: scheme,
      ...(STORAGE ? { storageState: STORAGE } : {}),
    });
    // An init script runs in every page before the app's own code.
    if (INIT) await ctx.addInitScript({ path: resolve(INIT) });

    for (const route of ROUTES) {
      const label = `${route} ${vp.name} ${scheme}`;
      const page = await ctx.newPage();
      const consoleErrors = [];
      const failed = [];
      page.on('console', (m) => {
        // A failed resource load also logs a console error; the request check reports it.
        if (m.type() === 'error' && !m.text().startsWith('Failed to load resource')) consoleErrors.push(`console: ${short(m.text())}`);
      });
      page.on('pageerror', (e) => consoleErrors.push(`pageerror: ${short(e.message)}`));
      page.on('response', (r) => { if (r.status() >= 400) failed.push(`${r.status()} ${r.request().method()} ${short(r.url(), 160)}`); });
      page.on('requestfailed', (r) => failed.push(`${r.failure()?.errorText || 'failed'} ${r.method()} ${short(r.url(), 160)}`));
      try {
        await page.goto(BASE + route, { waitUntil: 'load', timeout: 30000 });
        await page.waitForLoadState('networkidle', { timeout: 5000 }).catch(() => {});
        await page.waitForTimeout(WAIT);
        const { axe, zero, overflow } = await checkPage(page);
        let shot = null;
        if (SHOTS) {
          const slug = route.replace(/[^a-zA-Z0-9]+/g, '_').replace(/^_+|_+$/g, '') || 'root';
          shot = join(SHOTS, `${slug}-${vp.name}-${scheme}.png`);
          await page.screenshot({ path: shot, fullPage: flag('full') });
        }
        const row = { label, axe, zero, overflow, consoleErrors: [...consoleErrors], failed: [...failed], shot };
        row.findings = axe.length + zero.length + (overflow.px > 0 ? 1 : 0) + row.consoleErrors.length + row.failed.length;
        rows.push(row);
        const axeName = AXE_MODE === 'all' ? 'axe' : 'contrast';
        console.log(`${row.findings ? 'FAIL' : ' ok '} ${label.padEnd(36)} ${axeName}=${axe.length} zero=${zero.length} overflow=${overflow.px}px console=${row.consoleErrors.length} requests=${row.failed.length}`);
      } catch (e) {
        rows.push({ label, error: short(e.message, 200), findings: 1 });
        console.log(`ERR  ${label.padEnd(36)} ${short(e.message, 120)}`);
      }
      await page.close();
    }
    await ctx.close();
  }
}
await browser.close();

const bad = rows.filter((r) => r.findings);
console.log(`\n=== ${rows.length} combinations, ${bad.length} with findings ===`);
for (const b of bad) {
  console.log(`\n${b.label}${b.error ? `  ERROR: ${b.error}` : ''}`);
  for (const n of (b.axe || []).slice(0, 8)) console.log(`  ${n.rule.padEnd(14)} ${short(n.target, 80)}\n                 ${short(n.msg, 240)}`);
  if ((b.axe || []).length > 8) console.log(`  ... ${b.axe.length - 8} more axe nodes`);
  for (const z of (b.zero || []).slice(0, 6)) console.log(`  zero-size      ${short(z.el, 80)} ${z.size}${z.clips ? ' clips' : ''} "${z.text}"`);
  if ((b.zero || []).length > 6) console.log(`  ... ${b.zero.length - 6} more zero-size elements`);
  if (b.overflow?.px > 0) {
    console.log(`  overflow       page is ${b.overflow.px}px wider than the viewport`);
    for (const c of b.overflow.culprits) console.log(`                 ${short(c.el, 80)} right=${c.right} width=${c.width}`);
  }
  for (const c of (b.consoleErrors || []).slice(0, 6)) console.log(`  ${c}`);
  for (const f of (b.failed || []).slice(0, 6)) console.log(`  request        ${f}`);
}
if (JSON_OUT) writeFileSync(JSON_OUT, JSON.stringify(rows, null, 2));
process.exit(bad.length ? 1 : 0);
