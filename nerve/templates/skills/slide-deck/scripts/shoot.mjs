// Screenshots and a layout check of the built deck (slide-deck skill).
// Copy into the deck folder and run there:
//   node shoot.mjs                  -> shots/sNN.png, every slide at its last click, canvas size
//   node shoot.mjs 4,8              -> slides 4 and 8 only
//   node shoot.mjs 3.0,3.1          -> slide 3 before and after click 1 (shots/s03-0.png, s03-1.png)
//   W=1280 OUT=review node shoot.mjs 4,8   -> smaller pictures in review/
// Prints page errors, remote requests (the deck must work offline) and layout
// findings: content that leaves the safe area, runs into the footer, or is clipped.
import { createRequire } from 'node:module';
import { homedir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { mkdirSync, existsSync } from 'node:fs';
const toolsDir = process.env.NERVE_BROWSER_DIR || join(homedir(), '.local/share/nerve-browser');
const require = createRequire(join(toolsDir, 'package.json'));
let chromium;
try { ({ chromium } = require('playwright')); }
catch { console.error(`playwright is not in ${toolsDir}: see the Browser setup step of the skill`); process.exit(2); }

const DIR = dirname(fileURLToPath(import.meta.url));
const HTML = join(DIR, 'deck.html');
if (!existsSync(HTML)) { console.error('no deck.html: run python3 -I build.py first'); process.exit(2); }
const url = pathToFileURL(HTML).href;
const outDir = join(DIR, process.env.OUT || 'shots');
mkdirSync(outDir, { recursive: true });

const b = await chromium.launch();
const p = await b.newPage({ viewport: { width: 1920, height: 1080 } });
const errs = [], remote = [];
p.on('pageerror', e => errs.push(e.message));
p.on('console', m => { if (m.type() === 'error') errs.push(m.text()); });
p.on('request', r => { if (!/^(file|data|about|blob):/.test(r.url())) remote.push(r.url()); });
await p.goto(url + '#1'); await p.waitForTimeout(300);
const { n, cw, ch } = await p.evaluate(() => {
  const cs = getComputedStyle(document.documentElement);
  return { n: document.querySelectorAll('.slide').length, cw: +cs.getPropertyValue('--slide-w') || 1920, ch: +cs.getPropertyValue('--slide-h') || 1080 };
});
const W = +(process.env.W || cw), H = Math.round(W * ch / cw);
await p.setViewportSize({ width: W, height: H });

// targets: [slide, click or null for the last click]
const targets = process.argv[2]
  ? process.argv[2].split(',').map(t => { const [s, c] = t.split('.'); return [+s, c === undefined ? null : +c]; })
  : Array.from({ length: n }, (_, i) => [i + 1, null]);
const findings = [];
for (const [i, c] of targets) {
  if (!(i >= 1 && i <= n)) { findings.push({ slide: i, out: ['no such slide'] }); continue; }
  await p.goto(url + '#' + i + '.' + (c === null ? 999 : c)); await p.reload(); await p.waitForTimeout(450);
  const o = await p.evaluate(() => {
    const s = document.querySelector('.slide.active'), r = s.getBoundingClientRect(), out = [];
    const k = r.width / (+getComputedStyle(document.documentElement).getPropertyValue('--slide-w') || 1920);
    const m = 60 * k;  // safe margin: 60 px of the canvas
    const f = s.querySelector('.foot'), foot = f ? f.getBoundingClientRect() : { top: r.bottom };
    const name = e => (typeof e.className === 'string' && e.className ? e.className : e.tagName.toLowerCase());
    s.querySelectorAll('*').forEach(e => {
      if (e.closest('.foot,.notes')) return;
      const cs = getComputedStyle(e);
      if (cs.display === 'none' || +cs.opacity === 0) return;
      const q = e.getBoundingClientRect(); if (!q.width || !q.height) return;
      const box = [q.left - r.left, q.top - r.top, q.right - r.left, q.bottom - r.top].map(v => Math.round(v / k)).join(',');
      if (q.right > r.right - m || q.bottom > foot.top - 4 * k || q.left < r.left + m || q.top < r.top + 20 * k)
        out.push('outside ' + name(e) + ' ' + box);
      else if (cs.overflow !== 'visible' && (e.scrollHeight > e.clientHeight + 2 || e.scrollWidth > e.clientWidth + 2))
        out.push('clipped ' + name(e) + ' ' + box);
    });
    return out.slice(0, 6);
  });
  const step = await p.evaluate(() => location.hash);
  if (o.length) findings.push({ slide: i, at: step, out: o });
  const file = 's' + String(i).padStart(2, '0') + (c === null ? '' : '-' + c) + '.png';
  await p.screenshot({ path: join(outDir, file) });
}
console.log('slides', n, 'canvas', cw + 'x' + ch, 'pictures', targets.length, 'in', outDir);
console.log('errors', JSON.stringify(errs));
console.log('remote requests', JSON.stringify([...new Set(remote)]));
console.log('layout findings', JSON.stringify(findings, null, 1));
await b.close();
