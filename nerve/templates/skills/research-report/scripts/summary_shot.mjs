// Render a summary card (assets/summary_card.html, filled in) to a PNG of
// 1440 x 900 CSS pixels at 2x (2880 x 1800), and check that all text fits.
//   node summary_shot.mjs <summary.html> <out.png>
// Exit 0: the text fits. Exit 1: the PNG is written so that you can look at
// it, but a box overflows; the output names the box. Exit 2: usage or setup.
import { createRequire } from 'node:module';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const toolsDir = process.env.NERVE_BROWSER_DIR || join(homedir(), '.local/share/nerve-browser');
const require = createRequire(join(toolsDir, 'package.json'));
let chromium;
try { ({ chromium } = require('playwright')); }
catch { console.error(`playwright is not in ${toolsDir}: see the Browser setup step of the skill`); process.exit(2); }

const [,, file, out] = process.argv;
if (!file || !out) { console.error('usage: node summary_shot.mjs <summary.html> <out.png>'); process.exit(2); }

const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 2, colorScheme: 'light' });
const errors = [];
page.on('pageerror', e => errors.push(e.message));
await page.goto(pathToFileURL(resolve(file)).href);
await page.evaluate(() => document.fonts.ready);
const report = await page.evaluate(() => {
  const over = [];
  for (const el of document.querySelectorAll('[data-fit]')) {
    const name = el.dataset.name || el.id;
    const tall = el.scrollHeight - el.clientHeight, wide = el.scrollWidth - el.clientWidth;
    if (tall > 1) over.push(`${name}: ${tall}px too tall`);
    if (wide > 1) over.push(`${name}: ${wide}px too wide`);
  }
  return { over, bar: document.querySelector('.bar')?.getAttribute('aria-label') ?? 'none' };
});
await page.locator('#card').screenshot({ path: out });
await browser.close();
console.log(`wrote ${out}; tally: ${report.bar}`);
if (errors.length) { console.log('PAGE ERRORS (check the DATA block):\n  ' + errors.join('\n  ')); process.exit(1); }
if (report.over.length) { console.log('DOES NOT FIT:\n  ' + report.over.join('\n  ')); process.exit(1); }
console.log('fits: yes');
