// PDF of the built deck, one page per slide with every click step shown, plus an
// overview picture and a notes picture for review (slide-deck skill).
// Copy into the deck folder and run there: node pdf.mjs
//   -> deck.pdf, review/overview.png, review/notes.png
import { createRequire } from 'node:module';
import { homedir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { mkdirSync, existsSync, statSync, readFileSync } from 'node:fs';
const toolsDir = process.env.NERVE_BROWSER_DIR || join(homedir(), '.local/share/nerve-browser');
const require = createRequire(join(toolsDir, 'package.json'));
let chromium;
try { ({ chromium } = require('playwright')); }
catch { console.error(`playwright is not in ${toolsDir}: see the Browser setup step of the skill`); process.exit(2); }

const DIR = dirname(fileURLToPath(import.meta.url));
const HTML = join(DIR, 'deck.html'), PDF = join(DIR, 'deck.pdf');
if (!existsSync(HTML)) { console.error('no deck.html: run python3 -I build.py first'); process.exit(2); }
const url = pathToFileURL(HTML).href;
mkdirSync(join(DIR, 'review'), { recursive: true });

const b = await chromium.launch();
let p = await b.newPage({ viewport: { width: 1280, height: 720 } });
await p.goto(url + '#2'); await p.waitForTimeout(300);
await p.keyboard.press('o'); await p.waitForTimeout(400);
await p.screenshot({ path: join(DIR, 'review/overview.png'), fullPage: true });
await p.keyboard.press('Escape'); await p.keyboard.press('n'); await p.waitForTimeout(600);  // after the fade
await p.screenshot({ path: join(DIR, 'review/notes.png') });
const { n, cw, ch } = await p.evaluate(() => {
  const cs = getComputedStyle(document.documentElement);
  return { n: document.querySelectorAll('.slide').length, cw: +cs.getPropertyValue('--slide-w') || 1920, ch: +cs.getPropertyValue('--slide-h') || 1080 };
});
await p.close();

p = await b.newPage({ viewport: { width: cw, height: ch } });
await p.goto(url + '#1'); await p.emulateMedia({ media: 'print' }); await p.waitForTimeout(300);
await p.pdf({ path: PDF, width: cw + 'px', height: ch + 'px', printBackground: true });
const pages = (readFileSync(PDF, 'latin1').match(/\/Type\s*\/Page[^s]/g) || []).length;
console.log('pdf deck.pdf', statSync(PDF).size, 'bytes,', pages, 'pages for', n, 'slides');
if (pages !== n) console.log('warning: page count and slide count differ: look for a slide taller than the canvas');
await b.close();
