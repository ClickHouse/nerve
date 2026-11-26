// Step 1 of the PPTX export (slide-deck skill): reads every slide of the built
// deck into pptx/items.json and pptx/img/*.png. Step 2 is pptx-build.py.
// Copy into the deck folder and run there: node pptx-export.mjs
// Text stays text; mockups (class in MOCK), img, svg, canvas and video become
// pictures. Each slide is read once per click state, so every element knows the
// click it shows at and the click it hides at, and a mockup that changes on a
// click gets one picture per state.
import { createRequire } from 'node:module';
import { homedir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { mkdirSync, rmSync, writeFileSync, existsSync } from 'node:fs';
const toolsDir = process.env.NERVE_BROWSER_DIR || join(homedir(), '.local/share/nerve-browser');
const require = createRequire(join(toolsDir, 'package.json'));
let chromium;
try { ({ chromium } = require('playwright')); }
catch { console.error(`playwright is not in ${toolsDir}: see the Browser setup step of the skill`); process.exit(2); }

const DIR = dirname(fileURLToPath(import.meta.url));
const HTML = join(DIR, 'deck.html'), OUT = join(DIR, 'pptx');
if (!existsSync(HTML)) { console.error('no deck.html: run python3 -I build.py first'); process.exit(2); }
const SCALE = 2;  // pictures at 2x
// Classes that become one picture each. Put class "mock" on the outer element of
// every mockup, or add the mockup's own class here. A missing class comes out as
// loose text boxes and shapes.
const MOCK = ['mock'];
// Classes left out: the template of the target app has its own footer.
const SKIP = ['notes', 'foot'];
const HEAD = ['h'];  // headline classes: the biggest goes into the title placeholder

function collect({ state, MOCK, SKIP, HEAD }) {
  const S = document.querySelector('.slide.active');
  S.querySelectorAll('[data-step]').forEach(e => e.classList.toggle('shown', +e.dataset.step <= state));
  S.querySelectorAll('[data-until]').forEach(e => e.classList.toggle('gone', state >= +e.dataset.until));
  const SR = S.getBoundingClientRect();
  const all = [...S.querySelectorAll('*')];
  const key = el => all.indexOf(el);
  const items = [];
  const rel = r => [r.left - SR.left, r.top - SR.top, r.width, r.height];
  const visible = el => {
    const cs = getComputedStyle(el);
    if (cs.display === 'none' || cs.visibility === 'hidden' || +cs.opacity === 0) return false;
    const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0;
  };
  const built = el => el.hasAttribute('data-step') || el.hasAttribute('data-until') || !!el.querySelector('[data-step],[data-until]');
  const PIC = ['img', 'svg', 'canvas', 'video'];
  const isInline = el => getComputedStyle(el).display === 'inline' && !PIC.includes(el.tagName.toLowerCase()) && !built(el);
  const transparent = c => !c || c === 'transparent' || /rgba\(.*,\s*0\)$/.test(c);
  const deco = el => {
    const cs = getComputedStyle(el);
    const side = s => ({ w: parseFloat(cs[`border${s}Width`]) || 0, c: cs[`border${s}Color`], s: cs[`border${s}Style`] });
    const sides = { top: side('Top'), right: side('Right'), bottom: side('Bottom'), left: side('Left') };
    const on = Object.entries(sides).filter(([, v]) => v.w > 0 && v.s !== 'none');
    const full = on.length === 4 && on.every(([, v]) => v.w === sides.top.w && v.c === sides.top.c);
    return { hasBg: !transparent(cs.backgroundColor), bg: cs.backgroundColor,
      hasBorder: full, bw: sides.top.w, bc: sides.top.c, bs: sides.top.s,
      bars: full ? [] : on.map(([k, v]) => ({ side: k, w: v.w, c: v.c })),  // one-side borders become thin rectangles
      radius: parseFloat(cs.borderTopLeftRadius) || 0,
      pad: [cs.paddingLeft, cs.paddingTop, cs.paddingRight, cs.paddingBottom].map(parseFloat) };
  };
  const st = el => {
    const cs = getComputedStyle(el);
    return { size: parseFloat(cs.fontSize), bold: parseInt(cs.fontWeight) >= 600, italic: cs.fontStyle === 'italic', color: cs.color,
      align: cs.textAlign, lh: cs.lineHeight, font: cs.fontFamily.split(',')[0].trim().replace(/^["']|["']$/g, ''),
      ls: parseFloat(cs.letterSpacing) || 0,  // 'normal' gives 0
      // a dotted bottom border (the .tbd placeholder) becomes a dotted underline
      u: (parseFloat(cs.borderBottomWidth) > 0 && /dotted|dashed/.test(cs.borderBottomStyle)) ? 'dotted' : cs.textDecorationLine.includes('underline') ? 'single' : '' };
  };
  const tt = (el, t) => {
    const v = getComputedStyle(el).textTransform;
    return v === 'uppercase' ? t.toUpperCase() : v === 'lowercase' ? t.toLowerCase() : t;
  };
  function visit(el) {
    if (el !== S && !visible(el)) return;
    const cl = el.classList, tag = el.tagName.toLowerCase();
    if (SKIP.some(c => cl.contains(c))) return;
    if (el !== S && (MOCK.some(c => cl.contains(c)) || PIC.includes(tag))) {
      el.setAttribute('data-xid', key(el));
      // a new picture is needed when something inside changes at this click
      const changes = el.querySelector(`[data-step="${state}"],[data-until="${state}"]`) !== null;
      items.push({ kind: 'pic', key: key(el), box: rel(el.getBoundingClientRect()), changes });
      return;
    }
    const parts = [];
    for (const c of el.childNodes) {
      if (c.nodeType === 3) { if (c.textContent.trim()) parts.push(c); }
      else if (c.nodeType === 1 && c.tagName === 'BR') parts.push(c);
      else if (c.nodeType === 1 && visible(c) && isInline(c)) parts.push(c);
    }
    const runs = [];
    for (const c of parts) {
      if (c.nodeType === 3) runs.push({ text: tt(el, c.textContent.replace(/\s+/g, ' ')), ...st(el) });
      else if (c.tagName === 'BR') runs.push({ text: '\n', ...st(el) });
      else runs.push({ text: c.innerText.replace(/[ \t]+/g, ' '), ...st(c) });
    }
    const text = runs.map(r => r.text).join('').trim();
    const d = el === S ? { hasBg: false, hasBorder: false, bars: [] } : deco(el);
    if (text) {
      const rg = document.createRange(); rg.setStartBefore(parts[0]); rg.setEndAfter(parts[parts.length - 1]);
      const lines = new Set([...rg.getClientRects()].filter(r => r.width > 1).map(r => Math.round(r.bottom / 8))).size;
      const kids = [...el.children].filter(c => !parts.includes(c) && visible(c)).length;
      items.push({ kind: 'text', key: key(el), box: rel(rg.getBoundingClientRect()), ebox: rel(el.getBoundingClientRect()), kids, runs, ...st(el), deco: d, lines,
        headline: HEAD.some(c => cl.contains(c)) });
    } else if (d.hasBg || d.hasBorder || d.bars.length) {
      items.push({ kind: 'shape', key: key(el), box: rel(el.getBoundingClientRect()), deco: d });
    }
    for (const c of el.children) if (!parts.includes(c)) visit(c);
  }
  visit(S);
  return items;
}

rmSync(OUT, { recursive: true, force: true });
mkdirSync(join(OUT, 'img'), { recursive: true });
const b = await chromium.launch();
const p = await b.newPage({ viewport: { width: 1920, height: 1080 }, deviceScaleFactor: SCALE });
const url = pathToFileURL(HTML).href;
await p.goto(url + '#1'); await p.waitForTimeout(400);
const deck = await p.evaluate(() => {
  const cs = getComputedStyle(document.documentElement), v = k => cs.getPropertyValue(k).trim();
  const probe = c => { const e = document.createElement('div'); e.style.color = c; document.body.appendChild(e); const r = getComputedStyle(e).color; e.remove(); return r; };
  const first = f => f.split(',')[0].trim().replace(/^["']|["']$/g, '');
  const h = document.querySelector('.slide .h');
  return { n: document.querySelectorAll('.slide').length, w: +v('--slide-w') || 1920, h: +v('--slide-h') || 1080,
    theme: { bg: probe(v('--bg') || '#fff'), ink: probe(v('--ink') || '#000'), accent: probe(v('--accent') || '#000'),
      font: first(getComputedStyle(document.body).fontFamily), headFont: h ? first(getComputedStyle(h).fontFamily) : first(getComputedStyle(document.body).fontFamily) } };
});
// Which font did the browser really draw? The PPTX names the first font of the
// stack; when the browser drew another one, the measured boxes do not fit.
await p.evaluate(() => {
  const S = document.querySelector('.slide.active'), body = getComputedStyle(document.body).fontFamily;
  const h = S.querySelector('.h'); if (h) h.setAttribute('data-xfont', 'head');
  const t = [...S.querySelectorAll('*')].find(e => !e.closest('.mock,.notes,.foot,.h') && getComputedStyle(e).fontFamily === body &&
    [...e.childNodes].some(c => c.nodeType === 3 && c.textContent.trim()));
  if (t) t.setAttribute('data-xfont', 'body');
});
const cdp = await p.context().newCDPSession(p);
async function drawn(sel) {
  try {
    const { root } = await cdp.send('DOM.getDocument');
    const { nodeId } = await cdp.send('DOM.querySelector', { nodeId: root.nodeId, selector: sel });
    if (!nodeId) return null;
    const { fonts } = await cdp.send('CSS.getPlatformFontsForNode', { nodeId });
    return fonts.sort((a, b) => b.glyphCount - a.glyphCount)[0] || null;
  } catch { return null; }
}
await cdp.send('DOM.enable'); await cdp.send('CSS.enable');
for (const [k, sel, what] of [['font', '[data-xfont="body"]', 'body'], ['headFont', '[data-xfont="head"]', 'headline']]) {
  const f = await drawn(sel);
  if (f && !f.isCustomFont && f.familyName.toLowerCase() !== deck.theme[k].toLowerCase())
    console.log(`warning: the ${what} font is "${deck.theme[k]}" in the CSS, but the browser drew "${f.familyName}" ` +
      '(the first font is not embedded and not installed here). The PPTX names the first font, so its text widths differ. ' +
      'Embed the font (references/look.md) and export again.');
}
await cdp.detach();
await p.setViewportSize({ width: deck.w, height: deck.h });
const slides = [];
for (let i = 1; i <= deck.n; i++) {
  await p.evaluate(i => { location.hash = '#' + i; }, i);
  await p.reload();
  await p.addStyleTag({ content: '*{transition:none!important;animation:none!important}' });
  await p.waitForTimeout(150);
  const meta = await p.evaluate(() => {
    const S = document.querySelector('.slide.active');
    const k = Math.max(0, ...[...S.querySelectorAll('[data-step]')].map(e => +e.dataset.step), ...[...S.querySelectorAll('[data-until]')].map(e => +e.dataset.until));
    const label = e => ((e.innerText || e.textContent || e.className || e.tagName) + '').trim().replace(/\s+/g, ' ').slice(0, 70);
    const builds = {};
    S.querySelectorAll('[data-step]').forEach(e => { const c = +e.dataset.step; (builds[c] = builds[c] || []).push('shows ' + label(e)); });
    S.querySelectorAll('[data-until]').forEach(e => { const c = +e.dataset.until; (builds[c] = builds[c] || []).push('hides ' + label(e)); });
    const cs = getComputedStyle(S);
    return { title: S.dataset.title || '', k, builds, bg: cs.backgroundColor, bgImage: cs.backgroundImage !== 'none',
      notes: [...S.querySelectorAll('.notes p')].map(x => x.innerText.trim()),
      media: !!S.querySelector('audio,video') || /new Audio\(/.test(S.outerHTML) };
  });
  // transparent slide background, so the corners of pictures stay clear
  await p.addStyleTag({ content: '.slide.active{background:transparent!important}html,body,#deck{background:transparent!important}' });
  const atoms = new Map();
  for (let s = 0; s <= meta.k; s++) {
    const items = await p.evaluate(collect, { state: s, MOCK, SKIP, HEAD });
    for (const it of items) {
      let a = atoms.get(it.key);
      if (!a) { a = { ...it, first: s, last: s, imgs: [] }; atoms.set(it.key, a); }
      a.last = s;
      if (it.kind === 'pic' && (a.imgs.length === 0 || it.changes)) {
        const f = `img/s${String(i).padStart(2, '0')}-${it.key}-${s}.png`;
        await p.locator(`.slide.active [data-xid="${it.key}"]`).screenshot({ path: join(OUT, f), omitBackground: true });
        a.imgs.push({ state: s, file: f, box: it.box });
      }
    }
  }
  slides.push({ n: i, ...meta, items: [...atoms.values()].sort((x, y) => x.key - y.key) });
  process.stdout.write(i + ' ');
}
writeFileSync(join(OUT, 'items.json'), JSON.stringify({ canvas: { w: deck.w, h: deck.h }, theme: deck.theme, slides }, null, 1));
console.log('\nslides', slides.length, 'canvas', deck.w + 'x' + deck.h, 'theme', JSON.stringify(deck.theme));
await b.close();
