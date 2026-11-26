#!/usr/bin/env node
// Check and render a diagram or chart to SVG and PNG (2x) in headless Chromium.
//
//   node render.mjs <file.mmd|file.vl.json|file.vg.json|file.svg> [--out <dir>] [--theme light|dark] [--width <px>]
//
// Exit 0: rendered. Exit 1: the input is bad (parse error, data from a url,
// unknown file type). Exit 2: the tools are missing (Playwright, the browser).
// Nothing loads from the network: the libraries come from the shared tools folder.

import { createRequire } from 'node:module';
import { homedir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';

const toolsDir = process.env.NERVE_BROWSER_DIR || join(homedir(), '.local/share/nerve-browser');
const require = createRequire(join(toolsDir, 'package.json'));
let chromium;
try { ({ chromium } = require('playwright')); }
catch { console.error(`playwright is not in ${toolsDir}: see the Browser setup step of the skill`); process.exit(2); }

const fail = (message, code = 1) => { console.error(`error: ${message}`); process.exit(code); };
const lib = (path) => {
  const file = join(toolsDir, 'node_modules', path);
  if (!existsSync(file)) fail(`${file} is missing: install mermaid, vega and vega-lite in ${toolsDir}`, 2);
  return file;
};

// ---- Arguments -------------------------------------------------------------
const usage = 'usage: render.mjs <file.mmd|file.vl.json|file.vg.json|file.svg> [--out <dir>] [--theme light|dark] [--width <px>]';
const args = process.argv.slice(2);
const opts = { theme: 'light', width: 640, out: null };
let input = null;
for (let i = 0; i < args.length; i++) {
  const a = args[i];
  if (a === '--out') opts.out = args[++i];
  else if (a === '--theme') opts.theme = args[++i];
  else if (a === '--width') opts.width = Number(args[++i]);
  else if (a.startsWith('--')) fail(`unknown option ${a}\n${usage}`);
  else input = a;
}
if (!input) fail(usage);
if (!['light', 'dark'].includes(opts.theme)) fail('--theme must be light or dark');
if (!(opts.width >= 100 && opts.width <= 4000)) fail('--width must be a number from 100 to 4000');
if (!existsSync(input)) fail(`${input} does not exist`);

const KINDS = [['.vl.json', 'vega-lite'], ['.vg.json', 'vega'], ['.mmd', 'mermaid'], ['.mermaid', 'mermaid'], ['.svg', 'svg']];
const match = KINDS.find(([suffix]) => input.toLowerCase().endsWith(suffix));
if (!match) fail(`unknown file type: name it .mmd, .vl.json, .vg.json or .svg\n${usage}`);
const [suffix, kind] = match;
const source = readFileSync(input, 'utf8');
const stem = basename(input).slice(0, -suffix.length);
const outDir = resolve(opts.out ?? dirname(input));
const warnings = [];

// ---- Theme for exported files: the chat's palette, fixed to light or dark ----
// (in the chat the app themes each figure itself and follows the reader's theme).
const THEMES = {
  light: { bg: '#ffffff', text: '#1c1f26', muted: '#5b6472', grid: '#e3e6eb', node: '#eceef2', border: '#8a919c',
    palette: ['#104ec6', '#a33c00', '#006170', '#007109', '#660099', '#8a6900', '#8800cc', '#067462', '#c10000', '#1d64ec'] },
  dark: { bg: '#22252b', text: '#e8eaed', muted: '#a3a9b3', grid: '#363a42', node: '#2c3039', border: '#7d8490',
    palette: ['#437eef', '#ff9457', '#33e4ff', '#66ff73', '#cc66ff', '#faff69', '#dd99ff', '#6df8e1', '#ff7575', '#6d9bf3'] },
};
const t = THEMES[opts.theme];
const font = 'Arial, "Liberation Sans", Helvetica, sans-serif';

function mermaidConfig() {
  const vars = {
    darkMode: opts.theme === 'dark', background: t.bg, fontFamily: font, fontSize: '14px', useGradient: false, dropShadow: 'none',
    primaryColor: t.node, primaryTextColor: t.text, primaryBorderColor: t.border, secondaryColor: t.node,
    tertiaryColor: t.bg, lineColor: t.muted, textColor: t.text, titleColor: t.text, edgeLabelBackground: t.bg,
    clusterBkg: t.bg, clusterBorder: t.border, noteBkgColor: t.node, noteTextColor: t.text, noteBorderColor: t.border,
    actorBkg: t.node, actorBorder: t.border, actorTextColor: t.text, signalColor: t.muted, signalTextColor: t.text,
    sequenceNumberColor: t.bg, labelBoxBkgColor: t.node, labelTextColor: t.text, loopTextColor: t.text, gridColor: t.grid,
    taskBkgColor: t.node, taskBorderColor: t.palette[0], taskTextColor: t.text, taskTextDarkColor: t.text,
    taskTextOutsideColor: t.text, activeTaskBkgColor: t.node, activeTaskBorderColor: t.palette[0],
    doneTaskBkgColor: t.grid, doneTaskBorderColor: t.border, critBorderColor: t.palette[8], critBkgColor: t.node,
    sectionBkgColor: t.bg, sectionBkgColor2: t.bg, altSectionBkgColor: t.bg, todayLineColor: t.palette[8],
    pieStrokeColor: t.bg, pieSectionTextColor: opts.theme === 'dark' ? '#000000' : '#ffffff',
    pieTitleTextColor: t.text, pieLegendTextColor: t.text, commitLabelFontSize: '11px', tagLabelFontSize: '11px',
    rowOdd: t.node, rowEven: t.bg, attributeBackgroundColorOdd: t.node, attributeBackgroundColorEven: t.bg,
    packet: { byteFontSize: '11px' },
  };
  // A hue washed into the background: section fills (timeline, mindmap, kanban) that text can sit on.
  const wash = (hex) => '#' + [1, 3, 5].map((i) => Math.round(parseInt(t.bg.slice(i, i + 2), 16) * 0.75
    + parseInt(hex.slice(i, i + 2), 16) * 0.25).toString(16).padStart(2, '0')).join('');
  t.palette.forEach((color, i) => {
    Object.assign(vars, { [`pie${i + 1}`]: color, [`git${i}`]: color, [`gitBranchLabel${i}`]: vars.pieSectionTextColor,
      [`cScale${i}`]: wash(color), [`cScaleInv${i}`]: color, [`cScalePeer${i}`]: t.border, [`cScaleLabel${i}`]: t.text });
  });
  return {
    startOnLoad: false, securityLevel: 'strict', theme: 'base', darkMode: opts.theme === 'dark', fontFamily: font,
    // Real SVG text, not HTML labels: the file then works in decks, PDFs and image tools.
    htmlLabels: false, flowchart: { htmlLabels: false },
    gantt: { useWidth: opts.width, axisFormat: '%b %-d', fontSize: 12, sectionFontSize: 12 },
    xyChart: { width: opts.width, height: Math.round(Math.min(400, Math.max(240, opts.width * 0.57))) },
    themeVariables: vars,
    themeCSS: [
      '[filter*="drop-shadow"] { filter: none; }',
      `.edgeLabel rect.background, .edgeLabel .label rect { opacity: 1; fill: ${t.bg}; stroke: ${t.bg}; stroke-width: 4px; }`,
      '.grid .tick text { font-size: 12px; }', // d3 axes default to 10 px
      '.c4-shape .label .c4-type, .treemapSectionValue { font-size: 11px !important; }',
    ].join('\n'),
  };
}

function vegaConfig() {
  const label = { labelColor: t.muted, labelFont: font, labelFontSize: 11, titleColor: t.text, titleFont: font, titleFontSize: 12 };
  return {
    background: t.bg, font, view: { stroke: null }, mark: { color: t.palette[0] },
    text: { color: t.text }, rule: { color: t.muted },
    axis: { ...label, domainColor: t.border, tickColor: t.border, gridColor: t.grid },
    axisTemporal: { grid: false, tickCount: 5 }, axisQuantitative: { tickCount: 5 }, axisBand: { ticks: false },
    scale: { barBandPaddingInner: 0.3 }, legend: label, header: label,
    title: { color: t.text, font, fontSize: 14, fontWeight: 600, anchor: 'start', subtitleColor: t.muted },
    range: { category: t.palette },
  };
}

// ---- Checks the chat makes before it draws --------------------------------
function assertSelfContained(spec, path = 'spec') {
  if (Array.isArray(spec)) return spec.forEach((item, i) => assertSelfContained(item, `${path}[${i}]`));
  if (!spec || typeof spec !== 'object') return;
  for (const [key, value] of Object.entries(spec)) {
    if (key === 'values' || key === 'datasets') continue; // data rows may have a column called url
    if (key === 'url' && typeof value === 'string') fail(`${path}.url points outside the spec; inline the data as "values" (the chat refuses it too)`);
    assertSelfContained(value, `${path}.${key}`);
  }
}

// A single-view chart without a size gets the chat's sizing: the given width, a 280 px plot.
function sized(spec) {
  const multi = ['concat', 'hconcat', 'vconcat', 'facet', 'repeat'].some((k) => k in spec)
    || ['row', 'column', 'facet'].some((c) => spec.encoding?.[c]);
  if (multi) return spec;
  const encodings = [spec, ...(spec.layer ?? [])].map((view) => view.encoding ?? {});
  const radial = encodings.some((e) => e.theta) && !encodings.some((e) => e.x || e.y);
  const discreteY = encodings.some((e) => ['nominal', 'ordinal'].includes(e.y?.type));
  const out = { ...spec };
  if (out.width === undefined) {
    out.width = radial ? 280 : opts.width;
    if (!radial && out.autosize === undefined) out.autosize = { type: 'fit-x', contains: 'padding' };
  }
  if (out.height === undefined && !discreteY) out.height = 280;
  return out;
}

// Time axes labelled in one style ("Oct 01", a year as "2027"), as the chat does.
const DATE_FORMAT = { year: '%Y', quarter: '%b %d', month: '%b %d', week: '%b %d', date: '%b %d', hours: '%H:%M', minutes: '%H:%M' };
function dateLabels(view) {
  for (const channel of ['x', 'y']) {
    const def = view.encoding?.[channel];
    if (def?.type !== 'temporal' || def.timeUnit || def.axis === null || def.axis?.format) continue;
    def.axis = { ...def.axis, format: DATE_FORMAT };
  }
  for (const key of ['layer', 'concat', 'hconcat', 'vconcat']) (view[key] ?? []).forEach(dateLabels);
  if (view.spec) dateLabels(view.spec);
}

let spec = null;
if (kind === 'vega' || kind === 'vega-lite') {
  try { spec = JSON.parse(source); } catch (e) { fail(`the spec is not valid JSON (${e.message})`); }
  if (!spec || typeof spec !== 'object' || Array.isArray(spec)) fail('the spec must be a JSON object');
  delete spec.usermeta; // the chat ignores it
  assertSelfContained(spec);
  if (kind === 'vega-lite') { dateLabels(spec); spec = sized(spec); }
  else {
    const signal = (name) => spec.signals?.some((s) => s.name === name);
    if (spec.width === undefined && !signal('width')) spec.width = opts.width;
    if (spec.height === undefined && !signal('height')) spec.height = 280;
  }
}
if (kind === 'mermaid') {
  const outside = /url\(\s*['"]?\s*(?!#)[^)\n]{0,40}|@import\b|\b(img|icon)\s*["']?\s*:\s*["']?\s*https?:[^\s"']{0,40}/i.exec(source);
  if (outside) fail(`the diagram loads from outside the picture (${outside[0]}); the chat refuses it`);
  const frontMatter = /^\s*---\r?\n([\s\S]*?)\r?\n---/.exec(source)?.[1] ?? '';
  if (/%%\{\s*init/.test(source) || /\b(theme|themeVariables|themeCSS)\s*:/.test(frontMatter)) {
    warnings.push('the diagram sets its own config or theme; the chat themes diagrams itself, so remove it before you send');
  }
}

// ---- Render in the page ----------------------------------------------------
const LIBS = { mermaid: ['mermaid/dist/mermaid.min.js'], 'vega-lite': ['vega/build/vega.min.js', 'vega-lite/build/vega-lite.min.js'],
  vega: ['vega/build/vega.min.js'], svg: [] };
const libs = LIBS[kind].map(lib);
let browser;
try { browser = await chromium.launch(); }
catch (e) { fail(`the browser does not start: ${e.message.split('\n')[0]}\n(see Known problems in the skill)`, 2); }
const page = await browser.newPage({ deviceScaleFactor: 2, timezoneId: 'UTC', viewport: { width: 1600, height: 1200 } });
await page.route('**/*', (route) => {
  warnings.push(`blocked a request to ${route.request().url().slice(0, 80)}: the chat loads nothing from the network`);
  return route.abort();
});
page.on('console', (msg) => {
  if (['warning', 'error'].includes(msg.type()) && !msg.text().startsWith('Failed to load resource')) warnings.push(msg.text());
});
await page.setContent(`<!doctype html><meta charset="utf-8"><body style="margin:0;background:${t.bg};color:${t.text};font-family:${font.replace(/"/g, "'")}"><div id="stage" style="display:inline-block"></div></body>`);

let result;
try {
  if (kind === 'mermaid') {
    await page.addScriptTag({ path: libs[0] });
    result = await page.evaluate(async ({ source, config }) => {
      mermaid.initialize(config);
      let parsed;
      try { parsed = await mermaid.parse(source); } catch (e) { return { error: String(e?.message ?? e) }; }
      const { svg } = await mermaid.render('diagram', source);
      document.getElementById('stage').innerHTML = svg;
      const el = document.querySelector('#stage > svg');
      // SVG-text labels keep entities such as #quot; as "&quot;": decode them as text.
      const walker = document.createTreeWalker(el, NodeFilter.SHOW_TEXT);
      const decoder = document.createElement('textarea');
      for (let n = walker.nextNode(); n; n = walker.nextNode()) {
        if (/&(#\d+|#x[\da-f]+|[a-z]+);/i.test(n.data) && !n.parentElement.closest('style')) { decoder.innerHTML = n.data; n.data = decoder.value; }
      }
      const [, , w, h] = el.getAttribute('viewBox').split(/[\s,]+/).map(Number);
      el.setAttribute('width', w); el.setAttribute('height', h); el.style.maxWidth = 'none';
      return { type: parsed.diagramType, nodes: el.querySelectorAll('g.node').length };
    }, { source, config: mermaidConfig() });
  } else if (kind === 'svg') {
    result = await page.evaluate(({ source, width }) => {
      const doc = new DOMParser().parseFromString(source, 'image/svg+xml');
      const err = doc.querySelector('parsererror');
      if (err || doc.documentElement.localName !== 'svg') return { error: err ? err.textContent.trim().split('\n')[0] : 'the file holds no <svg> root' };
      const removed = new Set();
      for (const el of doc.querySelectorAll('style, image, feImage, foreignObject, script')) removed.add(`<${el.localName}>`);
      for (const el of doc.querySelectorAll('*')) {
        for (const a of el.attributes) {
          if (/(^|:)href$|^src$/.test(a.name) && !a.value.trim().startsWith('#')) removed.add(`${a.name}="${a.value.slice(0, 40)}"`);
          if (/url\(\s*['"]?\s*(?!#)|@import|\\/i.test(a.value)) removed.add(`the ${a.name} attribute (a url() out of the picture, or a backslash)`);
          if (a.name === 'style' && /(^|[;\s])(position|z-index|inset|top|right|bottom|left|transform|translate)\s*:/i.test(a.value)) {
            removed.add('a style attribute with position, top, left, transform or z-index');
          }
        }
      }
      const svg = document.importNode(doc.documentElement, true);
      document.getElementById('stage').appendChild(svg);
      const box = svg.viewBox.baseVal;
      const px = (v) => (/^\s*[\d.]+(px)?\s*$/.test(v ?? '') ? parseFloat(v) : NaN);
      let w = px(svg.getAttribute('width')); let h = px(svg.getAttribute('height'));
      if (!(w > 0) && box?.width > 0) { w = box.width < 200 && !(h > 0) ? width : (h > 0 ? h * box.width / box.height : box.width); }
      if (!(h > 0) && box?.width > 0) h = w * box.height / box.width;
      if (!(w > 0 && h > 0)) return { error: 'the svg has no size: give it width and height, or a viewBox' };
      svg.setAttribute('width', w); svg.setAttribute('height', h);
      return { type: 'svg', removed: [...removed] };
    }, { source, width: opts.width });
  } else {
    for (const path of libs) await page.addScriptTag({ path });
    result = await page.evaluate(async ({ spec, kind, config }) => {
      const logged = [];
      // The fit-x sizing (the chat's too) makes vega-lite say "Dropping fit-y" for a category y axis: not a fault.
      const log = (...m) => { if (!/^Dropping "fit/.test(m.join(' '))) logged.push(m.join(' ')); };
      const logger = { level() { return this; }, info() { return this; }, debug() { return this; },
        warn(...m) { log(...m); return this; }, error(...m) { log(...m); return this; } };
      const refuse = () => Promise.reject(new Error('nothing loads from outside the spec'));
      try {
        const vgSpec = kind === 'vega-lite' ? vegaLite.compile(spec, { config, logger }).spec : spec;
        const view = new vega.View(vega.parse(vgSpec, kind === 'vega' ? config : undefined),
          { renderer: 'none', loader: { load: refuse, sanitize: refuse, http: refuse, file: refuse } });
        view.logger(logger);
        await view.runAsync();
        document.getElementById('stage').innerHTML = await view.toSVG();
        view.finalize();
        return { type: kind, logged };
      } catch (e) { return { error: String(e?.message ?? e) }; }
    }, { spec, kind, config: vegaConfig() });
    warnings.push(...(result.logged ?? []));
  }
} catch (e) {
  await browser.close();
  fail(`render failed: ${e.message.split('\n')[0]}`);
}

if (result.error) {
  await browser.close();
  // mermaid lists every token it expected; the first few say enough.
  const short = result.error.replace(/Expecting (.+), got (.+)/, (_, list, got) => {
    const options = list.split(/,\s*/);
    return `Expecting ${options.slice(0, 4).join(', ')}${options.length > 4 ? ', ...' : ''}; got ${got}`;
  });
  fail(`${kind} does not parse: ${short}`);
}
for (const what of result.removed ?? []) warnings.push(`the chat removes ${what} from an svg block (fine in a file)`);

// Size, smallest text at drawn size, and the files.
const info = await page.evaluate(({ bg, kind }) => {
  const svg = document.querySelector('#stage > svg');
  const rect = svg.getBoundingClientRect();
  const box = svg.viewBox?.baseVal;
  const scale = box?.width > 0 ? rect.width / box.width : 1;
  let smallest = Infinity;
  for (const el of svg.querySelectorAll('text, tspan, textPath')) {
    if (![...el.childNodes].some((n) => n.nodeType === 3 && n.textContent.trim())) continue;
    if (el.getClientRects().length === 0) continue;
    smallest = Math.min(smallest, parseFloat(getComputedStyle(el).fontSize) * scale);
  }
  const file = svg.cloneNode(true);
  if (kind === 'mermaid') {
    // A painted background, so text drawn for a dark theme stays readable in any viewer
    // (vega paints its own from the config; an svg file is kept as it was written).
    const back = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    const [x, y, w, h] = box?.width > 0 ? [box.x, box.y, box.width, box.height] : [0, 0, rect.width, rect.height];
    Object.entries({ x, y, width: w, height: h, class: 'export-background', style: `fill: ${bg}; stroke: none` })
      .forEach(([k, v]) => back.setAttribute(k, v));
    file.insertBefore(back, file.firstChild);
  }
  return { width: Math.round(rect.width), height: Math.round(rect.height),
    smallest: Number.isFinite(smallest) ? Math.round(smallest * 10) / 10 : null,
    svg: new XMLSerializer().serializeToString(file) };
}, { bg: t.bg, kind });

mkdirSync(outDir, { recursive: true });
const svgPath = join(outDir, `${stem}.svg`);
const pngPath = join(outDir, `${stem}.png`);
const written = [];
if (kind !== 'svg') { writeFileSync(svgPath, `<?xml version="1.0" encoding="UTF-8"?>\n${info.svg}\n`); written.push(svgPath); }
else if (resolve(svgPath) !== resolve(input)) { writeFileSync(svgPath, source); written.push(svgPath); }
await page.setViewportSize({ width: Math.max(info.width, 100) + 16, height: Math.max(info.height, 100) + 16 });
await page.locator('#stage > svg').screenshot({ path: pngPath });
written.push(pngPath);
await browser.close();

if (info.smallest !== null && info.smallest < 11) warnings.push(`the smallest text is ${info.smallest} px at drawn size; make it at least 11 px`);
if ((result.nodes ?? 0) > 15) warnings.push(`${result.nodes} nodes: split the diagram (about 15 at most)`);
for (const w of [...new Set(warnings)]) console.error(`warning: ${w}`);
const extra = [result.type !== kind ? result.type : null, result.nodes ? `${result.nodes} nodes` : null,
  info.smallest !== null ? `smallest text ${info.smallest} px` : null].filter(Boolean).join(', ');
console.log(`ok ${kind}${extra ? ` (${extra})` : ''} ${info.width}x${info.height} ${opts.theme} -> ${written.join(', ')}`);
