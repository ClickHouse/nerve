import { describe, expect, it } from 'vitest';
import { mountSvg, naturalSizeOf, sanitizeSvg } from './svg';

const wrap = (inner: string) =>
  `<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" viewBox="0 0 10 10">${inner}</svg>`;

describe('sanitizeSvg', () => {
  it('keeps the drawing and drops script, handlers and foreign content', () => {
    const clean = sanitizeSvg(wrap(
      '<script>alert(1)</script>'
      + '<circle cx="5" cy="5" r="4" fill="red" onclick="alert(1)" onload="alert(2)"/>'
      + '<foreignObject width="10" height="10"><div>html</div></foreignObject>'
      + '<text x="1" y="1">label</text>',
    ));
    expect(clean).toContain('<circle');
    expect(clean).toContain('fill="red"');
    expect(clean).toContain('label');
    expect(clean).not.toContain('script');
    expect(clean).not.toContain('onclick');
    expect(clean).not.toContain('onload');
    expect(clean).not.toContain('foreignObject');
    expect(clean).not.toContain('html');
  });

  it('keeps references inside the picture and drops every other URL', () => {
    const clean = sanitizeSvg(wrap(
      '<defs><linearGradient id="g"><stop offset="0"/></linearGradient>'
      + '<symbol id="s"><rect width="1" height="1"/></symbol></defs>'
      + '<rect width="10" height="10" fill="url(#g)" style="fill: url(#g); stroke: red"/>'
      + '<use href="#s"/>'
      + '<use href="https://example.invalid/sprite.svg#s"/>'
      + '<a href="https://example.invalid/?q=secret"><text>link</text></a>'
      + '<image href="https://example.invalid/track.png" width="1" height="1"/>'
      + '<filter id="f"><feImage href="https://example.invalid/f.png"/></filter>',
    ));
    expect(clean).toContain('fill="url(#g)"');
    expect(clean).toContain('style="fill: url(#g); stroke: red"');
    expect(clean).toContain('<use href="#s"');
    expect(clean).toContain('link');
    expect(clean).not.toContain('example.invalid');
    expect(clean).not.toContain('<image');
    expect(clean).not.toContain('feImage');
  });

  it('drops styles that would make the browser fetch', () => {
    const clean = sanitizeSvg(wrap(
      '<style>@import url("https://example.invalid/x.css"); rect { fill: blue }</style>'
      + '<style>text { font-weight: bold }</style>'
      + '<rect width="10" height="10" style="background: url(https://example.invalid/bg.png)"/>'
      + '<rect width="10" height="10" style="cursor: url(\'https://example.invalid/c.cur\'), pointer"/>'
      + '<text style="fill: green">ok</text>',
    ));
    expect(clean).not.toContain('example.invalid');
    expect(clean).not.toContain('@import');
    expect(clean).toContain('font-weight: bold');
    expect(clean).toContain('style="fill: green"');
    expect((clean.match(/<rect/g) ?? []).length).toBe(2);
    expect(clean).not.toContain('background');
    expect(clean).not.toContain('cursor');
  });

  it('drops styles that spell url() with CSS escapes', () => {
    const clean = sanitizeSvg(wrap(
      '<rect width="10" height="10" style="background:\\75 rl(https://example.invalid/a.png)"/>'
      + '<rect width="10" height="10" style="fill: \\u\\r\\l(https://example.invalid/b.svg#p)"/>'
      + '<style>rect { cursor: \\75\\72\\6c(https://example.invalid/c.cur) }</style>'
      + '<text style="fill: green">ok</text>',
    ));
    expect(clean).not.toContain('example.invalid');
    expect(clean).not.toContain('\\75');
    expect(clean).toContain('style="fill: green"');
    expect((clean.match(/<rect/g) ?? []).length).toBe(2);
  });

  it('refuses a block that is not an svg', () => {
    expect(() => sanitizeSvg('<div>not a picture</div>')).toThrow(/no <svg>/);
    expect(() => sanitizeSvg('plain text')).toThrow(/no <svg>/);
  });

  it('accepts an XML declaration in front of the root', () => {
    const clean = sanitizeSvg(`<?xml version="1.0" encoding="UTF-8"?>\n${wrap('<rect width="1" height="1"/>')}`);
    expect(clean).toMatch(/^<svg/);
    expect(clean).toContain('<rect');
  });
});

/** An <svg> element with `attributes`, parsed the way the page parses one. */
function svgWith(attributes: string): SVGSVGElement {
  const host = document.createElement('div');
  host.innerHTML = `<svg ${attributes}><rect width="1" height="1"/></svg>`;
  return host.querySelector('svg')!;
}

describe('naturalSizeOf', () => {
  it('reads width and height, in any absolute unit', () => {
    expect(naturalSizeOf(svgWith('width="400" height="200"'))).toEqual({ width: 400, height: 200 });
    expect(naturalSizeOf(svgWith('width="400px" height="200px" viewBox="0 0 10 10"'))).toEqual({ width: 400, height: 200 });
    expect(naturalSizeOf(svgWith('width="72pt" height="1in"'))).toEqual({ width: 96, height: 96 });
    expect(naturalSizeOf(svgWith('width="2em" height="25.4mm"'))).toEqual({ width: 32, height: 96 });
  });

  it('falls back to the viewBox, alone or for the missing side', () => {
    expect(naturalSizeOf(svgWith('viewBox="0 0 800 400"'))).toEqual({ width: 800, height: 400 });
    expect(naturalSizeOf(svgWith('viewBox="-10,-10,800,400"'))).toEqual({ width: 800, height: 400 });
    expect(naturalSizeOf(svgWith('width="400" viewBox="0 0 800 400"'))).toEqual({ width: 400, height: 200 });
    expect(naturalSizeOf(svgWith('height="100" viewBox="0 0 800 400"'))).toEqual({ width: 200, height: 100 });
  });

  it('takes a percentage as no size at all', () => {
    expect(naturalSizeOf(svgWith('width="100%" viewBox="0 0 800 400"'))).toEqual({ width: 800, height: 400 });
    expect(naturalSizeOf(svgWith('width="100%" height="50%" viewBox="0 0 640 480"'))).toEqual({ width: 640, height: 480 });
    expect(naturalSizeOf(svgWith('width="50vw" height="300" viewBox="0 0 400 300"'))).toEqual({ width: 400, height: 300 });
    expect(naturalSizeOf(svgWith('width="100%"'))).toBeUndefined();
    expect(naturalSizeOf(svgWith(''))).toBeUndefined();
    expect(naturalSizeOf(svgWith('viewBox="0 0 0 10"'))).toBeUndefined();
  });
});

describe('mountSvg', () => {
  it('states the natural size on the picture and in the file', async () => {
    const stage = document.createElement('div');
    const mounted = await mountSvg(stage, '<svg width="100%" viewBox="0 0 800 400"><rect width="800" height="400" fill="#0af"/></svg>', { theme: 'dark', width: 600 });
    expect(mounted.naturalSize).toEqual({ width: 800, height: 400 });
    const shown = stage.querySelector('svg')!;
    expect(shown.getAttribute('width')).toBe('800');
    expect(shown.getAttribute('height')).toBe('400');

    // The frame sizes the picture on screen; the file keeps the natural size.
    shown.style.width = '300px';
    shown.setAttribute('width', '300');
    const file = (await mounted.toSvg!())!;
    const doc = new DOMParser().parseFromString(file, 'image/svg+xml');
    expect(doc.querySelector('parsererror')).toBeNull();
    expect(doc.documentElement.namespaceURI).toBe('http://www.w3.org/2000/svg');
    expect(doc.documentElement.getAttribute('width')).toBe('800');
    expect(doc.documentElement.getAttribute('height')).toBe('400');
    expect(file).not.toContain('300px');

    mounted.cleanup();
    expect(stage.innerHTML).toBe('');
  });

  it('writes a file an XML reader accepts, still sanitized', async () => {
    const stage = document.createElement('div');
    const mounted = await mountSvg(
      stage,
      '<svg viewBox="0 0 10 10"><script>alert(1)</script><use xlink:href="#s"/><symbol id="s"/><text>a&nbsp;b &amp; c</text></svg>',
      { theme: 'light', width: 600 },
    );
    const file = (await mounted.toSvg!())!;
    const doc = new DOMParser().parseFromString(file, 'image/svg+xml');
    expect(doc.querySelector('parsererror')).toBeNull();
    expect(file).not.toContain('script');
    expect(file).not.toContain('&nbsp;');
    expect(doc.querySelector('text')?.textContent).toBe('a b & c');
    // The xlink prefix is declared wherever it is used.
    expect(doc.querySelector('use')?.getAttributeNS('http://www.w3.org/1999/xlink', 'href')).toBe('#s');
    expect(doc.documentElement.getAttribute('width')).toBe('10');
  });

  it('lets a small viewBox-only drawing take the column, and keeps its own size in the file', async () => {
    const stage = document.createElement('div');
    const mounted = await mountSvg(stage, '<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/></svg>', { theme: 'dark', width: 600 });
    expect(mounted.naturalSize).toBeUndefined();
    const shown = stage.querySelector('svg')!;
    expect(shown.hasAttribute('width')).toBe(false);
    expect(shown.style.width).toBe('100%');
    const file = (await mounted.toSvg!())!;
    const doc = new DOMParser().parseFromString(file, 'image/svg+xml');
    expect(doc.documentElement.getAttribute('width')).toBe('24');
    expect(doc.documentElement.getAttribute('height')).toBe('24');
  });

  it('leaves a picture with no size to the frame', async () => {
    const stage = document.createElement('div');
    const mounted = await mountSvg(stage, '<svg><circle r="4"/></svg>', { theme: 'dark', width: 600 });
    expect(mounted.naturalSize).toBeUndefined();
    expect(stage.querySelector('svg')!.hasAttribute('width')).toBe(false);
    expect(await mounted.toSvg!()).toMatch(/^<svg xmlns="http:\/\/www\.w3\.org\/2000\/svg"/);
  });
});
