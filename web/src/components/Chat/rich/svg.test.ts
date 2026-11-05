import { describe, expect, it } from 'vitest';
import { sanitizeSvg } from './svg';

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
