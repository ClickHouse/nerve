import { describe, expect, it, vi } from 'vitest';
import type { PreviewBundle } from '../../types/preview';
import {
  DOCUMENT_BUDGET_FACTOR, MAX_PARALLEL_READS, PREVIEW_POLICY, PreviewTimedOut, PreviewTooLarge, buildPreviewDocument,
  resolveAddress,
  srcsetCandidates,
} from './document';

/**
 * The document of a preview, made from the files of a bundle. A fake read
 * stands in for the file route: it records each path it is asked for, so a
 * test sees what was requested and what was not.
 */

const encoder = new TextEncoder();

function bundle(files: Record<string, string | Uint8Array>, entry = 'index.html', kind: PreviewBundle['kind'] = 'folder') {
  const types: Record<string, string> = {
    html: 'text/html', css: 'text/css', js: 'text/javascript', png: 'image/png', woff2: 'font/woff2', svg: 'image/svg+xml',
  };
  const bytes = Object.fromEntries(Object.entries(files).map(([p, v]) => [p, typeof v === 'string' ? encoder.encode(v) : v]));
  const description: PreviewBundle = {
    path: 'decks/demo', root: 'decks/demo', root_path: '/ws/decks/demo', kind, entry,
    files: Object.entries(bytes).map(([path, b]) => ({ path, size: b.length, type: types[path.split('.').pop()!] ?? 'application/octet-stream' })),
    size: 0, fingerprint: 'f', modified: 0,
  };
  const asked: string[] = [];
  const read = vi.fn(async (path: string) => {
    asked.push(path);
    const b = bytes[path];
    if (!b) throw new Error(`not in the bundle: ${path}`);
    return b.slice().buffer;
  });
  return { description, read, asked };
}

function parse(html: string): Document {
  return new DOMParser().parseFromString(html, 'text/html');
}

function decodeData(url: string): string {
  const base64 = url.slice(url.indexOf('base64,') + 'base64,'.length);
  return new TextDecoder().decode(Uint8Array.from(atob(base64), c => c.charCodeAt(0)));
}

const LIMIT = 10 * 1024 * 1024;

describe('resolveAddress', () => {
  const files = new Set(['index.html', 'css/a.css', 'img/b c.png']);
  it.each([
    ['css/a.css', '', 'css/a.css'],
    ['./css/a.css?v=2#x', '', 'css/a.css'],
    ['../img/b%20c.png', 'css/', 'img/b c.png'],
    ['a.css', 'css/', 'css/a.css'],
  ])('finds %s from "%s"', (address, folder, path) => {
    expect(resolveAddress(address, folder, files)).toBe(path);
  });

  it.each([
    'https://cdn.example.com/a.css', '//cdn.example.com/a.css', '/css/a.css', '../index.html', 'javascript:alert(1)',
    'missing.css', '', '%E0%A4%A', 'blob:https://example.com/x',
  ])('finds no file for %s', address => {
    expect(resolveAddress(address, '', files)).toBeNull();
  });

  it('keeps a data: address', () => {
    expect(resolveAddress('data:image/png;base64,AAAA', '', files)).toBe('data');
  });
});

describe('buildPreviewDocument', () => {
  it('puts the local styles, scripts, images and fonts into the document', async () => {
    const { description, read } = bundle({
      'index.html': '<!doctype html><html><head><link rel="stylesheet" href="css/deck.css"></head>'
        + '<body><img src="img/chart.png" srcset="img/chart.png 2x"><div style="background:url(img/chart.png)"></div>'
        + '<script src="js/app.js"></script></body></html>',
      'css/deck.css': '@font-face{font-family:x;src:url("../fonts/x.woff2") format("woff2")} body{background:url(../img/chart.png)}',
      'js/app.js': 'window.ran = 1;',
      'img/chart.png': new Uint8Array([0x89, 0x50, 0x4e, 0x47]),
      'fonts/x.woff2': new Uint8Array([1, 2, 3]),
    });
    const result = await buildPreviewDocument(description, read, LIMIT);
    const doc = parse(result.html);

    const link = doc.querySelector('link[rel=stylesheet]')!;
    expect(link.getAttribute('href')).toMatch(/^data:text\/css;charset=utf-8;base64,/);
    const css = decodeData(link.getAttribute('href')!);
    expect(css).toContain('url("data:font/woff2;base64,AQID")');
    expect(css).toMatch(/url\("data:image\/png;base64,[^"]+"\)/);

    expect(doc.querySelector('img')!.getAttribute('src')).toMatch(/^data:image\/png;base64,/);
    expect(doc.querySelector('img')!.getAttribute('srcset')).toMatch(/^data:image\/png;base64,\S+ 2x$/);
    expect(doc.querySelector('div')!.getAttribute('style')).toContain('url("data:image/png;base64,');
    const script = doc.querySelector('script')!;
    expect(decodeData(script.getAttribute('src')!)).toBe('window.ran = 1;');
    expect(result.source).toContain('<link rel="stylesheet" href="css/deck.css">');
    // Each file once.
    expect(read.mock.calls.map(c => c[0]).sort()).toEqual(['css/deck.css', 'fonts/x.woff2', 'img/chart.png', 'index.html', 'js/app.js']);
  });

  it('puts the policy that allows no network first into the head', async () => {
    const { description, read } = bundle({
      'index.html': '<html><head><meta http-equiv="refresh" content="0;url=https://example.com">'
        + '<meta http-equiv="Content-Security-Policy" content="default-src *"><base href="https://example.com/"></head><body>x</body></html>',
    });
    const doc = parse((await buildPreviewDocument(description, read, LIMIT)).html);
    const first = doc.head.firstElementChild!;
    expect(first.getAttribute('charset')).toBe('utf-8');
    const policies = doc.querySelectorAll('meta[http-equiv]');
    expect(policies).toHaveLength(1);
    expect(policies[0].getAttribute('http-equiv')).toBe('Content-Security-Policy');
    expect(policies[0].getAttribute('content')).toBe(PREVIEW_POLICY);
    expect(PREVIEW_POLICY).toContain("connect-src 'none'");
    expect(PREVIEW_POLICY).toContain("default-src 'none'");
    expect(PREVIEW_POLICY).not.toMatch(/https?:|\*/);
    expect(doc.querySelector('base')).toBeNull();
  });

  it('removes remote addresses and addresses outside the bundle, and never asks for them', async () => {
    const { description, read, asked } = bundle({
      'index.html': '<html><head>'
        + '<link rel="stylesheet" href="https://cdn.example.com/x.css">'
        + '<link rel="stylesheet" href="/abs.css">'
        + '<link rel="prerender" href="https://example.com/next">'
        + '<link rel="preconnect" href="https://example.com">'
        + '<link rel="prefetch" href="local.css">'
        + '<link rel="modulepreload" href="app.js">'
        + '<style>@import url("https://cdn.example.com/y.css"); body{background:url(https://example.com/bg.png)}</style>'
        + '</head><body>'
        + '<img src="https://example.com/t.png" srcset="https://example.com/a.png 1x, missing.png 2x">'
        + '<script src="https://cdn.example.com/lib.js">window.fallback = 1</script>'
        + '<script src="../outside.js"></script>'
        + '<video poster="https://example.com/p.png" src="missing.mp4"></video>'
        + '<iframe src="https://example.com/"></iframe><object data="x.swf"></object>'
        + '<a href="https://example.com/" ping="https://example.com/ping">link</a>'
        + '<svg><image href="https://example.com/i.png"></image><use href="#local"></use></svg>'
        + '<template><img src="https://example.com/in-template.png"></template>'
        + '</body></html>',
      'local.css': 'x',
      'app.js': 'x',
    });
    const html = (await buildPreviewDocument(description, read, LIMIT)).html;
    expect(asked).toEqual(['index.html']);
    const doc = parse(html);
    expect(doc.querySelectorAll('link')).toHaveLength(0);
    expect(doc.querySelectorAll('script')).toHaveLength(0);
    expect(html).not.toContain('window.fallback');
    const img = doc.querySelector('body > img')!;
    expect(img.hasAttribute('src')).toBe(false);
    expect(img.hasAttribute('srcset')).toBe(false);
    const video = doc.querySelector('video')!;
    expect(video.hasAttribute('poster')).toBe(false);
    expect(video.hasAttribute('src')).toBe(false);
    expect(doc.querySelector('iframe')!.hasAttribute('src')).toBe(false);
    expect(doc.querySelector('object')!.hasAttribute('data')).toBe(false);
    expect(doc.querySelector('a')!.hasAttribute('ping')).toBe(false);
    expect(doc.querySelector('svg image')!.hasAttribute('href')).toBe(false);
    expect(doc.querySelector('svg use')!.getAttribute('href')).toBe('#local');
    const template = doc.querySelector('template') as HTMLTemplateElement;
    expect(template.content.querySelector('img')!.hasAttribute('src')).toBe(false);
    // No address of another host is left in a resource, only the link of the anchor.
    expect(html.replace('href="https://example.com/"', '')).not.toMatch(/https?:\/\//);
  });

  it('keeps a script with </script> text in its file from breaking the document', async () => {
    const code = 'window.text = "</script><img src=x onerror=alert(1)>"; // </SCRIPT>';
    const { description, read } = bundle({
      'index.html': '<html><body><script src="app.js"></script><p id="after">after</p></body></html>',
      'app.js': code,
    });
    const doc = parse((await buildPreviewDocument(description, read, LIMIT)).html);
    expect(doc.querySelectorAll('script')).toHaveLength(1);
    expect(doc.querySelectorAll('img')).toHaveLength(0);
    expect(doc.getElementById('after')!.textContent).toBe('after');
    expect(decodeData(doc.querySelector('script')!.getAttribute('src')!)).toBe(code);
  });

  it('keeps a stylesheet with </style> text from breaking the document', async () => {
    const { description, read } = bundle({
      'index.html': '<html><head><link rel="stylesheet" href="a.css"></head><body><p id="after">after</p></body></html>',
      'a.css': 'p::after{content:"</style><script>alert(1)</script>"}',
    });
    const doc = parse((await buildPreviewDocument(description, read, LIMIT)).html);
    expect(doc.querySelectorAll('script')).toHaveLength(0);
    expect(doc.getElementById('after')).not.toBeNull();
  });

  it('inlines a stylesheet that imports another, and stops at a loop', async () => {
    const { description, read } = bundle({
      'index.html': '<html><head><link rel="stylesheet" href="a.css"></head><body></body></html>',
      'a.css': '@import "b.css"; a{color:red}',
      'b.css': '@import url(a.css); b{color:blue}',
    });
    const doc = parse((await buildPreviewDocument(description, read, LIMIT)).html);
    const a = decodeData(doc.querySelector('link')!.getAttribute('href')!);
    expect(a).toMatch(/^@import url\("data:text\/css;charset=utf-8;base64,[^"]+"\); a\{color:red\}$/);
    const b = decodeData(a.slice(a.indexOf('data:'), a.indexOf('")')));
    expect(b).toContain('b{color:blue}');
  });

  it('stops before it reads past the limit, with the sizes', async () => {
    const big = new Uint8Array(600);
    const { description, read, asked } = bundle({
      'index.html': '<html><body><img src="a.png"><img src="b.png"></body></html>',
      'a.png': big, 'b.png': big,
    });
    const limit = 1000;
    const error = await buildPreviewDocument(description, read, limit).catch(e => e);
    expect(error).toBeInstanceOf(PreviewTooLarge);
    expect((error as PreviewTooLarge).limit).toBe(limit);
    expect(asked.length).toBeLessThan(3);
  });

  it('counts only the files that the document uses', async () => {
    const { description, read } = bundle({
      'index.html': '<html><body>small</body></html>',
      'unused.png': new Uint8Array(5000),
    });
    const result = await buildPreviewDocument(description, read, 1000);
    expect(result.bytes).toBe(description.files.find(f => f.path === 'index.html')!.size);
  });

  it('shows an image bundle as the image', async () => {
    const { description, read } = bundle({ 'logo.svg': '<svg xmlns="http://www.w3.org/2000/svg"></svg>' }, 'logo.svg', 'image');
    const result = await buildPreviewDocument(description, read, LIMIT);
    expect(result.source).toBeNull();
    const img = parse(result.html).querySelector('img')!;
    expect(img.getAttribute('src')).toMatch(/^data:image\/svg\+xml;charset=utf-8;base64,/);
    expect(img.getAttribute('alt')).toBe('logo.svg');
  });

  it('rewrites the strings of image-set() and -webkit-image-set(), local and remote', async () => {
    const { description, read, asked } = bundle({
      'index.html': '<html><head><link rel="stylesheet" href="a.css"></head>'
        + '<body><div style="background-image:-webkit-image-set(\'img/b.png\' 1x, \'https://example.com/y.png\' 2x)"></div></body></html>',
      'a.css': 'h1{background-image:image-set("img/b.png" 1x, "https://example.com/x.png" 2x, '
        + 'url(img/b.png) 3x, "img/b.png" type("image/png"))}',
      'img/b.png': new Uint8Array([7, 7]),
    });
    const doc = parse((await buildPreviewDocument(description, read, LIMIT)).html);
    const css = decodeData(doc.querySelector('link')!.getAttribute('href')!);
    expect(css).not.toContain('example.com');
    expect(css).not.toContain('"img/b.png"');
    expect(css.match(/data:image\/png;base64,Bwc=/g)).toHaveLength(3);
    expect(css).toContain('"data:," 2x');
    expect(css).toContain('type("image/png")');
    const style = doc.querySelector('div')!.getAttribute('style')!;
    expect(style).toMatch(/^background-image:-webkit-image-set\("data:image\/png;base64,Bwc=" 1x, "data:," 2x\)$/);
    expect(asked.filter(p => p === 'img/b.png')).toHaveLength(1);
  });

  it('reads at most a few files at the same time', async () => {
    const names = Array.from({ length: 12 }, (_, i) => `img/${i}.png`);
    const files: Record<string, string | Uint8Array> = {
      'index.html': `<html><body>${names.map(n => `<img src="${n}">`).join('')}</body></html>`,
    };
    for (const n of names) files[n] = new Uint8Array([1]);
    const { description } = bundle(files);
    let running = 0;
    let most = 0;
    const read = async (path: string) => {
      running += 1;
      most = Math.max(most, running);
      await new Promise(r => setTimeout(r, 5));
      running -= 1;
      return (files[path] as Uint8Array | string) instanceof Uint8Array
        ? (files[path] as Uint8Array).slice().buffer
        : new TextEncoder().encode(files[path] as string).buffer;
    };
    const doc = parse((await buildPreviewDocument(description, read, LIMIT)).html);
    expect(doc.querySelectorAll('img[src^="data:image/png"]')).toHaveLength(12);
    expect(most).toBe(MAX_PARALLEL_READS);
  });

  it('counts the bytes that arrived, not the described size', async () => {
    const { description } = bundle({
      'index.html': '<html><body><img src="a.png"></body></html>',
      'a.png': new Uint8Array(10),
    });
    // The file grew after the description: the read hands back much more.
    const read = async (path: string) => (path === 'a.png'
      ? new Uint8Array(5000).buffer
      : new TextEncoder().encode('<html><body><img src="a.png"></body></html>').buffer);
    const error = await buildPreviewDocument(description, read, 1000).catch(e => e);
    expect(error).toBeInstanceOf(PreviewTooLarge);
    expect((error as PreviewTooLarge).bytes).toBeGreaterThan(5000);
  });

  it('stops a build that would repeat one large file many times, before it makes the document', async () => {
    const image = new Uint8Array(3000);
    const slides = Array.from({ length: 50 }, () => '<img src="big.png">').join('');
    const { description, read, asked } = bundle({ 'index.html': `<html><body>${slides}</body></html>`, 'big.png': image });
    const limit = 10_000;
    const error = await buildPreviewDocument(description, read, limit).catch(e => e);
    expect(error).toBeInstanceOf(PreviewTooLarge);
    expect((error as PreviewTooLarge).what).toBe('document');
    expect((error as PreviewTooLarge).limit).toBe(limit * DOCUMENT_BUDGET_FACTOR);
    // The file was read once.
    expect(asked.filter(p => p === 'big.png')).toHaveLength(1);
  });

  it('makes the data address of a file once and uses it for each reference within the budget', async () => {
    const { description, read, asked } = bundle({
      'index.html': `<html><body>${'<img src="a.png">'.repeat(5)}</body></html>`,
      'a.png': new Uint8Array([1, 2, 3]),
    });
    const doc = parse((await buildPreviewDocument(description, read, LIMIT)).html);
    const sources = Array.from(doc.querySelectorAll('img')).map(img => img.getAttribute('src'));
    expect(new Set(sources)).toEqual(new Set(['data:image/png;base64,AQID']));
    expect(asked.filter(p => p === 'a.png')).toHaveLength(1);
  });

  it('reads each stylesheet once, however the imports branch', async () => {
    // Each level imports the next one twice: without a cache 2^8 expansions.
    const files: Record<string, string | Uint8Array> = {
      'index.html': '<html><head><link rel="stylesheet" href="s0.css"></head><body></body></html>',
    };
    for (let i = 0; i < 8; i += 1) files[`s${i}.css`] = `@import "s${i + 1}.css"; @import url(s${i + 1}.css); .l${i}{color:red}`;
    files['s8.css'] = '.end{color:blue}';
    const { description } = bundle(files);
    const reads: string[] = [];
    const read = async (path: string) => {
      reads.push(path);
      const v = files[path];
      return (typeof v === 'string' ? new TextEncoder().encode(v) : v).slice().buffer;
    };
    const result = await buildPreviewDocument(description, read, LIMIT);
    expect(reads.filter(p => p.endsWith('.css')).sort()).toEqual(Object.keys(files).filter(p => p.endsWith('.css')).sort());
    // What the nesting repeats in the text is
    // counted in the document budget (next test).
    expect(result.html.length).toBeLessThan(LIMIT * DOCUMENT_BUDGET_FACTOR);
  });

  it('counts branching imports in the document budget', async () => {
    const files: Record<string, string | Uint8Array> = {
      'index.html': '<html><head><link rel="stylesheet" href="s0.css"></head><body></body></html>',
      'big.png': new Uint8Array(2000),
    };
    for (let i = 0; i < 6; i += 1) files[`s${i}.css`] = `@import "s${i + 1}.css"; @import "s${i + 1}.css"; a{background:url(big.png)}`;
    files['s6.css'] = 'b{background:url(big.png)}';
    const { description, read } = bundle(files);
    const error = await buildPreviewDocument(description, read, 5000).catch(e => e);
    expect(error).toBeInstanceOf(PreviewTooLarge);
    expect((error as PreviewTooLarge).what).toBe('document');
  });

  it('keeps a data address in srcset whole', async () => {
    const { description, read } = bundle({
      'index.html': '<html><body><img srcset="data:image/png;base64,AAAA 1x, a.png 2x, https://example.com/c.png 3x"></body></html>',
      'a.png': new Uint8Array([1, 2, 3]),
    });
    const doc = parse((await buildPreviewDocument(description, read, LIMIT)).html);
    expect(doc.querySelector('img')!.getAttribute('srcset')).toBe('data:image/png;base64,AAAA 1x, data:image/png;base64,AQID 2x');
  });
});

describe('srcsetCandidates', () => {
  it.each([
    ['a.png 1x, b.png 2x', [['a.png', '1x'], ['b.png', '2x']]],
    ['data:image/png;base64,AA,BB 1x,c.png', [['data:image/png;base64,AA,BB', '1x'], ['c.png', '']]],
    ['a.png, b.png 100w', [['a.png', ''], ['b.png', '100w']]],
    ['  ,, a.png  ', [['a.png', '']]],
    ['', []],
  ])('reads %j', (value, expected) => {
    expect(srcsetCandidates(value).map(c => [c.address, c.descriptor])).toEqual(expected);
  });
});

/**
 * Shapes of a page against the three properties every build must keep: it
 * ends (resolves or rejects, within a short time), it stays within the
 * document budget or is refused with the right kind of "too large", and it
 * reads each file at most once.
 */
describe('the builder over shapes of pages', () => {
  const SMALL_LIMIT = 100_000;
  const ENDS_WITHIN_MS = 5_000;
  type Expect = 'ok' | 'files' | 'document';
  type Shape = { name: string; files: Record<string, string | Uint8Array>; expect: Expect; check?: (html: string, reads: Map<string, number>) => void };

  const page = (head: string, body = '') => `<!doctype html><html><head>${head}</head><body>${body}</body></html>`;
  const link = (href: string) => `<link rel="stylesheet" href="${href}">`;
  const bytes = (n: number) => new Uint8Array(n);

  /** The stylesheet links of a document, also inside templates at any depth, and how many have a data: address. */
  function stylesheetLinks(html: string): { total: number; inlined: number } {
    const links: Element[] = [];
    const walk = (root: ParentNode) => {
      links.push(...Array.from(root.querySelectorAll('link[rel=stylesheet]')));
      for (const t of Array.from(root.querySelectorAll('template'))) walk((t as HTMLTemplateElement).content);
    };
    walk(parse(html));
    return {
      total: links.length,
      inlined: links.filter(l => (l.getAttribute('href') ?? '').startsWith('data:text/css')).length,
    };
  }

  function chain(length: number): Record<string, string> {
    const sheets: Record<string, string> = {};
    for (let i = 0; i < length; i += 1) sheets[`s${i}.css`] = `@import "s${i + 1}.css"; .l${i}{color:red}`;
    sheets[`s${length}.css`] = '.end{color:blue}';
    return sheets;
  }

  const many: Record<string, string | Uint8Array> = {};
  for (let i = 0; i < 999; i += 1) many[`img/${i}.png`] = bytes(10);
  many['index.html'] = page('', Object.keys(many).map(p => `<img src="${p}">`).join(''));

  const shapes: Shape[] = [
    { name: 'one sheet', expect: 'ok', files: { 'index.html': page(link('a.css')), 'a.css': 'b{background:url(i.png)}', 'i.png': bytes(100) } },
    {
      name: 'several sheets', expect: 'ok',
      files: {
        'index.html': page(['a', 'b', 'c', 'd', 'e'].map(n => link(`${n}.css`)).join('')),
        ...Object.fromEntries(['a', 'b', 'c', 'd', 'e'].map(n => [`${n}.css`, `.${n}{background:url(i.png)}`])),
        'i.png': bytes(100),
      },
    },
    {
      name: 'shared images', expect: 'ok',
      files: {
        'index.html': page('', Array.from({ length: 30 }, (_, i) => `<img src="${['x', 'y', 'z'][i % 3]}.png">`).join('')),
        'x.png': bytes(50), 'y.png': bytes(50), 'z.png': bytes(50),
      },
    },
    { name: 'a cycle of 1', expect: 'ok', files: { 'index.html': page(link('a.css')), 'a.css': '@import "a.css"; a{color:red}' } },
    {
      name: 'a cycle of 2, both linked', expect: 'ok',
      files: { 'index.html': page(link('a.css') + link('b.css')), 'a.css': '@import "b.css"; a{}', 'b.css': '@import "a.css"; b{}' },
      check: html => expect(parse(html).querySelectorAll('link[rel=stylesheet]').length).toBeGreaterThanOrEqual(1),
    },
    {
      name: 'a cycle of 3, all linked', expect: 'ok',
      files: {
        'index.html': page(link('a.css') + link('b.css') + link('c.css')),
        'a.css': '@import "b.css"; a{}', 'b.css': '@import "c.css"; b{}', 'c.css': '@import "a.css"; c{}',
      },
    },
    {
      name: 'one sheet linked twice and imported once', expect: 'ok',
      files: { 'index.html': page(link('a.css') + link('a.css') + link('b.css')), 'a.css': 'a{color:red}', 'b.css': '@import "a.css"; b{}' },
      check: html => expect(parse(html).querySelectorAll('link[rel=stylesheet]')).toHaveLength(3),
    },
    {
      name: 'an import chain at the depth limit', expect: 'ok',
      files: { 'index.html': page(link('s0.css')), ...chain(8) },
      check: (_html, reads) => expect(reads.get('s8.css')).toBe(1),
    },
    {
      name: 'an import chain one past the depth limit', expect: 'ok',
      files: { 'index.html': page(link('s0.css')), ...chain(9) },
      check: (_html, reads) => expect(reads.get('s9.css')).toBeUndefined(),
    },
    {
      name: 'one large file used many times', expect: 'document',
      files: { 'index.html': page('', '<img src="big.png">'.repeat(50)), 'big.png': bytes(30_000) },
    },
    { name: 'many small files at the count limit', expect: 'ok', files: many },
    {
      name: 'an entry near the size limit, full of escaped characters', expect: 'document',
      files: { 'index.html': page('', `<p>${'&'.repeat(95_000)}</p>`) },
    },
    {
      name: 'one sheet linked from several templates', expect: 'ok',
      files: { 'index.html': page('', '<template><link rel="stylesheet" href="w.css"><p>t</p></template>'.repeat(5)), 'w.css': 'p{color:red}' },
      check: html => expect(stylesheetLinks(html)).toEqual({ total: 5, inlined: 5 }),
    },
    {
      name: 'a sheet linked from the page and from a template', expect: 'ok',
      files: { 'index.html': page(link('w.css'), `<template>${link('w.css')}</template>`), 'w.css': 'p{color:red}' },
      check: html => expect(stylesheetLinks(html)).toEqual({ total: 2, inlined: 2 }),
    },
    {
      name: 'nested templates', expect: 'ok',
      files: {
        'index.html': page('', `<template>${link('w.css')}<template>${link('w.css')}<template>${link('w.css')}</template></template></template>`),
        'w.css': 'p{color:red}',
      },
      check: html => expect(stylesheetLinks(html)).toEqual({ total: 3, inlined: 3 }),
    },
    {
      name: 'a cycle inside a template', expect: 'ok',
      files: {
        'index.html': page('', `<template>${link('a.css')}${link('b.css')}</template><template>${link('a.css')}</template>`),
        'a.css': '@import "b.css"; a{}', 'b.css': '@import "a.css"; b{}',
      },
      check: html => expect(stylesheetLinks(html)).toEqual({ total: 3, inlined: 3 }),
    },
    { name: 'files past the limit', expect: 'files', files: { 'index.html': page('', '<img src="a.png"><img src="b.png">'), 'a.png': bytes(60_000), 'b.png': bytes(60_000) } },
  ];

  it.each(shapes.map(s => [s.name, s] as const))('%s', async (_name, shape) => {
    const { description } = bundle(shape.files);
    const reads = new Map<string, number>();
    const read = async (path: string) => {
      reads.set(path, (reads.get(path) ?? 0) + 1);
      const v = shape.files[path];
      return (typeof v === 'string' ? new TextEncoder().encode(v) : v).slice().buffer;
    };
    const hung = new Promise<'hung'>(resolve => setTimeout(() => resolve('hung'), ENDS_WITHIN_MS));
    const outcome = await Promise.race([
      buildPreviewDocument(description, read, SMALL_LIMIT).then(r => r, e => e as Error),
      hung,
    ]);
    expect(outcome, 'the build ends').not.toBe('hung');
    for (const [path, count] of reads) expect(count, `reads of ${path}`).toBe(1);
    if (shape.expect === 'ok') {
      expect(outcome).not.toBeInstanceOf(Error);
      const html = (outcome as { html: string }).html;
      expect(html.length).toBeLessThanOrEqual(SMALL_LIMIT * DOCUMENT_BUDGET_FACTOR);
      shape.check?.(html, reads);
    } else {
      expect(outcome).toBeInstanceOf(PreviewTooLarge);
      expect((outcome as PreviewTooLarge).what).toBe(shape.expect);
    }
  });

  it('ends a build that never gets its files, at the time limit', async () => {
    const { description } = bundle({ 'index.html': page('', '<img src="a.png">'), 'a.png': bytes(1) });
    const never = () => new Promise<ArrayBuffer>(() => {});
    const error = await buildPreviewDocument(description, never, SMALL_LIMIT, 50).catch(e => e);
    expect(error).toBeInstanceOf(PreviewTimedOut);
  });
});
