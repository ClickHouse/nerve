import type { PreviewBundle } from '../../types/preview';

/**
 * The document of a preview: one HTML document, made in the browser from the
 * files of a bundle, that the sandbox frame shows.
 *
 * The entry page is parsed with the browser's own parser (`DOMParser`). A
 * document made that way has no browsing context: it runs no script. Then
 * each address of a resource in it (a script, a stylesheet, an image, a
 * font, an address in CSS) is looked up in the bundle:
 *
 * - a file of the bundle: its bytes go into the document, as a `data:`
 *   address. A stylesheet gets the same for the addresses in it (`url()`,
 *   `@import`, the strings of `image-set()`).
 * - anything else (an address of another host, an absolute path, a file
 *   that is not in the bundle): the address is removed, where this code
 *   knows the construct. For every other construct (a CSS address in a
 *   form not listed above, an address that a script makes) the policy of
 *   the frame allows no request. So nothing is ever asked of such an
 *   address, by the server or by the page.
 *
 * Parsing and building load nothing in the panel: measured in Chromium for
 * every resource element and attribute, `<link>` of each kind, CSS
 * `url()` and `@import`, `<base>` and `<meta http-equiv=refresh>`.
 *
 * Links that load a page in the background (`prefetch`, `prerender`,
 * `preconnect` and the like), `<base>`, and `<meta http-equiv>` are removed.
 * A policy that allows no network goes first into the head, on top of the
 * one that the sandbox document sends.
 *
 * Scripts and stylesheets go in as `data:` addresses, not as text: no text
 * of a file can end the element it would be put into (a `</script>` in a
 * script file stays data).
 *
 * The frame does the separation, not this: what the page does at run time
 * (a script that adds an image of another host) meets the policy of the
 * frame and the frame's lack of an origin.
 */

/**
 * The policy of a preview's document, on top of the one in the header of
 * the sandbox document. Inline scripts and styles, and `data:` and `blob:`
 * files. No address of any host.
 */
export const PREVIEW_POLICY = [
  "default-src 'none'",
  "script-src 'unsafe-inline' 'unsafe-eval' data: blob:",
  "style-src 'unsafe-inline' data:",
  'img-src data: blob:',
  'media-src data: blob:',
  'font-src data:',
  "connect-src 'none'",
  'worker-src blob:',
  "frame-src 'none'",
  "object-src 'none'",
  "base-uri 'none'",
  "form-action 'none'",
].join('; ');

/**
 * The preview is larger than a limit: the files it reads (`files`, the
 * source limit), or the text that the build makes of them (`document`, see
 * DOCUMENT_BUDGET_FACTOR).
 */
export class PreviewTooLarge extends Error {
  readonly bytes: number;
  readonly limit: number;
  readonly what: 'files' | 'document';

  constructor(bytes: number, limit: number, what: 'files' | 'document' = 'files') {
    super(what === 'files'
      ? `The files of this preview are larger than ${formatMiB(limit)}.`
      : `The document of this preview is larger than ${formatMiB(limit)}.`);
    this.bytes = bytes;
    this.limit = limit;
    this.what = what;
  }
}

/**
 * The text that one build may make, as a multiple of the source limit
 * (characters: each one is one byte of base64 or ASCII markup). It counts
 * the entry text and every address and stylesheet put into the document,
 * once for each use. A file goes into the document as base64, 4/3 of its
 * size; a file inside a stylesheet counts again inside the stylesheet's own
 * base64 address. So a bundle at the source limit whose bytes are all used
 * once, through one stylesheet, needs about 3.1 times the limit. With 4, a
 * build stops before the panel makes and posts a document of more than
 * about 40 MiB of text, whatever the page repeats.
 */
export const DOCUMENT_BUDGET_FACTOR = 4;

/** A size in MiB, for a person. */
export function formatMiB(bytes: number): string {
  return `${Math.round((bytes / (1024 * 1024)) * 10) / 10} MiB`;
}

/** Reads the bytes of one file of the bundle (a path relative to its root). */
export type ReadBundleFile = (path: string) => Promise<ArrayBuffer>;

export interface PreviewDocument {
  /** The document for the frame. */
  html: string;
  /** The text of the entry file, for the source view; null for an image. */
  source: string | null;
  /** The bytes of the files that went into the document. */
  bytes: number;
}

/** How deep stylesheets may import each other. Each stylesheet is expanded once (a cache), whatever the depth. */
const MAX_IMPORT_DEPTH = 8;
/** How many files of the bundle are read at the same time. */
export const MAX_PARALLEL_READS = 4;

/** The `rel` values of a `<link>` that stay (with their address in the bundle). */
const KEPT_LINKS = new Set(['stylesheet', 'icon', 'shortcut', 'apple-touch-icon', 'mask-icon']);

/** Attributes of a resource address, for the elements where they load something. */
const SOURCE_ELEMENTS = 'img[src],source[src],video[src],audio[src],track[src],input[src],script[src]';
/** Elements whose content the preview never loads: the policy refuses them anyway. */
const FRAME_ELEMENTS = 'iframe,frame,embed,object,portal';

const SCHEME = /^[a-z][a-z0-9+.-]*:/i;

const decoder = new TextDecoder('utf-8');
const encoder = new TextEncoder();

/** Base64 of bytes, in pieces: `String.fromCharCode` takes only so many arguments. */
function base64(bytes: Uint8Array): string {
  let binary = '';
  const step = 0x8000;
  for (let i = 0; i < bytes.length; i += step) {
    binary += String.fromCharCode(...bytes.subarray(i, i + step));
  }
  return btoa(binary);
}

function dataUrl(type: string, bytes: Uint8Array): string {
  const text = type.startsWith('text/') || type === 'image/svg+xml' ? `${type};charset=utf-8` : type;
  return `data:${text};base64,${base64(bytes)}`;
}

/** The folder of a bundle path, with a trailing `/` (`''` at the root). */
function folderOf(path: string): string {
  const slash = path.lastIndexOf('/');
  return slash < 0 ? '' : path.slice(0, slash + 1);
}

/**
 * The bundle path that `address` names from a file in `folder`, `'data'`
 * for a `data:` address (it carries its own bytes), or null for every
 * address that is not a file of the bundle.
 */
export function resolveAddress(address: string, folder: string, files: ReadonlySet<string>): string | 'data' | null {
  const value = address.trim();
  if (!value) return null;
  if (/^data:/i.test(value)) return 'data';
  // Another host, another scheme, or a path from the root of a host.
  if (SCHEME.test(value) || value.startsWith('/') || value.startsWith('\\')) return null;
  const bare = value.split('#')[0].split('?')[0];
  if (!bare) return null;
  let decoded: string;
  try {
    decoded = decodeURIComponent(bare);
  } catch {
    return null;
  }
  const names: string[] = [];
  for (const name of `${folder}${decoded}`.split('/')) {
    if (name === '' || name === '.') continue;
    if (name === '..') {
      if (names.length === 0) return null;
      names.pop();
    } else {
      names.push(name);
    }
  }
  const path = names.join('/');
  return files.has(path) ? path : null;
}

/** The files of a bundle that a document takes, each read once, within the limit. */
class Files {
  private readonly bundle: PreviewBundle;
  private readonly read: ReadBundleFile;
  private readonly limit: number;
  private readonly sizes = new Map<string, number>();
  private readonly types = new Map<string, string>();
  private readonly loaded = new Map<string, Promise<Uint8Array>>();
  /** The `data:` address of each file, made once. */
  private readonly addresses = new Map<string, Promise<string>>();
  /** The `data:` address of each stylesheet after its rewrite: only finished ones. */
  readonly stylesheets = new Map<string, string>();
  /** The text that the build has made so far (DOCUMENT_BUDGET_FACTOR). */
  private made = 0;
  private readonly budget: number;
  /** Reads that wait for one of the MAX_PARALLEL_READS places. */
  private readonly waiting: Array<() => void> = [];
  private reading = 0;
  readonly paths: ReadonlySet<string>;
  bytes = 0;

  constructor(bundle: PreviewBundle, read: ReadBundleFile, limit: number) {
    this.bundle = bundle;
    this.read = read;
    this.limit = limit;
    this.budget = limit * DOCUMENT_BUDGET_FACTOR;
    for (const file of bundle.files) {
      this.sizes.set(file.path, file.size);
      this.types.set(file.path, file.type);
    }
    this.paths = new Set(this.sizes.keys());
  }

  type(path: string): string {
    return this.types.get(path) ?? 'application/octet-stream';
  }

  /**
   * The bytes of `path`. Refused before the read when the described size
   * would pass the limit, and after it when the bytes that arrived do.
   */
  bytesOf(path: string): Promise<Uint8Array> {
    const known = this.loaded.get(path);
    if (known) return known;
    const described = this.sizes.get(path) ?? 0;
    this.bytes += described;
    if (this.bytes > this.limit) throw new PreviewTooLarge(this.bytes, this.limit);
    const reading = this.limited(() => this.read(path)).then(buffer => {
      // What arrived counts, not what the description said.
      this.bytes += buffer.byteLength - described;
      if (this.bytes > this.limit) throw new PreviewTooLarge(this.bytes, this.limit);
      return new Uint8Array(buffer);
    });
    this.loaded.set(path, reading);
    return reading;
  }

  /** Run `task` when fewer than MAX_PARALLEL_READS reads run. */
  private async limited<T>(task: () => Promise<T>): Promise<T> {
    if (this.reading >= MAX_PARALLEL_READS) await new Promise<void>(go => this.waiting.push(go));
    this.reading += 1;
    try {
      return await task();
    } finally {
      this.reading -= 1;
      this.waiting.shift()?.();
    }
  }

  async textOf(path: string): Promise<string> {
    return decoder.decode(await this.bytesOf(path));
  }

  /** Count `chars` of text that goes into the document; refused past the budget. */
  spend(chars: number): void {
    this.made += chars;
    if (this.made > this.budget) throw new PreviewTooLarge(this.made, this.budget, 'document');
  }

  /** Refuse a document of `chars` characters when it is longer than the budget. */
  check(chars: number): void {
    if (chars > this.budget) throw new PreviewTooLarge(chars, this.budget, 'document');
  }

  /** `text`, counted for one more use in the document. */
  use(text: string): string {
    this.spend(text.length);
    return text;
  }

  /** The `data:` address of `path` for one more use: made once, counted for each use. */
  async dataUrlOf(path: string): Promise<string> {
    let address = this.addresses.get(path);
    if (!address) {
      address = this.bytesOf(path).then(bytes => dataUrl(this.type(path), bytes));
      this.addresses.set(path, address);
    }
    return this.use(await address);
  }

  get entry(): string {
    return this.bundle.entry;
  }
}

/** What an address in CSS becomes when it is not a file of the bundle: an empty `data:` address. */
const NO_FILE = 'data:,';

const CSS_IMPORT = /@import\s+(?:url\(\s*(?:"([^"]*)"|'([^']*)'|([^)\s]*))\s*\)|"([^"]*)"|'([^']*)')([^;]*);?/gi;
const CSS_URL = /url\(\s*(?:"([^"]*)"|'([^']*)'|([^)'"\s]*))\s*\)/gi;
/** `image-set()` and `-webkit-image-set()`, with at most one level of parentheses inside (a `url()`). */
const CSS_IMAGE_SET = /((?:-webkit-)?image-set\()((?:[^()]|\([^()]*\))*)\)/gi;
/** A string in CSS: the address form of `image-set()`. Group 1 is a `type(` in front (a type, not an address). */
const CSS_STRING = /(type\(\s*)?(?:"([^"]*)"|'([^']*)')/gi;

/** As `replaceAsync`, one replacement after the other. */
async function replaceInOrder(
  text: string, pattern: RegExp, replace: (match: RegExpExecArray) => Promise<string>,
): Promise<string> {
  const matches: RegExpExecArray[] = [];
  pattern.lastIndex = 0;
  for (let match = pattern.exec(text); match; match = pattern.exec(text)) {
    matches.push(match);
    if (match[0].length === 0) pattern.lastIndex += 1;
  }
  let out = '';
  let last = 0;
  for (const match of matches) {
    out += text.slice(last, match.index) + await replace(match);
    last = match.index + match[0].length;
  }
  return out + text.slice(last);
}

async function replaceAsync(
  text: string, pattern: RegExp, replace: (match: RegExpExecArray) => Promise<string>,
): Promise<string> {
  const parts: Array<string | Promise<string>> = [];
  let last = 0;
  pattern.lastIndex = 0;
  for (let match = pattern.exec(text); match; match = pattern.exec(text)) {
    parts.push(text.slice(last, match.index), replace(match));
    last = match.index + match[0].length;
    if (match[0].length === 0) pattern.lastIndex += 1;
  }
  parts.push(text.slice(last));
  return (await Promise.all(parts)).join('');
}

/**
 * CSS with each address of the bundle as a `data:` address, and every other
 * address removed. An imported stylesheet goes the same way, as a `data:`
 * stylesheet of its own.
 */
async function rewriteCss(
  css: string, folder: string, files: Files, depth = 0, chain: ReadonlySet<string> = new Set(),
): Promise<string> {
  // One import after the other. `chain` holds the stylesheets whose rewrite
  // led here: an import of one of them is a cycle, and is dropped.
  const imported = await replaceInOrder(css, CSS_IMPORT, async match => {
    const address = match[1] ?? match[2] ?? match[3] ?? match[4] ?? match[5] ?? '';
    const path = resolveAddress(address, folder, files.paths);
    if (path === null || path === 'data' || depth >= MAX_IMPORT_DEPTH) return '';
    const url = await stylesheetUrl(path, files, depth + 1, chain);
    return url === null ? '' : `@import url("${url}")${match[6]};`;
  });
  const urls = await replaceAsync(imported, CSS_URL, async match => {
    const address = match[1] ?? match[2] ?? match[3] ?? '';
    const path = resolveAddress(address, folder, files.paths);
    if (path === 'data') return match[0];
    return `url("${path === null ? files.use(NO_FILE) : await files.dataUrlOf(path)}")`;
  });
  // The string form of an image set: `image-set("a.png" 1x, "b.png" 2x)`.
  return replaceAsync(urls, CSS_IMAGE_SET, async match => {
    const inner = await replaceAsync(match[2], CSS_STRING, async string => {
      if (string[1] !== undefined) return string[0];
      const path = resolveAddress(string[2] ?? string[3] ?? '', folder, files.paths);
      if (path === 'data') return string[0];
      return `"${path === null ? files.use(NO_FILE) : await files.dataUrlOf(path)}"`;
    });
    return `${match[1]}${inner})`;
  });
}

/**
 * The `data:` address of the stylesheet `path` after its rewrite, for one
 * more use, or null when `path` is in `chain` (the stylesheets whose rewrite
 * led here: a cycle, and the caller drops the import). A link from the page
 * has an empty chain, so it is never dropped.
 *
 * Only finished addresses are kept, and no call waits for another. The
 * build rewrites stylesheets in order, so each is rewritten once; should two
 * rewrites of one stylesheet still run at the same time, each makes its own
 * (counted in the budget like any other text), and neither is dropped.
 */
async function stylesheetUrl(
  path: string, files: Files, depth = 0, chain: ReadonlySet<string> = new Set(),
): Promise<string | null> {
  if (chain.has(path)) return null;
  const made = files.stylesheets.get(path);
  if (made !== undefined) return files.use(made);
  const css = await rewriteCss(await files.textOf(path), folderOf(path), files, depth, new Set([...chain, path]));
  const url = dataUrl('text/css', encoder.encode(css));
  if (!files.stylesheets.has(path)) files.stylesheets.set(path, url);
  return files.use(url);
}

/** An attribute that names a file: the file as a `data:` address, or no attribute. */
async function inlineAttribute(element: Element, name: string, folder: string, files: Files): Promise<void> {
  const path = resolveAddress(element.getAttribute(name) ?? '', folder, files.paths);
  if (path === 'data') return;
  if (path === null) element.removeAttribute(name);
  else element.setAttribute(name, await files.dataUrlOf(path));
}

/**
 * The candidates of a `srcset`: an address (a run without spaces, so a
 * `data:` address with its comma stays whole), then its descriptors up to
 * the next comma. As in the HTML standard, commas at the end of an address
 * are no part of it, and then the candidate has no descriptors.
 */
export function srcsetCandidates(value: string): Array<{ address: string; descriptor: string }> {
  const out: Array<{ address: string; descriptor: string }> = [];
  let i = 0;
  while (i < value.length) {
    while (i < value.length && /[\s,]/.test(value[i])) i += 1;
    if (i >= value.length) break;
    let end = i;
    while (end < value.length && !/\s/.test(value[end])) end += 1;
    let address = value.slice(i, end);
    i = end;
    let descriptor = '';
    if (/,+$/.test(address)) {
      address = address.replace(/,+$/, '');
    } else {
      let depth = 0;
      const start = i;
      while (i < value.length && (value[i] !== ',' || depth > 0)) {
        if (value[i] === '(') depth += 1;
        else if (value[i] === ')') depth = Math.max(0, depth - 1);
        i += 1;
      }
      descriptor = value.slice(start, i).trim();
    }
    if (address) out.push({ address, descriptor });
  }
  return out;
}

/** A `srcset` with the candidates that are files of the bundle, as `data:` addresses. */
async function inlineSrcset(element: Element, folder: string, files: Files): Promise<void> {
  const kept: string[] = [];
  for (const { address, descriptor: described } of srcsetCandidates(element.getAttribute('srcset') ?? '')) {
    const descriptor = described ? [described] : [];
    const path = resolveAddress(address, folder, files.paths);
    if (path === null) continue;
    const url = path === 'data' ? address : await files.dataUrlOf(path);
    kept.push([url, ...descriptor].join(' '));
  }
  if (kept.length > 0) element.setAttribute('srcset', kept.join(', '));
  else element.removeAttribute('srcset');
}

/** The addresses of the elements below `root` (also in the content of a `<template>`). */
async function inlineTree(root: ParentNode, folder: string, files: Files): Promise<void> {
  const work: Promise<void>[] = [];
  const all = (selector: string) => Array.from(root.querySelectorAll(selector));

  for (const element of all('base, meta[http-equiv]')) element.remove();
  for (const element of all(FRAME_ELEMENTS)) {
    for (const name of ['src', 'srcdoc', 'data']) element.removeAttribute(name);
  }
  // Stylesheets first, one after the other (links and <style> elements).
  for (const link of all('link')) {
    const rel = (link.getAttribute('rel') ?? '').toLowerCase().split(/\s+/).filter(Boolean);
    if (rel.length === 0 || !rel.every(token => KEPT_LINKS.has(token))) {
      link.remove();
      continue;
    }
    const path = resolveAddress(link.getAttribute('href') ?? '', folder, files.paths);
    if (path === null) {
      link.remove();
    } else if (path !== 'data') {
      const url = rel.includes('stylesheet') ? await stylesheetUrl(path, files) : await files.dataUrlOf(path);
      if (url === null) link.remove();
      else link.setAttribute('href', url);
    }
  }
  for (const style of all('style')) {
    style.textContent = await rewriteCss(style.textContent ?? '', folder, files);
  }
  for (const element of all(SOURCE_ELEMENTS)) {
    if (element.localName === 'script' && resolveAddress(element.getAttribute('src') ?? '', folder, files.paths) === null) {
      // A script that is not in the bundle is not run, and its inline text is not run instead.
      element.remove();
      continue;
    }
    work.push(inlineAttribute(element, 'src', folder, files));
  }
  for (const element of all('img[srcset], source[srcset]')) work.push(inlineSrcset(element, folder, files));
  for (const element of all('video[poster]')) work.push(inlineAttribute(element, 'poster', folder, files));
  for (const element of all('[background]')) work.push(inlineAttribute(element, 'background', folder, files));
  for (const element of all('a[ping], area[ping]')) element.removeAttribute('ping');
  // SVG: an address of another file in `href` (an image, a `use`, a script);
  // a link (`a`) is a navigation and stays as it is, and so does `#id`.
  for (const element of all('svg *')) {
    if (element.localName === 'a') continue;
    for (const name of ['href', 'xlink:href']) {
      const value = element.getAttribute(name);
      if (value === null || value.trim().startsWith('#')) continue;
      work.push(inlineAttribute(element, name, folder, files));
    }
  }
  for (const element of all('[style]')) {
    work.push(rewriteCss(element.getAttribute('style') ?? '', folder, files)
      .then(css => element.setAttribute('style', css)));
  }
  await Promise.all(work);
  // The content of each template after the rest, one after the other, as
  // the page itself: its stylesheets then find the finished ones.
  for (const template of all('template')) {
    await inlineTree((template as HTMLTemplateElement).content, folder, files);
  }
}

/** The head of Nerve's own that goes first into every preview document. */
function addOwnHead(doc: Document): void {
  const policy = doc.createElement('meta');
  policy.setAttribute('http-equiv', 'Content-Security-Policy');
  policy.setAttribute('content', PREVIEW_POLICY);
  const charset = doc.createElement('meta');
  charset.setAttribute('charset', 'utf-8');
  doc.head.prepend(charset, policy);
}

/**
 * The document as text, made once. Refused when it is longer than the
 * budget: the serializer escapes characters (`&` becomes `&amp;`), so the
 * text can be longer than what was counted.
 */
function serialize(doc: Document, files: Files): string {
  const html = `${doc.doctype ? '<!DOCTYPE html>' : ''}${doc.documentElement.outerHTML}`;
  files.check(html.length);
  return html;
}

/** The document of an image bundle: the image, at most as large as the frame. */
async function imageDocument(files: Files): Promise<string> {
  const doc = new DOMParser().parseFromString('<!DOCTYPE html><html><head></head><body></body></html>', 'text/html');
  const style = doc.createElement('style');
  style.textContent = 'html,body{margin:0;height:100%;}body{display:flex;align-items:center;justify-content:center;}'
    + 'img{max-width:100%;max-height:100%;object-fit:contain;}';
  doc.head.append(style);
  const img = doc.createElement('img');
  img.setAttribute('alt', files.entry);
  img.setAttribute('src', await files.dataUrlOf(files.entry));
  doc.body.append(img);
  addOwnHead(doc);
  return serialize(doc, files);
}

/**
 * The longest time one build may take. A last guard: every step of a build
 * ends by itself, so a build that takes longer is a fault, and it must not
 * leave the tab waiting.
 */
export const MAX_BUILD_MS = 30_000;

/** A build took longer than `MAX_BUILD_MS`. */
export class PreviewTimedOut extends Error {
  constructor(ms: number) {
    super(`The preview took longer than ${Math.round(ms / 1000)} s to build.`);
  }
}

/**
 * The document of a preview of `bundle`. `read` gets the bytes of one file;
 * each file is read at most once, and none past `limit` bytes in all
 * (`PreviewTooLarge`). The document text is at most `limit` times
 * DOCUMENT_BUDGET_FACTOR (`PreviewTooLarge`, `what: 'document'`). A build
 * that takes longer than `timeoutMs` ends with `PreviewTimedOut`.
 */
export async function buildPreviewDocument(
  bundle: PreviewBundle, read: ReadBundleFile, limit: number, timeoutMs: number = MAX_BUILD_MS,
): Promise<PreviewDocument> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(new PreviewTimedOut(timeoutMs)), timeoutMs);
  });
  try {
    return await Promise.race([build(bundle, read, limit), timeout]);
  } finally {
    clearTimeout(timer);
  }
}

async function build(bundle: PreviewBundle, read: ReadBundleFile, limit: number): Promise<PreviewDocument> {
  const files = new Files(bundle, read, limit);
  if (bundle.kind === 'image') {
    return { html: await imageDocument(files), source: null, bytes: files.bytes };
  }
  const source = files.use(await files.textOf(bundle.entry));
  const doc = new DOMParser().parseFromString(source, 'text/html');
  doc.documentElement.removeAttribute('manifest');
  await inlineTree(doc, folderOf(bundle.entry), files);
  addOwnHead(doc);
  return { html: serialize(doc, files), source, bytes: files.bytes };
}
