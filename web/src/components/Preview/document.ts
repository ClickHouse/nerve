import type { PreviewBundle } from '../../types/preview';

/**
 * The document of a preview: one HTML document, made in the browser from the
 * files of a bundle, that the sandbox frame shows.
 *
 * The entry page is parsed with the browser's own parser (`DOMParser`). A
 * document made that way is inert: it runs no script and loads nothing. Then
 * each address of a resource in it (a script, a stylesheet, an image, a
 * font, an address in CSS) is looked up in the bundle:
 *
 * - a file of the bundle: its bytes go into the document, as a `data:`
 *   address. A stylesheet gets the same for the addresses in it.
 * - anything else (an address of another host, an absolute path, a file
 *   that is not in the bundle): the address is removed. Nothing is ever
 *   asked of it, by the server or by the page.
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

/** The files of the bundle that a preview put into its document were more than the limit. */
export class PreviewTooLarge extends Error {
  readonly bytes: number;
  readonly limit: number;

  constructor(bytes: number, limit: number) {
    super(`The files of this preview are larger than ${formatMiB(limit)}.`);
    this.bytes = bytes;
    this.limit = limit;
  }
}

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

/** How deep stylesheets may import each other. */
const MAX_IMPORT_DEPTH = 8;

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
  readonly paths: ReadonlySet<string>;
  bytes = 0;

  constructor(bundle: PreviewBundle, read: ReadBundleFile, limit: number) {
    this.bundle = bundle;
    this.read = read;
    this.limit = limit;
    for (const file of bundle.files) {
      this.sizes.set(file.path, file.size);
      this.types.set(file.path, file.type);
    }
    this.paths = new Set(this.sizes.keys());
  }

  type(path: string): string {
    return this.types.get(path) ?? 'application/octet-stream';
  }

  /** The bytes of `path`. Refused before the read when it would pass the limit. */
  bytesOf(path: string): Promise<Uint8Array> {
    const known = this.loaded.get(path);
    if (known) return known;
    this.bytes += this.sizes.get(path) ?? 0;
    if (this.bytes > this.limit) throw new PreviewTooLarge(this.bytes, this.limit);
    const reading = this.read(path).then(buffer => new Uint8Array(buffer));
    this.loaded.set(path, reading);
    return reading;
  }

  async textOf(path: string): Promise<string> {
    return decoder.decode(await this.bytesOf(path));
  }

  async dataUrlOf(path: string): Promise<string> {
    return dataUrl(this.type(path), await this.bytesOf(path));
  }

  get entry(): string {
    return this.bundle.entry;
  }
}

/** What an address in CSS becomes when it is not a file of the bundle: an empty `data:` address. */
const NO_FILE = 'data:,';

const CSS_IMPORT = /@import\s+(?:url\(\s*(?:"([^"]*)"|'([^']*)'|([^)\s]*))\s*\)|"([^"]*)"|'([^']*)')([^;]*);?/gi;
const CSS_URL = /url\(\s*(?:"([^"]*)"|'([^']*)'|([^)'"\s]*))\s*\)/gi;

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
  css: string, folder: string, files: Files, depth = 0, seen: ReadonlySet<string> = new Set(),
): Promise<string> {
  const imported = await replaceAsync(css, CSS_IMPORT, async match => {
    const address = match[1] ?? match[2] ?? match[3] ?? match[4] ?? match[5] ?? '';
    const path = resolveAddress(address, folder, files.paths);
    if (path === null || path === 'data' || depth >= MAX_IMPORT_DEPTH || seen.has(path)) return '';
    const text = await rewriteCss(await files.textOf(path), folderOf(path), files, depth + 1, new Set([...seen, path]));
    return `@import url("${dataUrl('text/css', encoder.encode(text))}")${match[6]};`;
  });
  return replaceAsync(imported, CSS_URL, async match => {
    const address = match[1] ?? match[2] ?? match[3] ?? '';
    const path = resolveAddress(address, folder, files.paths);
    if (path === 'data') return match[0];
    return `url("${path === null ? NO_FILE : await files.dataUrlOf(path)}")`;
  });
}

/** An attribute that names a file: the file as a `data:` address, or no attribute. */
async function inlineAttribute(element: Element, name: string, folder: string, files: Files): Promise<void> {
  const path = resolveAddress(element.getAttribute(name) ?? '', folder, files.paths);
  if (path === 'data') return;
  if (path === null) element.removeAttribute(name);
  else element.setAttribute(name, await files.dataUrlOf(path));
}

/** A `srcset` with the candidates that are files of the bundle, as `data:` addresses. */
async function inlineSrcset(element: Element, folder: string, files: Files): Promise<void> {
  const kept: string[] = [];
  for (const candidate of (element.getAttribute('srcset') ?? '').split(',')) {
    const [address, ...descriptor] = candidate.trim().split(/\s+/);
    if (!address) continue;
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
      work.push((async () => {
        const url = rel.includes('stylesheet')
          ? dataUrl('text/css', encoder.encode(await rewriteCss(await files.textOf(path), folderOf(path), files)))
          : await files.dataUrlOf(path);
        link.setAttribute('href', url);
      })());
    }
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
  for (const style of all('style')) {
    work.push(rewriteCss(style.textContent ?? '', folder, files).then(css => { style.textContent = css; }));
  }
  for (const element of all('[style]')) {
    work.push(rewriteCss(element.getAttribute('style') ?? '', folder, files)
      .then(css => element.setAttribute('style', css)));
  }
  for (const template of all('template')) {
    work.push(inlineTree((template as HTMLTemplateElement).content, folder, files));
  }
  await Promise.all(work);
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

function serialize(doc: Document): string {
  return `${doc.doctype ? '<!DOCTYPE html>' : ''}${doc.documentElement.outerHTML}`;
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
  return serialize(doc);
}

/**
 * The document of a preview of `bundle`. `read` gets the bytes of one file;
 * each file is read at most once, and none past `limit` bytes in all
 * (`PreviewTooLarge`).
 */
export async function buildPreviewDocument(
  bundle: PreviewBundle, read: ReadBundleFile, limit: number,
): Promise<PreviewDocument> {
  const files = new Files(bundle, read, limit);
  if (bundle.kind === 'image') {
    return { html: await imageDocument(files), source: null, bytes: files.bytes };
  }
  const source = await files.textOf(bundle.entry);
  const doc = new DOMParser().parseFromString(source, 'text/html');
  doc.documentElement.removeAttribute('manifest');
  await inlineTree(doc, folderOf(bundle.entry), files);
  addOwnHead(doc);
  return { html: serialize(doc), source, bytes: files.bytes };
}
