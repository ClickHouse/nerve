/**
 * The source text of a generated kit page. The runtime puts it into the page
 * as JSON in `<script type="application/json" id="nerve-kit-source">`
 * (`nerve/artifacts/bundle.py`, `kit_page`). It is data: a page shows it as
 * text, never as markup of the page.
 */

export const SOURCE_BLOCK_ID = 'nerve-kit-source';
/**
 * The largest source of a Markdown or table page, in UTF-8 bytes: the limit
 * of the runtime (`MAX_SOURCE_BYTES` in `nerve/artifacts/bundle.py`). A page
 * checks it itself, before it parses or draws anything: any page of an
 * agent can name a kit script and carry its own data block.
 */
export const MAX_SOURCE_BYTES = 5 * 1024 * 1024;
/** The element that a generated page draws into. */
export const PAGE_ROOT_ID = 'nerve-kit-page';

export interface PageSource {
  /** The file name of the source in the workspace. */
  name: string;
  text: string;
}

/** The source of the page, or null when the page has none or it is not valid. */
export function readSource(doc: Document = document): PageSource | null {
  const block = doc.getElementById(SOURCE_BLOCK_ID);
  if (!block) return null;
  try {
    const value: unknown = JSON.parse(block.textContent ?? '');
    if (typeof value === 'object' && value !== null) {
      const { name, text } = value as Record<string, unknown>;
      if (typeof name === 'string' && typeof text === 'string') return { name, text };
    }
  } catch {
    // Not JSON: the page has no source.
  }
  return null;
}

/** The element that a page draws into: the page root, or the body. */
export function pageRoot(doc: Document = document): HTMLElement {
  return doc.getElementById(PAGE_ROOT_ID) ?? doc.body;
}

/** Run `start` when the document is parsed. */
export function whenParsed(start: () => void, doc: Document = document): void {
  if (doc.readyState === 'loading') doc.addEventListener('DOMContentLoaded', start, { once: true });
  else start();
}

/**
 * The UTF-8 length of `text`, counted up to just past `limit` (no copy of
 * the text). A lone surrogate counts as the three bytes of its replacement.
 */
export function utf8Length(text: string, limit = Number.MAX_SAFE_INTEGER): number {
  let bytes = 0;
  for (let i = 0; i < text.length && bytes <= limit; i += 1) {
    const c = text.charCodeAt(i);
    if (c < 0x80) bytes += 1;
    else if (c < 0x800) bytes += 2;
    else if (c >= 0xd800 && c <= 0xdbff && i + 1 < text.length) {
      const d = text.charCodeAt(i + 1);
      if (d >= 0xdc00 && d <= 0xdfff) {
        bytes += 4;
        i += 1;
      } else {
        bytes += 3;
      }
    } else bytes += 3;
  }
  return bytes;
}

/** The sentence for a source above `MAX_SOURCE_BYTES`, or null for one within it. */
export function sourceTooLarge(source: PageSource): string | null {
  if (utf8Length(source.text, MAX_SOURCE_BYTES) <= MAX_SOURCE_BYTES) return null;
  return `The file is larger than ${MAX_SOURCE_BYTES / (1024 * 1024)} MiB. A page shows at most that.`;
}
