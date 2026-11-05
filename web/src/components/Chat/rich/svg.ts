import DOMPurify from 'dompurify';
import type { Mount } from './types';

/**
 * A CSS value that would make the browser fetch something: `url(` not aimed
 * at a fragment, `@import`, `image-set(`, `src(`. `fill: url(#gradient)` is
 * fine and common; `background: url(https://…)` on the root `<svg>` would
 * load that URL — a URL the model wrote, possibly with data from a tool
 * result in its query string.
 */
const EXTERNAL_CSS = /url\(\s*['"]?\s*(?!#)|@import\b|image-set\(|\bsrc\(/i;

function hasExternalCss(value: string | null | undefined): boolean {
  return !!value && EXTERNAL_CSS.test(value);
}

const CONFIG = {
  USE_PROFILES: { svg: true, svgFilters: true },
  // DOMPurify leaves `use` out because its `href` may name another document;
  // here an `href` may only be a `#fragment` (the hook below), which makes
  // `use` the ordinary way to repeat a symbol.
  ADD_TAGS: ['use'],
  // `image` and `feImage` fetch their `href`; `foreignObject` embeds HTML.
  FORBID_TAGS: ['foreignObject', 'image', 'feImage', 'script', 'iframe', 'object', 'embed'],
};

/** Attributes that name a resource. In a chat picture they may only point inside it. */
const URL_ATTRIBUTES = new Set(['href', 'xlink:href', 'src']);

let purifier: ReturnType<typeof DOMPurify> | null = null;

/**
 * DOMPurify's own URI check only rules out `javascript:` and the like, and it
 * leaves `style` alone, so the routes to the network get their own hook:
 * references stay inside the picture (gradients, markers, clip paths, `use`d
 * symbols — `#fragment`s), and no attribute, `style` or `fill` or `filter`,
 * carries a `url()` aimed anywhere else. An own instance keeps the hooks off
 * the shared default one.
 */
function getPurifier() {
  if (purifier) return purifier;
  const instance = DOMPurify();
  instance.addHook('uponSanitizeAttribute', (_node, data) => {
    if (URL_ATTRIBUTES.has(data.attrName) && !data.attrValue.trim().startsWith('#')) {
      data.keepAttr = false;
      return;
    }
    if (hasExternalCss(data.attrValue)) {
      data.keepAttr = false;
    }
  });
  instance.addHook('uponSanitizeElement', (node, data) => {
    if (data.tagName === 'style' && hasExternalCss(node.textContent)) {
      node.textContent = '';
    }
  });
  purifier = instance;
  return instance;
}

const SVG_ROOT = /<svg[\s>]/i;

/**
 * The source reduced to an inline SVG that draws and does nothing else. Throws
 * when there is no `<svg>` to begin with, or none left after sanitizing.
 */
export function sanitizeSvg(source: string): string {
  const trimmed = source.trim();
  if (!SVG_ROOT.test(trimmed)) throw new Error('the block holds no <svg> element');
  const clean = getPurifier().sanitize(trimmed, CONFIG);
  if (!SVG_ROOT.test(clean)) throw new Error('nothing drawable is left after sanitizing');
  return clean;
}

/** The picture's size from its `width`/`height` attributes, else its viewBox. */
function naturalSizeOf(svg: SVGSVGElement): { width: number; height: number } | undefined {
  const w = svg.getAttribute('width') ?? '';
  const h = svg.getAttribute('height') ?? '';
  const width = parseFloat(w);
  const height = parseFloat(h);
  if (width > 0 && height > 0 && !w.endsWith('%') && !h.endsWith('%')) return { width, height };
  const box = (svg.getAttribute('viewBox') ?? '').trim().split(/[\s,]+/).map(Number);
  if (box.length === 4 && box[2] > 0 && box[3] > 0) return { width: box[2], height: box[3] };
  return undefined;
}

export const mountSvg: Mount = async (stage, source) => {
  const clean = sanitizeSvg(source);
  stage.innerHTML = clean;
  const element = stage.querySelector('svg');
  return {
    cleanup: () => {
      stage.innerHTML = '';
    },
    naturalSize: element ? naturalSizeOf(element) : undefined,
    toSvg: async () => clean,
  };
};
