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

/**
 * CSS escapes spell any of the above a second way (`\75 rl(` is `url(` to
 * the browser), and a picture's styling has no honest use for a backslash,
 * so one is treated as the external reference it may hide.
 */
function hasExternalCss(value: string | null | undefined): boolean {
  return !!value && (value.includes('\\') || EXTERNAL_CSS.test(value));
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

/** CSS pixels per unit, for the absolute units an SVG size may carry; the font-relative ones at the 16px default. */
const PX_PER_UNIT: Record<string, number> = {
  '': 1, px: 1, pt: 4 / 3, pc: 16, in: 96, cm: 96 / 2.54, mm: 96 / 25.4, q: 96 / 101.6, em: 16, rem: 16, ex: 8, ch: 8,
};

/** A `width`/`height` attribute in CSS pixels; NaN for a percentage, a viewport unit or nothing. */
function lengthPx(value: string | null): number {
  const m = /^\s*([0-9]*\.?[0-9]+(?:e[+-]?\d+)?)\s*([a-z]*)\s*$/i.exec(value ?? '');
  const scale = m ? PX_PER_UNIT[m[2].toLowerCase()] : undefined;
  return m && scale !== undefined ? parseFloat(m[1]) * scale : NaN;
}

/**
 * The size the browser draws the picture at: its `width` and `height`; one
 * of them and the viewBox's aspect ratio; else the viewBox itself. A
 * percentage says nothing about the picture (it sizes against the page) and
 * counts as absent.
 */
export function naturalSizeOf(svg: SVGSVGElement): { width: number; height: number } | undefined {
  const width = lengthPx(svg.getAttribute('width'));
  const height = lengthPx(svg.getAttribute('height'));
  const box = (svg.getAttribute('viewBox') ?? '').trim().split(/[\s,]+/).map(Number);
  const aspect = box.length === 4 && box[2] > 0 && box[3] > 0 ? box[3] / box[2] : NaN;
  if (width > 0 && height > 0) return { width, height };
  if (width > 0 && aspect > 0) return { width, height: width * aspect };
  if (height > 0 && aspect > 0) return { width: height / aspect, height };
  if (aspect > 0) return { width: box[2], height: box[3] };
  return undefined;
}

/**
 * The picture as a file of its own, at its natural size in place of a
 * percentage. XML serialization: it declares the SVG (and xlink) namespaces
 * a standalone document needs and inline SVG does without, and it writes no
 * `&nbsp;` or unclosed tag that a file cannot hold.
 */
function standalone(svg: SVGSVGElement, size: { width: number; height: number } | undefined): string {
  const copy = svg.cloneNode(true) as SVGSVGElement;
  if (size) {
    copy.setAttribute('width', String(size.width));
    copy.setAttribute('height', String(size.height));
  }
  return new XMLSerializer().serializeToString(copy);
}

/**
 * Below this viewBox width, a picture that states no width or height of its
 * own is taken as a unit-less drawing (an icon-like `0 0 24 24`), not as a
 * 24-pixel picture: it takes the column, as an SVG sized by its container
 * does anywhere else.
 */
const UNITLESS_BELOW = 200;

export const mountSvg: Mount = async (stage, source) => {
  const clean = sanitizeSvg(source);
  stage.innerHTML = clean;
  const element = stage.querySelector('svg');
  // The file is made from the picture as drawn, whatever is done to the
  // element on screen afterwards.
  const drawn = element?.cloneNode(true) as SVGSVGElement | undefined;
  let naturalSize = element ? naturalSizeOf(element) : undefined;
  if (element && naturalSize) {
    const declared = lengthPx(element.getAttribute('width')) > 0 || lengthPx(element.getAttribute('height')) > 0;
    if (!declared && naturalSize.width < UNITLESS_BELOW) {
      element.removeAttribute('width');
      element.removeAttribute('height');
      element.style.width = '100%';
      element.style.height = 'auto';
      naturalSize = undefined;
    } else {
      // The picture states its own size (`width="100%"` would stretch it to
      // the column); the frame fits or scrolls it.
      element.setAttribute('width', String(naturalSize.width));
      element.setAttribute('height', String(naturalSize.height));
    }
  }
  return {
    cleanup: () => {
      stage.innerHTML = '';
    },
    naturalSize,
    toSvg: async () => (drawn ? standalone(drawn, naturalSizeOf(drawn)) : null),
  };
};
