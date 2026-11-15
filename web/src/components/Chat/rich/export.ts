import type { RichKind } from './kinds';

/** A picture's size in CSS pixels. */
export interface Size {
  width: number;
  height: number;
}

const SVG_NS = 'http://www.w3.org/2000/svg';

/** What a block's files are called: `diagram.png`, `chart.svg`, `image.svg`. */
const FILE_STEM: Record<RichKind, string> = {
  mermaid: 'diagram',
  'vega-lite': 'chart',
  vega: 'chart',
  svg: 'image',
};

export function exportFileName(kind: RichKind, format: 'svg' | 'png'): string {
  return `${FILE_STEM[kind]}.${format}`;
}

/**
 * The largest canvas a PNG is drawn on. Past a browser's limit (Safari's is
 * the smallest, about 16.7 million pixels) the canvas silently stays blank,
 * so a huge picture is drawn at a lower scale instead.
 */
const MAX_CANVAS_SIDE = 16_384;
const MAX_CANVAS_AREA = 16_000_000;

/** A length attribute in CSS pixels: `640`, `640px`, `640.5`; null for `100%`, `10cm`, nothing. */
function pixels(value: string | null): number | null {
  const match = value && /^\s*(\d+(?:\.\d+)?|\.\d+)\s*(px)?\s*$/i.exec(value);
  const n = match ? parseFloat(match[1]) : NaN;
  return n > 0 ? n : null;
}

function viewBoxOf(svg: Element): [number, number, number, number] | null {
  const box = (svg.getAttribute('viewBox') ?? '').trim().split(/[\s,]+/).map(Number);
  return box.length === 4 && box.every(Number.isFinite) && box[2] > 0 && box[3] > 0
    ? [box[0], box[1], box[2], box[3]]
    : null;
}

/** The size an svg states for itself: its width and height in pixels, else its viewBox. */
function statedSize(svg: Element): Size | null {
  const width = pixels(svg.getAttribute('width'));
  const height = pixels(svg.getAttribute('height'));
  if (width && height) return { width, height };
  const box = viewBoxOf(svg);
  return box ? { width: box[2], height: box[3] } : null;
}

/**
 * The root `<svg>` of `markup`, parsed the lenient way the page parses it:
 * a renderer's export may be HTML-serialized (`<br>` in a label, no
 * `xmlns`), which an XML parser would refuse. The parsed document is inert —
 * nothing in it runs or loads.
 */
function parseSvg(markup: string): SVGSVGElement | null {
  const doc = new DOMParser().parseFromString(markup, 'text/html');
  return doc.querySelector('svg');
}

/** The size `markup`'s picture states for itself, or null. */
export function svgSizeOf(markup: string): Size | null {
  const svg = parseSvg(markup);
  return svg ? statedSize(svg) : null;
}

/**
 * Gives `svg` a viewBox when it has none, so that sizing it with CSS scales
 * the drawing rather than cropping it. The box is the size the svg states
 * for itself, else `size`.
 */
export function ensureViewBox(svg: SVGSVGElement, size: Size): void {
  if (svg.hasAttribute('viewBox')) return;
  const width = pixels(svg.getAttribute('width')) ?? size.width;
  const height = pixels(svg.getAttribute('height')) ?? size.height;
  svg.setAttribute('viewBox', `0 0 ${width} ${height}`);
}

/** The first svg drawn in `stage`, as markup an XML parser accepts. */
export function svgMarkupOf(stage: Element): string | null {
  const svg = stage.querySelector('svg');
  return svg ? new XMLSerializer().serializeToString(svg) : null;
}

export interface StandaloneOptions {
  /** Width and height for the file; without it the picture keeps the size it states. */
  size?: Size | null;
  /** A colour painted under the drawing, so text drawn for a dark page stays readable on a white one. */
  background?: string | null;
}

/**
 * `markup` as an SVG file: well-formed XML with its namespaces declared, a
 * width and height of its own, and none of the inline sizing the page gave
 * it (`max-width`, `width: 100%`). Null when there is no svg in it.
 */
export function standaloneSvg(markup: string, options: StandaloneOptions = {}): string | null {
  const svg = parseSvg(markup);
  if (!svg) return null;
  const size = options.size ?? statedSize(svg);
  if (size) {
    ensureViewBox(svg, size);
    svg.setAttribute('width', String(size.width));
    svg.setAttribute('height', String(size.height));
  }
  for (const property of ['width', 'height', 'max-width', 'max-height']) svg.style.removeProperty(property);
  if (!svg.getAttribute('style')?.trim()) svg.removeAttribute('style');
  if (options.background) {
    const [x, y, width, height] = viewBoxOf(svg) ?? [0, 0, size?.width ?? 0, size?.height ?? 0];
    const rect = svg.ownerDocument.createElementNS(SVG_NS, 'rect');
    rect.setAttribute('x', String(x));
    rect.setAttribute('y', String(y));
    rect.setAttribute('width', width ? String(width) : '100%');
    rect.setAttribute('height', height ? String(height) : '100%');
    // A style attribute, so a stylesheet in the picture (`rect { fill: … }`) cannot repaint it.
    rect.setAttribute('style', `fill: ${options.background}; stroke: none`);
    svg.insertBefore(rect, svg.firstChild);
  }
  return new XMLSerializer().serializeToString(svg);
}

/** Saves `blob` as a file called `fileName`, the way a download link would. */
export function downloadBlob(blob: Blob, fileName: string): void {
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = fileName;
  link.rel = 'noopener';
  link.style.display = 'none';
  // Attached for the click: some browsers ignore a click on a detached link.
  document.body.appendChild(link);
  link.click();
  link.remove();
  // The browser has taken its copy once the download starts; a slow disk may need a moment.
  setTimeout(() => URL.revokeObjectURL(url), 30_000);
}

/** Saves `svgText` as an SVG file. Returns false when there is no svg in it to save. */
export function downloadSvg(svgText: string, fileName: string, options: StandaloneOptions = {}): boolean {
  const file = standaloneSvg(svgText, options);
  if (!file) return false;
  downloadBlob(new Blob([`<?xml version="1.0" encoding="UTF-8"?>\n${file}`], { type: 'image/svg+xml' }), fileName);
  return true;
}

function loadImage(src: string): Promise<HTMLImageElement> {
  return new Promise((resolve, reject) => {
    const image = new Image();
    image.onload = () => resolve(image);
    image.onerror = () => reject(new Error('the picture could not be decoded'));
    image.src = src;
  });
}

/** A canvas `width` × `height` CSS pixels at `scale`, with `background` painted on it. */
function paintedCanvas(width: number, height: number, scale: number, background: string | null | undefined) {
  // Shrink the scale rather than ask for a canvas the browser would leave blank.
  const k = Math.min(scale, MAX_CANVAS_SIDE / width, MAX_CANVAS_SIDE / height, Math.sqrt(MAX_CANVAS_AREA / (width * height)));
  const canvas = document.createElement('canvas');
  canvas.width = Math.max(1, Math.round(width * k));
  canvas.height = Math.max(1, Math.round(height * k));
  const context = canvas.getContext('2d');
  if (!context) throw new Error('this browser cannot draw the picture');
  if (background) {
    context.fillStyle = background;
    context.fillRect(0, 0, canvas.width, canvas.height);
  }
  return { canvas, context };
}

function canvasToPng(canvas: HTMLCanvasElement): Promise<Blob> {
  return new Promise((resolve, reject) => {
    canvas.toBlob(blob => (blob ? resolve(blob) : reject(new Error('the picture could not be encoded'))), 'image/png');
  });
}

/**
 * `svgText` drawn as a PNG at `scale` device pixels per CSS pixel, over
 * `background`. The SVG is loaded as an image from a `data:` URL: an SVG
 * image loads nothing from the network, and a `data:` URL keeps the canvas
 * readable where a `blob:` one would mark it as tainted. Fonts the page
 * loaded itself are not available to an SVG image, so text falls back to
 * the system's sans-serif.
 */
export async function svgToPngBlob(
  svgText: string,
  scale: number,
  background?: string | null,
  size?: Size | null,
): Promise<Blob> {
  const parsed = parseSvg(svgText);
  const box = size ?? (parsed && statedSize(parsed)) ?? { width: 300, height: 150 };
  const file = standaloneSvg(svgText, { size: box });
  if (!file) throw new Error('there is no picture to draw');
  const image = await loadImage(`data:image/svg+xml;charset=utf-8,${encodeURIComponent(file)}`);
  const { canvas, context } = paintedCanvas(box.width, box.height, scale, background);
  context.drawImage(image, 0, 0, canvas.width, canvas.height);
  return canvasToPng(canvas);
}

/**
 * The PNG `png` composited over `background`. A renderer's own PNG keeps a
 * transparent background, and a chart drawn for the dark theme is light
 * text on nothing — unreadable in an image viewer's white window.
 */
export async function pngOver(png: Blob, background: string): Promise<Blob> {
  const url = URL.createObjectURL(png);
  try {
    const image = await loadImage(url);
    const { canvas, context } = paintedCanvas(image.naturalWidth || 1, image.naturalHeight || 1, 1, background);
    context.drawImage(image, 0, 0, canvas.width, canvas.height);
    return await canvasToPng(canvas);
  } finally {
    URL.revokeObjectURL(url);
  }
}
