import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  downloadBlob, downloadSvg, exportFileName, pngOver, standaloneSvg, svgSizeOf, svgToPngBlob,
} from './export';

const SVG_NS = 'http://www.w3.org/2000/svg';

function parseXml(text: string): Document {
  return new DOMParser().parseFromString(text, 'image/svg+xml');
}

/**
 * jsdom decodes no images and has no 2D context: a stand-in for both that
 * records what was drawn, where, and on what.
 */
function fakeDrawing({ decodes = true } = {}) {
  const images: string[] = [];
  class FakeImage {
    onload: (() => void) | null = null;
    onerror: (() => void) | null = null;
    naturalWidth = 640;
    naturalHeight = 360;
    private source = '';
    get src() { return this.source; }
    set src(value: string) {
      this.source = value;
      images.push(value);
      queueMicrotask(() => (decodes ? this.onload?.() : this.onerror?.()));
    }
  }
  const canvases: HTMLCanvasElement[] = [];
  const fills: { style: string; rect: number[] }[] = [];
  const draws: { src: string; rect: number[] }[] = [];
  const context = {
    fillStyle: '',
    fillRect(...rect: number[]) { fills.push({ style: this.fillStyle, rect }); },
    drawImage(image: FakeImage, ...rect: number[]) { draws.push({ src: image.src, rect }); },
  };
  vi.stubGlobal('Image', FakeImage);
  vi.spyOn(HTMLCanvasElement.prototype, 'getContext').mockImplementation(function (this: HTMLCanvasElement) {
    canvases.push(this);
    return context as unknown as CanvasRenderingContext2D;
  } as never);
  vi.spyOn(HTMLCanvasElement.prototype, 'toBlob').mockImplementation(function (
    this: HTMLCanvasElement, callback: BlobCallback, type?: string,
  ) {
    callback(new Blob([`${this.width}x${this.height}`], { type }));
  });
  return { images, canvases, fills, draws };
}

/** Object URLs and link clicks, which jsdom does not implement, caught on their way out. */
function catchDownloads() {
  const blobs = new Map<string, Blob>();
  const files: { name: string; blob: Blob; attached: boolean }[] = [];
  let next = 0;
  const createObjectURL = vi.fn((blob: Blob) => {
    const url = `blob:test/${++next}`;
    blobs.set(url, blob);
    return url;
  });
  const revokeObjectURL = vi.fn();
  const before = { createObjectURL: URL.createObjectURL, revokeObjectURL: URL.revokeObjectURL };
  Object.assign(URL, { createObjectURL, revokeObjectURL });
  vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(function (this: HTMLAnchorElement) {
    files.push({ name: this.download, blob: blobs.get(this.href)!, attached: this.isConnected });
  });
  return { files, createObjectURL, revokeObjectURL, restore: () => Object.assign(URL, before) };
}

let downloads: ReturnType<typeof catchDownloads>;

beforeEach(() => {
  downloads = catchDownloads();
});

afterEach(() => {
  downloads.restore();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe('exportFileName', () => {
  it('names a file after what the block is', () => {
    expect(exportFileName('mermaid', 'svg')).toBe('diagram.svg');
    expect(exportFileName('mermaid', 'png')).toBe('diagram.png');
    expect(exportFileName('vega-lite', 'png')).toBe('chart.png');
    expect(exportFileName('vega', 'svg')).toBe('chart.svg');
    expect(exportFileName('svg', 'png')).toBe('image.png');
  });
});

describe('svgSizeOf', () => {
  it('reads the size an svg states, in pixels, else its viewBox', () => {
    expect(svgSizeOf('<svg width="640" height="320px"></svg>')).toEqual({ width: 640, height: 320 });
    expect(svgSizeOf('<svg width="100%" viewBox="0 0 800 400"></svg>')).toEqual({ width: 800, height: 400 });
    expect(svgSizeOf('<svg width="10cm" height="5cm"></svg>')).toBeNull();
    expect(svgSizeOf('<p>no picture</p>')).toBeNull();
  });
});

describe('standaloneSvg', () => {
  it('turns an HTML-serialized svg into well-formed XML of its own size', () => {
    const file = standaloneSvg(
      '<svg width="100%" style="max-width: 320px; color: red" viewBox="0 0 320 120">'
      + '<foreignObject width="100" height="20"><div>one<br>two</div></foreignObject>'
      + '<text>a &amp; b</text></svg>',
      { size: { width: 320, height: 120 } },
    )!;
    const doc = parseXml(file);
    expect(doc.querySelector('parsererror')).toBeNull();
    const svg = doc.documentElement;
    expect(svg.namespaceURI).toBe(SVG_NS);
    expect(svg.getAttribute('width')).toBe('320');
    expect(svg.getAttribute('height')).toBe('120');
    // The page's sizing goes; the picture's own styling stays.
    expect(svg.getAttribute('style')).toBe('color: red;');
    expect(file).toContain('<br />');
    expect(file).toContain('a &amp; b');
  });

  it('keeps the size the svg states when it is given none', () => {
    const svg = parseXml(standaloneSvg('<svg width="40px" height="30"><circle r="4"/></svg>')!).documentElement;
    expect(svg.getAttribute('width')).toBe('40');
    expect(svg.getAttribute('height')).toBe('30');
    expect(svg.getAttribute('viewBox')).toBe('0 0 40 30');
  });

  it('gives an svg without a viewBox one, so a new size scales the drawing', () => {
    const svg = parseXml(standaloneSvg('<svg width="50" height="25"></svg>', { size: { width: 100, height: 50 } })!)
      .documentElement;
    expect(svg.getAttribute('viewBox')).toBe('0 0 50 25');
    expect(svg.getAttribute('width')).toBe('100');
  });

  it('paints a background under the whole drawing', () => {
    const file = standaloneSvg(
      '<svg width="100" height="50" viewBox="-8 -8 100 50"><style>rect { fill: red }</style><rect class="node"/></svg>',
      { background: '#101418' },
    )!;
    const first = parseXml(file).documentElement.firstElementChild!;
    expect(first.tagName).toBe('rect');
    expect([first.getAttribute('x'), first.getAttribute('y')]).toEqual(['-8', '-8']);
    expect([first.getAttribute('width'), first.getAttribute('height')]).toEqual(['100', '50']);
    expect(first.getAttribute('style')).toBe('fill: #101418; stroke: none');
  });

  it('is null for markup with no svg in it', () => {
    expect(standaloneSvg('<div>not a picture</div>')).toBeNull();
  });
});

describe('downloads', () => {
  it('saves a blob through a link, and lets go of its URL later', () => {
    vi.useFakeTimers();
    downloadBlob(new Blob(['x'], { type: 'text/plain' }), 'note.txt');
    expect(downloads.files).toEqual([{ name: 'note.txt', blob: expect.any(Blob), attached: true }]);
    expect(document.querySelector('a[download]')).toBeNull();
    expect(downloads.revokeObjectURL).not.toHaveBeenCalled();
    vi.runAllTimers();
    expect(downloads.revokeObjectURL).toHaveBeenCalledWith('blob:test/1');
  });

  it('saves an svg as an SVG file', async () => {
    expect(downloadSvg('<svg viewBox="0 0 10 10"><circle r="4"/></svg>', 'image.svg')).toBe(true);
    const [file] = downloads.files;
    expect(file.name).toBe('image.svg');
    expect(file.blob.type).toBe('image/svg+xml');
    const text = await file.blob.text();
    expect(text.startsWith('<?xml version="1.0" encoding="UTF-8"?>\n<svg xmlns="http://www.w3.org/2000/svg"')).toBe(true);
    expect(parseXml(text).querySelector('parsererror')).toBeNull();
  });

  it('saves nothing when there is no svg', () => {
    expect(downloadSvg('not markup', 'image.svg')).toBe(false);
    expect(downloads.createObjectURL).not.toHaveBeenCalled();
  });
});

describe('svgToPngBlob', () => {
  it('draws the svg at twice its size over the background', async () => {
    const drawing = fakeDrawing();
    const png = await svgToPngBlob('<svg width="300" height="100"><text>hi</text></svg>', 2, '#202020');
    expect(png.type).toBe('image/png');
    expect(await png.text()).toBe('600x200');
    expect(drawing.fills).toEqual([{ style: '#202020', rect: [0, 0, 600, 200] }]);
    expect(drawing.draws).toEqual([{ src: drawing.images[0], rect: [0, 0, 600, 200] }]);
    // Loaded as a data: URL, as a standalone file.
    const prefix = 'data:image/svg+xml;charset=utf-8,';
    expect(drawing.images[0].startsWith(prefix)).toBe(true);
    const file = decodeURIComponent(drawing.images[0].slice(prefix.length));
    expect(file).toContain(`xmlns="${SVG_NS}"`);
    expect(file).toContain('<text>hi</text>');
  });

  it('draws at the size it is given, scaling a viewBox-only svg to it', async () => {
    const drawing = fakeDrawing();
    await svgToPngBlob('<svg viewBox="0 0 10 5"></svg>', 2, null, { width: 400, height: 200 });
    expect([drawing.canvases[0].width, drawing.canvases[0].height]).toEqual([800, 400]);
    // No background asked for: nothing painted under the drawing.
    expect(drawing.fills).toEqual([]);
    const file = decodeURIComponent(drawing.images[0].split(',').slice(1).join(','));
    expect(file).toContain('width="400"');
    expect(file).toContain('viewBox="0 0 10 5"');
  });

  it('lowers the scale for a picture too large for a canvas', async () => {
    const drawing = fakeDrawing();
    await svgToPngBlob('<svg width="20000" height="1000"></svg>', 2, '#000000');
    const [canvas] = drawing.canvases;
    expect(canvas.width).toBe(16384);
    expect(canvas.height).toBe(819);
  });

  it('fails when the picture cannot be decoded', async () => {
    fakeDrawing({ decodes: false });
    await expect(svgToPngBlob('<svg width="10" height="10"></svg>', 2, '#000000')).rejects.toThrow(/decoded/);
  });
});

describe('pngOver', () => {
  it('composites a PNG over the background at its own size', async () => {
    const drawing = fakeDrawing();
    const png = await pngOver(new Blob(['raw'], { type: 'image/png' }), '#fdfdfd');
    expect(await png.text()).toBe('640x360');
    expect(drawing.images).toEqual(['blob:test/1']);
    expect(drawing.fills).toEqual([{ style: '#fdfdfd', rect: [0, 0, 640, 360] }]);
    expect(downloads.revokeObjectURL).toHaveBeenCalledWith('blob:test/1');
  });
});
