import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useThemeStore } from '../../../stores/themeStore';
import { MarkdownStreamingContext } from '../streamingContext';
import { RichCodeBlock } from './RichCodeBlock';

import type { Mount, Mounted } from './types';

const { mountMermaid } = vi.hoisted(() => ({
  mountMermaid: vi.fn<(...args: Parameters<Mount>) => Promise<Mounted>>(),
}));
vi.mock('./mermaid', () => ({ mountMermaid }));

// The figure's background per theme; the real tokens resolve CSS through a canvas.
const SURFACE = { dark: '#101418', light: '#fdfdfd' } as const;
vi.mock('./tokens', () => ({
  readThemeTokens: (theme: 'dark' | 'light') => ({ surface: SURFACE[theme] }),
}));

function draw(stage: HTMLElement, source: string, extra: Partial<Mounted> = {}): Mounted {
  stage.innerHTML = `<svg><text>${source}</text></svg>`;
  return { cleanup: () => { stage.innerHTML = ''; }, ...extra };
}

/** A renderer that reports the picture's natural size, as mermaid's and the SVG one do. */
function drawSized(width: number, height: number, extra: Partial<Mounted> = {}) {
  mountMermaid.mockImplementation(async (stage, source) =>
    draw(stage, source, { naturalSize: { width, height }, ...extra }));
}

/** A renderer that draws only when the test says so. */
function drawLater() {
  let finish!: () => void;
  mountMermaid.mockImplementationOnce((stage, source) => new Promise(resolve => {
    finish = () => resolve(draw(stage, source));
  }));
  return () => finish();
}

/**
 * jsdom lays nothing out and has no ResizeObserver: the canvas reports the
 * column width and picture height set here (0 while hidden, as a browser
 * would), and `resizeColumn` plays the observer's part.
 */
let columnWidth = 700;
let pictureHeight = 0;
const observers = new Set<{ callback: ResizeObserverCallback; targets: Element[] }>();
class FakeResizeObserver {
  private entry: { callback: ResizeObserverCallback; targets: Element[] };
  constructor(callback: ResizeObserverCallback) {
    this.entry = { callback, targets: [] };
    observers.add(this.entry);
  }
  observe(target: Element) { this.entry.targets.push(target); }
  unobserve() {}
  disconnect() { observers.delete(this.entry); }
}
function resizeColumn(width: number, height = pictureHeight) {
  columnWidth = width;
  pictureHeight = height;
  act(() => {
    for (const { callback, targets } of observers) {
      const entries = targets.map(target => ({ target, contentRect: { width, height } }));
      callback(entries as unknown as ResizeObserverEntry[], {} as ResizeObserver);
    }
  });
}

/**
 * The app stylesheet's text. vitest stubs out CSS imports here (`css: false`),
 * `?raw` ones included, so it is read from disk. The specifier is built so
 * that the app's type-check, which has no Node types, does not look for it,
 * and the base goes through a variable because Vite rewrites a literal
 * `new URL('…', import.meta.url)` into a served asset's URL.
 */
async function stylesheet(): Promise<string> {
  const fs: { readFileSync(path: string, encoding: 'utf8'): string } = await import(/* @vite-ignore */ `node:${'fs'}`);
  const here = import.meta.url;
  return fs.readFileSync(decodeURIComponent(new URL('../../../index.css', here).pathname), 'utf8');
}

const canvasMeasure = (value: () => number) => ({
  configurable: true,
  get(this: HTMLElement) {
    return this.classList.contains('rich-figure-canvas') && !this.hidden ? value() : 0;
  },
});

beforeEach(() => {
  columnWidth = 700;
  pictureHeight = 0;
  vi.stubGlobal('ResizeObserver', FakeResizeObserver);
  Object.defineProperty(HTMLElement.prototype, 'clientWidth', canvasMeasure(() => columnWidth));
  Object.defineProperty(HTMLElement.prototype, 'clientHeight', canvasMeasure(() => pictureHeight));
  mountMermaid.mockReset();
  mountMermaid.mockImplementation(async (stage, source) => {
    if (source.includes('BAD')) throw new Error('boom on line 1');
    return draw(stage, source);
  });
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  delete (HTMLElement.prototype as { clientWidth?: number }).clientWidth;
  delete (HTMLElement.prototype as { clientHeight?: number }).clientHeight;
  observers.clear();
});

function renderBlock(source: string, streaming: boolean) {
  const ui = (text: string, live: boolean) => (
    <MarkdownStreamingContext.Provider value={live}>
      <RichCodeBlock kind="mermaid" source={text} />
    </MarkdownStreamingContext.Provider>
  );
  const result = render(ui(source, streaming));
  return {
    update: (text: string, live: boolean) => result.rerender(ui(text, live)),
    unmount: () => result.unmount(),
  };
}

const canvas = () => document.querySelector<HTMLElement>('.rich-figure-canvas')!;
const frame = () => document.querySelector<HTMLElement>('figure.rich-figure')!;
const shown = () => canvas().querySelector('.rich-stage:not([data-pending]) text')?.textContent;
const picture = () => canvas().querySelector<SVGSVGElement>('.rich-stage:not([data-pending]) svg')!;
const rendered = () => screen.findByRole('img', { name: 'Rendered mermaid' });
const button = (name: string | RegExp) => screen.getByRole('button', { name });
const noButton = (name: string | RegExp) => screen.queryByRole('button', { name });
/** The full-size view's picture, which lives in a shadow root under the viewport's content. */
const shadowOf = (viewport: HTMLElement) => viewport.firstElementChild!.firstElementChild!.shadowRoot!;

/** Object URLs and link clicks, which jsdom does not implement, caught on their way out. */
function catchDownloads() {
  const blobs = new Map<string, Blob>();
  const files: { name: string; blob: Blob }[] = [];
  let next = 0;
  const before = { createObjectURL: URL.createObjectURL, revokeObjectURL: URL.revokeObjectURL };
  Object.assign(URL, {
    createObjectURL: (blob: Blob) => {
      const url = `blob:test/${++next}`;
      blobs.set(url, blob);
      return url;
    },
    revokeObjectURL: () => {},
  });
  vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(function (this: HTMLAnchorElement) {
    files.push({ name: this.download, blob: blobs.get(this.href)! });
  });
  return { files, blobs, restore: () => Object.assign(URL, before) };
}

/** jsdom decodes no images and has no 2D context: a stand-in that records what was drawn. */
function fakeDrawing({ encodes = true } = {}) {
  const images: string[] = [];
  class FakeImage {
    onload: (() => void) | null = null;
    onerror: (() => void) | null = null;
    naturalWidth = 800;
    naturalHeight = 400;
    private source = '';
    get src() { return this.source; }
    set src(value: string) {
      this.source = value;
      images.push(value);
      queueMicrotask(() => this.onload?.());
    }
  }
  const canvases: HTMLCanvasElement[] = [];
  const fills: string[] = [];
  const context = {
    fillStyle: '',
    fillRect() { fills.push(this.fillStyle); },
    drawImage() {},
  };
  vi.stubGlobal('Image', FakeImage);
  vi.spyOn(HTMLCanvasElement.prototype, 'getContext').mockImplementation(function (this: HTMLCanvasElement) {
    canvases.push(this);
    return context as unknown as CanvasRenderingContext2D;
  } as never);
  vi.spyOn(HTMLCanvasElement.prototype, 'toBlob').mockImplementation(function (
    this: HTMLCanvasElement, callback: BlobCallback, type?: string,
  ) {
    callback(encodes ? new Blob([`${this.width}x${this.height}`], { type }) : null);
  });
  return { images, canvases, fills };
}

describe('RichCodeBlock', () => {
  it('holds a quiet placeholder while a complete block renders, then shows a figure with no code header', async () => {
    const finish = drawLater();
    renderBlock('graph A', false);
    // The renderer module loads first; the mount itself is a microtask later.
    await act(async () => {});
    expect(mountMermaid).toHaveBeenCalledTimes(1);
    // It renders out of sight, at the column's width.
    expect(mountMermaid.mock.calls[0][2].width).toBe(700);
    expect(canvas().querySelector('.rich-stage[data-pending]')).not.toBeNull();
    // No source flashing by: the frame, holding a place, says what is coming.
    const placeholder = screen.getByRole('figure', { name: 'Diagram' });
    expect(placeholder).toHaveAttribute('data-placeholder');
    expect(placeholder).toHaveAttribute('aria-busy', 'true');
    expect(placeholder).toHaveTextContent('Rendering…');
    expect(document.querySelector('pre')).toBeNull();
    expect(screen.queryByRole('img', { name: 'Rendered mermaid' })).toBeNull();
    expect(screen.queryByRole('status')).toBeNull();

    await act(async () => { finish(); });
    const figure = screen.getByRole('figure', { name: 'Diagram' });
    expect(figure).not.toHaveAttribute('data-placeholder');
    expect(figure).not.toHaveAttribute('aria-busy');
    expect(figure).toContainElement(screen.getByRole('img', { name: 'Rendered mermaid' }));
    expect(shown()).toBe('graph A');
    expect(canvas().querySelector('[data-pending]')).toBeNull();
    expect(document.querySelector('pre')).toBeNull();
    expect(screen.queryByText('Rendering…')).toBeNull();
    // No header: the language label goes with the source view.
    expect(screen.queryByText('mermaid')).toBeNull();
    const tools = screen.getByRole('group', { name: 'Diagram tools' });
    expect([...tools.querySelectorAll('button')].map(b => b.getAttribute('aria-label')))
      .toEqual(['Expand', 'Download SVG', 'Download PNG', 'Source']);
  });

  it('shows the source when the picture is slow to come, and the picture when it does', async () => {
    vi.useFakeTimers();
    const finish = drawLater();
    renderBlock('graph slow', false);
    await act(async () => {});
    await act(async () => { vi.advanceTimersByTime(1499); });
    expect(frame()).toHaveAttribute('data-placeholder');
    expect(document.querySelector('pre')).toBeNull();
    await act(async () => { vi.advanceTimersByTime(1); });
    expect(frame()).not.toHaveAttribute('data-placeholder');
    expect(screen.queryByRole('figure')).toBeNull();
    expect(document.querySelector('pre code')?.textContent).toBe('graph slow');
    expect(screen.getByText('rendering…')).toBeInTheDocument();

    await act(async () => { finish(); });
    expect(screen.getByRole('img', { name: 'Rendered mermaid' })).toBeInTheDocument();
    expect(document.querySelector('pre')).toBeNull();
  });

  it('shows the source of a failed block at once, without waiting', async () => {
    renderBlock('BAD', false);
    const note = await screen.findByRole('status');
    expect(note.textContent).toContain("Couldn't render this mermaid: boom on line 1");
    expect(frame()).not.toHaveAttribute('data-placeholder');
    expect(document.querySelector('pre code')?.textContent).toBe('BAD');
  });

  it('holds the place of a picture drawn before at the height it had', async () => {
    pictureHeight = 240;
    const first = renderBlock('graph seen before', false);
    await rendered();
    first.unmount();
    // Drawn again — the finished message replacing the streamed one.
    const finish = drawLater();
    renderBlock('graph seen before', false);
    await act(async () => {});
    expect(frame()).toHaveAttribute('data-placeholder');
    expect(frame().style.minHeight).toBe(`${240 + 34}px`);
    await act(async () => { finish(); });
    expect(frame().style.minHeight).toBe('');
  });

  it('keeps the previous picture on screen until the next one is ready', async () => {
    const { update } = renderBlock('graph A', false);
    await rendered();
    expect(shown()).toBe('graph A');

    const finish = drawLater();
    update('graph B', false);
    await act(async () => {});
    expect(mountMermaid).toHaveBeenCalledTimes(2);
    expect(canvas().hidden).toBe(false);
    expect(shown()).toBe('graph A');
    expect(canvas().querySelectorAll('.rich-stage[data-pending]')).toHaveLength(1);
    await act(async () => { finish(); });
    expect(shown()).toBe('graph B');
    expect(canvas().querySelectorAll('svg')).toHaveLength(1);
  });

  it('while streaming, renders again once the text stops changing', async () => {
    vi.useFakeTimers();
    const { update } = renderBlock('gra', true);
    // The text at mount is tried once; later text waits for a pause.
    await act(async () => {});
    expect(mountMermaid).toHaveBeenCalledTimes(1);
    update('graph', true);
    update('graph TD', true);
    await act(async () => { vi.advanceTimersByTime(400); });
    expect(mountMermaid).toHaveBeenCalledTimes(1);
    expect(frame()).not.toHaveAttribute('data-placeholder');
    update('graph TD\n  A-->B', true);
    await act(async () => { vi.advanceTimersByTime(400); });
    expect(mountMermaid).toHaveBeenCalledTimes(1);
    await act(async () => { vi.advanceTimersByTime(300); });
    expect(mountMermaid).toHaveBeenCalledTimes(2);
    expect(mountMermaid.mock.calls[1][1]).toBe('graph TD\n  A-->B');
  });

  it('keeps showing the source of a streamed block once the stream ends, until the picture is ready', async () => {
    mountMermaid.mockImplementation(() => new Promise(() => {}));
    const { update } = renderBlock('graph TD\n  A-->B', true);
    await act(async () => {});
    update('graph TD\n  A-->B', false);
    await act(async () => {});
    expect(frame()).not.toHaveAttribute('data-placeholder');
    expect(document.querySelector('pre code')?.textContent).toBe('graph TD\n  A-->B');
  });

  it('keeps quiet about a failure while streaming, and explains it after', async () => {
    const { update } = renderBlock('BAD', true);
    await act(async () => {});
    expect(screen.queryByRole('status')).toBeNull();
    expect(screen.getByText('renders when complete')).toBeInTheDocument();
    expect(document.querySelector('pre code')?.textContent).toBe('BAD');

    update('BAD', false);
    const note = await screen.findByRole('status');
    expect(note.textContent).toContain("Couldn't render this mermaid: boom on line 1");
    expect(screen.getByText("couldn't render")).toBeInTheDocument();
    expect(noButton(/^(Source|Rendered)$/)).toBeNull();
    expect(document.querySelector('pre code')?.textContent).toBe('BAD');
  });

  it('renders again when the theme changes', async () => {
    renderBlock('graph A', false);
    await rendered();
    const before = useThemeStore.getState().resolved;
    expect(mountMermaid.mock.calls[0][2].theme).toBe(before);
    const other = before === 'dark' ? 'light' : 'dark';
    await act(async () => { useThemeStore.setState({ resolved: other }); });
    expect(mountMermaid).toHaveBeenCalledTimes(2);
    expect(mountMermaid.mock.calls[1][2].theme).toBe(other);
    await act(async () => { useThemeStore.setState({ resolved: before }); });
  });

  it('switches between the figure and its source, handing focus to the way back', async () => {
    renderBlock('graph A', false);
    const img = await rendered();
    await userEvent.click(button('Source'));
    expect(img.hidden).toBe(true);
    expect(screen.queryByRole('figure')).toBeNull();
    expect(screen.getByText('mermaid')).toBeInTheDocument();
    expect(button('Copy source')).toBeInTheDocument();
    expect(noButton('Expand')).toBeNull();
    expect(document.querySelector('pre code')?.textContent).toBe('graph A');
    expect(button('Rendered')).toHaveFocus();

    await userEvent.keyboard('{Enter}');
    expect(img.hidden).toBe(false);
    expect(screen.getByRole('figure', { name: 'Diagram' })).toBeInTheDocument();
    expect(document.querySelector('pre')).toBeNull();
    expect(button('Source')).toHaveFocus();
    await userEvent.keyboard('{Enter}');
    expect(button('Rendered')).toHaveFocus();
  });

  it('names the figure after the title its picture carries', async () => {
    mountMermaid.mockImplementationOnce(async (stage) => {
      stage.innerHTML = '<svg><title>Request flow</title><text>A</text></svg>';
      return { cleanup: () => { stage.innerHTML = ''; } };
    });
    const first = renderBlock('graph titled', false);
    expect(await screen.findByRole('figure', { name: 'Diagram: Request flow' })).toBeInTheDocument();
    first.unmount();
    // Vega puts a spec's description on its container.
    mountMermaid.mockImplementationOnce(async (stage) => {
      stage.innerHTML = '<div role="graphics-document" aria-label="Spend per week"><svg></svg></div>';
      return { cleanup: () => { stage.innerHTML = ''; } };
    });
    renderBlock('graph described', false);
    expect(await screen.findByRole('figure', { name: 'Diagram: Spend per week' })).toBeInTheDocument();
  });

  describe('toolbar', () => {
    it('shows on hover or keyboard focus only, so a mouse click does not pin it over the picture', async () => {
      const css = await stylesheet();
      const rich = css.slice(css.indexOf('.rich-figure {'), css.indexOf('/* Code blocks */'));
      expect(rich.length).toBeGreaterThan(1000);
      expect(rich).toMatch(/\.rich-figure-toolbar \{[^}]*opacity: 0;/);
      expect(rich).toContain('.rich-figure:hover > .rich-figure-toolbar,\n.rich-figure:has(:focus-visible) > .rich-figure-toolbar { opacity: 1; }');
      expect(rich).not.toContain(':focus-within');
      // A short picture, and a screen without hover, give it a row of its own.
      expect(rich).toMatch(/\.rich-figure\[data-short\] > \.rich-figure-toolbar \{[^}]*position: static;[^}]*opacity: 1;/);
      expect(rich).toMatch(/@media \(hover: none\) \{[^@]*\.rich-figure-toolbar \{[^}]*position: static;[^}]*opacity: 1;/);
    });

    it('gets a row of its own above a picture too short to share its corner', async () => {
      pictureHeight = 49;
      renderBlock('graph short', false);
      await rendered();
      expect(frame()).toHaveAttribute('data-short');
      // Taller, as at 1:1: it overlays the corner again, on hover.
      resizeColumn(700, 180);
      expect(frame()).not.toHaveAttribute('data-short');
      resizeColumn(700, 60);
      expect(frame()).toHaveAttribute('data-short');
      // Clicking a tool leaves no mark that would keep the toolbar up.
      resizeColumn(700, 180);
      await userEvent.click(button('Expand'));
      await userEvent.keyboard('{Escape}');
      await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
      expect(frame()).not.toHaveAttribute('data-short');
      expect(frame().getAttributeNames().filter(n => n.startsWith('data-'))).toEqual([]);
    });
  });

  describe('sizing', () => {
    it('fits a picture a little wider than the column, and offers 1:1', async () => {
      drawSized(800, 400);
      renderBlock('graph A', false);
      await rendered();
      // 700 / 800 keeps 14px text above 11px: scaled to the column.
      expect([picture().style.width, picture().style.height, picture().style.maxWidth]).toEqual(['100%', 'auto', '100%']);
      expect(noButton('Fit to width')).toBeNull();
      await userEvent.click(button('Actual size (1:1)'));
      expect([picture().style.width, picture().style.height, picture().style.maxWidth]).toEqual(['800px', '400px', 'none']);
      expect(button('Fit to width').textContent).toBe('Fit');
      await userEvent.click(button('Fit to width'));
      expect(picture().style.width).toBe('100%');
      expect(button('Actual size (1:1)').textContent).toBe('1:1');
    });

    it('fits down to 0.8 and shows anything wider at 1:1, to scroll', async () => {
      drawSized(875, 300);
      const first = renderBlock('graph A', false);
      await rendered();
      expect(picture().style.width).toBe('100%');
      first.unmount();
      drawSized(1000, 300);
      renderBlock('graph A', false);
      await rendered();
      // 700 / 1000 = 0.7 would take 14px labels below 11px.
      expect([picture().style.width, picture().style.height]).toEqual(['1000px', '300px']);
      expect(button('Fit to width')).toBeInTheDocument();
    });

    it('gives a picture that fits its natural size, with no toggle', async () => {
      drawSized(500, 200);
      renderBlock('graph A', false);
      await rendered();
      expect([picture().style.width, picture().style.height]).toEqual(['500px', '200px']);
      expect(noButton(/Fit to width|Actual size/)).toBeNull();
    });

    it('leaves a picture whose size the renderer did not report as it was drawn', async () => {
      renderBlock('graph A', false);
      await rendered();
      expect(picture().getAttribute('style')).toBeNull();
      expect(noButton(/Fit to width|Actual size/)).toBeNull();
    });

    it('takes a reported size of nothing as no size at all', async () => {
      drawSized(0, 0);
      renderBlock('graph A', false);
      await rendered();
      expect(picture().getAttribute('style')).toBeNull();
    });

    it('follows the column, and lays a chart out again once the resizing stops', async () => {
      const resize = vi.fn();
      drawSized(800, 400, { resize });
      renderBlock('graph A', false);
      await rendered();
      expect(picture().style.width).toBe('100%');
      vi.useFakeTimers();

      // Wide enough now: the picture fits at its own size, and the toggle goes.
      resizeColumn(1200);
      expect(picture().style.width).toBe('800px');
      expect(noButton(/Fit to width|Actual size/)).toBeNull();
      act(() => { vi.advanceTimersByTime(100); });
      resizeColumn(1250);
      act(() => { vi.advanceTimersByTime(149); });
      expect(resize).not.toHaveBeenCalled();
      act(() => { vi.advanceTimersByTime(1); });
      expect(resize).toHaveBeenCalledTimes(1);

      // Narrow again: below 0.8 it goes to 1:1.
      resizeColumn(500);
      expect(picture().style.width).toBe('800px');
      expect(button('Fit to width')).toBeInTheDocument();
      act(() => { vi.advanceTimersByTime(150); });
      expect(resize).toHaveBeenCalledTimes(2);
      // The same width again is no change.
      resizeColumn(500);
      act(() => { vi.advanceTimersByTime(150); });
      expect(resize).toHaveBeenCalledTimes(2);
    });

    it('keeps the reader\'s choice when the picture is drawn again', async () => {
      drawSized(800, 400);
      renderBlock('graph A', false);
      await rendered();
      await userEvent.click(button('Actual size (1:1)'));
      const before = useThemeStore.getState().resolved;
      await act(async () => { useThemeStore.setState({ resolved: before === 'dark' ? 'light' : 'dark' }); });
      expect(mountMermaid).toHaveBeenCalledTimes(2);
      expect(picture().style.width).toBe('800px');
      expect(button('Fit to width')).toBeInTheDocument();
      await act(async () => { useThemeStore.setState({ resolved: before }); });
    });
  });

  describe('full size', () => {
    it('opens the renderer\'s export full size, and gives focus back on close', async () => {
      const toSvg = vi.fn(async () =>
        '<svg xmlns="http://www.w3.org/2000/svg" width="1000" height="400"><text>exported</text></svg>');
      drawSized(1000, 400, { toSvg });
      renderBlock('graph A', false);
      await rendered();
      const expand = button('Expand');
      await userEvent.click(expand);
      const dialog = await screen.findByRole('dialog', { name: 'Diagram' });
      const viewport = screen.getByRole('img', { name: 'Diagram' });
      const svg = shadowOf(viewport).querySelector('svg')!;
      expect(svg.textContent).toBe('exported');
      expect(svg.style.width).toBe('1000px');
      expect(dialog).toHaveTextContent('100%');

      await userEvent.click(screen.getByRole('button', { name: 'Zoom in' }));
      expect(dialog).toHaveTextContent('125%');
      await userEvent.keyboard('{Escape}');
      await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
      await waitFor(() => expect(expand).toHaveFocus());
    });

    it('falls back to the drawn svg when the renderer exports none', async () => {
      renderBlock('graph A', false);
      await rendered();
      await userEvent.click(button('Expand'));
      const viewport = await screen.findByRole('img', { name: 'Diagram' });
      expect(shadowOf(viewport).querySelector('svg text')?.textContent).toBe('graph A');
    });
  });

  describe('downloads', () => {
    let downloads: ReturnType<typeof catchDownloads>;
    beforeEach(() => { downloads = catchDownloads(); });
    afterEach(() => downloads.restore());

    it('saves an SVG file on the figure\'s background', async () => {
      const toSvg = vi.fn(async () =>
        '<svg xmlns="http://www.w3.org/2000/svg" width="300" height="100" viewBox="0 0 300 100"><text>exported</text></svg>');
      drawSized(300, 100, { toSvg });
      renderBlock('graph A', false);
      await rendered();
      await userEvent.click(button('Download SVG'));
      await waitFor(() => expect(downloads.files).toHaveLength(1));
      const [file] = downloads.files;
      expect(file.name).toBe('diagram.svg');
      expect(file.blob.type).toBe('image/svg+xml');
      const doc = new DOMParser().parseFromString(await file.blob.text(), 'image/svg+xml');
      expect(doc.querySelector('parsererror')).toBeNull();
      expect(doc.documentElement.firstElementChild?.getAttribute('style'))
        .toBe(`fill: ${SURFACE[useThemeStore.getState().resolved]}; stroke: none`);
      expect(doc.querySelector('text')?.textContent).toBe('exported');
    });

    it('saves the renderer\'s own PNG, flattened onto the background', async () => {
      const drawing = fakeDrawing();
      const toPng = vi.fn(async () => new Blob(['raw'], { type: 'image/png' }));
      drawSized(300, 100, { toPng });
      renderBlock('graph A', false);
      await rendered();
      await userEvent.click(button('Download PNG'));
      await waitFor(() => expect(downloads.files).toHaveLength(1));
      expect(toPng).toHaveBeenCalledWith(2);
      const [file] = downloads.files;
      expect(file.name).toBe('diagram.png');
      // Drawn from the renderer's PNG at its own size, over the surface colour.
      expect(drawing.images).toEqual(['blob:test/1']);
      expect(await file.blob.text()).toBe('800x400');
      expect(drawing.fills).toEqual([SURFACE[useThemeStore.getState().resolved]]);
    });

    it('draws a PNG from the SVG when the renderer makes none', async () => {
      const drawing = fakeDrawing();
      drawSized(300, 100);
      renderBlock('graph A', false);
      await rendered();
      await userEvent.click(button('Download PNG'));
      await waitFor(() => expect(downloads.files).toHaveLength(1));
      expect(downloads.files[0].name).toBe('diagram.png');
      // Twice the natural size, from the drawn svg, over the surface colour.
      expect(await downloads.files[0].blob.text()).toBe('600x200');
      expect(drawing.images[0]).toMatch(/^data:image\/svg\+xml;charset=utf-8,/);
      expect(decodeURIComponent(drawing.images[0])).toContain('<text>graph A</text>');
      expect(drawing.fills).toEqual([SURFACE[useThemeStore.getState().resolved]]);
    });

    it('says so when the browser cannot make the PNG', async () => {
      fakeDrawing({ encodes: false });
      drawSized(300, 100);
      renderBlock('graph A', false);
      await rendered();
      await userEvent.click(button('Download PNG'));
      expect((await screen.findByRole('status')).textContent).toMatch(/Couldn't save this picture as PNG/);
      expect(downloads.files).toHaveLength(0);
    });
  });
});
