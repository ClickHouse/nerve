import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { readThemeTokens } from './tokens';
import { assertSelfContained, mountVega, parseSpec, tooltipHtml } from './vega';
import { TOOLTIP_THEME, vegaConfigFor } from './vegaTheme';

// vega-embed, stubbed: it records what it is asked to draw and with which
// options, and draws an empty <svg> of the size Vega would. `vega` carries
// Vega's log levels, as the real module's re-export does.
const embed = vi.hoisted(() => vi.fn());
vi.mock('vega-embed', () => ({ default: embed, vega: { None: 0, Error: 1, Warn: 2 } }));

interface EmbedOptions {
  mode?: string;
  renderer?: string;
  logLevel?: number;
  actions?: unknown;
  theme?: unknown;
  config?: Record<string, Record<string, unknown>>;
  loader?: { load: (uri: string) => Promise<string>; sanitize: (uri: string) => Promise<unknown> };
  tooltip?: { theme?: string; formatTooltip?: unknown };
}

function stubView() {
  const view = {
    toSVG: vi.fn(async () => '<svg xmlns="http://www.w3.org/2000/svg"></svg>'),
    toImageURL: vi.fn(async () => 'data:image/png;base64,'),
    toCanvas: vi.fn(async () => Object.assign(document.createElement('canvas'), { width: 40, height: 20 })),
    width: vi.fn(() => view),
    resize: vi.fn(() => view),
    runAsync: vi.fn(async () => view),
  };
  return view;
}

let view: ReturnType<typeof stubView>;
let finalize: ReturnType<typeof vi.fn>;

beforeEach(() => {
  view = stubView();
  finalize = vi.fn();
  embed.mockReset();
  embed.mockImplementation(async (el: HTMLElement, spec: Record<string, unknown>) => {
    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('width', String(typeof spec.width === 'number' ? spec.width + 66 : 600));
    svg.setAttribute('height', String(typeof spec.height === 'number' ? spec.height + 27 : 300));
    el.appendChild(svg);
    return { view, finalize };
  });
});

afterEach(() => {
  vi.restoreAllMocks();
});

const BAR = {
  data: { values: [{ week: 'W40', usd: 612 }, { week: 'W41', usd: 744 }] },
  mark: 'bar',
  encoding: { x: { field: 'week', type: 'ordinal' }, y: { field: 'usd', type: 'quantitative' } },
};

async function mount(spec: unknown, { mode = 'vega-lite', theme = 'dark', width = 0, stage = document.createElement('div') } = {}) {
  const mounted = await mountVega(mode as 'vega' | 'vega-lite')(stage, JSON.stringify(spec), { theme: theme as 'dark' | 'light', width });
  const [el, drawn, options] = embed.mock.calls.at(-1) as [HTMLElement, Record<string, unknown>, EmbedOptions];
  return { mounted, stage, el, drawn, options };
}

describe('assertSelfContained', () => {
  it('rejects data loaded from a URL, wherever the data sits', () => {
    expect(() => assertSelfContained({ data: { url: 'https://example.invalid/d.json' }, mark: 'bar' }))
      .toThrow(/spec\.data\.url points outside the spec/);
    expect(() => assertSelfContained({ layer: [{ data: { url: 'd.csv' }, mark: 'line' }] }))
      .toThrow(/spec\.layer\[0\]\.data\.url/);
    expect(() => assertSelfContained({
      data: { values: [] },
      transform: [{ lookup: 'id', from: { data: { url: 'https://example.invalid/l.json' }, key: 'id' } }],
    })).toThrow(/transform\[0\]\.from\.data\.url/);
    // A full Vega spec lists its data sets.
    expect(() => assertSelfContained({ data: [{ name: 'table', url: 'table.json' }], marks: [] }))
      .toThrow(/spec\.data\[0\]\.url/);
  });

  it('lets inline data through, even a column called url', () => {
    expect(() => assertSelfContained({
      data: { values: [{ url: 'https://example.invalid/page', hits: 3 }] },
      mark: 'bar',
      encoding: { x: { field: 'url' }, y: { field: 'hits' } },
    })).not.toThrow();
    expect(() => assertSelfContained({
      datasets: { table: [{ url: 'https://example.invalid/page' }] },
      data: { name: 'table' },
      mark: 'point',
      encoding: { url: { field: 'img' } },
    })).not.toThrow();
  });
});

describe('parseSpec', () => {
  it('wants a JSON object', () => {
    expect(() => parseSpec('not json')).toThrow(/not valid JSON/);
    expect(() => parseSpec('[1, 2]')).toThrow(/must be a JSON object/);
    expect(() => parseSpec('"bar"')).toThrow(/must be a JSON object/);
  });

  it('drops usermeta, which vega-embed would read embed options from', () => {
    const spec = parseSpec(JSON.stringify({
      usermeta: { embedOptions: { actions: { editor: true }, editorUrl: 'https://example.invalid' } },
      mark: 'bar',
      data: { values: [{ a: 1 }] },
    }));
    expect(spec).not.toHaveProperty('usermeta');
    expect(spec).toHaveProperty('mark', 'bar');
  });
});

describe('mountVega', () => {
  it('embeds with the chat theme, no action menu and a loader that refuses everything', async () => {
    const { el, options } = await mount(BAR);
    expect(embed).toHaveBeenCalledTimes(1);
    expect(options.actions).toBe(false);
    expect(options.mode).toBe('vega-lite');
    expect(options.renderer).toBe('svg');
    // Errors only: no console warnings for what a spec gets away with.
    expect(options.logLevel).toBe(1);
    // The app's own config, not a vega-themes theme.
    expect(options.theme).toBeUndefined();
    const tokens = readThemeTokens('dark');
    expect(options.config).toEqual(vegaConfigFor(tokens, 'vega-lite'));
    expect(options.config?.axis.labelColor).toBe(tokens.textMuted);
    expect(options.config?.range.category).toEqual(tokens.palette);
    await expect(options.loader?.load('https://example.invalid/d.json')).rejects.toThrow(/load nothing/);
    await expect(options.loader?.sanitize('https://example.invalid/i.png')).rejects.toThrow(/load nothing/);
    expect(options.tooltip).toEqual({ theme: TOOLTIP_THEME, formatTooltip: tooltipHtml });
    expect(el.tagName).toBe('DIV');
  });

  it('themes each theme in its own colours', async () => {
    const { options } = await mount(BAR, { theme: 'light' });
    expect(options.config?.axis.labelColor).toBe(readThemeTokens('light').textMuted);
    expect(readThemeTokens('light').textMuted).not.toBe(readThemeTokens('dark').textMuted);
  });

  it('hands vega-embed the spec with the chat sizing and without usermeta', async () => {
    const { drawn } = await mount({ ...BAR, usermeta: { embedOptions: { actions: true } } });
    expect(drawn).not.toHaveProperty('usermeta');
    expect(drawn).toMatchObject({ width: 'container', height: 280, autosize: { type: 'fit-x', contains: 'padding' } });
  });

  it('refuses a spec that points outside itself before anything is embedded', async () => {
    const spec = { data: { url: 'https://example.invalid/d.json' }, mark: 'bar' };
    await expect(mountVega('vega-lite')(document.createElement('div'), JSON.stringify(spec), { theme: 'dark', width: 0 }))
      .rejects.toThrow(/points outside the spec/);
    expect(embed).not.toHaveBeenCalled();
  });

  it('lets the spec\'s $schema name its library over the fence', async () => {
    const { options } = await mount({ ...BAR, $schema: 'https://vega.github.io/schema/vega-lite/v6.json' }, { mode: 'vega' });
    expect(options.mode).toBe('vega-lite');
    const vega = await mount({ marks: [] }, { mode: 'vega' });
    expect(vega.options.mode).toBe('vega');
  });

  it('has no natural size while it fills its container, and the drawn size when it does not', async () => {
    const container = await mount(BAR);
    expect(container.mounted.naturalSize).toBeUndefined();
    embed.mockClear();
    const fixed = await mount({ ...BAR, width: 420, height: 180 });
    expect(fixed.mounted.naturalSize).toEqual({ width: 486, height: 207 });
  });

  it('puts the legend on top on a narrow stage', async () => {
    const stage = document.createElement('div');
    Object.defineProperty(stage, 'clientWidth', { value: 360 });
    const { options } = await mount(BAR, { stage });
    expect(options.config?.legend).toMatchObject({ orient: 'top' });
  });

  it('exports SVG from the view, and PNG at the asked scale over the chart\'s surface', async () => {
    const { mounted } = await mount(BAR);
    await expect(mounted.toSvg?.()).resolves.toContain('<svg');
    expect(view.toSVG).toHaveBeenCalled();

    const ctx = { fillStyle: '', fillRect: vi.fn(), drawImage: vi.fn() };
    vi.spyOn(HTMLCanvasElement.prototype, 'getContext').mockReturnValue(ctx as unknown as CanvasRenderingContext2D);
    const png = new Blob(['png'], { type: 'image/png' });
    vi.spyOn(HTMLCanvasElement.prototype, 'toBlob').mockImplementation(function (this: HTMLCanvasElement, done: BlobCallback) {
      done(png);
    });
    await expect(mounted.toPng?.(2)).resolves.toBe(png);
    expect(view.toCanvas).toHaveBeenCalledWith(2);
    // A dark chart's light text stays readable as a file: the surface is painted first.
    expect(ctx.fillStyle).toBe(readThemeTokens('dark').surface);
    expect(ctx.fillRect).toHaveBeenCalledWith(0, 0, 40, 20);
    expect(ctx.drawImage).toHaveBeenCalledTimes(1);
  });

  it('re-lays a container-sized chart out at the stage\'s width on resize', async () => {
    const stage = document.createElement('div');
    const { mounted } = await mount(BAR, { stage });
    Object.defineProperty(stage, 'clientWidth', { value: 640, configurable: true });
    await mounted.resize?.();
    expect(view.width).toHaveBeenCalledWith(640);
    expect(view.resize).toHaveBeenCalled();
    expect(view.runAsync).toHaveBeenCalled();
  });

  it('leaves a fixed width alone on resize', async () => {
    const stage = document.createElement('div');
    Object.defineProperty(stage, 'clientWidth', { value: 640 });
    const { mounted } = await mount({ ...BAR, width: 420 }, { stage });
    await mounted.resize?.();
    expect(view.width).not.toHaveBeenCalled();
    expect(view.resize).toHaveBeenCalled();
  });

  it('takes the frame\'s width when the stage is not laid out yet', async () => {
    await mount(BAR, { width: 700 });
    expect(view.width).toHaveBeenCalledWith(700);
  });

  it('cleans up: finalizes the view and empties the stage', async () => {
    const { mounted, stage } = await mount(BAR);
    expect(stage.childElementCount).toBe(1);
    mounted.cleanup();
    expect(finalize).toHaveBeenCalled();
    expect(stage.childElementCount).toBe(0);
  });

  it('installs the tooltip style once', async () => {
    await mount(BAR);
    embed.mockClear();
    await mount(BAR);
    expect(document.head.querySelectorAll('#nerve-vega-tooltip-style')).toHaveLength(1);
    expect(document.getElementById('nerve-vega-tooltip-style')?.textContent).toContain(`${TOOLTIP_THEME}-theme`);
  });
});

describe('tooltipHtml', () => {
  // vega-tooltip's own sanitizer.
  const escape = (value: unknown) => String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;');

  it('lays a datum out as a title and a table of fields', () => {
    const html = tooltipHtml({ title: 'W41', week: 'W41', usd: 744, skipped: undefined }, escape, 2);
    expect(html).toBe(
      '<h2>W41</h2><table>'
      + '<tr><td class="key">week</td><td class="value">W41</td></tr>'
      + '<tr><td class="key">usd</td><td class="value">744</td></tr>'
      + '</table>',
    );
  });

  it('shows an image field as text instead of loading it', () => {
    const html = tooltipHtml({ image: 'https://example.invalid/pixel.png', n: 1 }, escape, 2);
    expect(html).not.toContain('<img');
    expect(html).toContain('<td class="key">image</td><td class="value">https://example.invalid/pixel.png</td>');
  });

  it('escapes markup in keys and values', () => {
    const html = tooltipHtml({ '<b>key</b>': '<img src=x onerror=alert(1)>' }, escape, 2);
    expect(html).not.toContain('<img');
    expect(html).not.toContain('<b>');
    expect(html).toContain('&lt;img src=x');
  });

  it('prints arrays, nested values and plain values the way vega-tooltip does', () => {
    expect(tooltipHtml(['a', { b: 1 }], escape, 2)).toBe('[a, {"b":1}]');
    expect(tooltipHtml({ deep: { a: { b: { c: 1 } } } }, escape, 1)).toContain('{"a":{"b":"[Object]"}}');
    expect(tooltipHtml(42, escape, 2)).toBe('42');
    expect(tooltipHtml({}, escape, 2)).toBe('{}');
  });
});
