import { parse, View } from 'vega';
import { compile, type TopLevelSpec } from 'vega-lite';
import { describe, expect, it } from 'vitest';
import { luminance, readThemeTokens, type ThemeTokens } from './tokens';
import {
  contrast,
  DATE_LABELS,
  DEFAULT_HEIGHT,
  isContainerSized,
  schemaMode,
  TOOLTIP_THEME,
  tooltipCss,
  vegaConfigFor,
  withDefaults,
} from './vegaTheme';

const FONT = 'Inter, ui-sans-serif, system-ui, sans-serif';

// The app's tokens as the browser resolves them, in both themes, the palette
// in tokens.ts's order: blue, orange, emerald, cyan, yellow, purple, teal,
// red, violet, sky.
const DARK: ThemeTokens = {
  theme: 'dark',
  bg: '#1f1f1c',
  surface: '#282828',
  surfaceRaised: '#2d2d2d',
  border: '#323232',
  text: '#ffffff',
  textMuted: '#bfbfbf',
  textFaint: '#979797',
  accent: '#faff69',
  palette: ['#437eef', '#ff9457', '#66ff73', '#33e4ff', '#faff69', '#cc66ff', '#6df8e1', '#ff7575', '#dd99ff', '#6d9bf3'],
  fontFamily: FONT,
};
const LIGHT: ThemeTokens = {
  theme: 'light',
  bg: '#f6f7fa',
  surface: '#ffffff',
  surfaceRaised: '#fafbfc',
  border: '#e6e7e9',
  text: '#161517',
  textMuted: '#4b4a4c',
  textFaint: '#686869',
  accent: '#151515',
  palette: ['#104ec6', '#a33c00', '#007109', '#006170', '#856500', '#660099', '#067462', '#c10000', '#8800cc', '#1d64ec'],
  fontFamily: FONT,
};

type Block = Record<string, unknown>;

/** A config section, for reading values the tests pin down. */
function section(config: object, key: string): Block {
  return ((config as Record<string, unknown>)[key] ?? {}) as Block;
}

/** Warm reads as more red than blue, cool as more blue than red. */
function warmth(hex: string): number {
  const n = parseInt(hex.slice(1), 16);
  return ((n >> 16) & 255) - (n & 255);
}

/** Every string in the config that is meant as a colour. */
function colours(value: unknown, out: string[] = []): string[] {
  if (typeof value === 'string' && /^#|^rgb|^hsl/i.test(value)) out.push(value);
  else if (Array.isArray(value)) value.forEach(v => colours(v, out));
  else if (value && typeof value === 'object') Object.values(value).forEach(v => colours(v, out));
  return out;
}

describe('vegaConfigFor', () => {
  it.each([DARK, LIGHT])('draws a $theme chart in the app colours, as hex', (tokens) => {
    const config = vegaConfigFor(tokens);
    for (const colour of colours(config)) expect(colour).toMatch(/^#[0-9a-f]{6}$/);

    expect(config.background).toBe('transparent');
    expect(config.font).toBe(FONT);
    expect(section(config, 'view').stroke).toBeNull();

    const axis = section(config, 'axis');
    expect(axis).toMatchObject({
      labelColor: tokens.textMuted, labelFontSize: 11, labelFont: FONT, labelPadding: 6,
      titleColor: tokens.text, titleFontSize: 12, titleFontWeight: 500, titlePadding: 8,
    });
    expect(section(config, 'legend')).toMatchObject({
      labelColor: tokens.textMuted, titleColor: tokens.text, symbolSize: 80, orient: 'right',
    });
    expect(section(config, 'title')).toMatchObject({
      color: tokens.text, fontSize: 14, fontWeight: 600, anchor: 'start', offset: 12,
      subtitleColor: tokens.textMuted,
    });
    expect(section(config, 'text')).toMatchObject({ color: tokens.text, fill: tokens.text });
  });

  it.each([DARK, LIGHT])('gives a single $theme series the first palette hue, never the accent', (tokens) => {
    const config = vegaConfigFor(tokens);
    expect(section(config, 'mark').color).toBe(tokens.palette[0]);
    expect(section(config, 'mark').color).not.toBe(tokens.accent);
    expect(section(config, 'bar')).toMatchObject({ cornerRadiusEnd: 3 });
    expect(section(config, 'line')).toMatchObject({ strokeWidth: 2 });
    expect(section(config, 'point')).toMatchObject({ filled: true, size: 60 });
    // Slices and stacked segments are parted by the surface itself.
    expect(section(config, 'arc').stroke).toBe(tokens.surface);
    expect(section(config, 'bar').stroke).toBe(tokens.surface);
  });

  it('takes the palette, in order, as the categorical range', () => {
    const range = section(vegaConfigFor(LIGHT), 'range');
    expect(range.category).toEqual(LIGHT.palette);
    // A copy: Vega must not be handed the cached tokens' own array.
    expect(range.category).not.toBe(LIGHT.palette);
  });

  it.each([DARK, LIGHT])('keeps the $theme grid a hairline and the baseline a step stronger, on the surface', (tokens) => {
    const axis = section(vegaConfigFor(tokens), 'axis');
    const grid = contrast(axis.gridColor as string, tokens.surface);
    const domain = contrast(axis.domainColor as string, tokens.surface);
    expect(grid).toBeGreaterThanOrEqual(1.24);
    expect(grid).toBeLessThan(1.4);
    expect(domain).toBeGreaterThan(grid);
    expect(axis.tickColor).toBe(axis.domainColor);
  });

  it('does not lose the grid where the border token is fainter than the surface needs (dark)', () => {
    // The dark border is 1.15:1 on the figure and the page background is
    // darker still: a border faded towards it would draw nothing.
    expect(contrast(DARK.border, DARK.surface)).toBeLessThan(1.2);
    const grid = section(vegaConfigFor(DARK), 'axis').gridColor as string;
    expect(contrast(grid, DARK.surface)).toBeGreaterThanOrEqual(1.24);
  });

  it.each([DARK, LIGHT])('runs the $theme sequential ramps from the surface towards the text', (tokens) => {
    const range = section(vegaConfigFor(tokens), 'range');
    const ramp = range.ramp as string[];
    const ordinal = (range.ordinal as { scheme: string[] }).scheme;
    expect(ramp[1]).toBe(tokens.palette[0]);
    // "More" is more ink: away from the surface's luminance at the high end.
    const away = (c: string) => Math.abs(luminance(c) - luminance(tokens.surface));
    expect(away(ramp[2])).toBeGreaterThan(away(ramp[0]));
    // An ordinal step is a mark, so even the palest still reads.
    expect(contrast(ordinal[0], tokens.surface)).toBeGreaterThanOrEqual(2);
    expect(contrast((range.heatmap as string[])[0], tokens.surface)).toBeLessThan(contrast(ordinal[0], tokens.surface));
  });

  it.each([DARK, LIGHT])('diverges in $theme from a warm pole to a cool one through a near-surface neutral', (tokens) => {
    const [warm, neutral, cool] = section(vegaConfigFor(tokens), 'range').diverging as string[];
    expect(warm).toBe(tokens.palette[1]);
    expect(cool).toBe(tokens.palette[0]);
    // Two poles that read as opposites; blue against cyan would both be cool.
    expect(warmth(warm)).toBeGreaterThan(0);
    expect(warmth(cool)).toBeLessThan(0);
    // Each pole stands clear of the surface, the midpoint barely does.
    expect(contrast(warm, tokens.surface)).toBeGreaterThanOrEqual(3);
    expect(contrast(cool, tokens.surface)).toBeGreaterThanOrEqual(3);
    expect(contrast(neutral, tokens.surface)).toBeLessThan(1.5);
  });

  it('gives a full Vega spec its marks\' colours, and Vega-Lite none that would outrank a spec', () => {
    const vegaLite = vegaConfigFor(LIGHT, 'vega-lite');
    for (const mark of ['rect', 'symbol', 'path', 'shape', 'trail']) expect(vegaLite).not.toHaveProperty(mark);
    expect(section(vegaLite, 'line')).not.toHaveProperty('stroke');
    expect(section(vegaLite, 'area')).not.toHaveProperty('fill');

    const vega = vegaConfigFor(LIGHT, 'vega');
    for (const mark of ['rect', 'symbol', 'arc', 'area', 'trail']) expect(section(vega, mark).fill).toBe(LIGHT.palette[0]);
    for (const mark of ['line', 'path', 'shape']) expect(section(vega, mark).stroke).toBe(LIGHT.palette[0]);
    expect(section(vega, 'arc').stroke).toBe(LIGHT.surface);
    // Vega has no line drawn over an area, so its areas are not a pale wash.
    expect(section(vega, 'area')).not.toHaveProperty('fillOpacity');
  });

  it('puts the legend above a narrow chart, in rows', () => {
    expect(section(vegaConfigFor(DARK, 'vega-lite', { narrow: true }), 'legend')).toMatchObject({ orient: 'top', columns: 3 });
    expect(section(vegaConfigFor(DARK), 'legend')).not.toHaveProperty('columns');
  });

  it('works from the fallback tokens too (no stylesheet)', () => {
    for (const theme of ['dark', 'light'] as const) {
      const tokens = readThemeTokens(theme);
      const config = vegaConfigFor(tokens);
      expect(section(config, 'mark').color).toBe(tokens.palette[0]);
      for (const colour of colours(config)) expect(colour).toMatch(/^#[0-9a-f]{6}$/);
    }
  });
});

describe('withDefaults', () => {
  const values = [{ a: 'A', b: 28 }, { a: 'B', b: 55 }];
  const plain = { data: { values }, mark: 'bar', encoding: { x: { field: 'a', type: 'nominal' }, y: { field: 'b', type: 'quantitative' } } };

  it('fills the column and gives the plot a height when the spec names neither', () => {
    const before = JSON.stringify(plain);
    const out = withDefaults(plain, 'vega-lite');
    expect(out).toMatchObject({
      width: 'container',
      autosize: { type: 'fit-x', contains: 'padding' },
      height: DEFAULT_HEIGHT,
    });
    expect(isContainerSized(out)).toBe(true);
    expect(JSON.stringify(plain)).toBe(before);
  });

  it('leaves a size the spec chose', () => {
    const out = withDefaults({ ...plain, width: 420, height: 180 }, 'vega-lite');
    expect(out).toMatchObject({ width: 420, height: 180 });
    expect(out).not.toHaveProperty('autosize');
    expect(isContainerSized(out)).toBe(false);
    expect(withDefaults({ ...plain, width: 420 }, 'vega-lite')).toMatchObject({ width: 420, height: DEFAULT_HEIGHT });
  });

  it('gives a container height a plot height: a chat has no height to fill, Vega would draw 0 px', () => {
    const out = withDefaults({ ...plain, height: 'container' }, 'vega-lite');
    expect(out).toMatchObject({ width: 'container', height: DEFAULT_HEIGHT, autosize: { type: 'fit-x', contains: 'padding' } });
    const horizontal = { ...plain, height: 'container', encoding: { y: { field: 'a', type: 'nominal' }, x: { field: 'b', type: 'quantitative' } } };
    expect(withDefaults(horizontal, 'vega-lite')).not.toHaveProperty('height');
  });

  it('keeps an autosize the spec set, and does not fill a container it cannot fit', () => {
    const padded = withDefaults({ ...plain, autosize: 'pad' }, 'vega-lite');
    expect(padded).not.toHaveProperty('width');
    expect(padded).toMatchObject({ autosize: 'pad', height: DEFAULT_HEIGHT });
    const fitted = withDefaults({ ...plain, autosize: { type: 'fit', resize: true } }, 'vega-lite');
    expect(fitted).toMatchObject({ width: 'container', autosize: { type: 'fit', resize: true } });
  });

  it.each([
    ['concat', { concat: [plain, plain] }],
    ['hconcat', { hconcat: [plain, plain] }],
    ['vconcat', { vconcat: [plain, plain] }],
    ['facet', { data: { values }, facet: { field: 'a' }, spec: { mark: 'bar' } }],
    ['repeat', { data: { values }, repeat: ['b'], spec: { mark: 'bar' } }],
    ['a facet channel', { ...plain, encoding: { ...plain.encoding, column: { field: 'a' } } }],
  ])('sizes nothing in a %s spec, whose views size themselves', (_, spec) => {
    const out = withDefaults(spec, 'vega-lite');
    expect(out).toEqual(spec);
  });

  it('lets a category y axis keep its 20 px a row', () => {
    const horizontal = { ...plain, encoding: { y: { field: 'a', type: 'nominal' }, x: { field: 'b', type: 'quantitative' } } };
    const out = withDefaults(horizontal, 'vega-lite');
    expect(out).toMatchObject({ width: 'container' });
    expect(out).not.toHaveProperty('height');
  });

  it('gives a pie a square instead of the column', () => {
    const pie = { data: { values }, mark: { type: 'arc', innerRadius: 50 }, encoding: { theta: { field: 'b', type: 'quantitative' }, color: { field: 'a' } } };
    const out = withDefaults(pie, 'vega-lite');
    expect(out).toMatchObject({ width: DEFAULT_HEIGHT, height: DEFAULT_HEIGHT });
    expect(out).not.toHaveProperty('autosize');
    expect(isContainerSized(out)).toBe(false);
  });

  it('lays short inline category labels level while they fit side by side', () => {
    const x = (withDefaults(plain, 'vega-lite').encoding as Block).x as Block;
    // Two one-letter labels: 2 × (6.5 + 12) px.
    expect(x.axis).toEqual({ labelAngle: { expr: 'width >= 37 ? 0 : 270' } });
  });

  it('leaves category labels alone where the spec decided or the categories are not inline', () => {
    const angled = { ...plain, encoding: { ...plain.encoding, x: { field: 'a', type: 'nominal', axis: { labelAngle: -45 } } } };
    expect(((withDefaults(angled, 'vega-lite').encoding as Block).x as Block).axis).toEqual({ labelAngle: -45 });
    const configured = { ...plain, config: { axisX: { labelAngle: 0 } } };
    expect(((withDefaults(configured, 'vega-lite').encoding as Block).x as Block)).not.toHaveProperty('axis');
    const named = { ...plain, data: { name: 'table' } };
    expect(((withDefaults(named, 'vega-lite').encoding as Block).x as Block)).not.toHaveProperty('axis');
    const noAxis = { ...plain, encoding: { ...plain.encoding, x: { field: 'a', type: 'nominal', axis: null } } };
    expect(((withDefaults(noAxis, 'vega-lite').encoding as Block).x as Block).axis).toBeNull();
  });

  it('gives a full Vega spec the container width as a signal, which is all Vega takes', () => {
    const vega = { data: [{ name: 't', values }], marks: [] };
    const out = withDefaults(vega, 'vega');
    expect(out).not.toHaveProperty('width');
    expect(out).toMatchObject({ autosize: { type: 'fit-x', contains: 'padding' }, height: DEFAULT_HEIGHT });
    const signal = (out.signals as Block[]).find(s => s.name === 'width')!;
    expect(signal.init).toContain('containerSize()[0]');
    expect(signal.on).toEqual(expect.arrayContaining([expect.objectContaining({ events: 'container:resize' })]));
    expect(isContainerSized(out)).toBe(true);

    expect(withDefaults({ ...vega, width: 400, height: 200 }, 'vega')).toEqual({ ...vega, width: 400, height: 200 });
    const ownSignal = { ...vega, signals: [{ name: 'width', value: 300 }] };
    expect(withDefaults(ownSignal, 'vega').signals).toEqual(ownSignal.signals);
  });

  describe('dates', () => {
    const days = [{ day: '2026-10-01', n: 1 }, { day: '2026-10-02', n: 3 }, { day: null, n: 0 }];
    const line = {
      data: { values: days },
      mark: 'line',
      encoding: {
        x: { field: 'day', type: 'temporal' },
        y: { field: 'n', type: 'quantitative' },
        tooltip: [{ field: 'day', type: 'temporal' }, { field: 'n' }],
      },
    };
    const parseOf = (spec: Record<string, unknown>) =>
      ((withDefaults(spec, 'vega-lite').data as Block).format as Block | undefined)?.parse;

    it('reads inline date-only values as local dates, not as UTC midnight', () => {
      expect(parseOf(line)).toEqual({ day: "date:'%Y-%m-%d'" });
      // The values themselves stay as written.
      expect((withDefaults(line, 'vega-lite').data as Block).values).toBe(days);
    });

    it('also inside layers and composite views', () => {
      const layered = { data: { values: days }, layer: [{ mark: 'line', encoding: line.encoding }] };
      expect(parseOf(layered)).toEqual({ day: "date:'%Y-%m-%d'" });
      const concat = { data: { values: days }, hconcat: [{ mark: 'line', encoding: line.encoding }] };
      expect(parseOf(concat)).toEqual({ day: "date:'%Y-%m-%d'" });
    });

    it('leaves alone what is not only dates, or what the spec parses or reads in UTC itself', () => {
      const timed = { ...line, data: { values: [...days, { day: '2026-10-03T12:00:00', n: 2 }] } };
      expect(parseOf(timed)).toBeUndefined();
      const parsed = { ...line, data: { values: days, format: { parse: { day: 'date' } } } };
      expect(parseOf(parsed)).toEqual({ day: 'date' });
      const unparsed = { ...line, data: { values: days, format: { parse: null } } };
      expect(parseOf(unparsed)).toBeNull();
      const utc = { ...line, encoding: { ...line.encoding, x: { field: 'day', type: 'temporal', timeUnit: 'utcyearmonthdate' } } };
      expect(parseOf(utc)).toBeUndefined();
      const pinned = { ...line, encoding: { ...line.encoding, x: { field: 'day', type: 'temporal', scale: { domain: ['2026-10-01', '2026-10-09'] } } } };
      expect(parseOf(pinned)).toBeUndefined();
      expect(parseOf({ ...line, encoding: { ...line.encoding, x: { field: 'day', type: 'ordinal' }, tooltip: undefined } })).toBeUndefined();
    });

    const axisOf = (spec: Record<string, unknown>, channel = 'x') =>
      (((withDefaults(spec, 'vega-lite').encoding as Block)[channel] as Block).axis);

    it('labels a time axis in one style, keeping what else the axis says', () => {
      expect(axisOf(line)).toEqual({ format: DATE_LABELS });
      const titled = { ...line, encoding: { ...line.encoding, x: { field: 'day', type: 'temporal', axis: { title: 'Day' } } } };
      expect(axisOf(titled)).toEqual({ title: 'Day', format: DATE_LABELS });
      const vertical = { ...line, encoding: { x: { field: 'n', type: 'quantitative' }, y: { field: 'day', type: 'temporal' } } };
      expect(axisOf(vertical, 'y')).toEqual({ format: DATE_LABELS });
    });

    it('keeps the labels of a time unit, of an axis the spec labels itself, and of a spec\'s own config', () => {
      const unit = { ...line, encoding: { ...line.encoding, x: { field: 'day', type: 'temporal', timeUnit: 'day' } } };
      expect(axisOf(unit)).toBeUndefined();
      const formatted = { ...line, encoding: { ...line.encoding, x: { field: 'day', type: 'temporal', axis: { format: '%d %b' } } } };
      expect(axisOf(formatted)).toEqual({ format: '%d %b' });
      const expressed = { ...line, encoding: { ...line.encoding, x: { field: 'day', type: 'temporal', axis: { labelExpr: 'datum.label' } } } };
      expect(axisOf(expressed)).toEqual({ labelExpr: 'datum.label' });
      const hidden = { ...line, encoding: { ...line.encoding, x: { field: 'day', type: 'temporal', axis: null } } };
      expect(axisOf(hidden)).toBeNull();
      expect(axisOf({ ...line, config: { axisTemporal: { format: '%b' } } })).toBeUndefined();
    });

    it('labels time axes inside layers and composite views', () => {
      const layered = withDefaults({ data: { values: days }, layer: [{ mark: 'line', encoding: line.encoding }] }, 'vega-lite');
      expect((((layered.layer as Block[])[0].encoding as Block).x as Block).axis).toEqual({ format: DATE_LABELS });
      const concat = withDefaults({ data: { values: days }, vconcat: [{ mark: 'line', encoding: line.encoding }, plain] }, 'vega-lite');
      const views = concat.vconcat as Block[];
      expect(((views[0].encoding as Block).x as Block).axis).toEqual({ format: DATE_LABELS });
      expect(views[1]).toBe(plain);
    });
  });

  it('tells the libraries apart by the spec when no mode is given', () => {
    expect(withDefaults(plain)).toMatchObject({ width: 'container' });
    expect(withDefaults({ marks: [] })).not.toHaveProperty('width');
    expect(schemaMode({ $schema: 'https://vega.github.io/schema/vega-lite/v6.json' })).toBe('vega-lite');
    expect(schemaMode({ $schema: 'https://vega.github.io/schema/vega/v6.json' })).toBe('vega');
    expect(schemaMode({})).toBeUndefined();
  });
});

describe('compiled by Vega-Lite', () => {
  const values = [{ a: 'A', b: 28 }, { a: 'B', b: 55 }];
  const plain = { data: { values }, mark: 'bar', encoding: { x: { field: 'a', type: 'nominal' }, y: { field: 'b', type: 'quantitative' } } };

  function compiled(spec: Record<string, unknown>) {
    return compile(withDefaults(spec, 'vega-lite') as unknown as TopLevelSpec, { config: vegaConfigFor(LIGHT) }).spec;
  }

  interface Mark { type: string; marks?: Mark[]; encode?: { update?: { fill?: { value?: string } } } }

  /** The bars' fill; rounded bars sit inside a group that clips their stack. */
  function barFill(spec: Record<string, unknown>): unknown {
    const find = (marks: Mark[]): Mark | undefined =>
      marks.map(m => (m.type === 'rect' ? m : find(m.marks ?? []))).find(Boolean);
    return find(compiled(spec).marks as Mark[])?.encode?.update?.fill?.value;
  }

  it('takes the config and the defaults: the column\'s width, level labels', () => {
    const vg = compiled(plain);
    const width = (vg.signals ?? []).find(s => s.name === 'width') as { init?: string } | undefined;
    expect(width?.init).toContain('containerSize()[0]');
    const bottom = (vg.axes ?? []).find(a => a.orient === 'bottom' && 'labelAngle' in a);
    expect(bottom?.labelAngle).toEqual({ signal: 'width >= 37 ? 0 : 270' });
    expect(barFill(plain)).toBe(LIGHT.palette[0]);
  });

  it('parses inline dates with a local format', () => {
    const line = {
      data: { values: [{ day: '2026-10-01', n: 1 }, { day: '2026-10-02', n: 3 }] },
      mark: 'line',
      encoding: { x: { field: 'day', type: 'temporal' }, y: { field: 'n', type: 'quantitative' } },
    };
    const json = JSON.stringify(compiled(line).data);
    expect(json).toContain("timeParse(datum[\\\"day\\\"],'%Y-%m-%d')");
    expect(json).not.toMatch(/toDate|utcParse/);
  });

  it('hands Vega the time axis format as a multi-format, and leaves a time unit\'s labels to Vega-Lite', () => {
    const line = {
      data: { values: [{ day: '2026-10-01', n: 1 }, { day: '2026-10-05', n: 3 }] },
      mark: 'line',
      encoding: { x: { field: 'day', type: 'temporal' }, y: { field: 'n', type: 'quantitative' } },
    };
    const bottom = (compiled(line).axes ?? []).find(a => a.orient === 'bottom' && a.labels !== false);
    expect(bottom?.format).toEqual(DATE_LABELS);
    const weekdays = { ...line, encoding: { ...line.encoding, x: { field: 'day', type: 'temporal', timeUnit: 'day' } } };
    const unitAxis = (compiled(weekdays).axes ?? []).find(a => a.orient === 'bottom' && a.labels !== false);
    expect(unitAxis?.format).not.toEqual(DATE_LABELS);
  });

  /** The labels Vega draws along the bottom axis, from a headless render. */
  async function bottomLabels(days: string[]): Promise<string[]> {
    const line = {
      // Wide enough that no label is dropped for overlap: jsdom has no
      // canvas, so Vega only estimates how wide text is.
      width: 600,
      data: { values: days.map((day, i) => ({ day, n: i })) },
      mark: 'line',
      encoding: { x: { field: 'day', type: 'temporal' }, y: { field: 'n', type: 'quantitative' } },
    };
    const view = new View(parse(compiled(line)), { renderer: 'none' });
    try {
      const svg = await view.toSVG();
      const axis = /<g class="mark-group role-axis"[^>]*aria-label="X-axis[\s\S]*?(?=<g class="mark-group role-axis"|$)/.exec(svg)?.[0] ?? '';
      const labels = /<g class="mark-text role-axis-label"[^>]*>([\s\S]*?)<\/g>/.exec(axis)?.[1] ?? '';
      return [...labels.matchAll(/<text[^>]*>([^<]*)<\/text>/g)].map(m => m[1]);
    } finally {
      view.finalize();
    }
  }

  it('draws a short date axis in one style: "Oct 01 · Oct 02 · Oct 03"', async () => {
    // Oct 1 starts a month and a quarter and Oct 4 is a Sunday: Vega's own
    // labels would be "October", "Fri 02", "Sat 03", "Oct 04".
    const labels = await bottomLabels(['2026-10-01', '2026-10-02', '2026-10-03', '2026-10-04', '2026-10-05']);
    expect(labels).toEqual(['Oct 01', 'Oct 02', 'Oct 03', 'Oct 04', 'Oct 05']);
  });

  it('marks the turn of a year on a date axis with the year', async () => {
    const labels = await bottomLabels(['2026-12-29', '2026-12-30', '2026-12-31', '2027-01-01', '2027-01-02']);
    expect(labels).toEqual(['Dec 29', 'Dec 30', 'Dec 31', '2027', 'Jan 02']);
  });

  it('still lets a spec colour its own marks', () => {
    expect(barFill({ ...plain, config: { mark: { color: '#ff0000' } } })).toBe('#ff0000');
    expect(barFill({ ...plain, mark: { type: 'bar', color: '#00ff00' } })).toBe('#00ff00');
  });
});

describe('tooltipCss', () => {
  it('styles the tooltip theme with the app variables', () => {
    const css = tooltipCss(FONT);
    expect(css).toContain(`#vg-tooltip-element.${TOOLTIP_THEME}-theme`);
    expect(css).toContain('var(--theme-surface-raised)');
    expect(css).toContain('var(--theme-text-muted)');
    expect(css).toContain(FONT);
  });
});
