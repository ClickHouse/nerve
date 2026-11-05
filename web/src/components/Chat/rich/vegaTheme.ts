import type { Config as VegaConfig } from 'vega';
import type { Config as VegaLiteConfig } from 'vega-lite';
import { luminance, mix, type ThemeTokens } from './tokens';

/**
 * The chat's look for Vega and Vega-Lite charts: the app's colours and
 * typography instead of a stock theme, so that a chart reads as part of the
 * page rather than as an embedded widget — and the defaults a chart's spec
 * may leave open (its size, label angles, how its dates are read).
 *
 * All of it is pure: the config is computed from `ThemeTokens` alone and the
 * defaults from the spec alone, which is what lets the tests pin them down.
 * A chart is drawn on `tokens.surface`, the figure's background; `mountVega`
 * hands in the colour it actually finds behind the stage when it can.
 */

export type VegaMode = 'vega' | 'vega-lite';

/** WCAG contrast ratio of two hex colours, from 1 (none) to 21. */
export function contrast(a: string, b: string): number {
  const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x);
  return (hi + 0.05) / (lo + 0.05);
}

/**
 * `from` moved towards `to` for as long as it still passes `ok`. Along the way
 * the colours must pass less and less, which a contrast threshold against a
 * surface lying beyond both of them does. `from` itself when even it fails.
 */
function furthest(from: string, to: string, ok: (color: string) => boolean): string {
  if (!ok(from)) return from;
  if (ok(to)) return to;
  let pass = 0;
  let fail = 1;
  for (let i = 0; i < 20; i++) {
    const t = (pass + fail) / 2;
    if (ok(mix(from, to, t))) pass = t;
    else fail = t;
  }
  return mix(from, to, pass);
}

/** The colour closest to `faint`, on the way from `strong`, that still stands `ratio`:1 off `surface`. */
function atLeast(strong: string, faint: string, surface: string, ratio: number): string {
  return furthest(strong, faint, c => contrast(c, surface) >= ratio);
}

/*
 * Chart furniture is set by its contrast with the surface rather than by a
 * fixed mix of tokens. The app's border is 1.24:1 on the light figure but
 * only 1.15:1 on the dark one (#323232 on #282828), and in dark mode the page
 * background is darker than the figure, so "the border faded towards the
 * background" vanishes there. Gridlines sit a hairline off the surface, the
 * baseline a step above them; a sequential ramp's pale end stays a visible
 * mark (an ordinal step) or may all but melt into the surface (a heatmap's
 * zero).
 */
const GRID_CONTRAST = 1.25;
const DOMAIN_CONTRAST = 1.6;
const ORDINAL_FLOOR = 2;
const HEATMAP_FLOOR = 1.2;

/** A wash, not a block: the area's edge carries the series as a 2 px line. */
const AREA_FILL_OPACITY = 0.2;

/** Below this stage width a legend beside the plot would take the room the plot needs. */
export const NARROW_WIDTH = 480;

export interface ChartLayout {
  /** The stage is narrow (a phone): legends go above the plot, in rows. */
  narrow?: boolean;
}

/**
 * The config for a chart in the chat. Keys Vega and Vega-Lite share (`axis`,
 * `legend`, `title`, `range`, `style`, `text`, `rule`, `arc`, `line`) mean the
 * same in both. A full Vega spec (`mode: 'vega'`) also takes its marks'
 * default colours from per-mark keys; Vega-Lite would read those same keys as
 * defaults that outrank a spec's own `config.mark.color`, so it gets one
 * `mark.color` instead.
 */
export function vegaConfigFor(
  tokens: ThemeTokens,
  mode: VegaMode = 'vega-lite',
  { narrow = false }: ChartLayout = {},
): VegaLiteConfig {
  const { surface, text, textMuted, fontFamily: font, palette } = tokens;
  // A single series takes the first palette hue — never the accent, which is
  // the brand yellow in dark mode and near-black in light: poor marks both.
  const series = palette[0] ?? text;
  const grid = atLeast(text, tokens.border, surface, GRID_CONTRAST);
  const domain = atLeast(text, tokens.border, surface, DOMAIN_CONTRAST);
  // Sequential ramps run in one hue from the surface towards the text, so
  // "more" is "more ink" in both themes: light to dark on a light page, dark
  // to bright on a dark one.
  const rampEnd = mix(series, text, 0.5);
  const ordinalStart = furthest(series, surface, c => contrast(c, surface) >= ORDINAL_FLOOR);
  const heatmapStart = furthest(series, surface, c => contrast(c, surface) >= HEATMAP_FLOOR);
  // Diverging: a warm pole opposite the blue, through a neutral that reads
  // as "nothing" (the palette's fourth hue is its orange).
  const warm = palette[3] ?? rampEnd;
  const neutral = mix(surface, text, 0.1);
  const stroke = { strokeWidth: 2, strokeCap: 'round', strokeJoin: 'round' } as const;
  // Slices are told apart by a gap of the surface, not by an outline.
  const sliceGap = { stroke: surface, strokeWidth: 2 };

  const config: VegaLiteConfig = {
    background: 'transparent',
    font,
    // The figure is the frame; a box around the plotting area is chrome.
    view: { stroke: null },

    mark: { color: series },
    // A hairline of the surface between touching bars — stacked segments,
    // grouped bars, histogram bins — so neighbours read as two marks.
    bar: { cornerRadiusEnd: 3, stroke: surface, strokeWidth: 1 },
    line: stroke,
    point: { filled: true, size: 60 },
    circle: { size: 60 },
    square: { size: 60 },
    area: { fillOpacity: AREA_FILL_OPACITY, line: stroke },
    arc: sliceGap,
    // Vega-Lite's defaults for these two are black, which a dark page swallows.
    text: { color: text, fill: text, font, fontSize: 11 },
    rule: { color: textMuted, stroke: textMuted },

    axis: {
      domainColor: domain,
      tickColor: domain,
      gridColor: grid,
      domainWidth: 1,
      tickWidth: 1,
      gridWidth: 1,
      tickSize: 4,
      labelColor: textMuted,
      labelFont: font,
      labelFontSize: 11,
      labelPadding: 6,
      titleColor: text,
      titleFont: font,
      titleFontSize: 12,
      titleFontWeight: 500,
      titlePadding: 8,
    },
    // A value axis is read off its gridlines, which a rule and ticks along it
    // only repeat, and a handful of round values is enough (Vega-Lite's
    // default is one per 40 px: 20 labels across the column). A category
    // axis keeps the baseline its bars stand on.
    axisQuantitative: { domain: false, ticks: false, tickCount: 5 },
    // Time runs along its baseline and ticks; full-height lines for every
    // day would be a second grid to read through.
    axisTemporal: { grid: false, tickCount: 5 },
    axisBand: { ticks: false },

    legend: {
      ...(narrow ? { orient: 'top', columns: 3 } : { orient: 'right' }),
      labelColor: textMuted,
      labelFont: font,
      labelFontSize: 11,
      titleColor: text,
      titleFont: font,
      titleFontSize: 12,
      titleFontWeight: 500,
      titlePadding: 6,
      symbolSize: 80,
      rowPadding: 4,
      gradientStrokeColor: tokens.border,
      gradientStrokeWidth: 0,
      gradientThickness: 10,
    },
    header: {
      labelColor: textMuted,
      labelFont: font,
      labelFontSize: 11,
      titleColor: text,
      titleFont: font,
      titleFontSize: 12,
      titleFontWeight: 500,
    },
    title: {
      color: text,
      font,
      fontSize: 14,
      fontWeight: 600,
      anchor: 'start',
      offset: 12,
      subtitleColor: textMuted,
      subtitleFont: font,
      subtitleFontSize: 12,
      subtitlePadding: 4,
    },
    // Guide text no axis, legend or title setting reaches — a Vega spec's
    // hand-made guides, facet headers — lands on the same styles.
    style: {
      'guide-label': { font, fill: textMuted, fontSize: 11 },
      'guide-title': { font, fill: text, fontSize: 12, fontWeight: 500 },
      'group-title': { font, fill: text, fontSize: 14, fontWeight: 600 },
      'group-subtitle': { font, fill: textMuted, fontSize: 12 },
    },

    // A chart as wide as the column gives a few categories wide bands; air
    // between the bars keeps them from reading as blocks.
    scale: { barBandPaddingInner: 0.3 },

    range: {
      category: [...palette],
      ordinal: { scheme: [ordinalStart, series, rampEnd] },
      ramp: [ordinalStart, series, rampEnd],
      heatmap: [heatmapStart, series, rampEnd],
      diverging: [warm, neutral, series],
    },
  };
  if (mode === 'vega-lite') return config;

  // Vega has no `mark.color`, no area-with-line overlay to carry a pale fill,
  // and marks of its own (symbol, path, shape) that Vega-Lite does not.
  const marks: VegaConfig = {
    arc: { ...sliceGap, fill: series },
    area: { fill: series },
    line: { ...stroke, stroke: series },
    path: { stroke: series },
    rect: { fill: series },
    shape: { stroke: series },
    symbol: { fill: series, size: 60 },
    trail: { fill: series },
  };
  return { ...config, ...marks } as VegaLiteConfig;
}

type Spec = Record<string, unknown>;

/** Keys that make a Vega-Lite spec more than one view; those cannot take `width: "container"`. */
const COMPOSITE_KEYS = ['concat', 'hconcat', 'vconcat', 'facet', 'repeat'];
/** Encoding channels that facet a unit spec into a grid of views. */
const FACET_CHANNELS = ['row', 'column', 'facet'];

/** The plot height a single-view chart gets when its spec names none. */
export const DEFAULT_HEIGHT = 280;
/** The width a container-sized Vega chart takes while its container has none. */
const FALLBACK_WIDTH = 400;

function isRecord(value: unknown): value is Spec {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}

function autosizeType(autosize: unknown): string | undefined {
  if (typeof autosize === 'string') return autosize;
  if (isRecord(autosize) && typeof autosize.type === 'string') return autosize.type;
  return undefined;
}

/** Whether a Vega-Lite spec draws one view (a unit or a layer), the only kind that can fill a container. */
function isSingleView(spec: Spec): boolean {
  if (COMPOSITE_KEYS.some(key => key in spec)) return false;
  const encoding = isRecord(spec.encoding) ? spec.encoding : {};
  return !FACET_CHANNELS.some(channel => channel in encoding);
}

/**
 * Whether the view's y axis lists categories. Vega-Lite gives those 20 px a
 * category, which keeps a horizontal bar chart's bars slim and its labels
 * apart; a fixed height would stretch three bars into blocks and squeeze
 * thirty into slivers.
 */
function hasDiscreteY(spec: Spec): boolean {
  const views = [spec, ...(Array.isArray(spec.layer) ? spec.layer : [])];
  return views.some(view => {
    const y = isRecord(view) && isRecord(view.encoding) ? view.encoding.y : undefined;
    return isRecord(y) && (y.type === 'nominal' || y.type === 'ordinal');
  });
}

/**
 * Whether the view is a pie or a donut: angles, no positions. Its size is the
 * smaller of width and height, so the column's width would only push the
 * legend away from it; it takes a square instead.
 */
function isRadial(spec: Spec): boolean {
  const views = [spec, ...(Array.isArray(spec.layer) ? spec.layer : [])];
  const channels = views.flatMap(view => (isRecord(view) && isRecord(view.encoding) ? Object.keys(view.encoding) : []));
  return channels.includes('theta') && !channels.includes('x') && !channels.includes('y');
}

/** Roughly what a character of an 11 px Inter label takes, and the air two labels want between them. */
const LABEL_CHAR_PX = 6.5;
const LABEL_GAP_PX = 12;
/** Config blocks where a spec may have set its own x label angle. */
const LABEL_ANGLE_CONFIGS = ['axis', 'axisX', 'axisBand', 'axisXBand', 'axisDiscrete', 'axisXDiscrete', 'axisPoint', 'axisXPoint'];

/**
 * Vega-Lite stands a category axis's labels on end whatever their length —
 * "A", "B" and "C" included. Where the spec carries its categories inline,
 * they lie level for as long as the plot is wide enough to set them side by
 * side, and stand up only when it is not (a narrow column, many categories,
 * long names). An angle the spec chose itself is left alone.
 */
function withLevelLabels(spec: Spec): Spec {
  const encoding = isRecord(spec.encoding) ? spec.encoding : undefined;
  const x = encoding && isRecord(encoding.x) ? encoding.x : undefined;
  const field = x?.field;
  if (!encoding || !x || typeof field !== 'string') return spec;
  if ((x.type !== 'nominal' && x.type !== 'ordinal') || x.timeUnit !== undefined || x.bin !== undefined) return spec;
  if (x.axis === null || (isRecord(x.axis) && x.axis.labelAngle !== undefined)) return spec;
  const config = isRecord(spec.config) ? spec.config : {};
  if (LABEL_ANGLE_CONFIGS.some(key => {
    const block = config[key];
    return isRecord(block) && block.labelAngle !== undefined;
  })) return spec;

  const rows = isRecord(spec.data) && Array.isArray(spec.data.values) ? spec.data.values : [];
  const labels = new Set<string>();
  for (const row of rows) {
    const value = isRecord(row) ? row[field] : undefined;
    if (value !== undefined && value !== null) labels.add(String(value));
  }
  // The categories come from somewhere this cannot see (a transform, a
  // named dataset): Vega-Lite's default stands.
  if (labels.size === 0) return spec;
  const longest = Math.max(...[...labels].map(label => label.length));
  const needed = Math.ceil(labels.size * (longest * LABEL_CHAR_PX + LABEL_GAP_PX));
  const axis = isRecord(x.axis) ? x.axis : {};
  return {
    ...spec,
    encoding: { ...encoding, x: { ...x, axis: { ...axis, labelAngle: { expr: `width >= ${needed} ? 0 : 270` } } } },
  };
}

const DATE_ONLY = /^\d{4}-\d{2}-\d{2}$/;
const LOCAL_DATE = "date:'%Y-%m-%d'";

/**
 * Every field the spec draws as time, anywhere in it (layers, tooltips,
 * composite views). Null when one of them pins its scale's domain, which the
 * spec then states in the same UTC terms Vega-Lite reads its data in.
 */
function temporalFields(value: unknown, out = new Set<string>()): Set<string> | null {
  if (Array.isArray(value)) {
    for (const item of value) if (!temporalFields(item, out)) return null;
    return out;
  }
  if (!isRecord(value)) return out;
  if (value.type === 'temporal' && typeof value.field === 'string') {
    if (isRecord(value.scale) && value.scale.domain !== undefined) return null;
    out.add(value.field);
  }
  for (const child of Object.values(value)) if (!temporalFields(child, out)) return null;
  return out;
}

/**
 * Vega-Lite reads a date-only string ("2026-10-01") as midnight UTC, the way
 * JavaScript does, and then shows it in local time: a day early everywhere
 * west of Greenwich, on the axis and in the tooltip, and the points sit off
 * the day ticks. A field drawn as time whose inline values are all dates is
 * parsed as a local date instead, which is what was written. A spec that
 * deals in UTC itself, or parses the field itself, is left alone.
 */
function withLocalDates(spec: Spec): Spec {
  const data = isRecord(spec.data) ? spec.data : undefined;
  const rows = data && Array.isArray(data.values) ? data.values : [];
  if (!data || rows.length === 0 || /utc/i.test(JSON.stringify(spec))) return spec;
  const format = isRecord(data.format) ? data.format : {};
  if (format.parse === null) return spec;
  const parse = isRecord(format.parse) ? format.parse : {};
  const fields = [...(temporalFields(spec) ?? [])].filter(field => {
    if (field in parse) return false;
    const values = rows.map(row => (isRecord(row) ? row[field] : undefined)).filter(v => v !== undefined && v !== null);
    return values.length > 0 && values.every(v => typeof v === 'string' && DATE_ONLY.test(v));
  });
  if (fields.length === 0) return spec;
  const local = Object.fromEntries(fields.map(field => [field, LOCAL_DATE]));
  return { ...spec, data: { ...data, format: { ...format, parse: { ...parse, ...local } } } };
}

function definesSignal(spec: Spec, name: string): boolean {
  return Array.isArray(spec.signals) && spec.signals.some(s => isRecord(s) && s.name === name);
}

/** The library a spec's `$schema` names, when it names one. */
export function schemaMode(spec: Spec): VegaMode | undefined {
  const schema = typeof spec.$schema === 'string' ? spec.$schema : '';
  const match = /\/schema\/(vega-lite|vega)\//.exec(schema);
  return match ? (match[1] as VegaMode) : undefined;
}

function guessMode(spec: Spec): VegaMode {
  return schemaMode(spec)
    ?? (['mark', 'layer', 'encoding', ...COMPOSITE_KEYS].some(key => key in spec) ? 'vega-lite' : 'vega');
}

/**
 * The spec with the sizing the chat prefers wherever the spec leaves it open:
 * the column's width, and a 280 px plot. A spec that sizes itself, sets an
 * autosize that cannot fit a container, or draws several views keeps its own
 * layout. A Vega-Lite spec also gets level category labels where they fit
 * and its inline dates read as local dates (see `withLocalDates`). Returns a
 * new object and leaves `spec` as it was.
 *
 * Vega-Lite takes `width: "container"`. A full Vega spec has no such value —
 * its width is a number or a signal — so it gets the signal Vega-Lite would
 * compile that to: the container's width, re-read when the container resizes.
 */
export function withDefaults(spec: Spec, mode: VegaMode = guessMode(spec)): Spec {
  // A copy, always: the defaults are set on it, never on the caller's spec.
  const out: Spec = { ...(mode === 'vega-lite' ? withLocalDates(spec) : spec) };
  const fits = spec.autosize === undefined || ['fit', 'fit-x'].includes(autosizeType(spec.autosize) ?? '');

  if (mode === 'vega-lite') {
    if (!isSingleView(spec)) return out;
    if (spec.width === undefined && isRadial(spec)) {
      out.width = DEFAULT_HEIGHT;
    } else if (spec.width === undefined && fits) {
      out.width = 'container';
      if (spec.autosize === undefined) out.autosize = { type: 'fit-x', contains: 'padding' };
    }
    // `height: "container"` has nothing to fill here: a stage is as tall as
    // the chart in it, so Vega would draw the plot 0 px tall. It is taken as
    // no height at all.
    if (spec.height === undefined || spec.height === 'container') {
      delete out.height;
      if (!hasDiscreteY(spec)) out.height = DEFAULT_HEIGHT;
    }
    return withLevelLabels(out);
  }

  if (spec.width === undefined && !definesSignal(spec, 'width') && fits) {
    const size = `isFinite(containerSize()[0]) && containerSize()[0] > 0 ? containerSize()[0] : ${FALLBACK_WIDTH}`;
    out.signals = [
      ...(Array.isArray(spec.signals) ? spec.signals : []),
      {
        name: 'width',
        init: size,
        on: [{ events: 'window:resize', update: size }, { events: 'container:resize', update: size }],
      },
    ];
    if (spec.autosize === undefined) out.autosize = { type: 'fit-x', contains: 'padding' };
  }
  if (spec.height === undefined && !definesSignal(spec, 'height')) out.height = DEFAULT_HEIGHT;
  return out;
}

/** Whether a spec, as `withDefaults` left it, takes its width from its container. */
export function isContainerSized(spec: Spec): boolean {
  if (spec.width === 'container') return true;
  return Array.isArray(spec.signals) && spec.signals.some(s =>
    isRecord(s) && s.name === 'width' && typeof s.init === 'string' && s.init.includes('containerSize()'));
}

/** The tooltip theme's name; vega-tooltip puts `<name>-theme` on its element. */
export const TOOLTIP_THEME = 'nerve';

/**
 * vega-tooltip's element in the app's colours. There is one such element for
 * the whole page, appended to <body>, so it is styled with the `--theme-*`
 * variables and follows a theme switch on its own. Values lead and labels
 * follow: the reader has the series and is after the number.
 */
export function tooltipCss(fontFamily: string): string {
  const el = `#vg-tooltip-element.${TOOLTIP_THEME}-theme`;
  return `${el} {
  background: var(--theme-surface-raised);
  color: var(--theme-text);
  border: 1px solid var(--theme-border);
  border-radius: 6px;
  box-shadow: 0 4px 14px rgba(0, 0, 0, 0.18);
  padding: 6px 8px;
  font-family: ${fontFamily};
  font-size: 12px;
  line-height: 1.35;
}
${el} h2 { margin: 0 0 4px; font-size: 12px; font-weight: 600; }
${el} td.key { color: var(--theme-text-muted); text-align: left; padding-right: 10px; }
${el} td.value { color: var(--theme-text); font-weight: 600; font-variant-numeric: tabular-nums; }
`;
}
