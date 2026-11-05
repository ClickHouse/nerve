import type { Loader, View } from 'vega';
import type { VisualizationSpec } from 'vega-embed';
import { readThemeTokens, toHex } from './tokens';
import type { Mount } from './types';
import {
  isContainerSized,
  NARROW_WIDTH,
  schemaMode,
  TOOLTIP_THEME,
  tooltipCss,
  vegaConfigFor,
  withDefaults,
  type VegaMode,
} from './vegaTheme';

export type { VegaMode };

const refuse = () => Promise.reject(new Error('chat charts load nothing from outside the spec'));

/**
 * Vega fetches `data: {url}`, image marks and remote configs through its
 * loader. This one refuses every request, so a spec draws only what it
 * carries.
 */
const noNetwork: Loader = { load: refuse, sanitize: refuse, http: refuse, file: refuse };

/**
 * Throws for a spec that points at anything outside itself: a `url` under
 * `data`, in a `lookup` transform, anywhere but inside data rows. The loader
 * would refuse the fetch anyway; this names the problem before Vega reports
 * a failed load.
 */
export function assertSelfContained(spec: unknown, path = 'spec'): void {
  if (Array.isArray(spec)) {
    spec.forEach((item, i) => assertSelfContained(item, `${path}[${i}]`));
    return;
  }
  if (!spec || typeof spec !== 'object') return;
  for (const [key, value] of Object.entries(spec as Record<string, unknown>)) {
    // Data rows are values, not references — a column may well be called `url`.
    if (key === 'values' || key === 'datasets') continue;
    if (key === 'url' && typeof value === 'string') {
      throw new Error(`${path}.url points outside the spec; inline the data as "values"`);
    }
    assertSelfContained(value, `${path}.${key}`);
  }
}

/** The spec as an object vega-embed may be given. */
export function parseSpec(source: string): Record<string, unknown> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(source);
  } catch (err) {
    throw new Error(`the spec is not valid JSON (${(err as Error).message})`);
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error('the spec must be a JSON object');
  }
  const spec = parsed as Record<string, unknown>;
  // vega-embed reads embed options from `usermeta.embedOptions` and lets
  // them override the caller's — including the action that posts the spec
  // to an editor site. A spec has no say in how it is embedded here.
  delete spec.usermeta;
  assertSelfContained(spec);
  return spec;
}

function stringify(value: unknown, maxDepth: number): string {
  const seen: unknown[] = [];
  return JSON.stringify(value, function (this: unknown, _key: string, v: unknown) {
    if (!v || typeof v !== 'object') return v;
    seen.length = seen.indexOf(this) + 1;
    if (seen.length > maxDepth) return '[Object]';
    if (seen.includes(v)) return '[Circular]';
    seen.push(v);
    return v;
  });
}

/**
 * The tooltip's markup: vega-tooltip's own layout — a title, then a table of
 * fields — minus the one thing it does that reaches outside the page, which
 * is to turn a field called `image` into an <img> fetched from wherever the
 * value points. Here that is a row like any other. Every value still passes
 * through vega-tooltip's sanitizer, which escapes markup.
 */
export function tooltipHtml(value: unknown, sanitize: (value: unknown) => string, maxDepth: number): string {
  if (Array.isArray(value)) {
    return `[${value.map(v => sanitize(typeof v === 'string' ? v : stringify(v, maxDepth))).join(', ')}]`;
  }
  if (!value || typeof value !== 'object') return sanitize(value);
  const { title, ...fields } = value as Record<string, unknown>;
  let html = title ? `<h2>${sanitize(title)}</h2>` : '';
  const rows = Object.entries(fields).filter(([, v]) => v !== undefined);
  if (rows.length > 0) {
    html += '<table>';
    for (const [key, v] of rows) {
      const shown = v && typeof v === 'object' ? stringify(v, maxDepth) : v;
      html += `<tr><td class="key">${sanitize(key)}</td><td class="value">${sanitize(shown)}</td></tr>`;
    }
    html += '</table>';
  }
  return html || '{}';
}

const TOOLTIP_STYLE_ID = 'nerve-vega-tooltip-style';

function installTooltipStyle(fontFamily: string): void {
  if (document.getElementById(TOOLTIP_STYLE_ID)) return;
  const style = document.createElement('style');
  style.id = TOOLTIP_STYLE_ID;
  style.textContent = tooltipCss(fontFamily);
  document.head.appendChild(style);
}

/**
 * The colour the chart is drawn on: the first opaque background behind the
 * stage, as hex. Null where it cannot be read (a detached stage, jsdom).
 */
function backdropOf(stage: HTMLElement): string | null {
  for (let el: Element | null = stage; el; el = el.parentElement) {
    const color = getComputedStyle(el).backgroundColor;
    if (!color) continue;
    // Opaque is what composites to the same colour over black and over white.
    const overBlack = toHex(color, '#000000');
    if (overBlack === null) return null;
    if (overBlack === toHex(color, '#ffffff')) return overBlack;
  }
  return null;
}

/**
 * The chart as a PNG at `scale` device pixels per CSS pixel, over `background`.
 * The chart itself is transparent, and a dark-theme chart — light text, no
 * page behind it — would be unreadable as a file.
 */
async function rasterize(view: View, scale: number, background: string): Promise<Blob | null> {
  const drawn = await view.toCanvas(scale);
  const canvas = document.createElement('canvas');
  canvas.width = drawn.width;
  canvas.height = drawn.height;
  const ctx = canvas.getContext('2d');
  if (!ctx) return null;
  ctx.fillStyle = background;
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  ctx.drawImage(drawn, 0, 0);
  return new Promise(resolve => canvas.toBlob(resolve, 'image/png'));
}

/** The drawn chart's size: Vega gives its root <svg> the full width and height, axes and legends included. */
function drawnSize(stage: HTMLElement): { width: number; height: number } | undefined {
  const svg = stage.querySelector('svg');
  const width = parseFloat(svg?.getAttribute('width') ?? '');
  const height = parseFloat(svg?.getAttribute('height') ?? '');
  return width > 0 && height > 0 ? { width, height } : undefined;
}

export function mountVega(mode: VegaMode): Mount {
  return async (stage, source, { theme, width }) => {
    const parsed = parseSpec(source);
    // The fence names a library, and so may the spec; the spec knows better.
    const specMode = schemaMode(parsed) ?? mode;
    const spec = withDefaults(parsed, specMode);
    const containerSized = isContainerSized(spec);
    const themeTokens = readThemeTokens(theme);
    const tokens = { ...themeTokens, surface: backdropOf(stage) ?? themeTokens.surface };
    // Decided once per render: a phone stays narrow, and a chart rendered
    // again (new text, a theme switch) decides again.
    const stageWidth = stage.clientWidth || width;
    const narrow = stageWidth > 0 && stageWidth < NARROW_WIDTH;
    // vega + vega-lite + vega-embed are their own chunk, fetched the first
    // time a chat shows a chart.
    const { default: embed, vega } = await import('vega-embed');
    installTooltipStyle(tokens.fontFamily);
    const result = await embed(stage, spec as VisualizationSpec, {
      mode: specMode,
      renderer: 'svg',
      // Vega and Vega-Lite warn on the console about whatever in a spec they
      // quietly work around; a chat showing a chart is no place for that.
      // Errors still log, and a spec that cannot be drawn rejects here and
      // shows as the block's failure.
      logLevel: vega.Error,
      // No action menu: the figure around the chart has the downloads, the
      // block has its own source toggle, and "open in editor" would send the
      // spec and its data to another site.
      actions: false,
      config: vegaConfigFor(tokens, specMode, { narrow }),
      loader: noNetwork,
      tooltip: { theme: TOOLTIP_THEME, formatTooltip: tooltipHtml },
      downloadFileName: 'chart',
    });
    const { view } = result;
    // A chart that fills its container measures the stage itself, and finds
    // nothing while the stage is not laid out; the frame may know the width.
    if (containerSized && stage.clientWidth === 0 && width > 0) {
      view.width(width);
      await view.runAsync();
    }
    return {
      // finalize() also takes down a tooltip the chart is showing.
      cleanup: () => {
        result.finalize();
        stage.replaceChildren();
      },
      // A container-sized chart has no size of its own: it is the column's.
      naturalSize: containerSized ? undefined : drawnSize(stage),
      toSvg: () => view.toSVG(),
      toPng: (scale) => rasterize(view, scale, tokens.surface),
      resize: async () => {
        // Vega follows its container on its own; this is for the frame to
        // be sure the chart has caught up with a width it just measured.
        if (containerSized && stage.clientWidth > 0) view.width(stage.clientWidth);
        await view.resize().runAsync();
      },
    };
  };
}
