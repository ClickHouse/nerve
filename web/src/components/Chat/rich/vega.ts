import type { Loader } from 'vega';
import type { VisualizationSpec } from 'vega-embed';
import type { Mount } from './types';

export type VegaMode = 'vega' | 'vega-lite';

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

export function mountVega(mode: VegaMode): Mount {
  return async (stage, source, theme) => {
    const spec = parseSpec(source);
    // vega + vega-lite + vega-embed are their own chunk, fetched the first
    // time a chat shows a chart.
    const { default: embed } = await import('vega-embed');
    const result = await embed(stage, spec as VisualizationSpec, {
      mode,
      renderer: 'svg',
      // Save as SVG/PNG stays; "view source" is the block's own toggle, and
      // "open in editor" would send the spec and its data to another site.
      actions: { export: true, source: false, compiled: false, editor: false },
      theme: theme === 'dark' ? 'dark' : undefined,
      config: {
        background: 'transparent',
        font: 'Inter, ui-sans-serif, system-ui, sans-serif',
      },
      loader: noNetwork,
      tooltip: { theme },
      downloadFileName: 'chart',
    });
    return () => result.finalize();
  };
}
