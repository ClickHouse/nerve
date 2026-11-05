import { describe, expect, it } from 'vitest';
import { assertSelfContained, parseSpec } from './vega';

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
