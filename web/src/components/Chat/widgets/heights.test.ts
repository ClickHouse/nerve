import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { rememberWidgetHeight, resetWidgetHeights, widgetHeight, widgetKey } from './heights';

beforeEach(() => {
  resetWidgetHeights();
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.resetModules();
});

describe('widgetKey', () => {
  it('is the same for the same source and differs for another', () => {
    expect(widgetKey('<p>a</p>')).toBe(widgetKey('<p>a</p>'));
    expect(widgetKey('<p>a</p>')).not.toBe(widgetKey('<p>b</p>'));
    expect(widgetKey('')).toMatch(/^0:[0-9a-z]+$/);
    // Short, however long the source.
    expect(widgetKey('x'.repeat(100_000)).length).toBeLessThan(20);
  });
});

describe('the heights of widgets', () => {
  it('are unknown for a widget that was never drawn', () => {
    expect(widgetHeight(widgetKey('<p>new</p>'))).toBe(0);
  });

  it('are kept by source', () => {
    rememberWidgetHeight(widgetKey('<p>a</p>'), 240);
    rememberWidgetHeight(widgetKey('<p>b</p>'), 410);
    rememberWidgetHeight(widgetKey('<p>a</p>'), 260);
    expect(widgetHeight(widgetKey('<p>a</p>'))).toBe(260);
    expect(widgetHeight(widgetKey('<p>b</p>'))).toBe(410);
  });

  it('survive a reload of the page, in sessionStorage', async () => {
    rememberWidgetHeight(widgetKey('<p>kept</p>'), 333);
    expect(JSON.parse(sessionStorage.getItem('nerve-widget-heights')!)).toEqual([[widgetKey('<p>kept</p>'), 333]]);
    // A reload: the module, with its in-memory copy, starts over.
    vi.resetModules();
    const reloaded = await import('./heights');
    expect(reloaded.widgetHeight(reloaded.widgetKey('<p>kept</p>'))).toBe(333);
  });

  it('keep the newest hundred', () => {
    for (let i = 0; i < 130; i++) rememberWidgetHeight(`key-${i}`, 100 + i);
    const stored = JSON.parse(sessionStorage.getItem('nerve-widget-heights')!) as Array<[string, number]>;
    expect(stored).toHaveLength(100);
    expect(stored[0]).toEqual(['key-30', 130]);
    expect(stored[99]).toEqual(['key-129', 229]);
    expect(widgetHeight('key-0')).toBe(0);
    expect(widgetHeight('key-129')).toBe(229);
  });

  it('serve the page from memory when storage refuses', () => {
    vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => { throw new Error('denied'); });
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => { throw new Error('denied'); });
    rememberWidgetHeight('key', 180);
    expect(widgetHeight('key')).toBe(180);
  });

  it('take nothing from storage that is no height', () => {
    sessionStorage.setItem('nerve-widget-heights', JSON.stringify([['a', '300'], ['b', -5], 'c', ['d', 120]]));
    expect(widgetHeight('a')).toBe(0);
    expect(widgetHeight('b')).toBe(0);
    expect(widgetHeight('d')).toBe(120);
    sessionStorage.setItem('nerve-widget-heights', '{not json');
    expect(widgetHeight('d')).toBe(0);
  });
});
