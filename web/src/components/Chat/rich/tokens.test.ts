import { afterEach, describe, expect, it } from 'vitest';
import { luminance, mix, readThemeTokens, resetThemeTokens, toHex, withAlpha } from './tokens';

afterEach(() => resetThemeTokens());

describe('tokens', () => {
  it('falls back to a complete set where the browser cannot paint (jsdom)', () => {
    // jsdom has no canvas, so nothing resolves and the fallbacks stand.
    expect(toHex('rgb(1, 2, 3)')).toBeNull();
    for (const theme of ['dark', 'light'] as const) {
      const t = readThemeTokens(theme);
      expect(t.theme).toBe(theme);
      for (const key of ['bg', 'surface', 'surfaceRaised', 'border', 'text', 'textMuted', 'textFaint', 'accent'] as const) {
        expect(t[key]).toMatch(/^#[0-9a-f]{6}$/);
      }
      expect(t.palette.length).toBeGreaterThanOrEqual(8);
      expect(new Set(t.palette).size).toBe(t.palette.length);
      expect(t.fontFamily).toContain('Inter');
    }
    // Dark text is light and light text is dark, whatever the exact values.
    expect(luminance(readThemeTokens('dark').text)).toBeGreaterThan(luminance(readThemeTokens('dark').bg));
    expect(luminance(readThemeTokens('light').text)).toBeLessThan(luminance(readThemeTokens('light').bg));
  });

  it('mixes and alpha-blends hex colours', () => {
    expect(mix('#000000', '#ffffff', 0.5)).toBe('#808080');
    expect(mix('#ff0000', '#0000ff', 0)).toBe('#ff0000');
    expect(mix('#ff0000', '#0000ff', 1)).toBe('#0000ff');
    expect(mix('#ff0000', '#0000ff', 2)).toBe('#0000ff');
    expect(withAlpha('#ff8000', 0.25)).toBe('rgba(255, 128, 0, 0.25)');
    expect(luminance('#ffffff')).toBeCloseTo(1, 5);
    expect(luminance('#000000')).toBe(0);
  });
});
