import { afterEach, describe, expect, it, vi } from 'vitest';
import { contrastRatio, deltaE2000, luminance, mix, readThemeTokens, resetThemeTokens, toHex, withAlpha } from './tokens';

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

  it('never files one theme\'s colours under the other\'s name', async () => {
    // A browser in miniature: every variable resolves to one colour, and a
    // 2D context that paints what it is given (jsdom has neither).
    const rgb = (value: string): number[] | null => {
      const hex = /^#([0-9a-f]{6})$/i.exec(value);
      if (hex) return [0, 2, 4].map(i => parseInt(hex[1].slice(i, i + 2), 16));
      const fn = /^rgb\((\d+), (\d+), (\d+)\)$/.exec(value);
      return fn ? fn.slice(1).map(Number) : null;
    };
    let fill = '#000000';
    let painted = [0, 0, 0];
    const context = {
      get fillStyle() { return fill; },
      set fillStyle(value: string) {
        const parsed = rgb(value);
        if (parsed) fill = '#' + parsed.map(v => v.toString(16).padStart(2, '0')).join('');
      },
      clearRect() {},
      fillRect() { painted = rgb(fill) ?? painted; },
      getImageData() { return { data: [...painted, 255] }; },
    };
    vi.resetModules();
    const getContext = vi.spyOn(HTMLCanvasElement.prototype, 'getContext')
      .mockImplementation((() => context) as unknown as HTMLCanvasElement['getContext']);
    const computed = vi.spyOn(window, 'getComputedStyle')
      .mockImplementation(() => ({ color: 'rgb(10, 20, 30)' }) as CSSStyleDeclaration);
    const fresh = await import('./tokens');
    try {
      // The page shows dark; a render that started in light asks for light.
      document.documentElement.setAttribute('data-cui-theme', 'dark');
      expect(fresh.readThemeTokens('dark').bg).toBe('#0a141e');
      expect(fresh.readThemeTokens('light').bg).toBe('#ffffff');
      // Once the page shows light, light resolves; nothing stale was kept.
      document.documentElement.setAttribute('data-cui-theme', 'light');
      expect(fresh.readThemeTokens('light').bg).toBe('#0a141e');
    } finally {
      document.documentElement.removeAttribute('data-cui-theme');
      getContext.mockRestore();
      computed.mockRestore();
      vi.resetModules();
    }
  });

  it('keeps the first six series colours clearly apart, in both themes', () => {
    expect(deltaE2000('#000000', '#ffffff')).toBeCloseTo(100, 0);
    expect(deltaE2000('#437eef', '#437eef')).toBe(0);
    // The twins the order pushes to the end: blue/sky, violet/purple, orange/red.
    expect(deltaE2000('#437eef', '#6d9bf3')).toBeLessThan(12);
    for (const theme of ['dark', 'light'] as const) {
      const { palette } = readThemeTokens(theme);
      const six = palette.slice(0, 6);
      let worstPair = Infinity;
      for (let i = 0; i < six.length; i++) {
        for (let j = i + 1; j < six.length; j++) worstPair = Math.min(worstPair, deltaE2000(six[i], six[j]));
      }
      expect(worstPair, `${theme}: every pair among the first six`).toBeGreaterThanOrEqual(18);
      for (let i = 0; i < palette.length - 1; i++) {
        expect(deltaE2000(palette[i], palette[i + 1]), `${theme}: neighbours ${i},${i + 1}`).toBeGreaterThanOrEqual(20);
      }
    }
  });

  it('measures WCAG contrast either way round', () => {
    expect(contrastRatio('#000000', '#ffffff')).toBeCloseTo(21, 5);
    expect(contrastRatio('#ffffff', '#000000')).toBeCloseTo(21, 5);
    expect(contrastRatio('#777777', '#777777')).toBe(1);
    // #767676 is the classic "just passes AA on white" grey.
    expect(contrastRatio('#767676', '#ffffff')).toBeGreaterThanOrEqual(4.5);
    expect(contrastRatio('#777777', '#ffffff')).toBeLessThan(4.5);
    // Each theme's text reads on its own surfaces.
    for (const theme of ['dark', 'light'] as const) {
      const t = readThemeTokens(theme);
      expect(contrastRatio(t.text, t.surfaceRaised)).toBeGreaterThanOrEqual(4.5);
    }
  });
});
