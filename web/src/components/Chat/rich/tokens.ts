import type { ResolvedTheme } from '../../../stores/themeStore';

/**
 * The app's colours as hex strings, for renderers that take no CSS
 * variables (mermaid's theme variables, Vega's config). Read from the live
 * stylesheet through the browser — the `--theme-*` tokens are `var()` and
 * `color-mix()` expressions — with a fixed fallback set where that is not
 * possible (jsdom, a stylesheet that has not loaded).
 */
export interface ThemeTokens {
  theme: ResolvedTheme;
  bg: string;
  surface: string;
  surfaceRaised: string;
  border: string;
  text: string;
  textMuted: string;
  textFaint: string;
  accent: string;
  /** Categorical palette, in the order series should take the colours. */
  palette: string[];
  fontFamily: string;
}

const FONT_FAMILY = 'Inter, ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif';

const FALLBACK: Record<ResolvedTheme, ThemeTokens> = {
  dark: {
    theme: 'dark',
    bg: '#1a1c20',
    surface: '#22252b',
    surfaceRaised: '#2c3039',
    border: '#3d424d',
    text: '#e8eaed',
    textMuted: '#a3a9b3',
    textFaint: '#6b7280',
    accent: '#f6d53f',
    palette: ['#437eef', '#fb923c', '#38bdf8', '#34d399', '#dd99ff', '#facc15', '#a78bfa', '#f87171', '#2dd4bf', '#22d3ee'],
    fontFamily: FONT_FAMILY,
  },
  light: {
    theme: 'light',
    bg: '#ffffff',
    surface: '#f6f7f9',
    surfaceRaised: '#eceef2',
    border: '#d5d9e0',
    text: '#1c1f26',
    textMuted: '#5b6472',
    textFaint: '#9aa1ad',
    accent: '#b58900',
    palette: ['#104ec6', '#c2410c', '#0369a1', '#047857', '#8800cc', '#a16207', '#6d28d9', '#b91c1c', '#0f766e', '#0e7490'],
    fontFamily: FONT_FAMILY,
  },
};

const TOKEN_VARS: Record<Exclude<keyof ThemeTokens, 'theme' | 'palette' | 'fontFamily'>, string> = {
  bg: '--theme-bg',
  surface: '--theme-surface',
  surfaceRaised: '--theme-surface-raised',
  border: '--theme-border',
  text: '--theme-text',
  textMuted: '--theme-text-muted',
  textFaint: '--theme-text-faint',
  accent: '--theme-accent',
};

/** Hue tokens in the order series take them: alternating hue families, high contrast between neighbours. */
const PALETTE_VARS = [
  // Ordered so that neighbouring series stay apart for colour-blind readers
  // as well (blue next to violet does not, under deuteranopia); the hues
  // with a meaning — red, yellow, green, orange — keep their positions in
  // mermaid.ts's HUE map, which must change with this list.
  '--theme-hue-blue', '--theme-hue-orange', '--theme-hue-sky', '--theme-hue-emerald', '--theme-hue-violet',
  '--theme-hue-yellow', '--theme-hue-purple', '--theme-hue-red', '--theme-hue-teal', '--theme-hue-cyan',
];

let context2d: CanvasRenderingContext2D | null | undefined;

function canvasContext(): CanvasRenderingContext2D | null {
  if (context2d === undefined) {
    try {
      context2d = document.createElement('canvas').getContext('2d', { willReadFrequently: true });
    } catch {
      context2d = null;
    }
  }
  return context2d;
}

/**
 * `#rrggbb` for any colour the browser can paint — `rgb()`, `oklab()`,
 * `color(srgb …)` — composited over `over` when it has alpha; null when the
 * colour is not understood or there is no canvas (jsdom).
 */
export function toHex(color: string, over = '#ffffff'): string | null {
  const ctx = canvasContext();
  if (!ctx || !color) return null;
  const sentinel = '#010203';
  ctx.fillStyle = sentinel;
  ctx.fillStyle = color;
  if (ctx.fillStyle === sentinel && color.replace(/\s/g, '').toLowerCase() !== sentinel) return null;
  const parsed = ctx.fillStyle;
  ctx.clearRect(0, 0, 1, 1);
  ctx.fillStyle = over;
  ctx.fillRect(0, 0, 1, 1);
  ctx.fillStyle = parsed;
  ctx.fillRect(0, 0, 1, 1);
  const [r, g, b] = ctx.getImageData(0, 0, 1, 1).data;
  return rgbToHex(r, g, b);
}

function rgbToHex(r: number, g: number, b: number): string {
  return '#' + [r, g, b].map(v => Math.round(v).toString(16).padStart(2, '0')).join('');
}

function hexToRgb(hex: string): [number, number, number] {
  const m = /^#?([0-9a-f]{6})$/i.exec(hex.trim());
  if (!m) return [0, 0, 0];
  const n = parseInt(m[1], 16);
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
}

/** `a` blended towards `b` by `t` (0 → `a`, 1 → `b`), in sRGB. */
export function mix(a: string, b: string, t: number): string {
  const [ar, ag, ab] = hexToRgb(a);
  const [br, bg, bb] = hexToRgb(b);
  const k = Math.min(1, Math.max(0, t));
  return rgbToHex(ar + (br - ar) * k, ag + (bg - ag) * k, ab + (bb - ab) * k);
}

/** `rgba()` for a hex colour, for the renderers that accept alpha. */
export function withAlpha(hex: string, alpha: number): string {
  const [r, g, b] = hexToRgb(hex);
  return `rgba(${r}, ${g}, ${b}, ${Math.min(1, Math.max(0, alpha))})`;
}

/** Relative luminance (WCAG), 0 = black, 1 = white. */
export function luminance(hex: string): number {
  const [r, g, b] = hexToRgb(hex).map(v => {
    const c = v / 255;
    return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

/** WCAG contrast ratio of two hex colours, 1 (none) to 21 (black on white); order does not matter. */
export function contrastRatio(a: string, b: string): number {
  const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x);
  return (hi + 0.05) / (lo + 0.05);
}

/**
 * A CSS variable's colour as hex. The variable's value may itself be a
 * `var()` or `color-mix()`, so it is resolved through an element the browser
 * styles rather than read as text.
 */
function resolveVar(name: string, over: string): string | null {
  if (typeof document === 'undefined') return null;
  const probe = document.createElement('span');
  probe.style.color = `var(${name})`;
  document.documentElement.appendChild(probe);
  try {
    const computed = getComputedStyle(probe).color;
    return computed ? toHex(computed, over) : null;
  } finally {
    probe.remove();
  }
}

const cache = new Map<ResolvedTheme, ThemeTokens>();

/**
 * The theme the page shows, from the attribute themeStore keeps on <html>;
 * null where nothing has set it (tests, a page without the store).
 */
function shownTheme(): string | null {
  return typeof document === 'undefined' ? null : document.documentElement.getAttribute('data-cui-theme');
}

/** The tokens for `theme`, resolved once per theme from the stylesheet. */
export function readThemeTokens(theme: ResolvedTheme): ThemeTokens {
  const cached = cache.get(theme);
  if (cached) return cached;
  const fallback = FALLBACK[theme];
  // The variables resolve to the theme the page shows. Asked for the other
  // one (a render that started before a theme switch), they would be that
  // theme's colours filed under this one's name for the rest of the session.
  const shown = shownTheme();
  if (shown && shown !== theme) return fallback;
  const bg = resolveVar(TOKEN_VARS.bg, fallback.bg) ?? fallback.bg;
  let resolvedAny = bg !== fallback.bg;
  const pick = (name: string, fallbackValue: string): string => {
    const value = resolveVar(name, bg);
    if (value) resolvedAny = true;
    return value ?? fallbackValue;
  };
  const tokens: ThemeTokens = {
    theme,
    bg,
    surface: pick(TOKEN_VARS.surface, fallback.surface),
    surfaceRaised: pick(TOKEN_VARS.surfaceRaised, fallback.surfaceRaised),
    border: pick(TOKEN_VARS.border, fallback.border),
    text: pick(TOKEN_VARS.text, fallback.text),
    textMuted: pick(TOKEN_VARS.textMuted, fallback.textMuted),
    textFaint: pick(TOKEN_VARS.textFaint, fallback.textFaint),
    accent: pick(TOKEN_VARS.accent, fallback.accent),
    palette: PALETTE_VARS.map((name, i) => pick(name, fallback.palette[i % fallback.palette.length])),
    fontFamily: FONT_FAMILY,
  };
  // Fallbacks are not cached: the stylesheet may still be on its way.
  if (resolvedAny) cache.set(theme, tokens);
  return tokens;
}

/** Forgets resolved tokens, for tests and for a stylesheet that changed under the app. */
export function resetThemeTokens(): void {
  cache.clear();
}
