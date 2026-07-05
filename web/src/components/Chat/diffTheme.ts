import { useThemeStore } from '../../stores/themeStore';

// Shared theming for all @pierre/diffs renders in Nerve. Kept free of any
// @pierre/diffs import so it stays lightweight and can be used by components
// that lazy-load the (heavy) diff renderer.

// Shiki themes for the dark/light variants. github-* matches the palette Nerve
// already uses for markdown code blocks, so syntax colors stay consistent.
export const DIFF_THEME = { dark: 'github-dark', light: 'github-light' };

// Injected into the diffs-container shadow root via the library's `unsafeCSS`
// option, which lands in the highest CSS @layer (unsafe) — so it overrides the
// renderer's own theme styles, including the surface background the library
// sets on :host. Everything maps onto Nerve's --theme-* tokens, which are
// inherited across the shadow boundary and already adapt to light/dark.
export const DIFF_THEME_CSS = `:host {
  /* Surface flush with the Nerve panel background. */
  --diffs-bg: var(--theme-bg);
  /* Added/removed line tints — Nerve's muted diff backgrounds. */
  --diffs-bg-addition-override: var(--theme-diff-add-bg);
  --diffs-bg-deletion-override: var(--theme-diff-del-bg);
  /* +/- indicators, gutter numbers and inline emphasis derive from these. */
  --diffs-addition-color-override: var(--theme-diff-add);
  --diffs-deletion-color-override: var(--theme-diff-del);
}`;

/**
 * Shared @pierre/diffs options for every Nerve diff render — unified layout,
 * Nerve theming, and no library file header (Nerve renders its own chrome).
 * `themeType` follows the app theme store ('system' | 'light' | 'dark').
 * `wrap` switches long lines from horizontal scrolling (the default) to
 * soft wrapping.
 */
export function useDiffOptions(opts?: { wrap?: boolean }) {
  const themeType = useThemeStore((s) => s.preference);
  return {
    diffStyle: 'unified' as const,
    themeType,
    theme: DIFF_THEME,
    disableFileHeader: true,
    diffIndicators: 'classic' as const,
    unsafeCSS: DIFF_THEME_CSS,
    overflow: opts?.wrap ? ('wrap' as const) : ('scroll' as const),
  };
}
