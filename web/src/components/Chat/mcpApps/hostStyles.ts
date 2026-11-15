import type { McpUiStyles } from '@modelcontextprotocol/ext-apps/app-bridge';
import { mix, readThemeTokens, withAlpha, type ThemeTokens } from '../rich/tokens';

/**
 * The chat's look, as the CSS variables that MCP Apps standardizes
 * (`hostContext.styles.variables`), so that a view can sit in the chat
 * without looking foreign.
 *
 * A view runs in a document of its own and reads none of the chat's
 * stylesheet, so the values are plain colours, resolved from the theme
 * tokens. Each colour is a `light-dark()` pair, as the specification asks:
 * the view picks the side with its `color-scheme`, which it sets from
 * `hostContext.theme`.
 */

/** Where each hue sits in `ThemeTokens.palette`. */
const BLUE = 0;
const ORANGE = 1;
const EMERALD = 3;
const RED = 8;

type ColorKey = Extract<keyof McpUiStyles, `--color-${string}`>;

/** The colours of one theme. */
function colors(t: ThemeTokens): Record<ColorKey, string> {
  const info = t.palette[BLUE];
  const warning = t.palette[ORANGE];
  const success = t.palette[EMERALD];
  const danger = t.palette[RED];
  /** A surface with a little of `hue` in it, for a notice. */
  const tinted = (hue: string) => mix(t.surface, hue, 0.16);
  /** The edge of a tinted surface. */
  const edged = (hue: string) => mix(t.surface, hue, 0.45);
  return {
    // A view sits on a card, like a tool call: the card's colour is its page.
    '--color-background-primary': t.surface,
    '--color-background-secondary': t.surfaceRaised,
    '--color-background-tertiary': mix(t.surfaceRaised, t.border, 0.5),
    '--color-background-inverse': t.text,
    '--color-background-ghost': withAlpha(t.surface, 0),
    '--color-background-info': tinted(info),
    '--color-background-danger': tinted(danger),
    '--color-background-success': tinted(success),
    '--color-background-warning': tinted(warning),
    '--color-background-disabled': withAlpha(t.surfaceRaised, 0.5),

    '--color-text-primary': t.text,
    '--color-text-secondary': t.textMuted,
    '--color-text-tertiary': t.textFaint,
    '--color-text-inverse': t.surface,
    '--color-text-ghost': withAlpha(t.textMuted, 0.5),
    '--color-text-info': info,
    '--color-text-danger': danger,
    '--color-text-success': success,
    '--color-text-warning': warning,
    '--color-text-disabled': withAlpha(t.text, 0.5),

    '--color-border-primary': t.border,
    '--color-border-secondary': mix(t.border, t.text, 0.25),
    '--color-border-tertiary': mix(t.border, t.surface, 0.5),
    '--color-border-inverse': withAlpha(t.surface, 0.3),
    '--color-border-ghost': withAlpha(t.border, 0),
    '--color-border-info': edged(info),
    '--color-border-danger': edged(danger),
    '--color-border-success': edged(success),
    '--color-border-warning': edged(warning),
    '--color-border-disabled': withAlpha(t.border, 0.5),

    // The focus ring is the accent, as in the chat.
    '--color-ring-primary': t.accent,
    '--color-ring-secondary': t.textMuted,
    '--color-ring-inverse': t.surface,
    '--color-ring-info': info,
    '--color-ring-danger': danger,
    '--color-ring-success': success,
    '--color-ring-warning': warning,
  };
}

/** What does not change with the theme. Sizes are the chat's own scale. */
const FIXED: Omit<McpUiStyles, ColorKey | '--font-sans'> = {
  '--font-mono': 'Inconsolata, ui-monospace, "SFMono-Regular", Consolas, monospace',

  '--font-weight-normal': '400',
  '--font-weight-medium': '500',
  '--font-weight-semibold': '600',
  '--font-weight-bold': '700',

  '--font-text-xs-size': '0.75rem',
  '--font-text-sm-size': '0.875rem',
  '--font-text-md-size': '1rem',
  '--font-text-lg-size': '1.125rem',

  '--font-heading-xs-size': '0.75rem',
  '--font-heading-sm-size': '0.875rem',
  '--font-heading-md-size': '1rem',
  '--font-heading-lg-size': '1.125rem',
  '--font-heading-xl-size': '1.25rem',
  '--font-heading-2xl-size': '1.5rem',
  '--font-heading-3xl-size': '2rem',

  '--font-text-xs-line-height': '1.35',
  '--font-text-sm-line-height': '1.45',
  '--font-text-md-line-height': '1.5',
  '--font-text-lg-line-height': '1.55',

  '--font-heading-xs-line-height': '1.4',
  '--font-heading-sm-line-height': '1.4',
  '--font-heading-md-line-height': '1.4',
  '--font-heading-lg-line-height': '1.35',
  '--font-heading-xl-line-height': '1.3',
  '--font-heading-2xl-line-height': '1.25',
  '--font-heading-3xl-line-height': '1.25',

  '--border-radius-xs': '2px',
  '--border-radius-sm': '4px',
  '--border-radius-md': '6px',
  '--border-radius-lg': '8px',
  '--border-radius-xl': '12px',
  '--border-radius-full': '9999px',

  '--border-width-regular': '1px',

  '--shadow-hairline': '0 1px 2px 0 rgba(0, 0, 0, 0.05)',
  '--shadow-sm': '0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px -1px rgba(0, 0, 0, 0.1)',
  '--shadow-md': '0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -2px rgba(0, 0, 0, 0.1)',
  '--shadow-lg': '0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -4px rgba(0, 0, 0, 0.1)',
};

/**
 * The variables for a view, from the tokens of both themes. Read on each
 * call: the tokens of the theme on screen come from the live stylesheet, the
 * other theme's from the fixed set until it has been shown once.
 */
export function hostStyleVariables(
  light: ThemeTokens = readThemeTokens('light'),
  dark: ThemeTokens = readThemeTokens('dark'),
): McpUiStyles {
  const lightColors = colors(light);
  const darkColors = colors(dark);
  const paired = Object.fromEntries(
    (Object.keys(lightColors) as ColorKey[]).map(key => [key, `light-dark(${lightColors[key]}, ${darkColors[key]})`]),
  ) as Record<ColorKey, string>;
  return { ...paired, '--font-sans': light.fontFamily, ...FIXED };
}
