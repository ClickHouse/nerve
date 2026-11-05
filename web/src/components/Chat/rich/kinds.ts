/**
 * The fenced-code languages the chat renders instead of printing.
 *
 * A ```mermaid block becomes a diagram, a ```vega-lite (or ```vega) block a
 * chart, an ```svg block the picture itself. Every other fence stays a code
 * block. The model is told which languages render here by the system prompt's
 * "Rendering" line (nerve/agent/prompts.py), so the two lists must agree.
 */
export type RichKind = 'mermaid' | 'vega-lite' | 'vega' | 'svg';

const KIND_BY_LANGUAGE: Record<string, RichKind> = {
  mermaid: 'mermaid',
  'vega-lite': 'vega-lite',
  vegalite: 'vega-lite',
  vega: 'vega',
  svg: 'svg',
};

/** The kind a fence language renders as, or null for an ordinary code block. */
export function richKindFor(language: string | undefined): RichKind | null {
  if (!language) return null;
  return KIND_BY_LANGUAGE[language.toLowerCase()] ?? null;
}

/**
 * Passed to rehype-highlight as `plainText`: highlighting these would be
 * wasted work, and highlighting `svg` (an XML alias) would split the source
 * into spans that the renderer then has to join back together.
 */
export const RICH_LANGUAGES: readonly string[] = Object.keys(KIND_BY_LANGUAGE);

export const KIND_LABEL: Record<RichKind, string> = {
  mermaid: 'mermaid',
  'vega-lite': 'vega-lite',
  vega: 'vega',
  svg: 'svg',
};

/**
 * Above this many characters the block stays source. A diagram or a spec this
 * large is not something a person reads in a chat, and rendering it could
 * stall the tab for everyone else's messages.
 */
export const MAX_RICH_SOURCE_CHARS = 200_000;
