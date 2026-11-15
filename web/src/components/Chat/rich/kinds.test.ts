import { describe, expect, it } from 'vitest';
import { KIND_LABEL, RICH_LANGUAGES, richKindFor } from './kinds';

describe('richKindFor', () => {
  it.each([
    ['mermaid', 'mermaid'],
    ['vega-lite', 'vega-lite'],
    ['vegalite', 'vega-lite'],
    ['vega', 'vega'],
    ['svg', 'svg'],
    ['widget', 'widget'],
  ])('renders a ```%s fence as %s', (language, kind) => {
    expect(richKindFor(language)).toBe(kind);
  });

  it('takes the language whatever its case', () => {
    expect(richKindFor('Widget')).toBe('widget');
    expect(richKindFor('MERMAID')).toBe('mermaid');
  });

  it.each([
    // HTML is code a person asked for, far more often than a thing to run.
    'html', 'htm', 'xml', 'xhtml',
    // Names that only look like the one that runs.
    'widgets', 'widge', 'html-widget', 'widget-html', 'jsx', 'javascript', 'js', 'css', 'text', '',
  ])('leaves a ```%s fence an ordinary code block', (language) => {
    expect(richKindFor(language)).toBeNull();
  });

  it('leaves a fence without a language an ordinary code block', () => {
    expect(richKindFor(undefined)).toBeNull();
  });
});

describe('RICH_LANGUAGES', () => {
  it('names every language that renders, so the highlighter leaves their source in one piece', () => {
    expect([...RICH_LANGUAGES].sort()).toEqual(['mermaid', 'svg', 'vega', 'vega-lite', 'vegalite', 'widget']);
    for (const language of RICH_LANGUAGES) expect(richKindFor(language)).not.toBeNull();
  });
});

describe('KIND_LABEL', () => {
  it('labels the pictures; a widget has a block of its own', () => {
    expect(Object.keys(KIND_LABEL).sort()).toEqual(['mermaid', 'svg', 'vega', 'vega-lite']);
  });
});
