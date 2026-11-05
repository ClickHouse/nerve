import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

/**
 * The stylesheet is part of the security boundary: a model-written picture
 * must stay inside its block whatever CSS it carries. jsdom does no layout,
 * so the rule is checked as text. vitest runs from `web/`.
 */
const css = readFileSync(join(process.cwd(), 'src', 'index.css'), 'utf8');

describe('rich block containment', () => {
  it('contains positioned descendants of the canvas and of every stage', () => {
    expect(css).toMatch(/\.rich-figure-canvas \{[^}]*contain: layout paint;/);
    expect(css).toMatch(/\.rich-stage \{ contain: layout paint; \}/);
  });
});
