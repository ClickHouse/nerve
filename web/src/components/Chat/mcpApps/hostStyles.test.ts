import { describe, expect, it } from 'vitest';
import { readThemeTokens } from '../rich/tokens';
import { hostStyleVariables } from './hostStyles';

/** The variable names of the MCP Apps specification (2026-01-26, "Theming"). */
const STANDARD = [
  '--color-background-primary', '--color-background-secondary', '--color-background-tertiary',
  '--color-background-inverse', '--color-background-ghost', '--color-background-info', '--color-background-danger',
  '--color-background-success', '--color-background-warning', '--color-background-disabled',
  '--color-text-primary', '--color-text-secondary', '--color-text-tertiary', '--color-text-inverse',
  '--color-text-info', '--color-text-danger', '--color-text-success', '--color-text-warning',
  '--color-text-disabled', '--color-text-ghost',
  '--color-border-primary', '--color-border-secondary', '--color-border-tertiary', '--color-border-inverse',
  '--color-border-ghost', '--color-border-info', '--color-border-danger', '--color-border-success',
  '--color-border-warning', '--color-border-disabled',
  '--color-ring-primary', '--color-ring-secondary', '--color-ring-inverse', '--color-ring-info',
  '--color-ring-danger', '--color-ring-success', '--color-ring-warning',
  '--font-sans', '--font-mono',
  '--font-weight-normal', '--font-weight-medium', '--font-weight-semibold', '--font-weight-bold',
  '--font-text-xs-size', '--font-text-sm-size', '--font-text-md-size', '--font-text-lg-size',
  '--font-heading-xs-size', '--font-heading-sm-size', '--font-heading-md-size', '--font-heading-lg-size',
  '--font-heading-xl-size', '--font-heading-2xl-size', '--font-heading-3xl-size',
  '--font-text-xs-line-height', '--font-text-sm-line-height', '--font-text-md-line-height', '--font-text-lg-line-height',
  '--font-heading-xs-line-height', '--font-heading-sm-line-height', '--font-heading-md-line-height',
  '--font-heading-lg-line-height', '--font-heading-xl-line-height', '--font-heading-2xl-line-height',
  '--font-heading-3xl-line-height',
  '--border-radius-xs', '--border-radius-sm', '--border-radius-md', '--border-radius-lg', '--border-radius-xl',
  '--border-radius-full',
  '--border-width-regular',
  '--shadow-hairline', '--shadow-sm', '--shadow-md', '--shadow-lg',
];

describe('hostStyleVariables', () => {
  // jsdom resolves no stylesheet: these are the fixed token sets.
  const light = readThemeTokens('light');
  const dark = readThemeTokens('dark');
  const variables = hostStyleVariables(light, dark) as Record<string, string>;

  it('gives every standardized variable a value, and no other', () => {
    expect(Object.keys(variables).sort()).toEqual([...STANDARD].sort());
    for (const key of STANDARD) expect(variables[key], key).toMatch(/\S/);
  });

  it('gives each colour as a light-dark() pair of plain colours', () => {
    const colours = Object.entries(variables).filter(([key]) => key.startsWith('--color-'));
    expect(colours).toHaveLength(37);
    for (const [key, value] of colours) {
      expect(value, key).toMatch(/^light-dark\((#[0-9a-f]{6}|rgba\(\d+, \d+, \d+, [\d.]+\)), (#[0-9a-f]{6}|rgba\(\d+, \d+, \d+, [\d.]+\))\)$/);
    }
  });

  it('maps the chat tokens: the card is the page of a view, the text is its text', () => {
    expect(variables['--color-background-primary']).toBe(`light-dark(${light.surface}, ${dark.surface})`);
    expect(variables['--color-background-secondary']).toBe(`light-dark(${light.surfaceRaised}, ${dark.surfaceRaised})`);
    expect(variables['--color-text-primary']).toBe(`light-dark(${light.text}, ${dark.text})`);
    expect(variables['--color-text-secondary']).toBe(`light-dark(${light.textMuted}, ${dark.textMuted})`);
    expect(variables['--color-border-primary']).toBe(`light-dark(${light.border}, ${dark.border})`);
    expect(variables['--color-ring-primary']).toBe(`light-dark(${light.accent}, ${dark.accent})`);
    expect(variables['--font-sans']).toBe(light.fontFamily);
  });

  it('carries no reference to the chat stylesheet, which a view cannot read', () => {
    for (const [key, value] of Object.entries(variables)) expect(value, key).not.toMatch(/var\(|color-mix\(/);
  });
});
