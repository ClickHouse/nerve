import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { Button, type ButtonVariant } from './Button';
import { IconButton, type IconButtonVariant } from './IconButton';

/**
 * These specs exist for one bug, which is invisible to every other check we
 * have and which `tsc`, eslint and the build all pass straight through.
 *
 * Tailwind v4 emits same-property utilities in alphabetical order of class
 * name. Between two colour classes on one element, the winner is therefore the
 * later-*sorting* name — not the one written last in the `class` attribute, and
 * not the one from the "more specific" lookup table. `.text-accent` sorts
 * before every other colour token in this app, so a selected treatment appended
 * after a coloured base lost every time: `<Button variant="ghost" active>`
 * rendered muted, and `variant="tab" active` kept a transparent border.
 *
 * Nothing about that is visible in jsdom, which applies no CSS at all. What can
 * be asserted is the structural rule that makes the ordering irrelevant: an
 * element must never carry two classes that set the same property. Enforcing
 * that here is what keeps the fix from quietly regressing the next time a
 * variant gains a colour.
 */

/** Classes that set `color`. The `text-xs`/`text-sm` steps set size, not colour. */
function textColours(el: Element): string[] {
  return [...el.classList].filter(
    (c) => c.startsWith('text-') && !/^text-(xs|sm|base|lg|xl|\[)/.test(c),
  );
}

/** Classes that set `background-color`, ignoring `hover:` and other variants. */
function backgrounds(el: Element): string[] {
  return [...el.classList].filter((c) => c.startsWith('bg-'));
}

/** Classes that set `border-color`. `border`/`border-b-2` set width, not colour. */
function borderColours(el: Element): string[] {
  return [...el.classList].filter(
    (c) => c.startsWith('border-') && !/^border-(b|t|l|r|x|y)?-?\d+$/.test(c),
  );
}

/**
 * Every variant, as a `Record` keyed by the union rather than a plain array.
 *
 * This is the part that keeps the suite honest while several people are adding
 * variants to these components: a new member of `ButtonVariant` leaves this
 * record missing a key, which is a *compile* error in this file. Whoever adds
 * the variant is made to list it here, and listing it is what enrols it in the
 * colour-collision checks below. An array would have gone quietly stale.
 */
const ALL_BUTTON_VARIANTS: Record<ButtonVariant, true> = {
  primary: true,
  secondary: true,
  ghost: true,
  subtle: true,
  danger: true,
  dangerGhost: true,
  dangerSolid: true,
  success: true,
  warning: true,
  info: true,
  accent: true,
  link: true,
  pill: true,
  tab: true,
};

const ALL_ICON_VARIANTS: Record<IconButtonVariant, true> = {
  ghost: true,
  subtle: true,
  primary: true,
  danger: true,
  dangerGhost: true,
};

const BUTTON_VARIANTS = Object.keys(ALL_BUTTON_VARIANTS) as ButtonVariant[];
const ICON_VARIANTS = Object.keys(ALL_ICON_VARIANTS) as IconButtonVariant[];

describe('Button colour classes are unambiguous', () => {
  for (const variant of BUTTON_VARIANTS) {
    for (const active of [false, true]) {
      it(`${variant}${active ? ' (active)' : ''} sets each colour property at most once`, () => {
        render(
          <Button variant={variant} active={active}>
            label
          </Button>,
        );
        const el = screen.getByRole('button');
        expect(textColours(el)).toHaveLength(1);
        expect(backgrounds(el).length).toBeLessThanOrEqual(1);
        expect(borderColours(el).length).toBeLessThanOrEqual(1);
      });
    }
  }

  it('renders the accent treatment when active, not the resting colour', () => {
    // The regression itself: `ghost` used to keep `text-text-muted` here.
    render(<Button variant="ghost" active>on</Button>);
    expect(textColours(screen.getByRole('button'))).toEqual(['text-accent']);
  });

  it('swaps the tab border rather than stacking it', () => {
    render(<Button variant="tab" active>on</Button>);
    const el = screen.getByRole('button');
    expect(el).toHaveClass('border-accent');
    expect(el).not.toHaveClass('border-transparent');
  });

  it('never puts white on the accent fill', () => {
    // MIGRATION REFERENCE §3: accent is ClickHouse yellow in dark mode, so
    // `text-white` on it is unreadable — and `.text-white` outsorts
    // `.text-on-accent`, so a call site could not correct it either.
    render(<Button variant="primary">go</Button>);
    const el = screen.getByRole('button');
    expect(el).toHaveClass('bg-accent');
    expect(el).not.toHaveClass('text-white');
  });

  it('drops the padding for a link, which cannot be overridden from outside', () => {
    render(<Button variant="link">more</Button>);
    const el = screen.getByRole('button');
    expect(el).not.toHaveClass('px-3');
    expect(el).toHaveClass('text-accent');
  });
});

describe('IconButton colour classes are unambiguous', () => {
  for (const variant of ICON_VARIANTS) {
    for (const active of [false, true]) {
      it(`${variant}${active ? ' (active)' : ''} sets each colour property at most once`, () => {
        render(
          <IconButton variant={variant} active={active} label="act">
            <svg />
          </IconButton>,
        );
        const el = screen.getByRole('button');
        expect(textColours(el)).toHaveLength(1);
        expect(backgrounds(el).length).toBeLessThanOrEqual(1);
      });
    }
  }

  it('renders the accent treatment when active, not the resting colour', () => {
    render(<IconButton variant="ghost" active label="act"><svg /></IconButton>);
    expect(textColours(screen.getByRole('button'))).toEqual(['text-accent']);
  });

  it('names itself from `label`, for both pointer and screen reader', () => {
    render(<IconButton label="Delete task"><svg /></IconButton>);
    const el = screen.getByRole('button', { name: 'Delete task' });
    expect(el).toHaveAttribute('title', 'Delete task');
  });
});
