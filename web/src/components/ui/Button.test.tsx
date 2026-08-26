import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { Badge } from './Badge';
import { Button, type ButtonVariant } from './Button';
import { Checkbox } from './Checkbox';
import { IconButton, type IconButtonVariant } from './IconButton';
import { Select } from './Select';
import { overridable } from './styles';
import { TextArea, TextField } from './TextField';

/**
 * These specs hold one rule that no other check can see: `tsc`, eslint and the
 * build all pass a class collision straight through.
 *
 * Tailwind v4 emits same-property utilities in alphabetical order of class
 * name. Between two colour classes on one element, the winner is therefore the
 * later-*sorting* name — not the one written last in the `class` attribute, and
 * not the one from the "more specific" lookup table. `.text-accent` sorts
 * before every other colour token in this app, so a selected treatment appended
 * after a coloured base loses.
 *
 * None of that is visible in jsdom, which applies no CSS at all. What can be
 * asserted is the structural rule that makes the ordering irrelevant: an
 * element must never carry two classes that set the same property.
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
 * The same collision, for the properties that are not colours.
 *
 * Tailwind orders numerically as well as alphabetically, so a *smaller* value
 * always sorts earlier and therefore loses: `size="md"` plus a call site's
 * `py-1` renders `py-2`, and `size="xs"` plus `px-0` renders `px-2`. The rule a
 * caller could actually rely on is "you may increase a spacing value but never
 * decrease it", which nobody will remember — so the variant and size tables must
 * simply never emit two of the same property, exactly as with colour.
 *
 * Grouped by the precise utility prefix rather than by CSS property, because
 * `px-2` and `pl-8` legitimately coexist: the longhand sorts after the shorthand
 * and is meant to win. Only a genuine duplicate — two `px-*`, two `rounded-*` —
 * is a bug.
 */
const SPACING_GROUPS = ['p', 'px', 'py', 'gap', 'rounded'] as const;

function duplicateSpacing(el: Element): string[] {
  const classes = [...el.classList].filter((c) => !c.includes(':'));
  const clashes: string[] = [];
  for (const group of SPACING_GROUPS) {
    const hits = classes.filter(
      (c) => c === group || c.startsWith(`${group}-`),
    );
    if (hits.length > 1) clashes.push(`${group}: ${hits.join(' + ')}`);
  }
  // Font size is the same shape: `text-xs` against `text-sm`.
  const sizes = classes.filter((c) => /^text-(2xs|xs|sm|base|lg|xl|display)$/.test(c));
  if (sizes.length > 1) clashes.push(`font-size: ${sizes.join(' + ')}`);
  return clashes;
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
  accentSoft: true,
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

/**
 * Hover feedback that only exists as a surface change disappears when the
 * parent already carries that surface. No other check can see it: the classes
 * are all present and well-formed, nothing collides, and the failure is purely
 * that the user sees nothing happen.
 *
 * `subtle` hovers *both* the text and the surface, and the surface half looks
 * like the redundant one, so "we already hover the background, drop the text
 * move" reads as an obvious cleanup. It is backwards: on a raised parent the
 * surface step is the invisible half, roughly 2/255 in dark and 4/255 in light
 * (see the comment on INACTIVE.subtle for the arithmetic). A comment cannot
 * fail a build. This can.
 */
describe('Button hover feedback survives its container', () => {
  it('subtle keeps a text hover: its surface hover is invisible on a raised parent', () => {
    // TasksPage's Board/List switch is the live example — `bg-surface-raised`
    // on the container, `subtle` segments sitting directly on it.
    render(<Button variant="subtle">List</Button>);
    expect(screen.getByRole('button')).toHaveClass('hover:text-text');
  });
});

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

  // Every variant against every size, because the clash is between the two
  // tables rather than inside either one.
  for (const variant of BUTTON_VARIANTS) {
    for (const size of ['xs', 'sm', 'md'] as const) {
      it(`${variant} at ${size} sets each spacing property at most once`, () => {
        render(
          <Button variant={variant} size={size}>
            label
          </Button>,
        );
        expect(duplicateSpacing(screen.getByRole('button'))).toEqual([]);
      });
    }
  }

  it('renders the accent treatment when active, not the resting colour', () => {
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
    // The accent is ClickHouse yellow in dark mode, so `text-white` on it is
    // unreadable — and `.text-white` outsorts `.text-on-accent`, so a call site
    // cannot correct it either.
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

/**
 * The caller's side of the same rule.
 *
 * Everything above pins the *primitive* to one class per property. That is
 * necessary and not sufficient: a call site that writes `className="px-0"` is
 * adding the second one from outside, and `.px-0` is emitted before `.px-2`, so
 * on class order alone the primitive's padding wins and the override is inert.
 * `overridable` drops the default instead.
 *
 * The assertions are `not.toHaveClass` on the default rather than `toHaveClass`
 * on the override: the override is present either way, so only the default
 * being gone tells a working merge from an inert one, and that is what jsdom can
 * see without a stylesheet.
 */
describe('a caller className replaces the primitive default', () => {
  const CASES = [
    // [what, props, className, displaced default, note]
    ['horizontal padding', { size: 'xs' } as const, 'px-0', 'px-2', 'PromptRewriteCard'],
    ['vertical padding', { size: 'sm' } as const, 'py-0.5', 'py-1.5', 'TodoPanel'],
    ['type size', { size: 'sm' } as const, 'text-sm', 'text-xs', 'TodoPanel'],
    ['gap', { size: 'md' } as const, 'gap-2.5', 'gap-2', 'ReviewLoopCard'],
    ['flex shrink', {} as const, 'shrink', 'shrink-0', 'NotificationsPage session link'],
    ['radius', { variant: 'pill' } as const, 'rounded', 'rounded-full', 'InteractiveQuestionCard'],
    ['radius', { variant: 'primary' } as const, 'rounded', 'rounded-lg', 'InteractiveQuestionCard'],
    ['radius', { variant: 'subtle' } as const, 'rounded-none', 'rounded', 'ChatInput menu'],
  ] as const;

  for (const [what, props, override, displaced, note] of CASES) {
    it(`${what}: ${override} displaces ${displaced} (${note})`, () => {
      render(
        <Button {...props} className={override}>
          label
        </Button>,
      );
      const el = screen.getByRole('button');
      expect(el).toHaveClass(override);
      expect(el).not.toHaveClass(displaced);
    });
  }

  it('leaves the properties the caller did not name alone', () => {
    // The failure mode on the other side: a merge that is too eager strips the
    // variant's colour or the size's type scale along with its padding.
    render(<Button variant="primary" size="sm" className="px-0">go</Button>);
    const el = screen.getByRole('button');
    expect(el).toHaveClass('text-on-accent', 'bg-accent', 'text-xs', 'gap-1.5');
    expect(el).not.toHaveClass('px-3');
  });

  it('keeps a longhand beside the shorthand it refines', () => {
    // `pl-8` indents the run-later submenu items under their parent row; it is
    // meant to sit *with* `px-3`, not to replace it. Tailwind gives the longhand
    // the win on its own, and dropping `px-3` here would lose the right padding.
    render(<Button size="sm" className="pl-8">1 hour</Button>);
    const el = screen.getByRole('button');
    expect(el).toHaveClass('px-3', 'pl-8');
  });

  it('lets a caller cancel a hover it does not want', () => {
    render(<Button variant="ghost" className="hover:bg-transparent">x</Button>);
    const el = screen.getByRole('button');
    expect(el).toHaveClass('hover:bg-transparent');
    expect(el).not.toHaveClass('hover:bg-surface-raised');
  });

  it('applies to IconButton, whose radius and box the composer overrides', () => {
    render(
      <IconButton label="More options" size="md" className="rounded-xl">
        <svg />
      </IconButton>,
    );
    const el = screen.getByRole('button');
    expect(el).toHaveClass('rounded-xl', 'w-10', 'h-10');
    // `md` already sets `rounded-xl`, so nothing is displaced here — the point
    // is that the size's box survives a radius override.
  });

  it('applies to Badge', () => {
    render(<Badge className="text-xs px-2">42</Badge>);
    const el = screen.getByText('42');
    expect(el).toHaveClass('text-xs', 'px-2');
    expect(el).not.toHaveClass('text-2xs', 'px-1.5');
  });

  it('applies to TextField and TextArea', () => {
    render(
      <>
        <TextField aria-label="answer" className="px-2.5 rounded" />
        <TextArea aria-label="prompt" className="text-sm" />
      </>,
    );
    const input = screen.getByRole('textbox', { name: 'answer' });
    expect(input).toHaveClass('px-2.5', 'rounded', 'py-2');
    expect(input).not.toHaveClass('px-3', 'rounded-lg');

    const area = screen.getByRole('textbox', { name: 'prompt' });
    expect(area).toHaveClass('text-sm', 'resize-none');
  });

  it('applies to Select', () => {
    render(<Select aria-label="status" fieldSize="sm" className="py-1.5" />);
    const el = screen.getByRole('combobox');
    expect(el).toHaveClass('py-1.5', 'px-2', 'cursor-pointer');
    expect(el).not.toHaveClass('py-1');
  });

  it('applies to Checkbox, on the input and on its label wrapper', () => {
    render(<Checkbox label="Archived" className="w-3.5" labelClassName="text-xs" />);
    const input = screen.getByRole('checkbox', { name: 'Archived' });
    expect(input).toHaveClass('w-3.5', 'accent-accent');

    const label = input.closest('label');
    expect(label).toHaveClass('text-xs');
    expect(label).not.toHaveClass('text-sm');
  });
});

/**
 * `overridable` resolves the caller against the tables and stops there.
 *
 * Running the whole string through tailwind-merge in one go would be shorter
 * and would also silently fix collisions *between* the size and variant tables,
 * which is what the rest of this file checks for. These specs pin the boundary,
 * because the "simplification" that erases it looks like an improvement in
 * review.
 */
describe('overridable leaves the primitive tables alone', () => {
  it('does not resolve a collision inside the defaults', () => {
    expect(overridable(['px-2 px-3'], 'text-sm')).toBe('px-2 px-3 text-sm');
  });

  it('does not resolve one while also applying an override', () => {
    expect(overridable(['px-2 px-3 py-1'], 'py-0')).toBe('px-2 px-3 py-0');
  });

  it('is a plain join when the caller passes nothing', () => {
    expect(overridable(['a b', false, 'c'])).toBe('a b c');
  });
});
