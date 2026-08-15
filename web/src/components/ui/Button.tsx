import { forwardRef, type ButtonHTMLAttributes, type ReactNode } from 'react';
import { cx } from './styles';

/**
 * The app's button.
 *
 * There were 239 hand-styled `<button>` elements across 74 files before this,
 * carrying 195 distinct class strings between them. Nearly all of those were
 * one of six shapes with the spacing or the disabled opacity drifting by a step
 * — `disabled:opacity-30`, `-40` and `-50` all appear, on buttons that do the
 * same job. The variants below are those six shapes, named.
 *
 * Deliberately **not** Click UI's `Button`: that one renders its content from a
 * `label` string plus an `iconLeft`/`iconRight` icon name, and takes no
 * children. Most buttons in this app hold real markup — an icon element, a
 * truncating span, a count badge — so wrapping it would have meant rewriting
 * every call site's content rather than just its classes. Click UI's `Button`
 * is still the right thing for a plain labelled button; import it directly.
 *
 * Everything the design system does contribute reaches this through the
 * semantic tokens (`bg-accent`, `text-text-muted`, `border-border-subtle`),
 * which is where Click UI's palette is bound.
 */

export type ButtonVariant =
  | 'primary'
  | 'secondary'
  | 'ghost'
  | 'subtle'
  | 'danger'
  | 'dangerGhost'
  | 'dangerSolid'
  | 'success'
  | 'warning'
  | 'info'
  | 'accent'
  | 'accentSoft'
  | 'link'
  | 'pill'
  | 'tab';

export type ButtonSize = 'xs' | 'sm' | 'md';

const VARIANTS: Record<ButtonVariant, string> = {
  /**
   * The page's one committing action.
   *
   * `text-on-accent`, never `text-white`: the accent is ClickHouse yellow in
   * dark mode, where white is unreadable on it. A call site cannot correct this
   * itself — Tailwind emits same-property colour utilities alphabetically, so
   * `.text-white` lands after `.text-on-accent` and wins at equal specificity.
   * It has to be right here.
   */
  primary: 'text-on-accent bg-accent hover:bg-accent-hover rounded-lg font-medium',
  /** Sits beside a primary without competing with it. */
  secondary:
    'text-text-secondary bg-surface-raised hover:bg-surface-hover border border-border rounded-lg',
  /**
   * Bare label; only the text colour moves. For "Cancel", "Show more".
   * Resting colour lives in INACTIVE — see the note there.
   */
  ghost: 'rounded',
  /**
   * The menu/list-row shape: no chrome at rest, a surface on hover.
   * Resting colour lives in INACTIVE — see the note there.
   */
  subtle: 'rounded',
  /**
   * Destructive, tinted. Uses `hue-red` rather than a stock `red-*` so it
   * follows the light theme — the hand-rolled `bg-red-600` buttons this
   * replaces are pinned to dark-theme values.
   */
  danger:
    'text-hue-red bg-hue-red/15 hover:bg-hue-red/25 rounded-md font-medium',
  /**
   * Affirmative, but *not* the page's primary action — "Accept as-is",
   * "Adopt & continue", "Start review loop". Green rather than accent, because
   * the accent is now ClickHouse yellow and reads as neither.
   *
   * Tinted, not solid, and that is forced rather than chosen: `--theme-success`
   * is Click UI's feedback *foreground*, which in dark mode is a pale mint
   * (#ccffd0). `bg-success text-white` measures 1.12:1 — the same bug as
   * `bg-accent text-white`, from the same cause. Text-on-tint is the only
   * pairing legible in both themes (10.5:1 dark, 7.3:1 light).
   */
  success:
    'text-success bg-success-bg border border-success-border hover:border-success rounded-md font-medium',
  /** Same shape as `success`, for a cautionary action. 5.7:1 dark, 5.3:1 light. */
  warning:
    'text-warning bg-warning-bg border border-warning-border hover:border-warning rounded-md font-medium',
  /** Same shape as `success`, for an informational action. 7.2:1 dark, 6.2:1 light. */
  info: 'text-info bg-info-bg border border-info-border hover:border-info rounded-md font-medium',
  /**
   * The accent member of the tinted family, for a committing action that must
   * not shout — a list of answer options where every one is equally valid, or a
   * button standing beside `success`/`danger` siblings that has to read as the
   * same kind of control. `primary` would be too loud on all of them, and
   * `accent` has no fill so it reads as a link.
   *
   * Like the other tinted variants it is never paired with `active`, so it sets
   * its colour directly rather than going through INACTIVE.
   */
  accentSoft:
    'text-accent bg-accent/15 border border-accent/30 hover:bg-accent/25 rounded-md font-medium',
  /**
   * An accent-coloured text button that is *not* a selection — "Clear filter",
   * "View processing session". Distinct from `link` (which drops the padding to
   * sit inside a sentence) and from `ghost active` (which would claim the
   * control is currently selected).
   */
  accent: 'text-accent hover:bg-surface-raised rounded',
  /**
   * Destructive, but quiet until pointed at — the row-level delete/remove/purge
   * treatment. Distinct from `danger`, which is already red at rest and shouts
   * from inside a list.
   */
  dangerGhost: 'text-text-dim hover:text-hue-red hover:bg-hue-red/10 rounded',
  /**
   * Destructive and unmissable — for the confirm step, not the trigger.
   *
   * `bg-error-solid`, not `bg-hue-red`. An earlier version of this comment
   * claimed `text-white` was safe here "because it sits on red, not on the
   * accent" — that was wrong. `hue-red` is an *identity* hue meant for text on
   * the page background, so it flips to a light #ff7575 in dark mode, where
   * white on it measures 2.61:1. `error-solid` is a theme-independent palette
   * entry (#c10000 both ways), giving 6.43:1 in either theme.
   */
  dangerSolid:
    'text-white bg-error-solid hover:bg-error-solid/90 rounded-md font-medium',
  /** An inline affordance that reads as a link but acts as a button. */
  link: 'text-accent hover:underline rounded',
  /** Filter chip. Pair with `active`. */
  pill: 'rounded-full border whitespace-nowrap',
  /** Underlined tab. Pair with `active`. */
  tab: 'font-medium border-b-2 rounded-none',
};

/**
 * The selected treatment, per variant. The app was already consistent about
 * this — an active control is accent-on-accent-tint — it just spelled the tint
 * three different ways (`/10`, `/15`, `/20`).
 *
 * **Any variant with an entry here must set no colour in VARIANTS.** Its
 * resting colour goes in INACTIVE instead, so that exactly one of the two sets
 * is ever on the element.
 *
 * That is not tidiness, it is the only thing that works. Tailwind v4 emits
 * same-property utilities in alphabetical order of class name, so between two
 * colour classes on one element the later-*sorting* name wins — not the one
 * written last, and not the one from the "more specific" map. `.text-accent`
 * sorts before every other colour token in this app; measured in the built
 * stylesheet it lands at offset 49085, against `.text-hue-red` 51903,
 * `.text-on-accent` 52978, `.text-text-dim` 53924, `.text-text-faint` 53967,
 * `.text-text-muted` 54167, `.text-text-secondary` 54214 and `.text-white`
 * 54310. Appending an accent `ACTIVE` after a coloured base therefore lost
 * silently, every time — `active` simply did nothing on `ghost` and `subtle`.
 * The same trap caught `tab`, whose `border-transparent` outsorted, and so
 * beat, `ACTIVE.tab`'s `border-accent`.
 *
 * `Button.test.tsx` pins the invariant so it cannot come back unnoticed.
 */
const ACTIVE: Partial<Record<ButtonVariant, string>> = {
  pill: 'bg-accent/15 text-accent border-accent/30',
  tab: 'text-accent border-accent',
  subtle: 'bg-accent/15 text-accent',
  ghost: 'text-accent',
};

const INACTIVE: Partial<Record<ButtonVariant, string>> = {
  pill: 'text-text-dim border-border hover:text-text-muted',
  tab: 'text-text-dim border-transparent hover:text-text-muted',
  ghost: 'text-text-muted hover:text-text-secondary',
  /**
   * The hover moves the *text* as well as the surface, because the surface
   * alone is not always visible: a segmented control puts `bg-surface-raised`
   * on the container and the segments sit directly on it, so a
   * `hover:bg-surface-raised` segment is hovering to the colour it is already
   * on and the control goes inert. (`TasksPage`'s Board/List switch is exactly
   * that shape.) A call site cannot patch it either — `hover:bg-surface-hover`
   * from a className loses to `hover:bg-surface-raised` on the same
   * alphabetical ordering described below.
   *
   * The text moves *up* from the resting colour rather than the resting colour
   * moving down: `subtle` is the menu/list-row variant, ~71 rows across the
   * app, and dimming all of them at rest to buy a hover state on one segmented
   * control would be a bad trade.
   */
  // `surface-hover`, not `surface-raised`: this variant is used for segmented
  // controls and list rows, which frequently sit *inside* a raised group, where
  // hovering to `surface-raised` is a no-op and the row appears dead. Hovering
  // to `surface-hover` is at least always a change, though on a raised parent
  // it is only a couple of units — a genuinely distinct step would need a token
  // the palette does not currently have.
  subtle: 'text-text-secondary hover:text-text hover:bg-surface-hover',
};

const SIZES: Record<ButtonSize, string> = {
  xs: 'px-2 py-1 text-xs gap-1',
  sm: 'px-3 py-1.5 text-xs gap-1.5',
  md: 'px-3 py-2 text-sm gap-2',
};

/**
 * `link` takes the type scale but not the padding — a link sitting in a
 * sentence cannot carry a button's box. The classes have to be dropped rather
 * than overridden: `p-0` from a call site would lose to `px-3` here, on the
 * same ordering rule described above.
 */
const LINK_SIZES: Record<ButtonSize, string> = {
  xs: 'text-xs gap-1',
  sm: 'text-xs gap-1.5',
  md: 'text-sm gap-2',
};

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: ButtonSize;
  /** Selected/current. Only `pill`, `tab`, `subtle` and `ghost` render it. */
  active?: boolean;
  /** Stretch to the container. Off by default; a bare `w-full` in the base
   *  classes would fight every caller that wants an intrinsic width. */
  fullWidth?: boolean;
  children?: ReactNode;
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(function Button(
  {
    variant = 'secondary',
    size = 'sm',
    active = false,
    fullWidth = false,
    className,
    type,
    children,
    ...rest
  },
  ref,
) {
  const state = active ? ACTIVE[variant] : INACTIVE[variant];
  return (
    <button
      ref={ref}
      // Buttons inside a form default to `submit` and will submit it. Almost
      // none of these are submit buttons, and the ones that are say so.
      type={type ?? 'button'}
      className={cx(
        'inline-flex items-center justify-center shrink-0 cursor-pointer',
        'transition-colors disabled:opacity-50 disabled:cursor-not-allowed',
        variant === 'link' ? LINK_SIZES[size] : SIZES[size],
        VARIANTS[variant],
        state,
        fullWidth && 'w-full',
        className,
      )}
      {...rest}
    >
      {children}
    </button>
  );
});
