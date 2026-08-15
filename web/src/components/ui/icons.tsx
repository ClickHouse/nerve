import type { ComponentProps, ComponentType, ReactNode, SVGProps } from 'react';
import { Icon as ClickIcon, type IconName } from '@clickhouse/click-ui/Icon';

/**
 * The app's icon set, on Click UI's glyphs behind lucide's call signature.
 *
 * 449 call sites across 77 files render icons as `<Name size={14}
 * className="..."/>`. Click UI ships the same idea under a different shape —
 * one `<Icon name="kebab-case"/>` component over a closed union of 174 names —
 * so switching sets by hand would have meant touching every one of those call
 * sites. This module is the adapter instead: one component per lucide name the
 * app actually uses, taking the props the app already passes. A page moves onto
 * the design system by changing `from 'lucide-react'` to `from
 * '../ui/icons'` — nothing else.
 *
 * Two things are deliberate about how the Click UI glyphs are wrapped:
 *
 * **Size stays in pixels.** Click UI sizes icons on a named scale (`xs` 12,
 * `sm` 16, `md` 20 …) but this app uses fourteen distinct pixel sizes, over
 * half of them 13 or 14, and some computed (`size={small ? 12 : 14}`). Snapping
 * to the nearest named step would have resized nearly every icon in the app, so
 * the pixel value is passed straight through as an explicit width/height
 * instead. Click UI's `size` scale is still reachable — it is what the `xs`…
 * `xxl` steps mean — but nothing here has to opt into it.
 *
 * **Icons are decorative and unnamed.** Click UI's `<Icon>` defaults to
 * `role="img"` with `aria-label` set to the glyph name, which would put 449
 * nodes announced as "cross" or "loading" into the accessibility tree and into
 * every `getByRole` query in the test suite. Every icon-only control in this app
 * already carries its own `title`/`aria-label`, so the icons are hidden and the
 * control keeps the name — which is also what lucide did.
 *
 * Where Click UI has no equivalent glyph, one is drawn here on the same 24×24
 * grid at the same 1.5 stroke with round caps and joins. Those are marked
 * `[drawn]` below. Everything else resolves to a real Click UI icon, so the set
 * tracks the design system as it changes.
 */

export interface IconProps
  extends Omit<SVGProps<SVGSVGElement>, 'ref' | 'width' | 'height'> {
  /** Pixel size, as lucide took it. Matches lucide's default of 24. */
  size?: number | string;
}

/** The shape every icon in this module has — lucide's, minus the parts unused here. */
export type Icon = ComponentType<IconProps>;

const cx = (...parts: Array<string | false | undefined>) =>
  parts.filter(Boolean).join(' ');

/**
 * A Click UI glyph in lucide's clothing.
 *
 * `<Icon>` renders the glyph inside a wrapper element and recolours it from
 * `currentColor` through its own stylesheet — which is what keeps the app's
 * `text-hue-red` / `text-text-faint` classes working on an icon whose paths
 * ship with a hard-coded stroke. `className` lands on that wrapper, so Tailwind
 * classes on the call site (`animate-spin`, `absolute left-2.5`) still apply to
 * the box the glyph occupies. `inline-flex` is forced because the wrapper is
 * `display: flex` by default, which would break an icon out onto its own line
 * in the handful of places one sits inline in text.
 */
function clickUI(name: IconName, displayName: string): Icon {
  function Glyph({ size = 24, className, ...rest }: IconProps) {
    // A bare number would land in the CSS custom property unitless, which is
    // not a valid length; the wrapper reads these as CSS, not as attributes.
    const dim = typeof size === 'number' ? `${size}px` : size;
    const props = {
      name,
      width: dim,
      height: dim,
      className: cx('inline-flex shrink-0', className),
      role: undefined,
      'aria-label': undefined,
      'aria-hidden': true,
      focusable: false,
      ...rest,
    };
    return <ClickIcon {...(props as ComponentProps<typeof ClickIcon>)} />;
  }
  Glyph.displayName = displayName;
  return Glyph;
}

/**
 * A glyph drawn here because Click UI has none, in Click UI's idiom: 24×24,
 * 1.5 stroke, round caps and joins, no fill, coloured by `currentColor`.
 */
function drawn(
  displayName: string,
  children: ReactNode,
  overrides?: SVGProps<SVGSVGElement>,
): Icon {
  function Glyph({ size = 24, className, ...rest }: IconProps) {
    return (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        width={size}
        height={size}
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth={1.5}
        strokeLinecap="round"
        strokeLinejoin="round"
        className={cx('shrink-0', className)}
        aria-hidden
        focusable={false}
        {...overrides}
        {...rest}
      >
        {children}
      </svg>
    );
  }
  Glyph.displayName = displayName;
  return Glyph;
}

/*
 * Shared geometry, lifted from the Click UI glyphs that already use it, so the
 * drawn icons sit on the same circle and the same page as the real ones.
 */

/** The circle from `check-in-circle` / `info-in-circle` / `question`. */
const RING = 'M12 21v0a9 9 0 0 1-9-9v0a9 9 0 0 1 9-9v0a9 9 0 0 1 9 9v0a9 9 0 0 1-9 9Z';
/** The page outline from `document`. */
const PAGE =
  'M18.003 21.004H5.998a2 2 0 0 1-2-2V4.996a2 2 0 0 1 2-2h12.005a2 2 0 0 1 2 2v14.006a2 2 0 0 1-2 2.001Z';
/** The shield from `secure`. */
const SHIELD =
  'M19.1 5.921a11.268 11.268 0 0 1-6.463-2.688.989.989 0 0 0-1.274 0A11.275 11.275 0 0 1 4.9 5.921a.988.988 0 0 0-.9.991v4.33c0 4.367 3.156 8.462 7.478 9.685.339.096.706.096 1.045 0C16.844 19.703 20 15.61 20 11.243v-4.33a.988.988 0 0 0-.9-.992Z';
/** A dot, in the sub-pixel arc form the Click UI glyphs use for one. */
const dot = (x: number, y: number) =>
  `M${x - 0.001} ${y}a.25.25 0 1 0 .002.5.25.25 0 0 0-.002-.5`;

/* ------------------------------------------------------------------ *
 * Direct equivalents
 * ------------------------------------------------------------------ */

export const Activity = clickUI('activity', 'Activity');
export const AlertTriangle = clickUI('warning', 'AlertTriangle');
export const ArrowLeft = clickUI('arrow-left', 'ArrowLeft');
export const ArrowRight = clickUI('arrow-right', 'ArrowRight');
export const BarChart3 = clickUI('bar-chart', 'BarChart3');
export const Bell = clickUI('bell', 'Bell');
export const BookOpen = clickUI('book', 'BookOpen');
export const Calendar = clickUI('calendar', 'Calendar');
export const Check = clickUI('check', 'Check');
export const CheckCheck = clickUI('double-check', 'CheckCheck');
export const ChevronDown = clickUI('chevron-down', 'ChevronDown');
export const ChevronLeft = clickUI('chevron-left', 'ChevronLeft');
export const ChevronRight = clickUI('chevron-right', 'ChevronRight');
export const Circle = clickUI('circle', 'Circle');
export const Copy = clickUI('copy', 'Copy');
export const Cpu = clickUI('cpu', 'Cpu');
export const Database = clickUI('database', 'Database');
export const DollarSign = clickUI('dollar', 'DollarSign');
export const Download = clickUI('download', 'Download');
export const Eye = clickUI('eye', 'Eye');
export const EyeOff = clickUI('eye-closed', 'EyeOff');
export const Filter = clickUI('filter', 'Filter');
export const Folder = clickUI('folder-closed', 'Folder');
export const FolderOpen = clickUI('folder-open', 'FolderOpen');
export const Globe = clickUI('globe', 'Globe');
export const History = clickUI('history', 'History');
export const Hourglass = clickUI('sand-glass', 'Hourglass');
export const Lightbulb = clickUI('light-bulb', 'Lightbulb');
export const List = clickUI('list-bulleted', 'List');
export const Lock = clickUI('lock', 'Lock');
export const Mail = clickUI('email', 'Mail');
export const Monitor = clickUI('display', 'Monitor');
export const Moon = clickUI('moon', 'Moon');
export const MoreHorizontal = clickUI('dots-horizontal', 'MoreHorizontal');
export const Pause = clickUI('pause', 'Pause');
export const Play = clickUI('play', 'Play');
export const Plug = clickUI('plug', 'Plug');
export const Plus = clickUI('plus', 'Plus');
export const Radio = clickUI('cell-tower', 'Radio');
export const Rocket = clickUI('rocket', 'Rocket');
export const Search = clickUI('search', 'Search');
export const Server = clickUI('server', 'Server');
export const ShieldCheck = clickUI('secure', 'ShieldCheck');
export const SlidersHorizontal = clickUI('settings', 'SlidersHorizontal');
export const Square = clickUI('square', 'Square');
export const Star = clickUI('star', 'Star');
export const Trash2 = clickUI('trash', 'Trash2');
export const Zap = clickUI('flash', 'Zap');

/** Click UI's `enter` is the same return arrow lucide draws for `CornerDownLeft`. */
export const CornerDownLeft = clickUI('enter', 'CornerDownLeft');
/** `popout` is the box-with-escaping-arrow glyph, i.e. lucide's external link. */
export const ExternalLink = clickUI('popout', 'ExternalLink');
/** The grip is Click UI's two columns of dots. */
export const GripVertical = clickUI('dots-vertical-double', 'GripVertical');
/** `disk` is a floppy — the save glyph, and close enough to a drive to serve both. */
export const Save = clickUI('disk', 'Save');
export const HardDrive = clickUI('disk', 'HardDrive');
/** `line-in-circle` is the prohibition sign. */
export const Ban = clickUI('line-in-circle', 'Ban');
/** `query` is the `>_` prompt in a rounded square. */
export const Terminal = clickUI('query', 'Terminal');
export const SquareTerminal = clickUI('query', 'SquareTerminal');
/**
 * Click UI's `loading` is the static spinner glyph — the animation lives in a
 * separate `loading-animated` icon. The static one is what lucide's `Loader2`
 * is too, and every call site in this app already spins it with `animate-spin`;
 * using the animated glyph would spin it twice.
 */
export const Loader2 = clickUI('loading', 'Loader2');

/* ------------------------------------------------------------------ *
 * Near-duplicates the app used inconsistently, folded onto one glyph
 * ------------------------------------------------------------------ */

export const CheckCircle = clickUI('check-in-circle', 'CheckCircle');
export const CheckCircle2 = CheckCircle;
export const CircleCheck = CheckCircle;

export const HelpCircle = clickUI('question', 'HelpCircle');
export const CircleHelp = HelpCircle;

export const Clock = clickUI('clock', 'Clock');
export const Clock3 = Clock;

export const Pencil = clickUI('pencil', 'Pencil');
export const Edit3 = Pencil;
export const FileEdit = Pencil;

export const RefreshCw = clickUI('refresh', 'RefreshCw');
export const RotateCw = RefreshCw;
export const RotateCcw = RefreshCw;

export const Sparkle = clickUI('sparkle', 'Sparkle');
export const Sparkles = Sparkle;

export const FileText = clickUI('document', 'FileText');
export const File = FileText;

export const MessageSquare = clickUI('chat', 'MessageSquare');
export const MessageCircle = MessageSquare;

/* ------------------------------------------------------------------ *
 * Compromises — the nearest Click UI glyph, not the same drawing
 * ------------------------------------------------------------------ */

/** Click UI has no dollar-in-circle; this is the bare `$`. */
export const CircleDollarSign = clickUI('dollar', 'CircleDollarSign');
/** `git-merge` — a merge graph where lucide draws a branch. Same family. */
export const GitBranch = clickUI('git-merge', 'GitBranch');
/** No hammer or wrench in the set; `gear` is the generic tool. */
export const Hammer = clickUI('gear', 'Hammer');
export const Wrench = clickUI('gear', 'Wrench');
/** Checkboxes become bullets — Click UI has no checklist glyph. */
export const ListTodo = clickUI('list-bulleted', 'ListTodo');
/** Two stacked pages; the same glyph Click UI uses for copy. */
export const Files = clickUI('copy', 'Files');
/** An alarm clock, kept distinct from the plain `clock` used for `Clock`. */
export const Timer = clickUI('alarm', 'Timer');
/** A lidded box. */
export const Archive = clickUI('cards', 'Archive');
/** `?` on a card — the nearest thing to a question in a speech bubble. */
export const MessageCircleQuestion = clickUI('support', 'MessageCircleQuestion');
/** The check is dropped; Click UI has only the plain magnifier. */
export const SearchCheck = clickUI('search', 'SearchCheck');
/** A loop with an arrowhead, where lucide draws a rectangular repeat. */
export const Repeat = clickUI('integrations', 'Repeat');
/** Nodes joined by edges — lucide draws boxes, same reading. */
export const Workflow = clickUI('tree-structure', 'Workflow');
/** Expand-in-place, rather than lucide's pair of diagonal arrows. */
export const Maximize2 = clickUI('expand-all', 'Maximize2');
/** A stop button, where lucide draws a cross in an octagon. Both read "halted". */
export const OctagonX = clickUI('stop', 'OctagonX');
/** The bare cross. */
export const X = clickUI('cross', 'X');
/**
 * `slide-out` is exactly this: a bar down the left edge with the arrow moving
 * away from the content. Its `PanelLeftOpen` counterpart is drawn below,
 * because Click UI's `slide-in` mirrors the bar to the right edge — which would
 * make the bar jump sides as the toggle flips.
 */
export const PanelLeftClose = clickUI('slide-out', 'PanelLeftClose');

/* ------------------------------------------------------------------ *
 * Drawn here — no Click UI equivalent
 * ------------------------------------------------------------------ */

/** [drawn] `!` in the ring the other in-circle glyphs use. */
export const AlertCircle = drawn(
  'AlertCircle',
  <>
    <path d={RING} />
    <path d="M12 7.75v5" />
    <path d={dot(12, 15.5)} />
  </>,
);

/** [drawn] Cross in the same ring. */
export const XCircle = drawn(
  'XCircle',
  <>
    <path d={RING} />
    <path d="M9.5 9.5 14.5 14.5M14.5 9.5 9.5 14.5" />
  </>,
);
export const CircleX = XCircle;

/** [drawn] The ring, dashed — a pending step that has not started. */
export const CircleDashed = drawn(
  'CircleDashed',
  <path d={RING} strokeDasharray="2.6 2.9" />,
);

/** [drawn] Click UI's square with Click UI's check inside it. */
export const CheckSquare = drawn(
  'CheckSquare',
  <>
    <rect x="3.75" y="3.75" width="16.5" height="16.5" rx="1.25" />
    <path d="M16 9.5 11 14.5 8 11.5" />
  </>,
);

/** [drawn] `slide-out` with the arrow reversed, so the bar stays on the left. */
export const PanelLeftOpen = drawn(
  'PanelLeftOpen',
  <>
    <path d="M6.836 12.015h11.867" />
    <path d="m13.923 7.505 4.78 4.51-4.78 4.508" />
    <path d="M5 7v10" />
  </>,
);

/*
 * The file-status family. Click UI's `document` carries three text lines; these
 * four drop them for a badge, so a changed file reads differently from a plain
 * one at 12px.
 */

/** [drawn] Page with a check. */
export const FileCheck = drawn(
  'FileCheck',
  <>
    <path d={PAGE} />
    <path d="m9 12.25 2 2 4-4" />
  </>,
);

/** [drawn] Page with a plus. */
export const FilePlus = drawn(
  'FilePlus',
  <>
    <path d={PAGE} />
    <path d="M12 9v6M9 12h6" />
  </>,
);

/** [drawn] Page with a cross. */
export const FileX = drawn(
  'FileX',
  <>
    <path d={PAGE} />
    <path d="m9.75 9.75 4.5 4.5M14.25 9.75l-4.5 4.5" />
  </>,
);

/** [drawn] Page with the plus-over-minus of a diff. */
export const FileDiff = drawn(
  'FileDiff',
  <>
    <path d={PAGE} />
    <path d="M12 8.5v3M10.5 10h3M10.25 15.25h3.5" />
  </>,
);

/** [drawn] A machine, for messages the agent wrote rather than the user. */
export const Bot = drawn(
  'Bot',
  <>
    <path d="M12 3.25V6" />
    <path d="M17.5 20.5h-11a2 2 0 0 1-2-2v-8a2 2 0 0 1 2-2h11a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2Z" />
    <path d="M9.25 13v1.75M14.75 13v1.75" />
  </>,
);

/** [drawn] Two lobes and a stem — the memory pages and the thinking blocks. */
export const Brain = drawn(
  'Brain',
  <>
    <path d="M12 6.4v11.4" />
    <path d="M12 7.1A3 3 0 0 0 6.9 8.6 2.6 2.6 0 0 0 5.2 11c0 .7.3 1.4.8 1.9a2.7 2.7 0 0 0 .5 3.3A3 3 0 0 0 9.6 18.8c1.4 0 2.4-.9 2.4-2.1" />
    <path d="M12 7.1a3 3 0 0 1 5.1 1.5 2.6 2.6 0 0 1 1.7 2.4c0 .7-.3 1.4-.8 1.9a2.7 2.7 0 0 1-.5 3.3 3 3 0 0 1-3.1 1.6c-1.4 0-2.4-.9-2.4-2.1" />
  </>,
);

/** [drawn] The composer's send control. */
export const Send = drawn(
  'Send',
  <>
    <path d="M21 3 14 21l-3.5-7.5L3 10 21 3Z" />
    <path d="M21 3 10.5 13.5" />
  </>,
);

/** [drawn] The composer's attach control. */
export const Paperclip = drawn(
  'Paperclip',
  <path d="M20 11.5 11.7 19.8a5 5 0 0 1-7.1-7.1l8.3-8.3a3.35 3.35 0 0 1 4.7 4.7l-8.3 8.3a1.7 1.7 0 0 1-2.4-2.4l7.6-7.6" />,
);

/** [drawn] Cost moving up. */
export const TrendingUp = drawn(
  'TrendingUp',
  <>
    <path d="M21 7.5 13.5 15l-5-5L3 15.5" />
    <path d="M15.5 7.5H21V13" />
  </>,
);

/** [drawn] Cost moving down. */
export const TrendingDown = drawn(
  'TrendingDown',
  <>
    <path d="M21 16.5 13.5 9l-5 5L3 8.5" />
    <path d="M15.5 16.5H21V11" />
  </>,
);

/** [drawn] A tray, kept distinct from `Mail` which it sits beside. */
export const Inbox = drawn(
  'Inbox',
  <>
    <path d="M20.5 12.5H16l-1.75 2.5h-4.5L8 12.5H3.5" />
    <path d="M6.6 5.6 3.5 12v5.5a2 2 0 0 0 2 2h13a2 2 0 0 0 2-2V12l-3.1-6.4a2 2 0 0 0-1.8-1.1H8.4a2 2 0 0 0-1.8 1.1Z" />
  </>,
);

/** [drawn] Click UI's bell, struck through. */
export const BellOff = drawn(
  'BellOff',
  <>
    <path d="M6.563 10.188c0-2.503 2.029-4.531 4.531-4.531h1.813c2.503 0 4.531 2.029 4.531 4.531v2.797c0 .53.211 1.039.586 1.414l.641.641c.375.375.586.884.586 1.414 0 1.044-.846 1.89-1.89 1.89H6.64c-1.044 0-1.89-.846-1.89-1.89 0-.53.211-1.039.586-1.414l.641-.641c.375-.375.586-.884.586-1.414v-2.797Z" />
    <path d="M9.708 18.344v.365A2.292 2.292 0 0 0 12 21a2.292 2.292 0 0 0 2.292-2.292v-.365" />
    <path d="m4 4 16 16" />
  </>,
);

/** [drawn] A note with a turned corner. */
export const StickyNote = drawn(
  'StickyNote',
  <>
    <path d="M4 5.5a2 2 0 0 1 2-2h8L20 9.5v9a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2v-13Z" />
    <path d="M14 3.5V8a2 2 0 0 0 2 2h4" />
    <path d="M8 13.75h8M8 17h5" />
  </>,
);

/** [drawn] Soft-wrap, for the diff viewer's toggle. */
export const WrapText = drawn(
  'WrapText',
  <>
    <path d="M4 5.5h16" />
    <path d="M4 11h11.5a3.75 3.75 0 0 1 0 7.5H12" />
    <path d="M14 16 11.5 18.5 14 21" />
    <path d="M4 18.5h3.5" />
  </>,
);

/** [drawn] The light half of the theme toggle; Click UI ships only the `moon`. */
export const Sun = drawn(
  'Sun',
  <>
    <path d="M12 16.75a4.75 4.75 0 1 0 0-9.5 4.75 4.75 0 0 0 0 9.5Z" />
    <path d="M12 2.5v2M12 19.5v2M2.5 12h2M19.5 12h2M5.28 5.28 6.7 6.7M17.3 17.3l1.42 1.42M5.28 18.72 6.7 17.3M17.3 6.7l1.42-1.42" />
  </>,
);

/** [drawn] Click UI's table outline with two dividers — the board view. */
export const Columns3 = drawn(
  'Columns3',
  <>
    <path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2Z" />
    <path d="M9 3v18M15 3v18" />
  </>,
);

/** [drawn] Leaving — a panel opening to the right with an arrow through it. */
export const LogOut = drawn(
  'LogOut',
  <>
    <path d="M15.5 20.5H6a2 2 0 0 1-2-2v-13a2 2 0 0 1 2-2h9.5" />
    <path d="M20.5 12h-9" />
    <path d="m17 8.5 3.5 3.5-3.5 3.5" />
  </>,
);

/** [drawn] A label with its eyelet. */
export const Tag = drawn(
  'Tag',
  <>
    <path d="M12.6 3.6A2 2 0 0 0 11.2 3H5a2 2 0 0 0-2 2v6.2a2 2 0 0 0 .6 1.4l7.8 7.8a2 2 0 0 0 2.8 0l6.2-6.2a2 2 0 0 0 0-2.8l-7.8-7.8Z" />
    <path d={dot(7.25, 7)} />
  </>,
);

/** [drawn] Click UI's shield carrying the `?` from its `question` glyph. */
export const ShieldQuestion = drawn(
  'ShieldQuestion',
  <>
    <path d={SHIELD} />
    <path d="M12 11.25V11c0-.817.505-1.26 1.011-1.6.494-.333.989-.767.989-1.567a2 2 0 1 0-4 0" />
    <path d={dot(12, 13.75)} />
  </>,
);

/**
 * [drawn] Click UI ships the GitHub mark as a *logo*, which hard-codes its fill
 * to black or white by theme and so ignores the `text-hue-purple` the call site
 * asks for. This is that same artwork, re-emitted on `currentColor`.
 */
export const Github = drawn(
  'Github',
  <path
    fillRule="evenodd"
    clipRule="evenodd"
    d="M32 5C17.0825 5 5 17.0825 5 32C5 43.9475 12.7287 54.0387 23.4612 57.6162C24.8112 57.8525 25.3175 57.0425 25.3175 56.3338C25.3175 55.6925 25.2838 53.5662 25.2838 51.305C18.5 52.5537 16.745 49.6512 16.205 48.1325C15.9012 47.3563 14.585 44.96 13.4375 44.3187C12.4925 43.8125 11.1425 42.5638 13.4038 42.53C15.53 42.4963 17.0487 44.4875 17.555 45.2975C19.985 49.3812 23.8663 48.2337 25.4188 47.525C25.655 45.77 26.3638 44.5887 27.14 43.9137C21.1325 43.2388 14.855 40.91 14.855 30.5825C14.855 27.6462 15.9012 25.2162 17.6225 23.3263C17.3525 22.6512 16.4075 19.8837 17.8925 16.1712C17.8925 16.1712 20.1538 15.4625 25.3175 18.9388C27.4775 18.3313 29.7725 18.0275 32.0675 18.0275C34.3625 18.0275 36.6575 18.3313 38.8175 18.9388C43.9813 15.4288 46.2425 16.1712 46.2425 16.1712C47.7275 19.8837 46.7825 22.6512 46.5125 23.3263C48.2337 25.2162 49.28 27.6125 49.28 30.5825C49.28 40.9437 42.9688 43.2388 36.9613 43.9137C37.94 44.7575 38.7838 46.3775 38.7838 48.9088C38.7838 52.52 38.75 55.4225 38.75 56.3338C38.75 57.0425 39.2563 57.8862 40.6063 57.6162C51.2713 54.0387 59 43.9137 59 32C59 17.0825 46.9175 5 32 5Z"
  />,
  { viewBox: '0 0 64 64', fill: 'currentColor', stroke: 'none' },
);
