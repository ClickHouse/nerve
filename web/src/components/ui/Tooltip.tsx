import type { ReactNode } from 'react';
import { Tooltip as ClickTooltip } from '@clickhouse/click-ui/Tooltip';

/**
 * A hover/focus tooltip, on Click UI's.
 *
 * This is one place the design system fits without argument. Click UI's
 * `Tooltip` is a Radix compound — `Root` / `Trigger` / `Content` — and its
 * provider is already mounted by `ClickUIProvider`, so the only thing missing
 * was an API short enough that converting a `title="…"` is a one-line change.
 *
 * The app has 138 native `title` attributes and no tooltip component at all.
 * A native title is fine and should stay wherever it is doing its job; reach
 * for this when the tip needs markup, needs to be readable (native titles are
 * slow to appear and unstyleable), or sits on something with no other hover
 * affordance.
 *
 * Note it does **not** give the trigger an accessible name — Radix's tooltip is
 * described-by, not labelled-by. An icon-only control still needs its own
 * `aria-label`; `IconButton` handles that from its required `label`.
 */
export function Tooltip({
  content,
  children,
  side = 'top',
  align = 'center',
  showArrow = true,
  maxWidth,
  /** Suppress the tip without unmounting the trigger — e.g. while disabled. */
  disabled = false,
  delayDuration,
}: {
  content: ReactNode;
  /** The trigger. Must be a single element that forwards its ref and props. */
  children: ReactNode;
  side?: 'top' | 'right' | 'bottom' | 'left';
  align?: 'start' | 'center' | 'end';
  showArrow?: boolean;
  maxWidth?: string;
  disabled?: boolean;
  delayDuration?: number;
}) {
  if (disabled || content === null || content === undefined || content === '') {
    return <>{children}</>;
  }
  return (
    <ClickTooltip delayDuration={delayDuration}>
      {/* `asChild` so the trigger is the caller's own button rather than a
          wrapper div — a div around a flex child changes the layout, and around
          a button it breaks the hit area. */}
      <ClickTooltip.Trigger asChild>{children}</ClickTooltip.Trigger>
      <ClickTooltip.Content side={side} align={align} showArrow={showArrow} maxWidth={maxWidth}>
        {content}
      </ClickTooltip.Content>
    </ClickTooltip>
  );
}
