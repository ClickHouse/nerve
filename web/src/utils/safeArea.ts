import type { CSSProperties } from 'react';

/**
 * Safe-area padding for an overlay that is positioned against the viewport.
 *
 * `viewport-fit=cover` lets the layout run under the notch, the home indicator
 * and the rounded corners — which is what makes the background continuous —
 * but it puts *content* there unless something pays the inset back. AppShell
 * pays it for everything laid out inside it, and a `position: fixed` element is
 * not: it is laid out against the viewport, so the shell's padding box never
 * reaches it and it has to pay its own.
 *
 * `anchor` names the vertical edge the surface is pinned to, since only that
 * one can collide with a corner: `left` for a left drawer, `right` for a right
 * drawer, `both` for a surface spanning the full width.
 */
export function safeAreaInsets(anchor: 'left' | 'right' | 'both' = 'both'): CSSProperties {
  return {
    paddingTop: 'env(safe-area-inset-top)',
    paddingBottom: 'env(safe-area-inset-bottom)',
    ...(anchor !== 'right' && { paddingLeft: 'env(safe-area-inset-left)' }),
    ...(anchor !== 'left' && { paddingRight: 'env(safe-area-inset-right)' }),
  };
}
