import type { ReactNode } from 'react';

/**
 * The header strip every list page starts with: icon + title, filter pills,
 * an optional search box and optional right-hand actions.
 *
 * It exists because the pattern had been copy-pasted per page as a single
 * non-wrapping flex row. That row reaches ~750px once the pills and the
 * search box are in it, so on a 412px viewport the *page itself* scrolled
 * sideways and every filter past the third was unreachable.
 *
 * The single row returns at `lg`, not `md`. The desktop shell also spends 56px
 * on the nav rail, so a 768px viewport leaves ~664px of content — less than the
 * widest header needs (Notifications: nine pills plus two actions). Even `lg`
 * is not enough for that one, which is why the filter strip keeps
 * `overflow-x-auto` at every width: a header that outgrows its container
 * scrolls inside itself instead of spilling into the controls after it.
 *
 * Nothing here is reordered with CSS. Below `lg` the actions are rendered in
 * the title row and the desktop copy is dropped from the DOM (and vice versa),
 * so tab order follows what is on screen at both sizes — a flex `order` swap
 * would move them visually while leaving the keyboard to step through every
 * filter and the search box first.
 *
 * This is Nerve's layout, not a library component: the three rules above are
 * answers to this app's viewport and this app's headers, and none of them is a
 * shape a design system ships. Every colour is a theme token —
 * `border-border-subtle`, `bg-bg` and the text colours — so this header
 * repaints with the rest of the app and holds no colour of its own.
 *
 * For the slots: `filters` takes `Button variant="pill"` with `active`, `search`
 * takes a `TextField`, and `actions` takes `Button`/`IconButton` — the
 * primitives beside this file.
 */
export function PageHeader({ leading, icon, title, filters, search, actions }: {
  /**
   * Sits ahead of the icon. For pages whose side pane collapses into a
   * drawer on mobile, this is where its toggle goes — mirroring the chat
   * header, so the control is in the same place on every page that has one.
   */
  leading?: ReactNode;
  icon?: ReactNode;
  title: ReactNode;
  /** Filter pills. Laid out by the caller; scrolled horizontally here. */
  filters?: ReactNode;
  search?: ReactNode;
  actions?: ReactNode;
}) {
  return (
    <div className="border-b border-border-subtle bg-bg shrink-0 px-4 lg:px-6 py-2.5 lg:py-3
      flex flex-wrap lg:flex-nowrap items-center gap-x-4 gap-y-2">
      {/* Row one below `lg`: the title, with the page's primary buttons pinned
          opposite it. `w-full` is what guarantees the filters and search a line
          of their own — leaving that to the other items overflowing the line is
          how the break silently collapsed on the page with no visible actions.
          With an icon, desktop keeps the 16px icon-to-title gap and the 24px
          run-out to the filters that the per-page headers already had. */}
      <div className={`flex items-center gap-2 min-w-0 w-full lg:w-auto lg:flex-none
        ${icon ? 'lg:gap-4 lg:mr-2' : ''}`}>
        {leading}
        {icon}
        <h1 className="text-lg font-semibold truncate">{title}</h1>
        {actions && (
          <div className="ml-auto flex shrink-0 items-center gap-2 lg:hidden">{actions}</div>
        )}
      </div>

      {filters && (
        // The negative margins let the strip run to both screen edges, so a
        // half-visible pill signals "there is more this way" instead of looking
        // like a clipped layout. The width has to grow by the same 2rem the
        // margins take back — `w-full` alone would only shift the strip left
        // and leave it stopping 32px short of the right edge. (Which is also
        // why this cannot use `basis-full`: a non-auto flex-basis would
        // override the width and take the bleed with it.)
        <div className="w-[calc(100%+2rem)] lg:w-auto min-w-0
          -mx-4 px-4 lg:mx-0 lg:px-0
          overflow-x-auto
          [scrollbar-width:none] [&::-webkit-scrollbar]:hidden">
          <div className="flex items-center gap-1 w-max">{filters}</div>
        </div>
      )}

      {search && (
        <div className="w-full lg:w-auto">{search}</div>
      )}

      {/* Desktop copy of the actions: last in the DOM so `ml-auto` pins it to
          the right edge without dragging the filters and search along with it. */}
      {actions && (
        <div className="hidden lg:flex items-center gap-2 shrink-0 lg:ml-auto">
          {actions}
        </div>
      )}
    </div>
  );
}
