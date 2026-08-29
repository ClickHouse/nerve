import { useEffect } from 'react'

/**
 * Keep Click UI's toast viewport from taking focus when F8 is pressed.
 *
 * `ClickUIProvider` unconditionally nests a `ToastProvider`, which
 * unconditionally renders a Radix `Toast.Viewport`. Radix gives that viewport a
 * default hotkey of F8 and implements it as a bare
 * `document.addEventListener('keydown', …)` that calls `viewport.focus()`.
 * Click UI renders `<RadixUIToast.Viewport className={…} />` with no other
 * props, so its `hotkey` — which Radix would accept as `[]` to switch the
 * behaviour off, it guards on `hotkey.length !== 0` — cannot be reached from
 * outside the package.
 *
 * Nerve has no Click UI toast call sites, so the viewport is a permanently
 * empty, invisible `<ol tabindex="-1">`. Pressing F8 therefore moves focus out
 * of whatever the user was typing in and into nothing, with no visible
 * feedback. That is worse than it sounds: the page-level shortcut handlers
 * decide whether to act by asking whether focus is in a text field, so after an
 * F8 the next plain `b`, `l`, `n` or `/` runs a shortcut instead of typing a
 * character. It can also pull focus out of an open modal.
 *
 * `stopPropagation` on the **capture** phase at `window` is the earliest point
 * in the dispatch, so the event never reaches the `document` listener Radix
 * registered — regardless of which mounted first. `preventDefault` is
 * deliberately NOT called: F8 belongs to the browser and the platform (it is
 * "resume script execution" while devtools are paused), and this is about one
 * library's listener, not about the key.
 *
 * The cost is that F8 reaches nothing below `window` — including React's own
 * synthetic handlers, which are attached at the root container. Nothing in this
 * app binds F8, and other `window`-capture listeners still receive it
 * (`stopPropagation`, not `stopImmediatePropagation`).
 *
 * **Remove this** once Click UI forwards viewport props — passing
 * `config={{ toast: { hotkey: [] } }}` through `ClickUIProvider` would then say
 * the same thing declaratively — or exposes a way to opt out of the toast layer
 * altogether. Verified against @clickhouse/click-ui 0.10.0 /
 * @radix-ui/react-toast 1.2.2.
 */
export function useToastHotkeyShim(): void {
  useEffect(() => {
    const swallow = (event: KeyboardEvent) => {
      // Matched on `code` because that is what Radix matches on:
      // `hotkey.every((key) => event[key] || event.code === key)`.
      if (event.code === 'F8') event.stopPropagation()
    }
    window.addEventListener('keydown', swallow, true)
    return () => window.removeEventListener('keydown', swallow, true)
  }, [])
}
