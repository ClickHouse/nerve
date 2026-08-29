import { ClickUIProvider } from '@clickhouse/click-ui'
import App from './App'
import { useToastHotkeyShim } from './hooks/useToastHotkeyShim'
import { useThemeStore } from './stores/themeStore'

/**
 * Feeds Click UI the theme Nerve's own store already resolved.
 *
 * Lives here rather than in `main.tsx` because that file is the app's entry
 * point: it exists to run side effects (the cascade-order-critical `index.css`
 * import, the font faces, `createRoot`) and exports nothing. A component
 * declared alongside those side effects cannot be hot-reloaded — React Fast
 * Refresh only tracks components in modules whose exports are all components —
 * so every theme tweak would full-reload the page.
 */
export function ThemedApp() {
  const resolved = useThemeStore((s) => s.resolved)
  // Lives here because this is the component that mounts `ClickUIProvider`, so
  // the workaround and the thing it works around are removed together. Mount
  // order does not matter — see the note in the hook.
  useToastHotkeyShim()
  return (
    // persistTheme={false}: themeStore owns persistence (key `nerve-theme`, and
    // it can hold 'system', which Click UI cannot represent). Letting Click UI
    // also write localStorage would give us two sources of truth that disagree.
    <ClickUIProvider theme={resolved} persistTheme={false}>
      <App />
    </ClickUIProvider>
  )
}
