import { ClickUIProvider } from '@clickhouse/click-ui'
import App from './App'
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
  return (
    // persistTheme={false}: themeStore owns persistence (key `nerve-theme`, and
    // it can hold 'system', which Click UI cannot represent). Letting Click UI
    // also write localStorage would give us two sources of truth that disagree.
    <ClickUIProvider theme={resolved} persistTheme={false}>
      <App />
    </ClickUIProvider>
  )
}
