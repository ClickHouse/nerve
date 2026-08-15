// MUST be the first import. index.css opens with the `@layer` statement that
// fixes cascade order, and a layer's position is set where it is FIRST seen.
// Click UI's stylesheets arrive via its JS import, so if that import were
// evaluated first its `@layer clickui {...}` block would pin `clickui` to the
// weakest position and Tailwind's preflight would flatten every Click UI
// control. Import order here is load-bearing, not cosmetic.
import './index.css'

import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import { ClickUIProvider } from '@clickhouse/click-ui'
import App from './App'
import { useThemeStore } from './stores/themeStore'

/** Feeds Click UI the theme Nerve's own store already resolved. */
function ThemedApp() {
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

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <BrowserRouter>
      <ThemedApp />
    </BrowserRouter>
  </StrictMode>,
)
