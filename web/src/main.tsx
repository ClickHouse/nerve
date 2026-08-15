import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import { ClickUIProvider } from '@clickhouse/click-ui'
import './index.css'
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
