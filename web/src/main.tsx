// MUST be the first import. index.css opens with the `@layer` statement that
// fixes cascade order, and a layer's position is set where it is FIRST seen.
// Click UI's stylesheets arrive via its JS import, so if that import were
// evaluated first its `@layer clickui {...}` block would pin `clickui` to the
// weakest position and Tailwind's preflight would flatten every Click UI
// control. Import order here is load-bearing, not cosmetic.
import './index.css'

// Click UI names Inter (regular) and Inconsolata (mono) in its typography
// tokens but ships no font files — the package contains zero @font-face rules.
// Self-hosted via @fontsource rather than a Google Fonts <link>, so the app
// keeps working offline and behind a firewall, and no request leaves the box.
// These come after index.css: they are pure @font-face declarations with no
// @layer of their own, so they cannot disturb the cascade order above, and
// keeping index.css first preserves the invariant this file depends on.
// Weights match what the UI actually uses: 400 body, 500 `font-medium`,
// 600 `font-semibold` / markdown headings, 700 `font-bold`.
// ('Basier Square', Click UI's display face, is commercial — we do not ship it
// and it falls back to Inter.)
import '@fontsource/inter/400.css'
import '@fontsource/inter/500.css'
import '@fontsource/inter/600.css'
import '@fontsource/inter/700.css'
import '@fontsource/inconsolata/400.css'
import '@fontsource/inconsolata/500.css'

import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
// ThemedApp pulls in Click UI. That is safe here and not a violation of the
// import-order rule above: this import is evaluated after `./index.css`, so
// the layer statement has already fixed the cascade order by the time Click
// UI's own `@layer clickui` block is seen.
import { ThemedApp } from './ThemedApp'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <BrowserRouter>
      <ThemedApp />
    </BrowserRouter>
  </StrictMode>,
)
