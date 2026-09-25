/// <reference types="vitest/config" />
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      '/api': 'http://localhost:8900',
      '/ws': {
        target: 'ws://localhost:8900',
        ws: true,
      },
    },
  },
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ['./src/test/setup.ts'],
    // Our own specs only — the default glob walks node_modules too.
    include: ['src/**/*.test.{ts,tsx}'],
    // Components are asserted on behaviour and ARIA, never on Tailwind
    // classes, so there is nothing to gain from processing the stylesheet.
    css: false,
    server: {
      deps: {
        // Click UI's components import their own CSS alongside themselves.
        // Left external, those imports reach Node directly and it refuses the
        // `.css` extension; run through Vite they resolve to the stub that
        // `css: false` installs. Any spec that renders a Click UI component
        // needs this.
        inline: ['@clickhouse/click-ui'],
      },
    },
  },
})
