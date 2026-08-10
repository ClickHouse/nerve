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
  },
})
