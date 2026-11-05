import type { Mermaid } from 'mermaid';
import type { Mount } from './types';

// mermaid is a few megabytes; it is fetched the first time a chat shows a
// diagram, and never for a chat that shows none.
let loading: Promise<Mermaid> | null = null;

function loadMermaid(): Promise<Mermaid> {
  loading ??= import('mermaid').then(module => module.default);
  return loading;
}

let renderSeq = 0;

/** The first lines of a mermaid parse error; the full text quotes the grammar. */
function describe(err: unknown): string {
  const message = err instanceof Error ? err.message : String(err);
  const lines = message.split('\n').map(line => line.trim()).filter(Boolean);
  return lines.slice(0, 2).join(' ').slice(0, 240) || 'the diagram could not be parsed';
}

export const mountMermaid: Mount = async (stage, source, theme) => {
  const mermaid = await loadMermaid();
  // `initialize` resets the global configuration, so every render states all
  // of it. Strict security: labels are sanitized, and `click` callbacks and
  // links in the diagram are ignored — the diagram is drawn, never run.
  mermaid.initialize({
    startOnLoad: false,
    securityLevel: 'strict',
    theme: theme === 'dark' ? 'dark' : 'default',
    fontFamily: 'Inter, ui-sans-serif, system-ui, sans-serif',
  });
  try {
    // A syntax error is reported here, with the diagram's own message.
    await mermaid.parse(source);
  } catch (err) {
    throw new Error(describe(err));
  }
  const id = `nerve-mermaid-${++renderSeq}`;
  const { svg } = await mermaid.render(id, source);
  stage.innerHTML = svg;
  return () => {
    stage.innerHTML = '';
  };
};
