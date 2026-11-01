import { useEffect, useId, useState } from 'react';
import DOMPurify from 'dompurify';
import { CodeBlock } from './CodeBlock';
import { useThemeStore } from '../../stores/themeStore';

// Mermaid has process-wide configuration. Serialize theme + render so two
// diagrams mounted together cannot race their global theme settings.
let rendering: Promise<unknown> = Promise.resolve();

// On a syntax error only, retry literal punctuation inside multiline sequence
// notes. Mermaid otherwise treats a semicolon as a new command. Keep the stored
// source unchanged and do not reinterpret valid semicolon-separated diagrams.
function escapeNoteSemicolons(source: string): string {
  if (!/^\s*sequenceDiagram\b/.test(source)) return source;
  return source.replace(/^(\s*Note\s+(?:over|left of|right of)\s+[^:\n]+:)([^\n]*)$/gim,
    (_, prefix: string, text: string) => {
      // A valid inline sequence command must stay a command, even when an
      // unrelated line triggered the retry. Ambiguous lines stay unchanged.
      const punctuation = text.replace(/#[a-zA-Z0-9]+;/g, '');
      if (/;[^\n]*:/.test(punctuation)) return prefix + text;
      if (/;\s*(?:[^;\s:]+\s*(?:-[->)x]|<<)|(?:Note|participant|actor|create|destroy|activate|deactivate|alt|else|end|loop|par|and|opt|break|rect|critical|option|autonumber|box|par_over|title|properties|details|accTitle|accDescr|show|hide)\b)/i.test(punctuation)) return prefix + text;
      return prefix + text.replace(/#[a-zA-Z0-9]+;|;/g, token => token === ';' ? '#59;' : token);
    });
}

export function MermaidBlock({ source }: { source: string }) {
  const id = useId().replace(/[^a-zA-Z0-9]/g, '');
  const theme = useThemeStore(s => s.resolved);
  const [result, setResult] = useState<{ source: string; theme: string; svg?: string; error?: string; escapedNotes?: boolean }>();
  useEffect(() => {
    let cancelled = false;
    rendering = rendering.catch(() => {}).then(async () => {
      if (cancelled) return;
      try {
        if (source.length > 50_000) throw new Error('Diagram exceeds the display limit');
        // Do not allow source-controlled configuration, CSS, links or remote
        // image/icon resources. Strict rendering alone does not block fetches.
        if (/%%\{|^\s*---|\b(?:click|link|links|href|img|image|icon)\b|https?:|data:|@import|url\s*\(/im.test(source)) {
          throw new Error('Diagram contains unsupported links or configuration');
        }
        const { default: mermaid } = await import('mermaid');
        mermaid.initialize({
          startOnLoad: false, securityLevel: 'strict', theme: theme === 'dark' ? 'dark' : 'default',
          suppressErrorRendering: true, maxTextSize: 50_000, maxEdges: 500,
          flowchart: { htmlLabels: false },
          // Pale source highlights otherwise obscure the dark theme's light text.
          themeCSS: theme === 'dark' ? 'rect.rect { fill-opacity: 0.16; }' : '',
        });
        let svg: string;
        let escapedNotes = false;
        try {
          ({ svg } = await mermaid.render(`mermaid${id}`, source));
        } catch (error) {
          const escaped = escapeNoteSemicolons(source);
          if (escaped === source) throw error;
          ({ svg } = await mermaid.render(`mermaid${id}notes`, escaped));
          escapedNotes = true;
        }
        const safe = DOMPurify.sanitize(svg, {
          USE_PROFILES: { svg: true, svgFilters: true },
          FORBID_TAGS: ['foreignObject', 'image', 'a', 'script'],
        });
        if (!cancelled) setResult({ source, theme, svg: safe, escapedNotes });
      } catch (error) {
        if (!cancelled) setResult({ source, theme, error: error instanceof Error ? error.message : 'Invalid diagram' });
      }
    });
    return () => { cancelled = true; };
  }, [source, theme, id]);
  const current = result?.source === source && result.theme === theme ? result : undefined;
  return <div className="my-3 rounded-lg border border-border p-3">
    {current?.svg && <div role="img" aria-label="Mermaid diagram" className="overflow-auto" dangerouslySetInnerHTML={{ __html: current.svg }} />}
    {current?.error && <p role="alert">Diagram unavailable: {current.error}</p>}
    {!current && <p className="text-text-muted">Rendering diagram…</p>}
    {current?.escapedNotes && <p className="text-sm text-text-muted">Note punctuation was escaped for display; the original source is preserved.</p>}
    <details open={!!current?.error}>
      <summary className="cursor-pointer text-sm">Diagram source</summary>
      <CodeBlock className="language-mermaid">{source}</CodeBlock>
    </details>
  </div>;
}
