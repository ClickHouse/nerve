import { useContext, useEffect, useRef, useState } from 'react';
import { useThemeStore } from '../../../stores/themeStore';
import { copyToClipboard } from '../../../utils/clipboard';
import { IconButton, Modal } from '../../ui';
import { Check, Copy, Download, Maximize2 } from '../../ui/icons';
import { MarkdownStreamingContext } from '../streamingContext';
import { KIND_LABEL, MAX_RICH_SOURCE_CHARS, type RichKind } from './kinds';
import type { Mount, Mounted } from './types';
import { useSettledValue } from './useSettledValue';

// Each renderer, with its library, loads the first time a chat shows a block
// of its kind; a chat without diagrams pays for none of them.
const MOUNTS: Record<RichKind, Mount> = {
  mermaid: (stage, source, context) =>
    import('./mermaid').then(m => m.mountMermaid(stage, source, context)),
  'vega-lite': (stage, source, context) =>
    import('./vega').then(m => m.mountVega('vega-lite')(stage, source, context)),
  vega: (stage, source, context) =>
    import('./vega').then(m => m.mountVega('vega')(stage, source, context)),
  svg: (stage, source, context) =>
    import('./svg').then(m => m.mountSvg(stage, source, context)),
};

/**
 * Kinds whose result is one picture: it can be saved as a file and opened at
 * full size. Vega charts are interactive and bring their own export menu.
 */
const PICTURE_KINDS: ReadonlySet<RichKind> = new Set(['mermaid', 'svg']);

/** While the text still streams, a render is tried once it has stopped changing for this long. */
const SETTLE_MS = 600;

type Status = 'rendering' | 'rendered' | 'failed';

/** The render on screen. It is replaced only by the next successful one. */
interface Shown {
  stage: HTMLElement;
  mounted: Mounted;
}

/** How the last attempt went, and what it was an attempt at. */
interface Outcome {
  kind: RichKind;
  theme: string;
  source: string;
  error: string | null;
}

function describe(err: unknown): string {
  const message = err instanceof Error ? err.message : String(err);
  return message.trim().slice(0, 300) || 'unknown error';
}

function downloadSvg(canvas: HTMLElement, name: string) {
  const svg = canvas.querySelector('svg');
  if (!svg) return;
  const clone = svg.cloneNode(true) as SVGElement;
  if (!clone.getAttribute('xmlns')) clone.setAttribute('xmlns', 'http://www.w3.org/2000/svg');
  const url = URL.createObjectURL(new Blob([clone.outerHTML], { type: 'image/svg+xml' }));
  const link = document.createElement('a');
  link.href = url;
  link.download = `${name}.svg`;
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

/**
 * The picture for the full-size view: the same SVG at its natural width, to
 * be scrolled rather than shrunk. mermaid states a diagram's width as
 * `max-width` and scales it to fit the column; an SVG of its own has a
 * `width` or a `viewBox`.
 */
function fullSizeMarkup(canvas: HTMLElement): string | null {
  const svg = canvas.querySelector('svg');
  if (!svg) return null;
  const clone = svg.cloneNode(true) as SVGSVGElement;
  const widthAttr = clone.getAttribute('width') ?? '';
  const natural = parseFloat(clone.style.maxWidth)
    || (widthAttr.endsWith('%') ? 0 : parseFloat(widthAttr))
    || clone.viewBox?.baseVal?.width
    || 0;
  clone.style.maxWidth = 'none';
  clone.style.height = 'auto';
  if (natural > 0) clone.style.width = `${Math.round(natural)}px`;
  return clone.outerHTML;
}

/**
 * A fenced code block the chat renders — a mermaid diagram, a Vega chart,
 * an SVG — with its source one click away.
 *
 * The source is rendered once it has settled (a streamed block changes with
 * every token batch), into a stage element of its own; the previous stage
 * stays on screen until the new one has rendered, so a theme change or more
 * streamed text never blanks the block. A failure after the text has
 * finished shows the source with the renderer's message; during the stream
 * it shows the source quietly, since the text may simply not be complete.
 */
export function RichCodeBlock({ kind, source }: { kind: RichKind; source: string }) {
  const streaming = useContext(MarkdownStreamingContext);
  const theme = useThemeStore(s => s.resolved);
  const settled = useSettledValue(source, streaming ? SETTLE_MS : 0);
  const tooLarge = settled.length > MAX_RICH_SOURCE_CHARS;

  const canvasRef = useRef<HTMLDivElement>(null);
  const shownRef = useRef<Shown | null>(null);
  const [outcome, setOutcome] = useState<Outcome | null>(null);
  const [hasRender, setHasRender] = useState(false);
  const [showSource, setShowSource] = useState(false);
  const [copied, setCopied] = useState(false);
  // The markup of the full-size dialog while it is open.
  const [fullSize, setFullSize] = useState<string | null>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || tooLarge) return;
    const stage = document.createElement('div');
    stage.className = 'rich-stage';
    canvas.appendChild(stage);
    let cancelled = false;
    MOUNTS[kind](stage, settled, { theme, width: canvas.clientWidth }).then(
      (mounted) => {
        if (cancelled) {
          mounted.cleanup();
          stage.remove();
          return;
        }
        const previous = shownRef.current;
        shownRef.current = { stage, mounted };
        previous?.mounted.cleanup();
        previous?.stage.remove();
        setOutcome({ kind, theme, source: settled, error: null });
        setHasRender(true);
      },
      (err: unknown) => {
        stage.remove();
        if (cancelled) return;
        setOutcome({ kind, theme, source: settled, error: describe(err) });
      },
    );
    return () => {
      cancelled = true;
      if (shownRef.current?.stage !== stage) stage.remove();
    };
  }, [kind, settled, theme, tooLarge]);

  // The outcome counts only for the attempt it came from; until the current
  // one reports, the block is rendering (and keeps the previous picture).
  const current = outcome && outcome.kind === kind && outcome.theme === theme && outcome.source === settled
    ? outcome
    : null;
  const status: Status = tooLarge ? 'failed' : !current ? 'rendering' : current.error ? 'failed' : 'rendered';
  const error = tooLarge
    ? `too large to render (${settled.length.toLocaleString()} characters)`
    : current?.error ?? null;

  // The render on screen outlives the effect that made it; it goes with the block.
  useEffect(() => () => {
    shownRef.current?.mounted.cleanup();
    shownRef.current?.stage.remove();
    shownRef.current = null;
  }, []);

  const handleCopy = async () => {
    if (!(await copyToClipboard(source))) return;
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const label = KIND_LABEL[kind];
  const failed = status === 'failed' && !streaming;
  const canvasVisible = !showSource && hasRender && !failed;
  const hint = failed
    ? "couldn't render"
    : streaming && status !== 'rendered'
      ? 'renders when complete'
      : status === 'rendering' && !hasRender
        ? 'rendering…'
        : null;

  return (
    <div className="relative group my-2" data-rich-kind={kind}>
      <div className="flex items-center justify-between gap-2 bg-surface-raised border border-border rounded-t-md px-3 py-1">
        <span className="text-xs text-text-dim font-mono truncate">
          {label}
          {hint && (
            <span className={`ml-2 font-sans ${failed ? 'text-warning' : 'text-text-faint'}`}>{hint}</span>
          )}
        </span>
        <span className="flex items-center gap-1">
          {hasRender && !failed && (
            <button
              type="button"
              onClick={() => setShowSource(v => !v)}
              className="text-xs text-text-dim hover:text-text px-1.5 py-0.5 rounded hover:bg-surface-hover"
            >
              {showSource ? 'Rendered' : 'Source'}
            </button>
          )}
          {canvasVisible && PICTURE_KINDS.has(kind) && (
            <>
              <IconButton
                label="Expand"
                size="xs"
                onClick={() => {
                  const markup = canvasRef.current && fullSizeMarkup(canvasRef.current);
                  if (markup) setFullSize(markup);
                }}
              >
                <Maximize2 size={14} />
              </IconButton>
              <IconButton
                label="Download SVG"
                size="xs"
                onClick={() => { if (canvasRef.current) downloadSvg(canvasRef.current, label); }}
              >
                <Download size={14} />
              </IconButton>
            </>
          )}
          <IconButton label={copied ? 'Copied' : 'Copy source'} size="xs" onClick={handleCopy}>
            {copied ? <Check size={14} className="text-hue-emerald" /> : <Copy size={14} />}
          </IconButton>
        </span>
      </div>
      {failed && error && (
        <div role="status" className="border-x border-border bg-warning-bg text-warning text-xs px-3 py-1.5">
          Couldn't render this {label}: {error}. Showing the source.
        </div>
      )}
      <div
        ref={canvasRef}
        hidden={!canvasVisible}
        className="rich-block-canvas"
        role={kind === 'svg' || kind === 'mermaid' ? 'img' : undefined}
        aria-label={`Rendered ${label}`}
      />
      {!canvasVisible && (
        <pre className="!mt-0 !rounded-t-none !border-t-0">
          <code className={`language-${label}`}>{source}</code>
        </pre>
      )}
      <Modal
        open={fullSize !== null}
        onClose={() => setFullSize(null)}
        title={`${label} at full size`}
        size="wide"
        className="max-h-[92vh]"
      >
        {/* The markup is the picture already on screen — mermaid's strict
            output or the sanitized SVG — at a different size. */}
        <div className="rich-block-expanded" dangerouslySetInnerHTML={{ __html: fullSize ?? '' }} />
      </Modal>
    </div>
  );
}
