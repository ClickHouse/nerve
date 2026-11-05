import { useContext, useEffect, useLayoutEffect, useRef, useState, type ReactNode } from 'react';
import { useThemeStore, type ResolvedTheme } from '../../../stores/themeStore';
import { copyToClipboard } from '../../../utils/clipboard';
import { IconButton } from '../../ui';
import { Check, CodeXml, Copy, Download, Maximize2 } from '../../ui/icons';
import { MarkdownStreamingContext } from '../streamingContext';
import {
  downloadBlob, downloadSvg, ensureViewBox, exportFileName, pngOver, svgMarkupOf, svgSizeOf, svgToPngBlob,
  type Size,
} from './export';
import { KIND_LABEL, MAX_RICH_SOURCE_CHARS, type RichKind } from './kinds';
import { Lightbox } from './Lightbox';
import { readThemeTokens } from './tokens';
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

/** What the figure is, for its accessible name and the full-size view's title. */
const FIGURE_NAME: Record<RichKind, string> = {
  mermaid: 'Diagram',
  'vega-lite': 'Chart',
  vega: 'Chart',
  svg: 'Image',
};

/**
 * Kinds drawn in the app's colours. Their SVG file keeps the figure's
 * background: text drawn for the dark theme would vanish on the white page a
 * viewer opens a file on. An SVG of the model's own is saved as it was drawn.
 */
const THEMED_KINDS: ReadonlySet<RichKind> = new Set(['mermaid', 'vega-lite', 'vega']);

/** While the text still streams, a render is tried once it has stopped changing for this long. */
const SETTLE_MS = 600;

/**
 * A picture wider than the column is scaled down to fit it only while it
 * stays at least this large; below that the text is too small to read, so
 * it is shown at 1:1 and scrolls instead.
 */
const MIN_FIT_SCALE = 0.6;

/** A width change reaches the renderer once the column has stopped resizing for this long. */
const RESIZE_DEBOUNCE_MS = 150;

/** PNG downloads are drawn at this many device pixels per CSS pixel. */
const PNG_SCALE = 2;

/** How long a note about a failed download stays up. */
const NOTICE_MS = 5000;

type Status = 'rendering' | 'rendered' | 'failed';

/** The reader's choice for a picture wider than the column: scaled to fit it, or 1:1 with a scrollbar. */
type Choice = 'fit' | 'actual';

/** The render on screen. It is replaced only by the next successful one. */
interface Shown {
  stage: HTMLElement;
  mounted: Mounted;
  /** The picture's natural size, when the renderer reported a usable one. */
  natural: Size | null;
  /** The theme it was drawn for, whose background its files are painted over. */
  theme: ResolvedTheme;
  /** The canvas width the render was last laid out at. */
  width: number;
}

/** What the render on screen means for the frame, as state. */
interface Picture {
  naturalSize: Size | null;
  /** There is an SVG to open full size and to save. */
  exportable: boolean;
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

/** A reported size the frame can lay a picture out by: finite and above zero, else null. */
function usableSize(size: Size | undefined): Size | null {
  return size && size.width > 0 && size.height > 0 && Number.isFinite(size.width) && Number.isFinite(size.height)
    ? size
    : null;
}

/**
 * How a picture sits in a canvas `width` pixels wide: whether it is wider
 * than the column, and if so whether it is scaled to fit. One the renderer
 * gave no size for is left as it was drawn (a container-sized chart).
 */
function layoutFor(natural: Size | null | undefined, width: number, choice: Choice | null) {
  if (!natural || width <= 0 || natural.width <= width + 0.5) return { overflowing: false, fit: false };
  return { overflowing: true, fit: choice ? choice === 'fit' : width / natural.width >= MIN_FIT_SCALE };
}

/**
 * Sizes the picture in `stage`: scaled to the column, or at its natural size,
 * which scrolls when it is wider than the column. The svg's own width and
 * height attributes are not trusted to say either (`width="100%"`).
 */
function applySizing(stage: HTMLElement, natural: Size | null, fit: boolean) {
  const svg = stage.querySelector('svg');
  if (!svg || !natural) return;
  if (fit) {
    ensureViewBox(svg, natural);
    svg.style.width = '100%';
    svg.style.height = 'auto';
    svg.style.maxWidth = '100%';
  } else {
    svg.style.width = `${natural.width}px`;
    svg.style.height = `${natural.height}px`;
    svg.style.maxWidth = 'none';
  }
}

/** The colour a picture sits on in the figure, as hex — what PNG and themed SVG files are painted over. */
function figureBackground(theme: ResolvedTheme): string {
  return readThemeTokens(theme).surface;
}

/** The size the picture on screen is drawn at, for one whose renderer did not say. */
function drawnSize(stage: HTMLElement): Size | null {
  const rect = stage.querySelector('svg')?.getBoundingClientRect();
  return rect && rect.width > 0 && rect.height > 0 ? { width: rect.width, height: rect.height } : null;
}

/** The picture on screen as SVG markup: the renderer's export, else the svg it drew. */
async function pictureMarkup({ stage, mounted }: Shown): Promise<string | null> {
  try {
    const exported = await mounted.toSvg?.();
    if (exported) return exported;
  } catch {
    // The drawn svg will do.
  }
  return svgMarkupOf(stage);
}

/** A button of the figure's toolbar: an icon, or a short word ("1:1", "SVG"). */
function ToolButton({ label, onClick, text = false, children }: {
  label: string;
  onClick: () => void;
  text?: boolean;
  children: ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      title={label}
      aria-label={label}
      className={`inline-flex items-center justify-center h-7 rounded-md text-text-muted hover:text-text hover:bg-surface-hover cursor-pointer transition-colors ${
        text ? 'px-1.5 text-xs font-medium tabular-nums' : 'w-7'
      }`}
    >
      {children}
    </button>
  );
}

/**
 * A fenced code block the chat renders — a mermaid diagram, a Vega chart,
 * an SVG — as a figure, with its source one click away.
 *
 * The source is rendered once it has settled (a streamed block changes with
 * every token batch), into a stage element of its own, out of sight; the
 * previous stage stays on screen until the new one has rendered, so a theme
 * change or more streamed text never blanks the block. A failure after the
 * text has finished shows the source with the renderer's message; during the
 * stream it shows the source quietly, since the text may simply not be
 * complete.
 *
 * A picture wider than the column is scaled to fit while it stays readable
 * (MIN_FIT_SCALE) and is shown at 1:1 with a scrollbar otherwise; the toolbar
 * switches between the two. Expand opens it full size (Lightbox); Download
 * saves it as SVG or PNG.
 */
export function RichCodeBlock({ kind, source }: { kind: RichKind; source: string }) {
  const streaming = useContext(MarkdownStreamingContext);
  const theme = useThemeStore(s => s.resolved);
  const settled = useSettledValue(source, streaming ? SETTLE_MS : 0);
  const tooLarge = settled.length > MAX_RICH_SOURCE_CHARS;

  const canvasRef = useRef<HTMLDivElement>(null);
  const shownRef = useRef<Shown | null>(null);
  // The canvas width and the reader's choice, for the callbacks that size a
  // picture outside a render (a render finishing, the column resizing).
  const widthRef = useRef(0);
  const choiceRef = useRef<Choice | null>(null);
  const busyRef = useRef(false);
  const noticeTimerRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const [outcome, setOutcome] = useState<Outcome | null>(null);
  const [picture, setPicture] = useState<Picture | null>(null);
  const [canvasWidth, setCanvasWidth] = useState(0);
  const [choice, setChoice] = useState<Choice | null>(null);
  const [showSource, setShowSource] = useState(false);
  const [copied, setCopied] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  // The full-size view while it is open.
  const [lightbox, setLightbox] = useState<{ markup: string; size: Size } | null>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || tooLarge) return;
    const stage = document.createElement('div');
    stage.className = 'rich-stage';
    // Rendered at the canvas width but out of sight, until it replaces the picture on screen.
    stage.setAttribute('data-pending', '');
    canvas.appendChild(stage);
    let cancelled = false;
    MOUNTS[kind](stage, settled, { theme, width: canvas.clientWidth || widthRef.current }).then(
      (mounted) => {
        if (cancelled) {
          mounted.cleanup();
          stage.remove();
          return;
        }
        // A hidden canvas (the source is showing) measures 0; the last width stands.
        const width = canvas.clientWidth || widthRef.current;
        widthRef.current = width;
        const natural = usableSize(mounted.naturalSize);
        applySizing(stage, natural, layoutFor(natural, width, choiceRef.current).fit);
        stage.removeAttribute('data-pending');
        const previous = shownRef.current;
        shownRef.current = { stage, mounted, natural, theme, width };
        previous?.mounted.cleanup();
        previous?.stage.remove();
        setOutcome({ kind, theme, source: settled, error: null });
        setPicture({
          naturalSize: natural,
          exportable: !!mounted.toSvg || !!stage.querySelector('svg'),
        });
        setCanvasWidth(width);
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

  // The column's width decides fit or 1:1, and a container-sized chart lays
  // itself out again for it once the resizing has stopped.
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || typeof ResizeObserver === 'undefined') return;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const observer = new ResizeObserver((entries) => {
      const width = Math.round(entries[entries.length - 1].contentRect.width);
      if (width <= 0 || width === widthRef.current) return;
      widthRef.current = width;
      setCanvasWidth(width);
      clearTimeout(timer);
      timer = setTimeout(() => {
        const shown = shownRef.current;
        if (!shown || shown.width === widthRef.current) return;
        shown.width = widthRef.current;
        Promise.resolve(shown.mounted.resize?.()).catch(() => {});
      }, RESIZE_DEBOUNCE_MS);
    });
    observer.observe(canvas);
    return () => {
      observer.disconnect();
      clearTimeout(timer);
    };
  }, []);

  const layout = layoutFor(picture?.naturalSize, canvasWidth, choice);
  useLayoutEffect(() => {
    const shown = shownRef.current;
    if (shown) applySizing(shown.stage, shown.natural, layout.fit);
  }, [layout.fit, picture]);

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
    clearTimeout(noticeTimerRef.current);
  }, []);

  const flash = (message: string) => {
    setNotice(message);
    clearTimeout(noticeTimerRef.current);
    noticeTimerRef.current = setTimeout(() => setNotice(null), NOTICE_MS);
  };

  const handleCopy = async () => {
    if (!(await copyToClipboard(source))) return;
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const toggleFit = () => {
    const next: Choice = layout.fit ? 'actual' : 'fit';
    choiceRef.current = next;
    setChoice(next);
  };

  const openFullSize = async () => {
    const shown = shownRef.current;
    if (!shown) return;
    const markup = await pictureMarkup(shown);
    if (!markup) return;
    const size = shown.natural ?? svgSizeOf(markup) ?? drawnSize(shown.stage) ?? { width: 800, height: 600 };
    setLightbox({ markup, size });
  };

  const saveSvg = async () => {
    const shown = shownRef.current;
    if (!shown) return;
    const markup = await pictureMarkup(shown);
    const saved = !!markup && downloadSvg(markup, exportFileName(kind, 'svg'), {
      size: shown.natural,
      background: THEMED_KINDS.has(kind) ? figureBackground(shown.theme) : null,
    });
    if (!saved) flash("Couldn't save this picture as SVG.");
  };

  const savePng = async () => {
    const shown = shownRef.current;
    if (!shown || busyRef.current) return;
    busyRef.current = true;
    try {
      const background = figureBackground(shown.theme);
      let png: Blob | null = null;
      if (shown.mounted.toPng) {
        const drawn = await shown.mounted.toPng(PNG_SCALE).catch(() => null);
        if (drawn) png = await pngOver(drawn, background).catch(() => drawn);
      }
      if (!png) {
        const markup = await pictureMarkup(shown);
        if (markup) png = await svgToPngBlob(markup, PNG_SCALE, background, shown.natural);
      }
      if (png) downloadBlob(png, exportFileName(kind, 'png'));
      else flash("Couldn't save this picture as PNG.");
    } catch {
      // Safari refuses to read back a canvas an SVG with HTML labels was drawn on.
      flash("Couldn't save this picture as PNG in this browser.");
    } finally {
      busyRef.current = false;
    }
  };

  const label = KIND_LABEL[kind];
  const name = FIGURE_NAME[kind];
  const hasRender = picture !== null;
  const failed = status === 'failed' && !streaming;
  const figureShown = !showSource && hasRender && !failed;
  const hint = failed
    ? "couldn't render"
    : streaming && status !== 'rendered'
      ? 'renders when complete'
      : status === 'rendering' && !hasRender
        ? 'rendering…'
        : null;

  return (
    <div className="rich-block my-2" data-rich-kind={kind}>
      {/* The figure stays laid out while it has nothing to show: a chart that
          sizes to its container measures the canvas as it renders. */}
      <figure
        className="rich-figure"
        data-collapsed={figureShown ? undefined : ''}
        aria-hidden={figureShown ? undefined : true}
        aria-label={figureShown ? name : undefined}
      >
        {figureShown && (
          <div className="rich-figure-toolbar" role="group" aria-label={`${name} tools`}>
            {layout.overflowing && (
              <ToolButton label={layout.fit ? 'Actual size (1:1)' : 'Fit to width'} onClick={toggleFit} text>
                {layout.fit ? '1:1' : 'Fit'}
              </ToolButton>
            )}
            {picture?.exportable && (
              <>
                <ToolButton label="Expand" onClick={openFullSize}>
                  <Maximize2 size={14} />
                </ToolButton>
                <span aria-hidden="true" className="mx-0.5 h-4 w-px bg-border" />
                <Download size={13} aria-hidden="true" className="ml-1 mr-0.5 text-text-faint" />
                <ToolButton label="Download SVG" onClick={saveSvg} text>SVG</ToolButton>
                <ToolButton label="Download PNG" onClick={savePng} text>PNG</ToolButton>
                <span aria-hidden="true" className="mx-0.5 h-4 w-px bg-border" />
              </>
            )}
            <ToolButton label="Source" onClick={() => setShowSource(true)}>
              <CodeXml size={14} />
            </ToolButton>
          </div>
        )}
        <div
          ref={canvasRef}
          hidden={showSource}
          className="rich-figure-canvas"
          role={figureShown ? 'img' : undefined}
          aria-label={figureShown ? `Rendered ${label}` : undefined}
        />
        {figureShown && notice && (
          <div role="status" className="rich-figure-notice">{notice}</div>
        )}
      </figure>
      {!figureShown && (
        <>
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
                  onClick={() => setShowSource(false)}
                  className="text-xs text-text-dim hover:text-text px-1.5 py-0.5 rounded hover:bg-surface-hover"
                >
                  Rendered
                </button>
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
          {/* The `!` overrides graft the header above onto `.markdown-content pre`. */}
          <pre className="!mt-0 !rounded-t-none !border-t-0">
            <code className={`language-${label}`}>{source}</code>
          </pre>
        </>
      )}
      {lightbox && (
        <Lightbox
          open
          onClose={() => setLightbox(null)}
          title={name}
          label={label}
          markup={lightbox.markup}
          size={lightbox.size}
        />
      )}
    </div>
  );
}
