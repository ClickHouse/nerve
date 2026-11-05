import {
  useContext, useEffect, useLayoutEffect, useRef, useState, type CSSProperties, type ReactNode, type Ref,
} from 'react';
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
const RENDERERS: Record<RichKind, () => Promise<Mount>> = {
  mermaid: () => import('./mermaid').then(m => m.mountMermaid),
  'vega-lite': () => import('./vega').then(m => m.mountVega('vega-lite')),
  vega: () => import('./vega').then(m => m.mountVega('vega')),
  svg: () => import('./svg').then(m => m.mountSvg),
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
 * A picture wider than the column is scaled down to fit it only while its
 * smallest text stays at least this large; below that it is shown at 1:1 and
 * scrolls instead.
 */
const MIN_READABLE_TEXT_PX = 11;

/** The smallest text assumed in a picture whose renderer did not say. */
const DEFAULT_TEXT_PX = 14;

/** A width change reaches the renderer once the column has stopped resizing for this long. */
const RESIZE_DEBOUNCE_MS = 150;

/**
 * A complete block whose first picture has started drawing holds a quiet
 * placeholder this long before it shows its source instead. The time counts
 * from the block's own render, not from its wait behind other blocks.
 */
const PLACEHOLDER_MS = 1500;

/**
 * A block drawn before (its height is known) holds its placeholder until the
 * picture or a failure arrives, and shows its source only after this long, in
 * case its renderer never finishes. Also the longest one render keeps the
 * blocks behind it waiting (see `inLane`).
 */
const STALLED_MS = 10_000;

/**
 * Below this height a picture gets the toolbar on a row of its own above it:
 * the toolbar (34px, 8px in from the corner) would otherwise cover a good
 * part of it whenever it shows.
 */
const SHORT_PICTURE_PX = 72;

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
  /** The smallest text in it at natural size, when the renderer reported it. */
  minTextPx: number | null;
  /** The theme it was drawn for, whose background its files are painted over. */
  theme: ResolvedTheme;
  /** The canvas width the render was last laid out at. */
  width: number;
}

/** What the render on screen means for the frame, as state. */
interface Picture {
  naturalSize: Size | null;
  minTextPx: number | null;
  /** There is an SVG to open full size and to save. */
  exportable: boolean;
  /** A title the picture carries, for the figure's accessible name. */
  title: string | null;
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

/** A reported text size the frame can judge readability by: finite and above zero, else null. */
function usableTextPx(px: number | undefined): number | null {
  return typeof px === 'number' && px > 0 && Number.isFinite(px) ? px : null;
}

/** Where each kind's renders wait their turn; see `inLane`. */
const lanes = new Map<RichKind, Promise<void>>();

/**
 * Runs `task` once every render of `kind` asked for before it has finished.
 * mermaid draws one diagram at a time anyway (its configuration is global);
 * queueing here as well means `task` starting is this block's own render
 * starting, which is what the placeholder's patience is measured from. The
 * lane moves on when a render finishes, is abandoned (its block went, or
 * asked for another render), or has run for STALLED_MS.
 */
function inLane<T>(kind: RichKind, task: () => Promise<T>): { result: Promise<T>; abandon: () => void } {
  const previous = lanes.get(kind) ?? Promise.resolve();
  let abandon!: () => void;
  const abandoned = new Promise<void>(resolve => { abandon = resolve; });
  const result = previous.then(task);
  lanes.set(kind, previous.then(() => new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, STALLED_MS);
    const done = () => {
      clearTimeout(timer);
      resolve();
    };
    result.then(done, done);
    abandoned.then(done);
  })));
  return { result, abandon };
}

/**
 * The canvas height each block source was last drawn at, so that a block
 * drawn again — the finished message replacing the streamed one, the page
 * reloaded, a chat opened again — holds its place while it renders instead of
 * jumping. Kept in sessionStorage under a hash of the source, the newest
 * MAX_HEIGHT_HINTS; the in-memory copy serves where storage is unavailable.
 */
const HEIGHT_HINTS_KEY = 'nerve-rich-heights';
const MAX_HEIGHT_HINTS = 200;
const memoryHints = new Map<string, number>();

function hintKey(kind: RichKind, source: string): string {
  let hash = 0x811c9dc5;
  for (let i = 0; i < source.length; i++) {
    hash ^= source.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return `${kind}:${source.length}:${(hash >>> 0).toString(36)}`;
}

/** The stored hints, oldest first; null when this page has no usable storage. */
function storedHints(): Map<string, number> | null {
  try {
    const raw = window.sessionStorage.getItem(HEIGHT_HINTS_KEY);
    const entries: unknown = raw ? JSON.parse(raw) : [];
    if (!Array.isArray(entries)) return new Map();
    return new Map(entries.filter((entry): entry is [string, number] =>
      Array.isArray(entry) && typeof entry[0] === 'string' && typeof entry[1] === 'number' && entry[1] > 0));
  } catch {
    return null;
  }
}

function newest(hints: Map<string, number>, key: string, height: number) {
  hints.delete(key);
  hints.set(key, height);
  while (hints.size > MAX_HEIGHT_HINTS) hints.delete(hints.keys().next().value!);
}

function heightHintFor(kind: RichKind, source: string): number {
  const key = hintKey(kind, source);
  return memoryHints.get(key) ?? storedHints()?.get(key) ?? 0;
}

function rememberHeight(kind: RichKind, source: string, height: number) {
  const key = hintKey(kind, source);
  newest(memoryHints, key, height);
  const stored = storedHints();
  if (!stored) return;
  newest(stored, key, height);
  try {
    window.sessionStorage.setItem(HEIGHT_HINTS_KEY, JSON.stringify([...stored]));
  } catch {
    // Storage full or refused: the in-memory hints serve this page.
  }
}

/** Labels a renderer gives every picture of its kind, which name none of them (Vega's default description). */
const DEFAULT_LABEL = /^(vega(-lite)? )?(visualization|chart)$/i;

/**
 * A title the drawn picture carries, if it has one that is cheap to find: an
 * svg's own `<title>` (mermaid's `accTitle`), a title drawn as text (Vega's,
 * mermaid's), or a Vega spec's `description` (on its container as
 * `aria-label`) — but not Vega's default description.
 */
function pictureTitle(stage: HTMLElement): string | null {
  const svg = stage.querySelector('svg');
  const documents = [stage, ...stage.querySelectorAll('[role~="graphics-document"]')];
  const candidates = [
    svg?.querySelector(':scope > title')?.textContent,
    stage.querySelector('.role-title-text text, .flowchartTitleText, .pieTitleText, .titleText, .chart-title')?.textContent,
    documents.find(el => el.matches('[role~="graphics-document"][aria-label]'))?.getAttribute('aria-label'),
  ];
  const title = candidates
    .map(text => text?.replace(/\s+/g, ' ').trim())
    .find(text => text && !DEFAULT_LABEL.test(text));
  return title ? title.slice(0, 120) : null;
}

/**
 * How a picture sits in a canvas `width` pixels wide: whether it is wider
 * than the column, and if so whether it is scaled to fit — which it is by
 * default while its smallest text stays readable. One the renderer gave no
 * size for is left as it was drawn (a container-sized chart).
 */
function layoutFor(natural: Size | null | undefined, width: number, choice: Choice | null, minTextPx: number | null | undefined) {
  if (!natural || width <= 0 || natural.width <= width + 0.5) return { overflowing: false, fit: false };
  const readable = (width / natural.width) * (minTextPx ?? DEFAULT_TEXT_PX) >= MIN_READABLE_TEXT_PX;
  return { overflowing: true, fit: choice ? choice === 'fit' : readable };
}

/**
 * Sizes the picture in `stage`: scaled down to the column, or at its natural
 * size, which scrolls when it is wider than the column. The svg's own width
 * and height attributes are not trusted to say either (`width="100%"`).
 */
function applySizing(stage: HTMLElement, natural: Size | null, fit: boolean) {
  const svg = stage.querySelector('svg');
  if (!svg || !natural) return;
  if (fit) {
    ensureViewBox(svg, natural);
    svg.style.width = '100%';
    svg.style.height = 'auto';
    // Scaled down to the column, never up past its own size.
    svg.style.maxWidth = `${natural.width}px`;
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
function ToolButton({ label, onClick, text = false, ref, children }: {
  label: string;
  onClick: () => void;
  text?: boolean;
  ref?: Ref<HTMLButtonElement>;
  children: ReactNode;
}) {
  return (
    <button
      ref={ref}
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
 * change or more streamed text never blanks the block. Renders of one kind
 * take turns (`inLane`). While the text streams the block shows its source
 * until a picture is ready; a block that arrives complete (a chat being
 * opened) holds a quiet placeholder instead — at its last known height, if
 * it was drawn before — and shows its source only if its own render is slow.
 * A failure after the text has finished shows the source with the renderer's
 * message; during the stream it shows the source quietly, since the text may
 * simply not be complete.
 *
 * A picture wider than the column is scaled to fit while its smallest text
 * stays readable and is shown at 1:1 with a scrollbar otherwise; the toolbar
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
  // The control a Source/Rendered switch hands focus to: the other one.
  const sourceButtonRef = useRef<HTMLButtonElement>(null);
  const renderedButtonRef = useRef<HTMLButtonElement>(null);
  const switchedRef = useRef(false);
  const [outcome, setOutcome] = useState<Outcome | null>(null);
  const [picture, setPicture] = useState<Picture | null>(null);
  const [canvasWidth, setCanvasWidth] = useState(0);
  // The width the fit decision and the toolbar go by. It follows the canvas
  // at once, except for a picture that draws itself again for a new width:
  // that one keeps its decision until the new drawing is there.
  const [layoutWidth, setLayoutWidth] = useState(0);
  const [canvasHeight, setCanvasHeight] = useState(0);
  const [choice, setChoice] = useState<Choice | null>(null);
  const [showSource, setShowSource] = useState(false);
  const [copied, setCopied] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  // The full-size view while it is open.
  const [lightbox, setLightbox] = useState<{ markup: string; size: Size } | null>(null);
  // A block that first appeared mid-stream has shown its source already; it
  // keeps showing it until the picture is ready, rather than a placeholder.
  const [mountedStreaming] = useState(streaming);
  const [heightHint] = useState(() => heightHintFor(kind, source));
  // This block's own render has begun: it is no longer waiting its turn.
  const [drawing, setDrawing] = useState(false);
  // The placeholder has waited long enough; the source is better than nothing.
  const [slow, setSlow] = useState(false);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || tooLarge) return;
    const stage = document.createElement('div');
    stage.className = 'rich-stage';
    // Rendered at the canvas width but out of sight, until it replaces the picture on screen.
    stage.setAttribute('data-pending', '');
    canvas.appendChild(stage);
    let cancelled = false;
    const turn = inLane(kind, async () => {
      // Asked for something else, or gone, while it waited: nothing to draw.
      if (cancelled) return null;
      // Its turn: from here the placeholder's patience runs, the renderer's
      // download included (a stalled chunk still ends in the source).
      setDrawing(true);
      const mount = await RENDERERS[kind]();
      if (cancelled) return null;
      return mount(stage, settled, { theme, width: canvas.clientWidth || widthRef.current });
    });
    turn.result.then(
      (mounted) => {
        if (cancelled || !mounted) {
          mounted?.cleanup();
          stage.remove();
          return;
        }
        // A hidden canvas (the source is showing) measures 0; the last width stands.
        const width = canvas.clientWidth || widthRef.current;
        widthRef.current = width;
        const natural = usableSize(mounted.naturalSize);
        const minTextPx = usableTextPx(mounted.minTextPx);
        applySizing(stage, natural, layoutFor(natural, width, choiceRef.current, minTextPx).fit);
        stage.removeAttribute('data-pending');
        const previous = shownRef.current;
        shownRef.current = { stage, mounted, natural, minTextPx, theme, width };
        previous?.mounted.cleanup();
        previous?.stage.remove();
        // The picture's height decides the toolbar's row; the hint for the
        // next time is the canvas's whole height, a 1:1 picture's scrollbar
        // included.
        const height = canvas.clientHeight;
        if (height > 0) {
          rememberHeight(kind, settled, canvas.offsetHeight || height);
          setCanvasHeight(height);
        }
        setOutcome({ kind, theme, source: settled, error: null });
        setPicture({
          naturalSize: natural,
          minTextPx,
          exportable: !!mounted.toSvg || !!stage.querySelector('svg'),
          title: pictureTitle(stage),
        });
        setCanvasWidth(width);
        setLayoutWidth(width);
      },
      (err: unknown) => {
        stage.remove();
        if (cancelled) return;
        setOutcome({ kind, theme, source: settled, error: describe(err) });
      },
    );
    return () => {
      cancelled = true;
      turn.abandon();
      if (shownRef.current?.stage !== stage) stage.remove();
    };
  }, [kind, settled, theme, tooLarge]);

  // The column's width decides fit or 1:1, and a container-sized chart lays
  // itself out again for it once the resizing has stopped. The picture's
  // height decides whether the toolbar needs a row of its own.
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || typeof ResizeObserver === 'undefined') return;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const observer = new ResizeObserver((entries) => {
      const box = entries[entries.length - 1].contentRect;
      const width = Math.round(box.width);
      const height = Math.round(box.height);
      // A hidden canvas (the source is showing) measures 0 both ways; what it measured last stands.
      if (width <= 0) return;
      if (height > 0) setCanvasHeight(height);
      if (width === widthRef.current) return;
      widthRef.current = width;
      setCanvasWidth(width);
      // A picture that will not draw itself again is decided for the new width now.
      if (!shownRef.current?.mounted.resize) setLayoutWidth(width);
      clearTimeout(timer);
      timer = setTimeout(() => {
        const shown = shownRef.current;
        if (!shown) return;
        // A chart drawn to the column's width has drawn itself again, and may
        // report another size and text size (on the same Mounted) and a new
        // svg: size that one, and decide fit and the toolbar for the width.
        const settle = () => {
          if (shownRef.current !== shown) return;
          shown.natural = usableSize(shown.mounted.naturalSize);
          shown.minTextPx = usableTextPx(shown.mounted.minTextPx);
          const settledWidth = widthRef.current;
          applySizing(shown.stage, shown.natural, layoutFor(shown.natural, settledWidth, choiceRef.current, shown.minTextPx).fit);
          setPicture(before => before && { ...before, naturalSize: shown.natural, minTextPx: shown.minTextPx });
          setLayoutWidth(settledWidth);
          const height = canvas.clientHeight;
          if (height > 0) setCanvasHeight(height);
        };
        if (!shown.mounted.resize || shown.width === widthRef.current) {
          settle();
          return;
        }
        shown.width = widthRef.current;
        Promise.resolve(shown.mounted.resize()).then(settle, settle);
      }, RESIZE_DEBOUNCE_MS);
    });
    observer.observe(canvas);
    return () => {
      observer.disconnect();
      clearTimeout(timer);
    };
  }, []);

  const layout = layoutFor(picture?.naturalSize, layoutWidth, choice, picture?.minTextPx);
  // While a picture draws itself again for a new width, it keeps the decision
  // it had; one shown at its own size that would now stick out of a narrower
  // column is scaled into it meanwhile rather than given a scrollbar.
  const redrawing = layoutWidth !== canvasWidth;
  const sticksOut = !!picture?.naturalSize && canvasWidth > 0 && picture.naturalSize.width > canvasWidth + 0.5;
  const sizedToFit = layout.fit || (redrawing && !layout.overflowing && sticksOut);
  useLayoutEffect(() => {
    const shown = shownRef.current;
    if (shown) applySizing(shown.stage, shown.natural, sizedToFit);
  }, [sizedToFit, picture]);

  // The outcome counts only for the attempt it came from; until the current
  // one reports, the block is rendering (and keeps the previous picture).
  const current = outcome && outcome.kind === kind && outcome.theme === theme && outcome.source === settled
    ? outcome
    : null;
  const status: Status = tooLarge ? 'failed' : !current ? 'rendering' : current.error ? 'failed' : 'rendered';
  const error = tooLarge
    ? `too large to render (${settled.length.toLocaleString()} characters)`
    : current?.error ?? null;

  const hasRender = picture !== null;
  const awaitingFirst = !hasRender && status === 'rendering' && !streaming && !mountedStreaming;
  // The placeholder's patience runs from the block's own render, not its wait in line.
  useEffect(() => {
    if (!awaitingFirst || !drawing) return;
    const timer = setTimeout(() => setSlow(true), heightHint > 0 ? STALLED_MS : PLACEHOLDER_MS);
    return () => clearTimeout(timer);
  }, [awaitingFirst, drawing, heightHint]);

  // After a Source/Rendered switch, focus moves to the control that switches back.
  useEffect(() => {
    if (!switchedRef.current) return;
    switchedRef.current = false;
    (showSource ? renderedButtonRef : sourceButtonRef).current?.focus();
  }, [showSource]);

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

  const switchView = (toSource: boolean) => {
    switchedRef.current = true;
    setShowSource(toSource);
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

  const label = KIND_LABEL[kind];
  const name = FIGURE_NAME[kind];
  const fullName = picture?.title ? `${name}: ${picture.title}` : name;

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

  const failed = status === 'failed' && !streaming;
  const figureShown = !showSource && hasRender && !failed;
  const placeholder = awaitingFirst && !slow;
  const short = figureShown
    ? canvasHeight > 0 && canvasHeight < SHORT_PICTURE_PX
    : placeholder && heightHint > 0 && heightHint < SHORT_PICTURE_PX;
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
          sizes to its container measures the canvas as it renders. A
          placeholder for a picture drawn before holds the height it had (the
          stylesheet adds the frame around `--rich-hint`). */}
      <figure
        className="rich-figure"
        data-collapsed={figureShown || placeholder ? undefined : ''}
        data-placeholder={placeholder ? '' : undefined}
        data-hint={placeholder && heightHint > 0 ? '' : undefined}
        data-short={short ? '' : undefined}
        aria-hidden={figureShown || placeholder ? undefined : true}
        aria-busy={placeholder || undefined}
        aria-label={figureShown || placeholder ? fullName : undefined}
        style={placeholder && heightHint > 0 ? { '--rich-hint': `${heightHint}px` } as CSSProperties : undefined}
      >
        {placeholder && <div className="rich-figure-placeholder">Rendering…</div>}
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
            <ToolButton ref={sourceButtonRef} label="Source" onClick={() => switchView(true)}>
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
      {!figureShown && !placeholder && (
        <>
          <div className="rich-source-bar flex items-center justify-between gap-2 bg-surface-raised border border-border rounded-t-md px-3 py-1">
            <span className="text-xs text-text-dim font-mono truncate">
              {label}
              {hint && (
                <span className={`ml-2 font-sans ${failed ? 'text-warning' : 'text-text-faint'}`}>{hint}</span>
              )}
            </span>
            <span className="flex items-center gap-1">
              {hasRender && !failed && (
                <button
                  ref={renderedButtonRef}
                  type="button"
                  onClick={() => switchView(false)}
                  className="text-xs text-text-dim hover:text-text px-1.5 py-0.5 rounded hover:bg-surface-hover cursor-pointer"
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
          title={fullName}
          label={label}
          markup={lightbox.markup}
          size={lightbox.size}
        />
      )}
    </div>
  );
}
