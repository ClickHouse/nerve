import {
  useEffect, useId, useLayoutEffect, useRef, useState,
  type KeyboardEvent as ReactKeyboardEvent, type MouseEvent as ReactMouseEvent,
  type PointerEvent as ReactPointerEvent, type ReactNode,
} from 'react';
import { IconButton, Modal } from '../../ui';
import { ChevronLeft, ChevronRight, X, ZoomIn, ZoomOut } from '../../ui/icons';
import { ensureViewBox, type Size } from './export';

const MIN_ZOOM = 0.1;
const MAX_ZOOM = 8;
/** One click of the zoom buttons, or one press of + or −. */
const ZOOM_STEP = 1.25;
/** One arrow key press, in CSS pixels. */
const PAN_STEP = 48;
/** The gap 1:1 and Fit leave between the picture and the viewport's edge. */
const MARGIN = 24;
/** A touch that moves at least this far sideways, in CSS pixels, is a swipe. */
const SWIPE_MIN = 50;
/** A touch that moves this far, in CSS pixels, has a direction: sideways a swipe, up or down a drag. */
const TOUCH_SLOP = 10;

/**
 * The step of a touch that moved by (dx, dy): 1 to the next picture (a swipe
 * to the left), -1 to the previous one, 0 when it is no swipe (too short, or
 * more up or down than sideways).
 */
function swipeStep(dx: number, dy: number): -1 | 0 | 1 {
  if (Math.abs(dx) < SWIPE_MIN || Math.abs(dx) < 2 * Math.abs(dy)) return 0;
  return dx < 0 ? 1 : -1;
}

// The arrows over the picture show on hover and on keyboard focus. Tailwind's
// hover variants apply only where the screen has hover, so on a touch screen
// the arrows stay hidden (a swipe steps there) and take no taps.
const NAV_CLASS = 'absolute top-1/2 -translate-y-1/2 flex items-center justify-center w-10 h-10 rounded-full '
  + 'bg-black/55 text-white shadow cursor-pointer transition-opacity hover:bg-black/70 '
  + 'opacity-0 group-hover/stage:opacity-100 focus-visible:opacity-100 [@media(hover:none)]:pointer-events-none '
  + 'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent/70';

/** The arrow button that had focus when the picture changed. */
type NavFocus = { current: 'prev' | 'next' | null };

/**
 * Where the picture sits: scaled by `zoom` about its top-left corner and
 * moved by `x`, `y` from where it sits at 1:1 (see `origin`).
 */
interface View {
  zoom: number;
  x: number;
  y: number;
}

const ONE_TO_ONE: View = { zoom: 1, x: 0, y: 0 };

/** Shadow-root styles: the picture at the size the view sets, nothing inherited from the chat's markdown rules. */
const PICTURE_CSS = ':host { display: block; } svg { display: block; max-width: none; }';

function clampZoom(zoom: number): number {
  return Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, zoom));
}

/**
 * The picture's corner along one axis at 1:1: centred when it fits the
 * viewport, MARGIN in from the start when it does not, so a long diagram
 * opens at its beginning. The content's CSS `left`/`top` say the same.
 */
function origin(viewport: number, picture: number): number {
  return Math.max(MARGIN, (viewport - picture) / 2);
}

/** `view` zoomed by `factor` about the viewport point (px, py), which keeps the picture point under it. */
function zoomAt(view: View, factor: number, px: number, py: number, viewport: Size, picture: Size): View {
  const zoom = clampZoom(view.zoom * factor);
  const k = zoom / view.zoom;
  const ox = origin(viewport.width, picture.width);
  const oy = origin(viewport.height, picture.height);
  return {
    zoom,
    x: px - ox - (px - ox - view.x) * k,
    y: py - oy - (py - oy - view.y) * k,
  };
}

/** The picture scaled to fit inside the viewport, centred. */
function fitView(viewport: Size, picture: Size): View {
  const zoom = clampZoom(Math.min(
    (viewport.width - 2 * MARGIN) / picture.width,
    (viewport.height - 2 * MARGIN) / picture.height,
  ));
  return {
    zoom,
    x: (viewport.width - picture.width * zoom) / 2 - origin(viewport.width, picture.width),
    y: (viewport.height - picture.height * zoom) / 2 - origin(viewport.height, picture.height),
  };
}

export interface LightboxProps {
  open: boolean;
  onClose: () => void;
  /** What the picture is — "Diagram", "Chart" — the dialog's name. */
  title: string;
  /** The block's language, shown beside the title. */
  label?: string;
  /** The picture as SVG markup: a renderer's export, or the svg drawn in the chat. */
  markup?: string;
  /** Or a raster image's URL, shown in an <img>. */
  src?: string;
  /** The picture's size at 1:1, in CSS pixels. */
  size: Size;
  /** `fit-down`: open scaled to fit when the picture is larger than the window, else at 1:1. */
  initialView?: 'actual' | 'fit-down';
  /**
   * Step through a set of pictures: the position, and what the arrow
   * buttons, the keys and a swipe do. The first picture has no previous
   * one and the last no next one.
   */
  nav?: { index: number; count: number; onPrev: () => void; onNext: () => void };
  /** Extra toolbar controls, before Close (Open original, Download). */
  actions?: ReactNode;
}

/**
 * A rendered block's picture at full size, nearly the whole viewport: 1:1 to
 * begin with, zoomed by the wheel about the cursor (or by pinching), moved
 * by dragging, with buttons and keys for zoom in, zoom out, 1:1 and fit.
 * Escape closes it (the Modal's own handling).
 */
export function Lightbox({ open, onClose, ...view }: LightboxProps) {
  const navFocusRef = useRef<'prev' | 'next' | null>(null);
  const nav = view.nav && view.nav.count > 1 ? view.nav : undefined;
  return (
    <Modal open={open} onClose={onClose} size="full" ariaLabel={view.title}>
      {/* A new picture starts from its own view; the dialog, and focus in it, stay.
          The place in the set is in the key: two images of a chat can have one source. */}
      <LightboxView key={`${nav?.index ?? ''}:${view.src ?? view.markup}`} onClose={onClose} navFocusRef={navFocusRef} {...view} />
      {nav && <p className="sr-only" aria-live="polite">{`${view.title}, picture ${nav.index + 1} of ${nav.count}`}</p>}
    </Modal>
  );
}

function ViewButton({ label, onClick, children }: { label: string; onClick: () => void; children: ReactNode }) {
  return (
    <button
      type="button"
      onClick={onClick}
      title={label}
      aria-label={label}
      className="inline-flex items-center justify-center h-8 min-w-8 px-1.5 rounded text-xs font-medium tabular-nums text-text-muted hover:text-text hover:bg-surface-hover cursor-pointer transition-colors"
    >
      {children}
    </button>
  );
}

function LightboxView({
  onClose, title, label, markup = '', src, size, initialView = 'actual', nav, actions, navFocusRef,
}: Omit<LightboxProps, 'open'> & { navFocusRef: NavFocus }) {
  const [view, setView] = useState<View>(ONE_TO_ONE);
  const [dragging, setDragging] = useState(false);
  const viewportRef = useRef<HTMLDivElement>(null);
  const hostRef = useRef<HTMLDivElement>(null);
  const prevRef = useRef<HTMLButtonElement>(null);
  const nextRef = useRef<HTMLButtonElement>(null);
  // Pointers down on the viewport, where each was last seen: one drags, two pinch.
  const pointersRef = useRef(new Map<number, { x: number; y: number }>());
  // The touch that can become a swipe, and the view when it started.
  const swipeRef = useRef<{ id: number; x: number; y: number; view: View; sideways: boolean } | null>(null);
  const hintId = useId();
  const steps = nav && nav.count > 1 ? nav : undefined;
  const hasPrev = !!steps && steps.index > 0;
  const hasNext = !!steps && steps.index < steps.count - 1;

  // The picture goes into a shadow root: its ids (markers, gradients) and its
  // <style> cannot collide with the same picture still drawn in the chat.
  useLayoutEffect(() => {
    const host = hostRef.current;
    if (!host || src) return;
    const root: ShadowRoot | HTMLElement = host.shadowRoot
      ?? (typeof host.attachShadow === 'function' ? host.attachShadow({ mode: 'open' }) : host);
    // Markup a renderer produced from a source it has already made safe —
    // mermaid's strict output, the sanitized SVG, Vega's export — the same
    // picture that is on screen in the chat.
    root.innerHTML = `<style>${PICTURE_CSS}</style>${markup}`;
    const svg = root.querySelector('svg');
    if (svg) {
      ensureViewBox(svg, size);
      svg.style.width = `${size.width}px`;
      svg.style.height = `${size.height}px`;
    }
    return () => {
      root.innerHTML = '';
    };
  }, [markup, size, src]);

  // A screenshot larger than the window opens whole, not at its top-left corner.
  useLayoutEffect(() => {
    if (initialView !== 'fit-down') return;
    const rect = viewportRef.current?.getBoundingClientRect();
    if (!rect || !rect.width || !rect.height) return;
    const fitted = fitView(rect, size);
    if (fitted.zoom < 1) setView(fitted);
  }, [initialView, size]);

  // Wheel zoom needs preventDefault, which React's passive wheel listener cannot do.
  useEffect(() => {
    const viewport = viewportRef.current;
    if (!viewport) return;
    const onWheel = (e: WheelEvent) => {
      e.preventDefault();
      const rect = viewport.getBoundingClientRect();
      const delta = e.deltaMode === 1 ? e.deltaY * 16 : e.deltaMode === 2 ? e.deltaY * rect.height : e.deltaY;
      // A trackpad pinch arrives as a wheel event with ctrlKey and small deltas.
      const factor = Math.exp(-delta * (e.ctrlKey ? 0.01 : 0.0015));
      setView(v => zoomAt(v, factor, e.clientX - rect.left, e.clientY - rect.top, rect, size));
    };
    viewport.addEventListener('wheel', onWheel, { passive: false });
    return () => viewport.removeEventListener('wheel', onWheel);
  }, [size]);

  // Focus starts on the picture, so the keys work at once. A frame later, so
  // the Modal has already noted the control to give focus back to on close.
  // After a step with an arrow button that had focus, that button keeps it,
  // or the other one when this picture is the first or the last.
  useEffect(() => {
    const frame = requestAnimationFrame(() => {
      const wanted = navFocusRef.current;
      navFocusRef.current = null;
      const arrow = wanted === 'next' ? nextRef.current ?? prevRef.current
        : wanted === 'prev' ? prevRef.current ?? nextRef.current : null;
      (arrow ?? viewportRef.current)?.focus({ preventScroll: true });
    });
    return () => cancelAnimationFrame(frame);
  }, [navFocusRef]);

  const viewportBox = () => {
    const rect = viewportRef.current?.getBoundingClientRect();
    return { left: rect?.left ?? 0, top: rect?.top ?? 0, width: rect?.width ?? 0, height: rect?.height ?? 0 };
  };
  const zoomBy = (factor: number) => {
    const box = viewportBox();
    setView(v => zoomAt(v, factor, box.width / 2, box.height / 2, box, size));
  };
  const actualSize = () => setView(ONE_TO_ONE);
  const fit = () => setView(fitView(viewportBox(), size));
  const pan = (dx: number, dy: number) => setView(v => ({ ...v, x: v.x + dx, y: v.y + dy }));
  // The whole width of the picture is in the window: Left, Right and a swipe
  // step through the set, as there is nothing to move to sideways.
  const fitsWidth = () => size.width * view.zoom <= viewportBox().width;
  const stepBy = (by: -1 | 1, from?: 'prev' | 'next') => {
    navFocusRef.current = from ?? null;
    if (by < 0) nav?.onPrev();
    else nav?.onNext();
  };
  // Double-click switches between 1:1 and fit.
  const toggleFit = () => {
    if (view.zoom === 1 && view.x === 0 && view.y === 0) fit();
    else actualSize();
  };

  const onKeyDown = (e: ReactKeyboardEvent) => {
    if (e.ctrlKey || e.metaKey || e.altKey) return;
    const step = e.shiftKey ? PAN_STEP * 4 : PAN_STEP;
    switch (e.key) {
      case '+': case '=': zoomBy(ZOOM_STEP); break;
      case '-': case '_': zoomBy(1 / ZOOM_STEP); break;
      case '0': actualSize(); break;
      case 'PageUp': case 'p': if (!nav) return; stepBy(-1); break;
      case 'PageDown': case 'n': if (!nav) return; stepBy(1); break;
      // Arrows move the view over the picture, as scrolling would. In a set,
      // Left and Right step while the picture fits the width of the window.
      case 'ArrowLeft': if (steps && !e.shiftKey && fitsWidth()) stepBy(-1); else pan(step, 0); break;
      case 'ArrowRight': if (steps && !e.shiftKey && fitsWidth()) stepBy(1); else pan(-step, 0); break;
      case 'ArrowUp': pan(0, step); break;
      case 'ArrowDown': pan(0, -step); break;
      default: return;
    }
    // Handled here: the app's own shortcuts behind the dialog must not act on it too.
    e.preventDefault();
    e.stopPropagation();
  };

  const onPointerDown = (e: ReactPointerEvent<HTMLDivElement>) => {
    if (e.pointerType === 'mouse' && e.button !== 0) return;
    e.currentTarget.setPointerCapture?.(e.pointerId);
    const pointers = pointersRef.current;
    pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
    setDragging(true);
    const swipe = swipeRef.current;
    if (swipe) {
      // A second finger makes it a pinch, from where the picture was.
      swipeRef.current = null;
      setView(swipe.view);
    } else if (e.pointerType === 'touch' && pointers.size === 1 && steps && fitsWidth()) {
      swipeRef.current = { id: e.pointerId, x: e.clientX, y: e.clientY, view, sideways: false };
    }
  };
  const onPointerMove = (e: ReactPointerEvent<HTMLDivElement>) => {
    const pointers = pointersRef.current;
    const last = pointers.get(e.pointerId);
    if (!last) return;
    const now = { x: e.clientX, y: e.clientY };
    pointers.set(e.pointerId, now);
    const swipe = swipeRef.current;
    if (swipe?.id === e.pointerId) {
      const dx = now.x - swipe.x;
      const dy = now.y - swipe.y;
      if (!swipe.sideways) {
        if (Math.max(Math.abs(dx), Math.abs(dy)) < TOUCH_SLOP) return;
        if (Math.abs(dy) > Math.abs(dx)) {
          // Up or down: no swipe, a drag that moves the picture (a tall one).
          swipeRef.current = null;
          setView({ ...swipe.view, x: swipe.view.x + dx, y: swipe.view.y + dy });
          return;
        }
        swipe.sideways = true;
      }
      // The picture follows the finger sideways. Past the first or the last
      // picture it moves a third as far: nothing comes.
      const blocked = (dx > 0 && !hasPrev) || (dx < 0 && !hasNext);
      setView({ ...swipe.view, x: swipe.view.x + (blocked ? dx / 3 : dx) });
      return;
    }
    if (pointers.size === 1) {
      pan(now.x - last.x, now.y - last.y);
      return;
    }
    // Two pointers: zoom by how far apart they moved, about their midpoint.
    const other = [...pointers].find(([id]) => id !== e.pointerId)?.[1];
    const before = other ? Math.hypot(last.x - other.x, last.y - other.y) : 0;
    if (!other || !before) return;
    const after = Math.hypot(now.x - other.x, now.y - other.y);
    const box = viewportBox();
    const midX = (now.x + other.x) / 2 - box.left;
    const midY = (now.y + other.y) / 2 - box.top;
    // The midpoint moved by half of this pointer's move.
    const shiftX = (now.x - last.x) / 2;
    const shiftY = (now.y - last.y) / 2;
    setView(v => zoomAt({ ...v, x: v.x + shiftX, y: v.y + shiftY }, after / before, midX, midY, box, size));
  };
  const onPointerEnd = (e: ReactPointerEvent<HTMLDivElement>) => {
    pointersRef.current.delete(e.pointerId);
    if (pointersRef.current.size === 0) setDragging(false);
    const swipe = swipeRef.current;
    if (swipe?.id !== e.pointerId) return;
    swipeRef.current = null;
    setView(swipe.view);
    if (e.type !== 'pointerup') return;
    const by = swipeStep(e.clientX - swipe.x, e.clientY - swipe.y);
    if (by === 1 && hasNext) stepBy(1);
    if (by === -1 && hasPrev) stepBy(-1);
  };
  const onArrow = (e: ReactMouseEvent<HTMLButtonElement>, by: -1 | 1) => {
    const had = document.activeElement === e.currentTarget;
    stepBy(by, had ? (by < 0 ? 'prev' : 'next') : undefined);
  };

  return (
    <div className="rich-lightbox flex flex-col h-full rounded-b-xl overflow-hidden" onKeyDown={onKeyDown}>
      <div className="flex items-center gap-1 pl-4 pr-2 py-1.5 border-b border-border shrink-0">
        <h2 className="mr-auto min-w-0 truncate text-sm font-medium text-text">
          {title}
          {label && <span className="ml-2 font-mono text-xs font-normal text-text-faint">{label}</span>}
        </h2>
        {steps && (
          <span className="mr-2 shrink-0 whitespace-nowrap text-xs tabular-nums text-text-muted">{steps.index + 1} / {steps.count}</span>
        )}
        <span className="hidden md:inline mr-2 text-2xs text-text-faint">Scroll to zoom · drag to move</span>
        <div role="group" aria-label="Zoom" className="flex items-center gap-0.5">
          <IconButton label="Zoom out" onClick={() => zoomBy(1 / ZOOM_STEP)}>
            <ZoomOut size={16} />
          </IconButton>
          <output aria-label="Zoom level" className="w-12 text-center text-xs tabular-nums text-text-muted">
            {Math.round(view.zoom * 100)}%
          </output>
          <IconButton label="Zoom in" onClick={() => zoomBy(ZOOM_STEP)}>
            <ZoomIn size={16} />
          </IconButton>
          <ViewButton label="Actual size (1:1)" onClick={actualSize}>1:1</ViewButton>
          <ViewButton label="Fit to window" onClick={fit}>Fit</ViewButton>
        </div>
        <span aria-hidden="true" className="mx-1 h-5 w-px bg-border" />
        {actions}
        <IconButton label="Close dialog" onClick={onClose}>
          <X size={18} />
        </IconButton>
      </div>
      <div className="group/stage relative flex flex-col flex-1 min-h-0">
        <div
          ref={viewportRef}
          className="rich-lightbox-viewport"
          data-dragging={dragging ? '' : undefined}
          tabIndex={0}
          role="img"
          aria-label={title}
          aria-describedby={hintId}
          onPointerDown={onPointerDown}
          onPointerMove={onPointerMove}
          onPointerUp={onPointerEnd}
          onPointerCancel={onPointerEnd}
          onDoubleClick={toggleFit}
        >
          <div
            className="rich-lightbox-content"
            style={{
              width: size.width,
              height: size.height,
              left: `max(${MARGIN}px, calc(50% - ${size.width / 2}px))`,
              top: `max(${MARGIN}px, calc(50% - ${size.height / 2}px))`,
              transform: `translate(${view.x}px, ${view.y}px) scale(${view.zoom})`,
            }}
          >
            {src ? (
              <img
                src={src}
                alt=""
                draggable={false}
                className="block max-w-none select-none"
                // Zoomed in, a screenshot's pixels stay crisp instead of blurring.
                style={{ width: size.width, height: size.height, imageRendering: view.zoom >= 2 ? 'pixelated' : 'auto' }}
              />
            ) : (
              <div ref={hostRef} />
            )}
          </div>
        </div>
        {/* Beside the viewport, not in it: its pointer capture would take the click. */}
        {hasPrev && (
          <button ref={prevRef} type="button" aria-label="Previous picture" title="Previous picture (←)"
            onClick={e => onArrow(e, -1)} className={`${NAV_CLASS} left-3`}>
            <ChevronLeft size={22} aria-hidden="true" />
          </button>
        )}
        {hasNext && (
          <button ref={nextRef} type="button" aria-label="Next picture" title="Next picture (→)"
            onClick={e => onArrow(e, 1)} className={`${NAV_CLASS} right-3`}>
            <ChevronRight size={22} aria-hidden="true" />
          </button>
        )}
      </div>
      <p id={hintId} className="sr-only">
        Drag or use the arrow keys to move the picture; plus and minus zoom, 0 shows it at actual size.
        {steps ? ' While the picture fits the width of the window, Left and Right show the previous and next picture; N and P, or Page Down and Page Up, always do.' : ''}
      </p>
    </div>
  );
}
