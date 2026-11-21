import {
  useEffect, useId, useLayoutEffect, useRef, useState,
  type KeyboardEvent as ReactKeyboardEvent, type PointerEvent as ReactPointerEvent, type ReactNode,
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
  /** Step through a set of pictures: the position, and what the buttons and keys do. */
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
  return (
    <Modal open={open} onClose={onClose} size="full" ariaLabel={view.title}>
      {/* A new picture starts from its own view; the dialog, and focus in it, stay. */}
      <LightboxView key={view.src ?? view.markup} onClose={onClose} {...view} />
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
  onClose, title, label, markup = '', src, size, initialView = 'actual', nav, actions,
}: Omit<LightboxProps, 'open'>) {
  const [view, setView] = useState<View>(ONE_TO_ONE);
  const [dragging, setDragging] = useState(false);
  const viewportRef = useRef<HTMLDivElement>(null);
  const hostRef = useRef<HTMLDivElement>(null);
  // Pointers down on the viewport, where each was last seen: one drags, two pinch.
  const pointersRef = useRef(new Map<number, { x: number; y: number }>());
  const hintId = useId();

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
  useEffect(() => {
    const frame = requestAnimationFrame(() => viewportRef.current?.focus({ preventScroll: true }));
    return () => cancelAnimationFrame(frame);
  }, []);

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
      case 'PageUp': case 'p': if (!nav) return; nav.onPrev(); break;
      case 'PageDown': case 'n': if (!nav) return; nav.onNext(); break;
      // Arrows move the view over the picture, as scrolling would.
      case 'ArrowLeft': pan(step, 0); break;
      case 'ArrowRight': pan(-step, 0); break;
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
    pointersRef.current.set(e.pointerId, { x: e.clientX, y: e.clientY });
    setDragging(true);
  };
  const onPointerMove = (e: ReactPointerEvent<HTMLDivElement>) => {
    const pointers = pointersRef.current;
    const last = pointers.get(e.pointerId);
    if (!last) return;
    const now = { x: e.clientX, y: e.clientY };
    pointers.set(e.pointerId, now);
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
  };

  return (
    <div className="rich-lightbox flex flex-col h-full rounded-b-xl overflow-hidden" onKeyDown={onKeyDown}>
      <div className="flex items-center gap-1 pl-4 pr-2 py-1.5 border-b border-border shrink-0">
        <h2 className="mr-auto min-w-0 truncate text-sm font-medium text-text">
          {title}
          {label && <span className="ml-2 font-mono text-xs font-normal text-text-faint">{label}</span>}
        </h2>
        {nav && nav.count > 1 && (
          <div role="group" aria-label="Pictures" className="flex items-center gap-0.5 mr-2">
            <IconButton label="Previous picture" onClick={nav.onPrev}>
              <ChevronLeft size={16} />
            </IconButton>
            <span className="text-xs tabular-nums text-text-muted">{nav.index + 1} / {nav.count}</span>
            <IconButton label="Next picture" onClick={nav.onNext}>
              <ChevronRight size={16} />
            </IconButton>
          </div>
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
      <p id={hintId} className="sr-only">
        Drag or use the arrow keys to move the picture; plus and minus zoom, 0 shows it at actual size.
        {nav && nav.count > 1 ? ' N and P, or Page Down and Page Up, show the next and previous picture.' : ''}
      </p>
    </div>
  );
}
