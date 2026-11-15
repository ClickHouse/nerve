import type { ResolvedTheme } from '../../../stores/themeStore';

/** Undoes a mount: releases what the renderer holds and empties its element. */
export type Cleanup = () => void;

/** What the frame knows when it asks for a render. */
export interface MountContext {
  theme: ResolvedTheme;
  /**
   * The stage's inner width in CSS pixels, 0 when it is not laid out yet.
   * Container-sized charts take it as their width.
   */
  width: number;
}

/** A drawn block, as the renderer hands it back to the frame. */
export interface Mounted {
  /** Releases what the renderer holds and empties the stage. */
  cleanup: Cleanup;
  /**
   * The picture's natural size in CSS pixels when the renderer knows it.
   * The frame decides from it whether to fit the column or to scroll.
   */
  naturalSize?: { width: number; height: number };
  /**
   * The smallest text in the picture, in CSS pixels at natural size, when
   * the renderer knows it. The frame fits a picture only while that text
   * stays readable (≥ 11 px); without it, 14 px is assumed.
   */
  minTextPx?: number;
  /**
   * The picture as a standalone SVG document — Download SVG, the full-size
   * view. Resolves to null when the renderer has none.
   */
  toSvg?: () => Promise<string | null>;
  /**
   * The picture rasterized at `scale` device pixels per CSS pixel, for
   * renderers that rasterize better themselves (Vega). Without it the frame
   * rasterizes `toSvg()`.
   */
  toPng?: (scale: number) => Promise<Blob | null>;
  /** Lay out again after the stage's width changed (container-sized charts). */
  resize?: () => void | Promise<void>;
}

/**
 * Renders `source` into `stage`, an empty element that belongs to this render
 * alone. Rejects with an Error whose message a person can read ("Parse error
 * on line 3 …") when the source cannot be rendered. Must load nothing from
 * the network: the source is model output, and model output can carry
 * whatever a tool result put in front of the model.
 */
export type Mount = (stage: HTMLElement, source: string, context: MountContext) => Promise<Mounted>;
