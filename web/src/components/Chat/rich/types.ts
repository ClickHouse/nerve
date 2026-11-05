import type { ResolvedTheme } from '../../../stores/themeStore';

/** Undoes a mount: releases what the renderer holds and empties its element. */
export type Cleanup = () => void;

/**
 * Renders `source` into `stage`, an empty element that belongs to this render
 * alone, and resolves to the function that undoes it. Rejects with an Error
 * whose message a person can read ("Parse error on line 3 …") when the
 * source cannot be rendered. Must load nothing from the network: the source
 * is model output, and model output can carry whatever a tool result put in
 * front of the model.
 */
export type Mount = (stage: HTMLElement, source: string, theme: ResolvedTheme) => Promise<Cleanup>;
