/** Keys that scroll the page. Other keys (typing in the composer) do not stop the follow. */
const SCROLL_KEYS = new Set(['PageUp', 'PageDown', 'Home', 'End', 'ArrowUp', 'ArrowDown']);

/**
 * Keep a scroll container at its end while the height of its content settles.
 *
 * A row of the message list that was never on screen has an estimated height
 * (`.chat-row` in index.css). After a jump to the end, the rows near the end
 * render for the first time and take their real height, so the end moves.
 * This scrolls to the end on each animation frame. It stops when the content
 * height is the same for `stableFrames` frames, after `maxFrames` frames, or
 * when the user acts on the list: the wheel, a touch, a scroll key, or a mouse
 * press (which includes the scrollbar).
 *
 * `onDone` runs once when it stops, for any reason. The returned function
 * stops it.
 */
export function followScrollEnd(
  el: HTMLElement,
  onDone: () => void,
  { stableFrames = 3, maxFrames = 120 }: { stableFrames?: number; maxFrames?: number } = {},
): () => void {
  const listeners = new AbortController();
  let frames = 0;
  let stable = 0;
  let lastHeight = -1;
  let handle = 0;
  let done = false;

  const stop = () => {
    if (done) return;
    done = true;
    cancelAnimationFrame(handle);
    listeners.abort();
    onDone();
  };

  const options = { signal: listeners.signal, passive: true } as const;
  el.addEventListener('wheel', stop, options);
  el.addEventListener('touchstart', stop, options);
  el.addEventListener('mousedown', stop, options);
  window.addEventListener('keydown', (e) => {
    if (SCROLL_KEYS.has(e.key)) stop();
  }, options);

  const step = () => {
    el.scrollTop = el.scrollHeight;
    stable = el.scrollHeight === lastHeight ? stable + 1 : 0;
    lastHeight = el.scrollHeight;
    frames += 1;
    if (stable >= stableFrames || frames >= maxFrames) stop();
    else handle = requestAnimationFrame(step);
  };
  handle = requestAnimationFrame(step);
  return stop;
}
