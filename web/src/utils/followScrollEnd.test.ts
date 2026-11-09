import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { followScrollEnd } from './followScrollEnd';

/** A scroll container whose content height the test sets. */
function container(height: number) {
  const el = document.createElement('div');
  let scrollHeight = height;
  Object.defineProperty(el, 'scrollHeight', { get: () => scrollHeight });
  document.body.appendChild(el);
  return { el, grow: (h: number) => { scrollHeight = h; } };
}

let frames: FrameRequestCallback[] = [];
const runFrame = () => {
  const due = frames;
  frames = [];
  due.forEach(cb => cb(0));
};

beforeEach(() => {
  frames = [];
  vi.stubGlobal('requestAnimationFrame', (cb: FrameRequestCallback) => frames.push(cb));
  vi.stubGlobal('cancelAnimationFrame', () => { frames = []; });
});

afterEach(() => {
  vi.unstubAllGlobals();
  document.body.innerHTML = '';
});

describe('followScrollEnd', () => {
  it('follows the end while the content grows, then stops', () => {
    const { el, grow } = container(1000);
    const onDone = vi.fn();
    followScrollEnd(el, onDone, { stableFrames: 2 });

    runFrame();
    expect(el.scrollTop).toBe(1000);
    grow(5000);
    runFrame();
    expect(el.scrollTop).toBe(5000);
    expect(onDone).not.toHaveBeenCalled();

    runFrame();
    runFrame();
    expect(onDone).toHaveBeenCalledTimes(1);
    expect(frames).toHaveLength(0);
  });

  it('stops after maxFrames when the height never settles', () => {
    const { el, grow } = container(100);
    const onDone = vi.fn();
    followScrollEnd(el, onDone, { maxFrames: 3 });
    for (let i = 1; i <= 3; i++) {
      grow(100 * (i + 1));
      runFrame();
    }
    expect(onDone).toHaveBeenCalledTimes(1);
    expect(frames).toHaveLength(0);
  });

  it.each([
    ['the wheel', (el: HTMLElement) => el.dispatchEvent(new Event('wheel'))],
    ['a touch', (el: HTMLElement) => el.dispatchEvent(new Event('touchstart'))],
    ['the scrollbar', (el: HTMLElement) => el.dispatchEvent(new Event('mousedown'))],
    ['a scroll key', () => window.dispatchEvent(new KeyboardEvent('keydown', { key: 'PageUp' }))],
  ])('stops when the user scrolls with %s', (_, scroll) => {
    const { el, grow } = container(1000);
    const onDone = vi.fn();
    followScrollEnd(el, onDone);
    runFrame();

    scroll(el);
    expect(onDone).toHaveBeenCalledTimes(1);
    el.scrollTop = 200;
    grow(3000);
    runFrame();
    expect(el.scrollTop).toBe(200);
  });

  it('keeps following while the user types', () => {
    const { el } = container(1000);
    const onDone = vi.fn();
    followScrollEnd(el, onDone);
    window.dispatchEvent(new KeyboardEvent('keydown', { key: 'a' }));
    expect(onDone).not.toHaveBeenCalled();
  });

  it('the returned function stops it once', () => {
    const { el } = container(1000);
    const onDone = vi.fn();
    const stop = followScrollEnd(el, onDone);
    stop();
    stop();
    el.dispatchEvent(new Event('wheel'));
    expect(onDone).toHaveBeenCalledTimes(1);
    expect(frames).toHaveLength(0);
  });
});
