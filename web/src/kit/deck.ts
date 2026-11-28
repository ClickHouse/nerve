import { MAX_SLIDES, type HostMessage } from '../components/Preview/messages';
import { connectPage, postToHost } from './channel';

/**
 * The deck helper (`nerve-kit/deck.js` with `nerve-kit/deck.css`).
 *
 * The contract for a deck page:
 * - each `<section class="slide">` of the page is one slide, in the order
 *   of the document; a slide is 1280 by 720 CSS pixels;
 * - the page names `nerve-kit/deck.css` (a stylesheet link) and
 *   `nerve-kit/deck.js` (a script, after the slides or with `defer`);
 * - an element with the attribute `data-slide-number` in a slide gets the
 *   slide number ("3 / 12"); a slide without one gets a
 *   `.nerve-slide-number` element at its bottom right.
 *
 * The helper shows one slide at a time, scaled to fit the window, and
 * moves with the keys. `present` shows the slide with no margin. The print
 * layout (in `deck.css`) has one slide on each page. The helper talks to
 * the host with the messages of `components/Preview/messages.ts`; slides
 * count from 1.
 */

/** The size of a slide, in CSS pixels. `deck.css` and its print page use the same. */
export const SLIDE_WIDTH = 1280;
export const SLIDE_HEIGHT = 720;
/** The space around a slide, in CSS pixels, out of the present layout. */
export const SLIDE_MARGIN = 24;

export const SLIDE_SELECTOR = 'section.slide';
const CURRENT = 'nerve-current';
const DECK = 'nerve-deck';
const PRESENT = 'nerve-present';

const KEY_STEPS: Readonly<Record<string, 'next' | 'prev' | 'first' | 'last'>> = {
  ArrowRight: 'next',
  ArrowDown: 'next',
  PageDown: 'next',
  ' ': 'next',
  ArrowLeft: 'prev',
  ArrowUp: 'prev',
  PageUp: 'prev',
  Home: 'first',
  End: 'last',
};

export interface Deck {
  /** The current slide, from 1; 0 for a deck without slides. */
  readonly slide: number;
  readonly slides: number;
  readonly presenting: boolean;
  go(to: 'next' | 'prev' | 'first' | 'last' | number): void;
  present(on: boolean): void;
  close(): void;
}

function typing(target: EventTarget | null): boolean {
  if (!(target instanceof Element)) return false;
  return target.closest('input, textarea, select, [contenteditable]:not([contenteditable="false"])') !== null;
}

/** Start the helper on the slides of `doc`. */
export function startDeck(doc: Document = document, win: Window = window): Deck {
  const slides = Array.from(doc.querySelectorAll<HTMLElement>(SLIDE_SELECTOR));
  const total = slides.length;
  const root = doc.documentElement;
  root.classList.add(DECK);
  let current = total > 0 ? 1 : 0;
  // A host takes at most MAX_SLIDES: a longer deck works with its keys and
  // says no slide numbers to the host.
  const counted = total > 0 && total <= MAX_SLIDES;
  let presenting = false;

  slides.forEach((slide, i) => {
    const text = `${i + 1} / ${total}`;
    slide.setAttribute('aria-roledescription', 'slide');
    if (!slide.hasAttribute('aria-label')) slide.setAttribute('aria-label', text);
    const places = slide.querySelectorAll('[data-slide-number]');
    if (places.length > 0) {
      places.forEach(place => { place.textContent = text; });
    } else {
      const number = doc.createElement('div');
      number.className = 'nerve-slide-number';
      number.setAttribute('aria-hidden', 'true');
      number.textContent = text;
      slide.append(number);
    }
  });

  function fit(): void {
    const margin = presenting ? 0 : SLIDE_MARGIN;
    const width = Math.max(win.innerWidth - 2 * margin, 1);
    const height = Math.max(win.innerHeight - 2 * margin, 1);
    const scale = Math.min(width / SLIDE_WIDTH, height / SLIDE_HEIGHT);
    for (const slide of slides) slide.style.transform = `translate(-50%, -50%) scale(${scale})`;
  }

  function show(): void {
    slides.forEach((slide, i) => {
      const on = i + 1 === current;
      slide.classList.toggle(CURRENT, on);
      if (on) slide.removeAttribute('aria-hidden');
      else slide.setAttribute('aria-hidden', 'true');
    });
  }

  function go(to: 'next' | 'prev' | 'first' | 'last' | number): void {
    if (total === 0) return;
    let next = current;
    if (to === 'next') next = Math.min(current + 1, total);
    else if (to === 'prev') next = Math.max(current - 1, 1);
    else if (to === 'first') next = 1;
    else if (to === 'last') next = total;
    else if (Number.isInteger(to) && to >= 1 && to <= total) next = to;
    if (next === current) return;
    current = next;
    show();
    if (counted) postToHost({ type: 'slide', slide: current, slides: total }, win);
  }

  function present(on: boolean): void {
    presenting = on;
    root.classList.toggle(PRESENT, on);
    fit();
  }

  const onKey = (event: KeyboardEvent) => {
    if (event.defaultPrevented || event.altKey || event.ctrlKey || event.metaKey || typing(event.target)) return;
    const step = KEY_STEPS[event.key];
    if (!step) return;
    event.preventDefault();
    go(step);
  };
  const onResize = () => fit();

  show();
  fit();
  doc.addEventListener('keydown', onKey);
  win.addEventListener('resize', onResize);
  const connection = connectPage(
    () => (counted
      ? { type: 'ready', kind: 'deck', canPrint: true, slides: total, slide: current }
      : { type: 'ready', kind: 'deck', canPrint: true }),
    (message: HostMessage) => {
      if (message.type === 'slide') go(message.to);
      else if (message.type === 'present') present(message.on);
    },
    win,
  );

  return {
    get slide() { return current; },
    get slides() { return total; },
    get presenting() { return presenting; },
    go,
    present,
    close: () => {
      connection.close();
      doc.removeEventListener('keydown', onKey);
      win.removeEventListener('resize', onResize);
    },
  };
}
