import { afterEach, describe, expect, it } from 'vitest';
import { SLIDE_HEIGHT, SLIDE_MARGIN, SLIDE_WIDTH, startDeck, type Deck } from './deck';
import css from './deck.css?raw';
import { testWindow } from './testWindow';

const base = { channel: 'nerve-artifact', v: 1 };
const started: Deck[] = [];

function setup(body = '<section class="slide"><h1>One</h1></section><section class="slide"><h1>Two</h1><p>Team <span data-slide-number></span></p></section><section class="slide"><h1>Three</h1></section>') {
  document.body.innerHTML = body;
  const t = testWindow({ width: SLIDE_WIDTH / 2 + 2 * SLIDE_MARGIN, height: 2000 });
  const deck = startDeck(document, t.win);
  started.push(deck);
  const slides = Array.from(document.querySelectorAll<HTMLElement>('section.slide'));
  const visible = () => slides.findIndex(s => s.classList.contains('nerve-current')) + 1;
  return { t, deck, slides, visible };
}

function key(name: string, init: KeyboardEventInit = {}) {
  document.dispatchEvent(new KeyboardEvent('keydown', { key: name, bubbles: true, cancelable: true, ...init }));
}

afterEach(() => {
  started.splice(0).forEach(deck => deck.close());
  document.body.innerHTML = '';
  document.documentElement.className = '';
});

describe('the deck helper', () => {
  it('shows slide 1 of 3 and says ready with the slides, from 1', () => {
    const { t, visible, slides } = setup();
    expect(visible()).toBe(1);
    expect(document.documentElement).toHaveClass('nerve-deck');
    expect(slides[1]).toHaveAttribute('aria-hidden', 'true');
    expect(slides[0]).not.toHaveAttribute('aria-hidden');
    expect(slides[0]).toHaveAttribute('aria-roledescription', 'slide');
    expect(t.of('ready')).toEqual([{ ...base, type: 'ready', kind: 'deck', canPrint: true, slides: 3, slide: 1 }]);
  });

  it('puts the slide number into each slide', () => {
    const { slides } = setup();
    expect(slides[0].querySelector('.nerve-slide-number')!.textContent).toBe('1 / 3');
    expect(slides[1].querySelector('[data-slide-number]')!.textContent).toBe('2 / 3');
    expect(slides[1].querySelector('.nerve-slide-number')).toBeNull();
    expect(slides[2]).toHaveAttribute('aria-label', '3 / 3');
  });

  it('moves with the keys and says each change', () => {
    const { t, visible } = setup();
    key('ArrowRight');
    expect(visible()).toBe(2);
    key(' ');
    key('PageDown');
    expect(visible()).toBe(3);
    key('ArrowLeft');
    expect(visible()).toBe(2);
    key('End');
    key('Home');
    expect(visible()).toBe(1);
    expect(t.of('slide')).toEqual([
      { ...base, type: 'slide', slide: 2, slides: 3 },
      { ...base, type: 'slide', slide: 3, slides: 3 },
      { ...base, type: 'slide', slide: 2, slides: 3 },
      { ...base, type: 'slide', slide: 3, slides: 3 },
      { ...base, type: 'slide', slide: 1, slides: 3 },
    ]);
  });

  it('leaves keys with a modifier and keys in a text field to the page', () => {
    const { visible } = setup('<section class="slide"><input></section><section class="slide"></section>');
    key('ArrowRight', { ctrlKey: true });
    expect(visible()).toBe(1);
    document.querySelector('input')!.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowRight', bubbles: true }));
    expect(visible()).toBe(1);
  });

  it('goes to the slide that the host names, and ignores a slide out of range', () => {
    const { t, visible } = setup();
    t.say({ ...base, type: 'slide', to: 3 });
    expect(visible()).toBe(3);
    t.say({ ...base, type: 'slide', to: 'first' });
    expect(visible()).toBe(1);
    t.say({ ...base, type: 'slide', to: 4 });
    t.say({ ...base, type: 'slide', to: 'next' }, {});
    expect(visible()).toBe(1);
    t.say({ ...base, type: 'slide', to: 'last' });
    expect(visible()).toBe(3);
  });

  it('answers hello with the current slide', () => {
    const { t } = setup();
    t.say({ ...base, type: 'slide', to: 2 });
    t.say({ ...base, type: 'hello' });
    expect(t.of('ready').at(-1)).toEqual({ ...base, type: 'ready', kind: 'deck', canPrint: true, slides: 3, slide: 2 });
  });

  it('scales the slide to the window, with no margin while it presents', () => {
    const { t, slides, deck } = setup();
    expect(slides[0].style.transform).toBe('translate(-50%, -50%) scale(0.5)');
    t.say({ ...base, type: 'present', on: true });
    expect(deck.presenting).toBe(true);
    expect(document.documentElement).toHaveClass('nerve-present');
    const scale = (SLIDE_WIDTH / 2 + 2 * SLIDE_MARGIN) / SLIDE_WIDTH;
    expect(slides[0].style.transform).toBe(`translate(-50%, -50%) scale(${scale})`);
    t.say({ ...base, type: 'present', on: false });
    expect(document.documentElement).not.toHaveClass('nerve-present');
    expect(SLIDE_HEIGHT).toBe(720);
  });

  it('prints on the message of the host', () => {
    const { t } = setup();
    t.say({ ...base, type: 'print' });
    expect(t.print).toHaveBeenCalledTimes(1);
  });

  it('says no slide numbers to the host for more slides than a host takes', () => {
    const { t, deck } = setup('<section class="slide"></section>'.repeat(10_001));
    expect(t.of('ready')).toEqual([{ ...base, type: 'ready', kind: 'deck', canPrint: true }]);
    deck.go('next');
    expect(deck.slide).toBe(2);
    expect(t.of('slide')).toEqual([]);
  });

  it('says ready with no slides for a page without slides', () => {
    const { t, deck } = setup('<p>No slides</p>');
    expect(deck.slide).toBe(0);
    expect(t.of('ready')).toEqual([{ ...base, type: 'ready', kind: 'deck', canPrint: true }]);
    deck.go('next');
    expect(t.of('slide')).toEqual([]);
  });
});

describe('the print layout of deck.css', () => {
  it('has one slide of the slide size on each page, every slide shown', () => {
    expect(css).toContain(`@page { size: ${SLIDE_WIDTH}px ${SLIDE_HEIGHT}px; margin: 0; }`);
    const print = css.slice(css.indexOf('@media print'));
    expect(print).toMatch(/section\.slide:not\(\.nerve-current\)\s*\{[^}]*display: block !important;[^}]*transform: none !important;[^}]*break-after: page;/);
    expect(css).toMatch(new RegExp(`width: ${SLIDE_WIDTH}px;\\s*height: ${SLIDE_HEIGHT}px;`));
  });
});
