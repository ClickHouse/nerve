import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { GIVEN_MS, KeyboardGuard, PRESS_MS, PROOF_WAIT_MS, TAB_MS, WATCH_MS, type Pressed } from './keyboard';

/**
 * The guard of the keyboard, with a frame of jsdom that loads nothing. The
 * test plays the person (pointer and keys in the page), the frame (which the
 * page sees only as `document.activeElement`) and the sandbox page in the
 * frame, which the guard asks whether the person has just pressed in its
 * content (`ask`, answered through `answer`).
 */

let place: HTMLDivElement;
let frame: HTMLIFrameElement;
let field: HTMLTextAreaElement;
let guard: KeyboardGuard;
let taken: ReturnType<typeof vi.fn<() => void>>;
let ask: ReturnType<typeof vi.fn<() => void>>;
let active: { mockRestore: () => void } | null;
/** What the sandbox page answers when asked: at once, or not at all (`silent`). */
let sandboxPage: Pressed | 'silent';

const veil = () => place.querySelector<HTMLDivElement>('[data-frame-veil]')!;
const awake = () => veil().style.pointerEvents === 'none';
const pointer = (target: Element, type = 'pointermove') => target.dispatchEvent(new Event(type, { bubbles: true }));
const key = (name: string) => document.body.dispatchEvent(new KeyboardEvent('keydown', { key: name, bubbles: true }));

/** The frame has the keyboard, as the page sees it. With `heard`, the page also hears that it lost it. */
function frameTakesKeyboard({ heard = true } = {}) {
  active?.mockRestore();
  // What had the keyboard in the page loses it, as in a browser.
  if (document.activeElement instanceof HTMLElement) document.activeElement.blur();
  active = vi.spyOn(document, 'activeElement', 'get').mockReturnValue(frame);
  if (heard) window.dispatchEvent(new Event('blur'));
}

/** The person clicks in the frame: the pointer is on it, the browser counts the press, the keyboard follows. */
function personClicksInFrame() {
  pointer(veil());
  sandboxPage = true;
  frameTakesKeyboard();
}

/** The person puts the cursor into the message field. */
function personFocusesField() {
  active?.mockRestore();
  active = null;
  window.dispatchEvent(new Event('focus'));
  field.focus();
}

/** The person clicks on the page, on nothing that takes the keyboard. */
function personClicksPage() {
  active?.mockRestore();
  active = null;
  window.dispatchEvent(new Event('focus'));
}

function newGuard(): KeyboardGuard {
  return new KeyboardGuard(frame, place, { onTaken: taken, ask });
}

beforeEach(() => {
  vi.useFakeTimers();
  field = document.createElement('textarea');
  place = document.createElement('div');
  frame = document.createElement('iframe');
  place.appendChild(frame);
  document.body.append(field, place);
  taken = vi.fn<() => void>(() => { frame.remove(); });
  sandboxPage = false;
  ask = vi.fn<() => void>(() => {
    const says = sandboxPage;
    // The answer is a message: it comes a moment after the question.
    if (says !== 'silent') setTimeout(() => guard.answer(says), 1);
  });
  active = null;
  field.focus();
  guard = newGuard();
  guard.start();
});

afterEach(() => {
  guard.stop();
  active?.mockRestore();
  vi.useRealTimers();
  vi.restoreAllMocks();
  document.body.innerHTML = '';
});

describe('the veil', () => {
  it('lies over the frame and takes the pointer while the person is elsewhere', () => {
    expect(place.lastElementChild).toBe(veil());
    expect(place.style.position).toBe('relative');
    expect(veil().style.position).toBe('absolute');
    expect(veil().style.inset).toBe('0px');
    expect(veil()).toHaveAttribute('aria-hidden', 'true');
    expect(awake()).toBe(false);
  });

  it('lets everything through once the pointer is on it', () => {
    pointer(veil());
    expect(awake()).toBe(true);
  });

  it('lets everything through once a finger touches it, and that touch does nothing else', () => {
    const press = new Event('pointerdown', { bubbles: true, cancelable: true });
    veil().dispatchEvent(press);
    expect(awake()).toBe(true);
    // The page keeps its keyboard: the press is not the browser's to act on.
    expect(press.defaultPrevented).toBe(true);
    expect(document.activeElement).toBe(field);
  });

  it('is back when the touch that woke it turns into a scroll of the page: that was no press', () => {
    pointer(veil(), 'pointerdown');
    pointer(veil(), 'pointercancel');
    expect(awake()).toBe(false);
    // And the frame may not take the keyboard on the strength of it.
    pointer(veil());
    frameTakesKeyboard();
    vi.advanceTimersByTime(PROOF_WAIT_MS);
    expect(taken).toHaveBeenCalledTimes(1);
  });

  it.each(['pointermove', 'pointerdown'])('is back when the page sees a %s elsewhere', (type) => {
    pointer(veil());
    pointer(field, type);
    expect(awake()).toBe(false);
    // And wakes again when the person comes back.
    pointer(veil());
    expect(awake()).toBe(true);
  });

  it('stays away while the pointer is inside the frame\'s place', () => {
    pointer(veil());
    pointer(place);
    expect(awake()).toBe(true);
  });

  it('is back when the person types in the page', () => {
    pointer(veil());
    key('a');
    expect(awake()).toBe(false);
  });

  it('is back when an element of the page gets the keyboard, whoever gave it', () => {
    const button = document.createElement('button');
    document.body.appendChild(button);
    pointer(veil());
    button.focus();
    expect(awake()).toBe(false);
  });

  it('stays away for a Tab key, which may be on its way into the frame', () => {
    pointer(veil());
    key('Tab');
    expect(awake()).toBe(true);
  });

  it('goes with the guard', () => {
    guard.stop();
    expect(place.querySelector('[data-frame-veil]')).toBeNull();
  });
});

describe('a frame that takes the keyboard', () => {
  it('is reported at once when the person is elsewhere, and the keyboard goes back where it was', () => {
    const focus = vi.spyOn(field, 'focus');
    frameTakesKeyboard();
    expect(taken).toHaveBeenCalledTimes(1);
    expect(focus).toHaveBeenCalledTimes(1);
    // Nobody is asked: no press of the person's can have done this.
    expect(ask).not.toHaveBeenCalled();
  });

  it('is left alone when the person clicked in it: the sandbox page is asked, and says so', () => {
    personClicksInFrame();
    expect(ask).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(PROOF_WAIT_MS * 4);
    expect(taken).not.toHaveBeenCalled();
  });

  it('is left alone when the answer comes just before the wait is over', () => {
    sandboxPage = 'silent';
    pointer(veil());
    frameTakesKeyboard();
    vi.advanceTimersByTime(PROOF_WAIT_MS - 1);
    guard.answer(true);
    vi.advanceTimersByTime(PROOF_WAIT_MS * 4);
    expect(taken).not.toHaveBeenCalled();
  });

  it('is left alone once the press is known, whatever the person does in the next moment', () => {
    personClicksInFrame();
    vi.advanceTimersByTime(1);
    // The pointer is out of the frame again before the wait would have been over.
    pointer(field);
    vi.advanceTimersByTime(PROOF_WAIT_MS * 4);
    expect(taken).not.toHaveBeenCalled();
  });

  it('is reported when the pointer only rests on it and the sandbox page knows of no press', () => {
    const focus = vi.spyOn(field, 'focus');
    sandboxPage = false;
    pointer(veil());
    frameTakesKeyboard();
    expect(taken).not.toHaveBeenCalled();
    // The answer is "no": no need to wait any longer.
    vi.advanceTimersByTime(1);
    expect(taken).toHaveBeenCalledTimes(1);
    expect(focus).toHaveBeenCalledTimes(1);
  });

  it('is reported when the sandbox page does not answer in time', () => {
    sandboxPage = 'silent';
    pointer(veil());
    frameTakesKeyboard();
    vi.advanceTimersByTime(PROOF_WAIT_MS - 1);
    expect(taken).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(taken).toHaveBeenCalledTimes(1);
  });

  it('is asked about again when the press it was let in for was a while ago', () => {
    personClicksInFrame();
    vi.advanceTimersByTime(1);
    // The page has the keyboard for a moment (the pointer stays on the frame), long after the press.
    personClicksPage();
    vi.advanceTimersByTime(PRESS_MS);
    sandboxPage = false;
    frameTakesKeyboard();
    vi.advanceTimersByTime(1);
    expect(ask).toHaveBeenCalledTimes(2);
    expect(taken).toHaveBeenCalledTimes(1);
  });

  it('is not asked about again while the press it was let in for is fresh', () => {
    personClicksInFrame();
    vi.advanceTimersByTime(1);
    personClicksPage();
    vi.advanceTimersByTime(PRESS_MS - 2);
    frameTakesKeyboard();
    vi.advanceTimersByTime(PROOF_WAIT_MS * 4);
    expect(ask).toHaveBeenCalledTimes(1);
    expect(taken).not.toHaveBeenCalled();
  });

  it('is left alone when the person pressed on the veil: the page saw that press itself', () => {
    pointer(veil(), 'pointerdown');
    frameTakesKeyboard();
    vi.advanceTimersByTime(PROOF_WAIT_MS * 4);
    expect(ask).not.toHaveBeenCalled();
    expect(taken).not.toHaveBeenCalled();
  });

  it('is reported when a press counted, the person went elsewhere, and only the pointer came back', () => {
    pointer(veil(), 'pointerdown');
    pointer(field);
    pointer(veil());
    // The press on the veil counts no longer; the sandbox page knows of none in the content.
    sandboxPage = false;
    frameTakesKeyboard();
    vi.advanceTimersByTime(1);
    expect(ask).toHaveBeenCalledTimes(1);
    expect(taken).toHaveBeenCalledTimes(1);
  });

  it('is left alone under a pointer that came back when the browser still counts a press in it (it counts one for five seconds)', () => {
    personClicksInFrame();
    vi.advanceTimersByTime(1);
    pointer(field, 'pointerdown');
    personFocusesField();
    // Only the pointer comes back, within the time the browser counts the press of before.
    pointer(veil());
    sandboxPage = true;
    frameTakesKeyboard();
    vi.advanceTimersByTime(PROOF_WAIT_MS * 4);
    expect(taken).not.toHaveBeenCalled();
  });

  it('is reported at once when the pointer is elsewhere, whatever the sandbox page would say', () => {
    sandboxPage = true;
    frameTakesKeyboard();
    expect(taken).toHaveBeenCalledTimes(1);
    expect(ask).not.toHaveBeenCalled();
  });

  it('is left alone when a Tab key brought the keyboard there', () => {
    key('Tab');
    vi.advanceTimersByTime(TAB_MS - 1);
    frameTakesKeyboard();
    vi.advanceTimersByTime(PROOF_WAIT_MS * 4);
    expect(taken).not.toHaveBeenCalled();
    expect(ask).not.toHaveBeenCalled();
  });

  it('is reported when the Tab key was a while ago', () => {
    key('Tab');
    vi.advanceTimersByTime(TAB_MS);
    frameTakesKeyboard();
    expect(taken).toHaveBeenCalledTimes(1);
  });

  it('is left alone when the page\'s own script hands it the keyboard: a dialog that closes gives it back to what had it', () => {
    // The person was in the frame, a dialog of the chat took the keyboard, the person answered it elsewhere.
    personClicksInFrame();
    vi.advanceTimersByTime(1);
    const button = document.createElement('button');
    document.body.appendChild(button);
    active?.mockRestore();
    active = null;
    button.focus();
    pointer(button, 'pointerdown');
    expect(awake()).toBe(false);

    // The dialog closes: the page puts the keyboard back.
    const own = vi.spyOn(HTMLIFrameElement.prototype, 'focus').mockImplementation(() => {});
    frame.focus();
    expect(own).toHaveBeenCalledTimes(1);
    frameTakesKeyboard();
    vi.advanceTimersByTime(PROOF_WAIT_MS * 4);

    expect(taken).not.toHaveBeenCalled();
    expect(ask).toHaveBeenCalledTimes(1);
  });

  it('is reported when the page\'s own hand was a while ago', () => {
    vi.spyOn(HTMLIFrameElement.prototype, 'focus').mockImplementation(() => {});
    frame.focus();
    vi.advanceTimersByTime(GIVEN_MS);
    frameTakesKeyboard();
    expect(taken).toHaveBeenCalledTimes(1);
  });

  it('is reported when the person was at the frame, went back to typing a message, and the frame took the keyboard then', () => {
    personClicksInFrame();
    vi.advanceTimersByTime(1);
    // Back to the message field: a click there, then keys.
    pointer(field, 'pointerdown');
    personFocusesField();
    key('h');
    // The browser still counts the press of a moment ago: it counts for nothing with the pointer elsewhere.
    sandboxPage = true;
    frameTakesKeyboard();
    expect(taken).toHaveBeenCalledTimes(1);
  });

  it('is reported when the person clicked on the page, on no field, and the frame took the keyboard back at once', () => {
    personClicksInFrame();
    vi.advanceTimersByTime(1);
    pointer(document.body, 'pointerdown');
    personClicksPage();
    frameTakesKeyboard();
    expect(taken).toHaveBeenCalledTimes(1);
  });

  it('is reported when the pointer still rests on the frame but the person types in the page', () => {
    pointer(veil());
    key('h');
    key('i');
    sandboxPage = true;
    frameTakesKeyboard();
    expect(taken).toHaveBeenCalledTimes(1);
  });

  it('is reported within a moment when the page hears nothing of it', () => {
    frameTakesKeyboard({ heard: false });
    vi.advanceTimersByTime(WATCH_MS - 1);
    expect(taken).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(taken).toHaveBeenCalledTimes(1);
  });

  it('is not reported again while it keeps the keyboard it was given', () => {
    personClicksInFrame();
    vi.advanceTimersByTime(1);
    // The pointer leaves; the person types in the frame (the page sees no key), for longer than a press holds.
    pointer(field);
    sandboxPage = false;
    vi.advanceTimersByTime(PRESS_MS * 2);
    window.dispatchEvent(new Event('blur'));
    expect(taken).not.toHaveBeenCalled();
    expect(ask).toHaveBeenCalledTimes(1);
  });

  it('is reported once', () => {
    frameTakesKeyboard();
    window.dispatchEvent(new Event('blur'));
    vi.advanceTimersByTime(WATCH_MS * 4);
    expect(taken).toHaveBeenCalledTimes(1);
  });

  it('is asked about once and reported once when it was in doubt twice', () => {
    sandboxPage = 'silent';
    pointer(veil());
    frameTakesKeyboard();
    // The page has the keyboard for a moment, and the frame takes it again.
    personClicksPage();
    pointer(veil());
    frameTakesKeyboard();
    vi.advanceTimersByTime(PROOF_WAIT_MS * 4);
    expect(ask).toHaveBeenCalledTimes(1);
    expect(taken).toHaveBeenCalledTimes(1);
  });

  it('leaves the keyboard alone when what had it before is gone', () => {
    field.remove();
    frameTakesKeyboard();
    expect(taken).toHaveBeenCalledTimes(1);
  });
});

describe('what the sandbox page says', () => {
  it('counts for nothing when nobody asked: a "no" stops no frame, a "yes" lets none in', () => {
    guard.answer(false);
    expect(taken).not.toHaveBeenCalled();

    // A "yes" out of turn, then the frame takes the keyboard under a resting pointer.
    pointer(veil());
    guard.answer(true);
    sandboxPage = false;
    frameTakesKeyboard();
    vi.advanceTimersByTime(1);
    expect(ask).toHaveBeenCalledTimes(1);
    expect(taken).toHaveBeenCalledTimes(1);
  });

  it('is heard once for each question', () => {
    sandboxPage = 'silent';
    pointer(veil());
    frameTakesKeyboard();
    guard.answer(true);
    // A second answer, to no question.
    guard.answer(false);
    vi.advanceTimersByTime(PROOF_WAIT_MS * 4);
    expect(taken).not.toHaveBeenCalled();
  });

  it('lets a frame under the pointer keep the keyboard where the browser cannot tell of a press: the page cannot know better', () => {
    sandboxPage = 'unknown';
    pointer(veil());
    frameTakesKeyboard();
    vi.advanceTimersByTime(PROOF_WAIT_MS * 4);
    expect(taken).not.toHaveBeenCalled();
  });

  it('is not asked for in such a browser either when the pointer is not on the frame', () => {
    sandboxPage = 'unknown';
    frameTakesKeyboard();
    expect(taken).toHaveBeenCalledTimes(1);
    expect(ask).not.toHaveBeenCalled();
  });
});

describe('a guard that was stopped', () => {
  it('reports nothing and listens to nothing', () => {
    const removed = vi.spyOn(document, 'removeEventListener');
    pointer(veil());
    guard.stop();
    frameTakesKeyboard();
    vi.advanceTimersByTime(WATCH_MS * 4);
    expect(taken).not.toHaveBeenCalled();
    expect(ask).not.toHaveBeenCalled();
    expect(removed.mock.calls.map(([type]) => type).sort()).toEqual(['focusin', 'keydown', 'pointerdown', 'pointermove']);
    // The page's own state is as it was.
    personFocusesField();
    expect(document.activeElement).toBe(field);
  });

  it('reports nothing of a frame it was in doubt about, whatever is answered then', () => {
    sandboxPage = 'silent';
    pointer(veil());
    frameTakesKeyboard();
    guard.stop();
    guard.answer(false);
    vi.advanceTimersByTime(PROOF_WAIT_MS * 4);
    expect(taken).not.toHaveBeenCalled();
  });

  it('gives the frame element its own focus() back', () => {
    expect(Object.hasOwn(frame, 'focus')).toBe(true);
    guard.stop();
    expect(Object.hasOwn(frame, 'focus')).toBe(false);
    expect(frame.focus).toBe(HTMLIFrameElement.prototype.focus);
  });

  it('can be stopped twice', () => {
    guard.stop();
    guard.stop();
    expect(place.querySelector('[data-frame-veil]')).toBeNull();
  });
});
