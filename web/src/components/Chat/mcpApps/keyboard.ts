/**
 * Who has the keyboard: a frame gets it when the person goes to it, and not
 * by asking for it.
 *
 * A script in a frame can give its own frame the keyboard at any time, with
 * no click (`window.focus()` and a field's `focus()`), whatever the frame's
 * sandbox says; no policy of the browser forbids it. What the person types
 * next then goes to the frame: the view of an MCP App or a widget could take
 * the keyboard while the person writes a message, and read the message. The
 * page cannot give the keyboard back for good either (the frame takes it
 * again a moment later). It can see the theft, and remove the frame at once:
 * then the frame has read nothing, or the one key that was on its way.
 *
 * To tell a theft from the person's own click, the page must know where the
 * person is. It sees nothing of a pointer that is inside a frame, so the
 * frame sleeps under a veil, an empty element of the page laid over it:
 *
 * - The pointer moves onto the veil, or a finger touches it: the person is
 *   there. The veil lets everything through from then on (the frame is
 *   awake).
 * - The pointer moves or presses anywhere else in the page, a key goes to
 *   the page, or an element of the page gets the keyboard: the person is
 *   somewhere else. The veil is back.
 *
 * The page's own script may hand the keyboard to a frame too: a dialog of
 * the chat that closes gives the keyboard back to what had it before, with
 * `focus()` on the frame element. That is no taking either. The content
 * cannot call it: it has no hold on the frame element.
 *
 * The keyboard going to a frame, without a Tab key or the page's own hand
 * just before, is then one of two things:
 *
 * - The frame sleeps. No click of the person's can have done this: `onTaken`
 *   is called at once, the frame's owner removes it, and the keyboard goes
 *   back where it was.
 * - The frame is awake. The person clicked in it, or the frame took the
 *   keyboard under a pointer that only rests on it. The sandbox page in the
 *   frame says which: the guard asks it (`ask`), and it answers with what the
 *   browser counts as a press of the person's in its content (`answer`). The
 *   browser counts a press in a frame for the frames around it and for no
 *   other, so the sandbox page, which is Nerve's own, knows of a press in
 *   the content inside it, and the content cannot make one up. With a press,
 *   the keyboard is the frame's. Without one, or without an answer by
 *   PROOF_WAIT_MS, `onTaken`.
 *
 * A press on the veil itself is the person's press on the frame, seen by the
 * page: it wakes the frame and does nothing else, and the keyboard may follow
 * it into the frame (the browser gives it to a frame that a finger tapped).
 * A touch that turns into a scroll of the page was no press: the veil is
 * back.
 *
 * What this costs: a click on a frame under a pointer that has not moved
 * since the person typed, and the first tap on a frame on a touch screen,
 * wake the frame and do nothing else.
 */

/** Why a frame that took the keyboard was stopped, in words for the person. */
export const TAKEN_REASON = 'it took the keyboard without a click in it.';

/** The keyboard that goes into the frame this long after a Tab key was the person's own. */
export const TAB_MS = 500;
/** The keyboard that goes into the frame this long after the page's own script handed it over was the page's doing. */
export const GIVEN_MS = 500;
/** How often the guard looks who has the keyboard, for the cases the page hears nothing of. */
export const WATCH_MS = 250;
/** A press in the frame holds this long: as long as the browser itself counts a press as just done. */
export const PRESS_MS = 5_000;
/** How long the sandbox page's answer may take after the keyboard went to the frame. */
export const PROOF_WAIT_MS = 150;

/**
 * What the sandbox page says of a press: there was one, there was none, or
 * its browser cannot tell (`unknown`: the frame then keeps the keyboard it
 * took while awake, since the page cannot know better).
 */
export type Pressed = boolean | 'unknown';

export interface KeyboardGuardOptions {
  /** The frame took the keyboard, and not by the person's press. Must remove the frame from the page. */
  onTaken: () => void;
  /** Ask the sandbox page in the frame whether the person has just pressed in its content. Its answer goes to `answer`. */
  ask: () => void;
}

export class KeyboardGuard {
  private readonly frame: HTMLIFrameElement;
  private readonly place: HTMLElement;
  private readonly options: KeyboardGuardOptions;
  private readonly veil: HTMLDivElement;
  private awake = false;
  private tabAt = 0;
  /** When the page's own script last called `focus()` on the frame element. */
  private givenAt = -Infinity;
  /** The frame had the keyboard at the last look. */
  private held = false;
  /** What had the keyboard before the frame: an element of the page. */
  private before: Element | null = null;
  /** When the person last pressed on the frame, as far as the page knows: the sandbox page's word, or a press on the veil. */
  private pressedAt = -Infinity;
  private watch: ReturnType<typeof setInterval> | undefined;
  /** Runs while the guard waits for the sandbox page's answer. */
  private doubt: ReturnType<typeof setTimeout> | undefined;
  private stopped = false;

  /** `frame` is in `place`, which holds nothing else. */
  constructor(frame: HTMLIFrameElement, place: HTMLElement, options: KeyboardGuardOptions) {
    this.frame = frame;
    this.place = place;
    this.options = options;
    this.veil = document.createElement('div');
    this.veil.setAttribute('data-frame-veil', '');
    this.veil.setAttribute('aria-hidden', 'true');
    this.veil.style.cssText = 'position:absolute;inset:0;';
  }

  start(): void {
    // The veil lies over the frame, and over nothing else.
    this.place.style.position = 'relative';
    this.place.appendChild(this.veil);
    this.veil.addEventListener('pointermove', this.wake);
    this.veil.addEventListener('pointerdown', this.onVeilPress);
    this.veil.addEventListener('pointercancel', this.rest);
    document.addEventListener('keydown', this.onKey, true);
    document.addEventListener('focusin', this.onFocusIn, true);
    window.addEventListener('focus', this.onWindowFocus);
    window.addEventListener('blur', this.look);
    // `focus()` on the frame element is the page's own script speaking (the
    // content has no hold on the element): note it, then do as asked.
    this.frame.focus = (options?: FocusOptions): void => {
      this.givenAt = Date.now();
      HTMLIFrameElement.prototype.focus.call(this.frame, options);
    };
    this.before = document.activeElement === this.frame ? null : document.activeElement;
    this.watch = setInterval(this.look, WATCH_MS);
  }

  stop(): void {
    if (this.stopped) return;
    this.stopped = true;
    this.rest();
    this.veil.removeEventListener('pointermove', this.wake);
    this.veil.removeEventListener('pointerdown', this.onVeilPress);
    this.veil.removeEventListener('pointercancel', this.rest);
    document.removeEventListener('keydown', this.onKey, true);
    document.removeEventListener('focusin', this.onFocusIn, true);
    window.removeEventListener('focus', this.onWindowFocus);
    window.removeEventListener('blur', this.look);
    // The element's own `focus` again.
    delete (this.frame as { focus?: unknown }).focus;
    clearInterval(this.watch);
    clearTimeout(this.doubt);
    this.doubt = undefined;
    this.veil.remove();
  }

  /**
   * The sandbox page's answer to `ask`. The caller has checked that it is
   * the page in this frame that speaks. An answer nobody asked for says
   * nothing.
   */
  answer(pressed: Pressed): void {
    if (this.stopped || this.doubt === undefined) return;
    clearTimeout(this.doubt);
    this.doubt = undefined;
    if (pressed === false) {
      this.taken();
      return;
    }
    if (pressed === true) this.pressedAt = Date.now();
  }

  /** The person is at the frame: everything goes through to it. */
  private readonly wake = (): void => {
    if (this.awake) return;
    this.awake = true;
    this.veil.style.pointerEvents = 'none';
    // Of a pointer inside the frame the page hears nothing; it hears when the pointer is elsewhere again.
    document.addEventListener('pointermove', this.onPointerElsewhere, true);
    document.addEventListener('pointerdown', this.onPointerElsewhere, true);
  };

  /** The person pressed on the frame while it slept: it wakes, and the press does nothing else. */
  private readonly onVeilPress = (event: Event): void => {
    // Nothing else: the page keeps its keyboard and starts no selection.
    event.preventDefault();
    // A press the page saw itself.
    this.pressedAt = Date.now();
    this.wake();
  };

  /** The person is somewhere else: the veil is back. */
  private readonly rest = (): void => {
    if (!this.awake) return;
    this.awake = false;
    // A press on the frame counts while the person stays at it.
    this.pressedAt = -Infinity;
    this.veil.style.pointerEvents = '';
    document.removeEventListener('pointermove', this.onPointerElsewhere, true);
    document.removeEventListener('pointerdown', this.onPointerElsewhere, true);
  };

  private readonly onPointerElsewhere = (event: Event): void => {
    if (!(event.target instanceof Node) || !this.place.contains(event.target)) this.rest();
  };

  private readonly onKey = (event: KeyboardEvent): void => {
    // A key reaches the page only while the page has the keyboard. Tab moves
    // it on, maybe into the frame; any other key says the person is typing here.
    if (event.key === 'Tab') this.tabAt = Date.now();
    else this.rest();
  };

  private readonly onFocusIn = (event: FocusEvent): void => {
    if (!(event.target instanceof Element) || event.target === this.frame) return;
    // The keyboard is with an element of the page: not with the frame, and
    // whoever put it there (the person, or the page for the person) is not
    // at the frame.
    this.before = event.target;
    this.held = false;
    this.rest();
  };

  /** The page has the keyboard again (a click on the page itself): the frame has it no longer. */
  private readonly onWindowFocus = (): void => {
    this.held = false;
  };

  /** Look who has the keyboard. Called when the page loses it, and every WATCH_MS. */
  private readonly look = (): void => {
    const has = document.activeElement === this.frame;
    const entered = has && !this.held;
    this.held = has;
    if (!entered || Date.now() - this.tabAt < TAB_MS || Date.now() - this.givenAt < GIVEN_MS) return;
    if (!this.awake) {
      this.taken();
      return;
    }
    if (Date.now() - this.pressedAt < PRESS_MS) return;
    // Awake, and no press the page knows of: the sandbox page knows.
    if (this.doubt !== undefined) return;
    this.doubt = setTimeout(() => {
      this.doubt = undefined;
      this.taken();
    }, PROOF_WAIT_MS);
    this.options.ask();
  };

  private taken(): void {
    const back = this.before;
    // Said once: the guard's work is done.
    this.stop();
    this.options.onTaken();
    // With the frame gone the keyboard is the page's again: back to where it was.
    if (back instanceof HTMLElement && back.isConnected) back.focus();
  }
}
