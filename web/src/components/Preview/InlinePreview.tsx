import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from 'react';
import { api } from '../../api/client';
import { usePreviewStore } from '../../stores/previewStore';
import type { PreviewBundle } from '../../types/preview';
import { claimWidgetSlot, type WidgetSlot } from '../Chat/widgets/budget';
import { useNear } from '../Chat/widgets/near';
import { Loader2 } from '../ui/icons';
import { buildPreviewDocument } from './document';
import { downloadSource } from './download';
import { PreviewHost } from './host';
import { fileEventNamesBundle } from './live';
import type { ContentMessage } from './messages';
import { canPreviewInline, openPreview } from './open';
import { isStale, MAX_RESTARTS, problemText } from './problems';

/**
 * The preview of a file inside the chat, under the card of a file that the
 * agent sent (`send_file`). It is the Preview tab's bundle in the Preview
 * tab's sandbox frame (`document.ts`, `host.ts`), at a fixed size.
 *
 * - An image has none: the card shows the image itself.
 * - A deck (a page that says `ready` with the kind `deck`) shows its first
 *   slide, 16 by 9, and takes no input. A click opens the Preview tab, which
 *   has the slide controls and Present.
 * - Every other page (an HTML page, a folder, the table page of a CSV file,
 *   the page of a Markdown file) is INLINE_HEIGHT high and works in place.
 *   The download button of a table or Markdown page saves its source file,
 *   as in the tab (`downloadSource`); a failed download is one line under
 *   the frame, and the page stays.
 * - The frame takes one of the places of the widgets
 *   (`Chat/widgets/budget.ts`), so the chat runs at most MAX_LIVE_WIDGETS
 *   frames in all. The preview is built when the card comes near the screen,
 *   and built again when a file event names a file of its bundle. It does
 *   not poll: the Preview tab does.
 * - When the bundle cannot be shown (for example, a file outside the
 *   workspace, or a file that is gone), one line under the card says why.
 */

/** The height of an inline preview that is not a deck, in CSS pixels. */
const INLINE_HEIGHT = 360;

const CHANGING = 'The files change while the preview is read.';

const TEXT_BUTTON = 'text-xs text-text-dim hover:text-text px-1.5 py-0.5 rounded hover:bg-surface-hover cursor-pointer';

interface FrameState {
  /** The frame holds a place of the budget. */
  live: boolean;
  /** Near the screen, but every place is held by a frame that is near it too. */
  waiting: boolean;
  /** The built document; none without a place. */
  doc: { html: string; id: number } | null;
}

/**
 * The place of one inline preview in the budget of the chat's frames
 * (`Chat/widgets/budget.ts`), and the document that the place shows. When
 * another frame takes the place, the document goes too.
 */
class FramePlace {
  private slot: WidgetSlot | null = null;
  /** Stops the reads of the builds of a place that the card lost. */
  private reads: AbortController | null = null;
  /** Counts the places that the card took: a build belongs to the place it started with. */
  private taken = 0;
  private near = false;
  /** The last build failed: no place until the person asks again (Try again). */
  private failed = false;
  private state: FrameState = { live: false, waiting: false, doc: null };
  private readonly listeners = new Set<() => void>();

  readonly subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  };

  readonly get = (): FrameState => this.state;

  /** The place the card holds now (0 when none), for `owns`. */
  get generation(): number {
    return this.slot ? this.taken : 0;
  }

  /** Stops when the card loses the place of `generation`. */
  get signal(): AbortSignal | undefined {
    return this.reads?.signal;
  }

  /** The card still holds the place of `generation`: a build of an earlier place changes nothing. */
  owns(generation: number): boolean {
    return this.slot !== null && this.taken === generation;
  }

  /**
   * The card came near the screen or left it. `asked`: the person clicked
   * (Load preview, Try again), so a place is always found. A card whose
   * build failed takes a place only when asked.
   */
  sync(near: boolean, asked = false): void {
    this.near = near;
    if (this.slot) {
      this.slot.seen(near);
      return;
    }
    if (asked) this.failed = false;
    if ((!near && !asked) || this.failed) return;
    const slot = claimWidgetSlot(this.evicted, asked);
    if (!slot) {
      this.set({ live: false, waiting: true, doc: null });
      return;
    }
    this.slot = slot;
    this.taken += 1;
    this.reads = new AbortController();
    this.set({ live: true, waiting: false, doc: null });
  }

  /** The document to show, or none. Only while the place is held. */
  show(doc: FrameState['doc']): void {
    if (this.slot) this.set({ ...this.state, doc });
  }

  /** The preview failed: the place is free for another frame, and the card takes none until asked. */
  fail(): void {
    this.slot?.release();
    this.slot = null;
    this.stopReads();
    this.failed = true;
    this.set({ live: false, waiting: false, doc: null });
  }

  /** The card is gone: the place is free. */
  release(): void {
    this.slot?.release();
    this.slot = null;
    this.stopReads();
    this.state = { live: false, waiting: false, doc: null };
  }

  /** Another frame took the place. Near the screen, the card offers to load the preview again. */
  private readonly evicted = (): void => {
    this.slot = null;
    this.stopReads();
    this.set({ live: false, waiting: this.near, doc: null });
  };

  private stopReads(): void {
    this.reads?.abort();
    this.reads = null;
  }

  private set(next: FrameState): void {
    this.state = next;
    for (const listener of this.listeners) listener();
  }
}

/** The sandbox frame. A new key starts a new frame; a new `id` hands over `html` again. */
function SandboxFrame({ html, id, onContent, onFail, onShown, onHandOver, hostRef }: {
  html: string;
  id: number;
  onContent: (message: ContentMessage) => void;
  onFail: (reason: string) => void;
  /** Called with the document that the frame was just given. */
  onShown: (id: number) => void;
  /** A document went to the frame: what the page before said counts no more. */
  onHandOver: () => void;
  /** The host of the frame, while it exists. */
  hostRef: React.MutableRefObject<PreviewHost | null>;
}) {
  const place = useRef<HTMLDivElement>(null);
  const handlers = useRef({ onContent, onFail, onHandOver });
  useEffect(() => {
    handlers.current = { onContent, onFail, onHandOver };
  }, [onContent, onFail, onHandOver]);

  useEffect(() => {
    const created = new PreviewHost({
      onContent: message => handlers.current.onContent(message),
      onFail: reason => handlers.current.onFail(reason),
      onHandOver: () => handlers.current.onHandOver(),
    });
    if (place.current) created.start(place.current);
    hostRef.current = created;
    return () => {
      created.dispose();
      if (hostRef.current === created) hostRef.current = null;
    };
  }, [hostRef]);

  useEffect(() => {
    hostRef.current?.show(html);
    onShown(id);
    // The identity decides a hand-over: each build is a new document, also with the same text.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [id, onShown]);

  // The keyboard guard makes this element `position: relative`, so it fills its place by its size.
  return <div ref={place} className="h-full w-full" />;
}

/** The inline preview of `path`, or nothing for a path that has none or while previews are off. */
export function InlinePreview({ path }: { path: string }) {
  const enabled = usePreviewStore(s => s.enabled);
  if (!enabled || !canPreviewInline(path)) return null;
  return <InlinePreviewFrame path={path} />;
}

function InlinePreviewFrame({ path }: { path: string }) {
  const maxBytes = usePreviewStore(s => s.maxBytes);
  const fileChange = usePreviewStore(s => s.fileChange);
  const blockRef = useRef<HTMLDivElement>(null);
  const near = useNear(blockRef);

  const [place] = useState(() => new FramePlace());
  const { live, waiting, doc } = useSyncExternalStore(place.subscribe, place.get);
  const [problem, setProblem] = useState<string | null>(null);
  const [frameError, setFrameError] = useState<string | null>(null);
  const [frameKey, setFrameKey] = useState(0);
  const [deck, setDeck] = useState(false);
  /** A download that failed: one line under the frame, and the page stays. */
  const [downloadError, setDownloadError] = useState<string | null>(null);
  /** Counts the downloads and the hand-overs: a failure shows only for the last download of the page in the frame. */
  const downloads = useRef(0);

  const alive = useRef(true);
  /** The place whose build runs now. */
  const running = useRef<number | null>(null);
  const again = useRef<'no' | 'read' | 'build'>('no');
  /** The fingerprint of the document in the frame. */
  const shown = useRef<string | null>(null);
  const builds = useRef(0);
  const bundle = useRef<PreviewBundle | null>(null);
  /** The document of the frame, the bundle it was built from, and whether it said `ready`. */
  const displayed = useRef<{ id: number; bundle: PreviewBundle; ready: boolean } | null>(null);
  /** The identity of the document that the frame was given last. */
  const shownId = useRef<number | null>(null);
  const hostRef = useRef<PreviewHost | null>(null);

  useEffect(() => {
    alive.current = true;
    return () => {
      alive.current = false;
      place.release();
    };
  }, [place]);

  /**
   * Read the description; build the document when the fingerprint changed
   * (or `build`). A build belongs to the place it started with: when the
   * card loses that place, its reads stop and its result changes nothing.
   */
  const refresh = useCallback(async (build: boolean) => {
    const generation = place.generation;
    const signal = place.signal;
    if (!place.owns(generation)) return;
    if (running.current === generation) {
      if (build || again.current === 'no') again.current = build ? 'build' : 'read';
      return;
    }
    running.current = generation;
    try {
      let force = build;
      let restarts = 0;
      for (;;) {
        again.current = 'no';
        try {
          const next = await api.getPreviewBundle(path, signal);
          if (!alive.current || !place.owns(generation)) return;
          bundle.current = next;
          if (force || next.fingerprint !== shown.current) {
            const built = await buildPreviewDocument(
              next, file => api.getPreviewFile(path, file, next.fingerprint, signal), maxBytes,
            );
            if (!alive.current || !place.owns(generation)) return;
            shown.current = next.fingerprint;
            builds.current += 1;
            displayed.current = { id: builds.current, bundle: next, ready: false };
            // Not a deck until the new page says so (onHandOver resets it again at the hand-over).
            setDeck(false);
            place.show({ html: built.html, id: builds.current });
          }
          setProblem(null);
        } catch (e) {
          // Another frame took the place while this one read: the card shows that, not this error.
          if (!alive.current || !place.owns(generation)) return;
          if (isStale(e) && restarts < MAX_RESTARTS) {
            restarts += 1;
            again.current = 'build';
          } else {
            shown.current = null;
            displayed.current = null;
            place.fail();
            // The inline preview builds again only on Try again.
            setProblem(isStale(e) ? CHANGING : problemText(e, ''));
          }
        }
        if (again.current === 'no') break;
        force = again.current === 'build';
      }
    } finally {
      if (running.current === generation) running.current = null;
    }
  }, [path, maxBytes, place]);

  // Near the screen: take a place, or tell the place where it is.
  useEffect(() => {
    place.sync(near);
  }, [place, near]);

  // A frame that got a place builds its document (again, after it lost the place).
  useEffect(() => {
    if (live) void refresh(true);
  }, [live, refresh]);

  // A file event of the chat that names a file of the bundle.
  useEffect(() => {
    if (!fileChange || !live || !bundle.current) return;
    if (fileEventNamesBundle(bundle.current, fileChange.path)) void refresh(false);
  }, [fileChange, live, refresh]);

  const onShown = useCallback((id: number) => {
    shownId.current = id;
    // A frame that was just given the document has not said `ready` for it.
    if (displayed.current?.id === id) displayed.current.ready = false;
  }, []);

  const onContent = useCallback((message: ContentMessage) => {
    const current = displayed.current;
    const isShown = current !== null && shownId.current === current.id;
    if (message.type === 'ready') {
      // Only the page of the document in the frame says what it is.
      if (!isShown) return;
      current.ready = true;
      setDeck(message.kind === 'deck');
      // A deck shows its first slide: it takes no input, so the person cannot move it back.
      if (message.kind === 'deck' && message.slide !== 1) {
        hostRef.current?.send({ type: 'slide', to: 'first' });
      }
    } else if (message.type === 'download') {
      // Only for the bundle of the document in the frame, after it said `ready` (`downloadSource`).
      if (!isShown || !current.ready) return;
      const attempt = ++downloads.current;
      setDownloadError(null);
      downloadSource(path, current.bundle).catch((e: unknown) => {
        // A newer download, or a new document in the frame, decides the line.
        if (alive.current && downloads.current === attempt) {
          setDownloadError(`The download failed: ${problemText(e, '')}`);
        }
      });
    }
  }, [path]);

  // A page that is not a deck until it says so, and no failed download of the page before.
  const onHandOver = useCallback(() => {
    setDeck(false);
    downloads.current += 1;
    setDownloadError(null);
  }, []);

  const onFail = useCallback((reason: string) => {
    place.fail();
    setFrameError(reason);
  }, [place]);

  const tryAgain = () => {
    setProblem(null);
    setDownloadError(null);
    setFrameError(null);
    setFrameKey(k => k + 1);
    place.sync(true, true);
  };

  const name = path.split('/').pop() || path;
  const note = problem ?? frameError;
  // What a page said of itself counts only while a document is shown.
  const isDeck = deck && doc !== null;

  // One element for the whole life of the card: `useNear` watches it.
  return (
    <div ref={blockRef}>
      {note !== null ? (
        <div role="status" className="mt-1 flex items-center gap-1 text-xs text-text-muted">
          <span className="min-w-0 break-words">No preview: {note}</span>
          <button type="button" onClick={tryAgain} className={TEXT_BUTTON}>Try again</button>
        </div>
      ) : (
        <figure
          aria-label={`Preview of ${name}`}
          aria-busy={!doc || undefined}
          className={`relative mt-1.5 mb-0 mx-0 w-full overflow-hidden rounded-lg border border-border-subtle bg-surface ${isDeck ? 'aspect-video' : ''}`}
          style={isDeck ? undefined : { height: INLINE_HEIGHT }}
        >
          {doc && (
            // A deck takes no input: no click, no focus, so it stays on its first slide.
            <div className="absolute inset-0" inert={isDeck}>
              <SandboxFrame
                key={frameKey} html={doc.html} id={doc.id} onContent={onContent} onFail={onFail}
                onShown={onShown} onHandOver={onHandOver} hostRef={hostRef}
              />
            </div>
          )}
          {doc && isDeck && (
            <button
              type="button"
              aria-label={`Open ${name} in the Preview tab`}
              title="Open in the Preview tab"
              onClick={() => openPreview(path)}
              className="absolute inset-0 cursor-pointer bg-transparent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent/60"
            />
          )}
          {!doc && (
            <div className="absolute inset-0 flex flex-col items-center justify-center gap-1.5 px-3 text-center text-xs text-text-faint">
              {waiting ? (
                <>
                  <button type="button" onClick={() => place.sync(true, true)} className={TEXT_BUTTON}>Load preview</button>
                  <span>Other previews or widgets are running. Loading this one stops the one used longest ago.</span>
                </>
              ) : live ? (
                <span className="flex items-center gap-2">
                  <Loader2 size={14} className="motion-safe:animate-spin" aria-hidden="true" /> Loading the preview...
                </span>
              ) : (
                <span>Preview</span>
              )}
            </div>
          )}
        </figure>
      )}
      {note === null && downloadError !== null && (
        <p role="status" className="mt-1 mb-0 text-xs text-error">{downloadError}</p>
      )}
    </div>
  );
}
