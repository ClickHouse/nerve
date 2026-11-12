import {
  useCallback, useContext, useEffect, useMemo, useRef, useState, useSyncExternalStore, type RefObject,
} from 'react';
import { useMcpAppsStore } from '../../../stores/mcpAppsStore';
import { useThemeStore } from '../../../stores/themeStore';
import { copyToClipboard } from '../../../utils/clipboard';
import { IconButton } from '../../ui';
import { Check, CodeXml, Copy, RotateCw } from '../../ui/icons';
import { MAX_RICH_SOURCE_CHARS } from '../rich/kinds';
import { MessageChatContext } from '../messageChat';
import { MarkdownStreamingContext } from '../streamingContext';
import { WidgetController } from './controller';
import { widgetHeight, widgetKey } from './heights';
import { widgetScope } from './storage';
import { WidgetSource } from './WidgetSource';

/**
 * A ```widget block: a small piece of HTML that the model wrote into its
 * answer, drawn live inside the message.
 *
 * The HTML runs in the sandbox that MCP Apps use — the frame of the sandbox
 * proxy, a page of this address without an origin, and inside it a frame
 * that has none either — with no network and no way to ask the chat for
 * anything (`host.ts`). The
 * source is never markup of this page: it goes to the frame as a message, and
 * is shown here as text.
 *
 * - While the block is still being written it holds a quiet placeholder; the
 *   widget is drawn once the text has stopped changing.
 * - The frame is opened when the block comes near the screen, and only a few
 *   widgets have one at a time (`budget.ts`).
 * - The source is one click away, with a copy button; Reload draws the
 *   widget again from it.
 * - What the widget saved in its `localStorage` is kept with the chat, on
 *   the server (`storage.ts`), and is there when the widget is drawn again.
 *   Reset forgets it. When it cannot be read or written, the widget runs
 *   all the same and the block says so in a few quiet words.
 * - Where it cannot be drawn — the feature is off, the sandbox does not
 *   answer, the source is too large — the block is a code block with its
 *   source and one line that says why.
 */

/** While the text still streams, the widget is drawn once it has stopped changing for this long. */
const SETTLE_MS = 800;

/** How far outside the screen a block counts as near it. */
const NEAR_MARGIN = '600px 0px';

/** The room a block holds for a widget that was never drawn. */
const PLACEHOLDER_HEIGHT = 96;

/**
 * The source to draw: the text itself once the message is complete; while it
 * streams, the text as it was when it last held still for SETTLE_MS, and
 * null before that. A streamed block changes with every token batch, and
 * half a widget is not worth a frame.
 */
function useSettledSource(source: string, streaming: boolean): string | null {
  const [settled, setSettled] = useState<string | null>(null);
  useEffect(() => {
    if (!streaming) return;
    const timer = setTimeout(() => setSettled(source), SETTLE_MS);
    return () => clearTimeout(timer);
  }, [source, streaming]);
  return streaming ? settled : source;
}

/**
 * True once this page knows whether it draws widgets: the one read of the
 * feature's config (the app starts it; this waits for the same read) has
 * answered. Until then a block holds its place instead of showing its source
 * for a moment.
 */
function useFeatureKnown(): boolean {
  const [known, setKnown] = useState(false);
  useEffect(() => {
    let cancelled = false;
    const done = () => {
      if (!cancelled) setKnown(true);
    };
    useMcpAppsStore.getState().load().then(done, done);
    return () => {
      cancelled = true;
    };
  }, []);
  return known;
}

/** True while the block is on the screen or close to it; always, where the browser cannot tell. */
function useNear(block: RefObject<HTMLElement | null>): boolean {
  const [near, setNear] = useState(() => typeof IntersectionObserver === 'undefined');
  useEffect(() => {
    const element = block.current;
    if (!element || typeof IntersectionObserver === 'undefined') return;
    const observer = new IntersectionObserver((entries) => {
      setNear(entries[entries.length - 1].isIntersecting);
    }, {
      rootMargin: NEAR_MARGIN,
      // The chat scrolls in an element of its own: the margin counts there too.
      scrollMargin: NEAR_MARGIN,
    } as IntersectionObserverInit);
    observer.observe(element);
    return () => observer.disconnect();
  }, [block]);
  return near;
}

function sizeOf(source: string): string {
  return source.length < 1000 ? `${source.length} characters` : `${(source.length / 1000).toFixed(1)}k characters`;
}

const TEXT_BUTTON = 'text-xs text-text-dim hover:text-text px-1.5 py-0.5 rounded hover:bg-surface-hover cursor-pointer';

/** What the block says, quietly, when what the widget holds is not kept. */
const NOTICE = {
  unavailable: 'saved state is unavailable: changes are not kept',
  unsaved: 'the last change is not saved yet',
} as const;

export function WidgetBlock({ source }: { source: string }) {
  const enabled = useMcpAppsStore(s => s.enabled);
  const featureKnown = useFeatureKnown();
  const theme = useThemeStore(s => s.resolved);
  const streaming = useContext(MarkdownStreamingContext);
  const settled = useSettledSource(source, streaming);
  const blockRef = useRef<HTMLDivElement>(null);
  const near = useNear(blockRef);

  // The chat of the message the block is part of, when it appeared, is the one its widget lives in.
  const chat = useContext(MessageChatContext);
  const [scope] = useState(() => widgetScope(chat ?? '', window.location.pathname));

  const [controller] = useState(() => new WidgetController());
  const view = useSyncExternalStore(controller.subscribe, controller.getSnapshot);
  const place = useCallback((element: HTMLDivElement | null) => controller.place(element), [controller]);

  const [showSource, setShowSource] = useState(false);
  const [copied, setCopied] = useState(false);
  // The control a Source/Rendered switch hands focus to: the other one.
  const sourceButtonRef = useRef<HTMLButtonElement>(null);
  const renderedButtonRef = useRef<HTMLButtonElement>(null);
  const switchedRef = useRef(false);

  const tooLarge = source.length > MAX_RICH_SOURCE_CHARS;
  const empty = !streaming && source.trim() === '';
  const off = !enabled;
  // What the frame gets: nothing while the feature is off, the text is still
  // being written, or it is not a thing to draw.
  const drawable = !off && !tooLarge && settled !== null && settled.trim() !== '' ? settled : null;

  useEffect(() => {
    controller.sync({ source: drawable, near, theme, scope });
  }, [controller, drawable, near, theme, scope]);

  useEffect(() => () => controller.stop(), [controller]);

  // After a Source/Rendered switch, focus moves to the control that switches back.
  useEffect(() => {
    if (!switchedRef.current) return;
    switchedRef.current = false;
    (showSource ? renderedButtonRef : sourceButtonRef).current?.focus();
  }, [showSource]);

  const switchView = (toSource: boolean) => {
    switchedRef.current = true;
    setShowSource(toSource);
  };

  const handleCopy = async () => {
    if (!(await copyToClipboard(source))) return;
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  // Why the block is a code block and not a widget, in one line.
  const failed = view.phase === 'failed';
  const reason = off
    ? (featureKnown ? 'widgets are off on this Nerve' : null)
    : tooLarge
      ? `too large to draw (${source.length.toLocaleString()} characters)`
      : empty
        ? 'empty'
        : failed
          ? (view.error ?? 'The widget could not be drawn.')
          : null;
  const asSource = reason !== null;
  const writing = !off && !tooLarge && streaming && settled === null;
  const framed = view.phase === 'loading' || view.phase === 'shown';
  const state = asSource ? 'source' : writing ? 'writing' : view.phase;
  // The room to hold while there is no frame: what the widget took before, if it was drawn before.
  const hint = useMemo(
    () => (drawable === null || framed ? 0 : widgetHeight(widgetKey(drawable))),
    [drawable, framed],
  );

  return (
    <div ref={blockRef} className="my-2" data-widget={state}>
      {/* Always in the page, hidden while the source shows: the frame's place
          must not come and go with what the block shows (a frame that leaves
          the page loads again, and a widget that failed must stay failed). */}
      <figure
        hidden={asSource || showSource}
        aria-label="Widget"
        aria-busy={view.phase !== 'shown' || undefined}
        className="m-0 overflow-hidden rounded-lg border border-border-subtle bg-surface"
      >
        {!asSource && (
          <div className="flex h-8 items-center justify-between gap-2 pl-3 pr-1.5">
            <span className="flex min-w-0 items-baseline gap-2">
              <span className="text-xs font-mono text-text-faint">widget</span>
              {framed && view.notice && (
                <span role="status" className="truncate text-xs text-text-faint">{NOTICE[view.notice]}</span>
              )}
            </span>
            <span role="group" aria-label="Widget tools" className="flex items-center gap-0.5">
              {framed && view.saved && (
                <button
                  type="button"
                  onClick={() => controller.reset()}
                  className={TEXT_BUTTON}
                  title="Forget what this widget saved and draw it again"
                >
                  Reset
                </button>
              )}
              {framed && (
                <IconButton label="Reload" size="xs" onClick={() => controller.load()}>
                  <RotateCw size={13} />
                </IconButton>
              )}
              <IconButton ref={sourceButtonRef} label="Source" size="xs" onClick={() => switchView(true)}>
                <CodeXml size={14} />
              </IconButton>
              <IconButton label={copied ? 'Copied' : 'Copy source'} size="xs" onClick={handleCopy}>
                {copied ? <Check size={14} className="text-hue-emerald" /> : <Copy size={14} />}
              </IconButton>
            </span>
          </div>
        )}
        <div
          className="relative px-3 pb-3"
          style={framed ? undefined : { minHeight: (hint || PLACEHOLDER_HEIGHT) + 12 }}
        >
          {/* The host puts the frame here. */}
          <div ref={place} data-widget-frame="" />
          {!asSource && view.phase !== 'shown' && (
            <div
              role="status"
              className="absolute inset-0 flex flex-col items-center justify-center gap-1.5 bg-surface px-3 text-center text-xs text-text-faint"
            >
              {writing ? (
                <span>Writing the widget… {sizeOf(source)}</span>
              ) : view.phase === 'waiting' ? (
                <>
                  <button type="button" onClick={() => controller.load()} className={TEXT_BUTTON}>
                    Load widget
                  </button>
                  <span>Other widgets are running. Loading this one stops the one used longest ago.</span>
                </>
              ) : view.phase === 'loading' ? (
                <span>Loading the widget…</span>
              ) : (
                <span>Widget</span>
              )}
            </div>
          )}
        </div>
      </figure>
      {asSource ? (
        <WidgetSource
          source={source}
          note={reason}
          warning={failed}
          actions={failed && (
            <button type="button" onClick={() => controller.load()} className={TEXT_BUTTON}>Retry</button>
          )}
        />
      ) : showSource && (
        <WidgetSource
          source={source}
          actions={(
            <button ref={renderedButtonRef} type="button" onClick={() => switchView(false)} className={TEXT_BUTTON}>
              Rendered
            </button>
          )}
        />
      )}
    </div>
  );
}
