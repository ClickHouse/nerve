import { useCallback, useEffect, useRef, useState } from 'react';
import { api, apiErrorDetail } from '../../api/client';
import { useHostedStore } from '../../api/hosted';
import { useChatStore } from '../../stores/chatStore';
import { usePreviewStore } from '../../stores/previewStore';
import type { PanelTab } from '../../types/chat';
import type { PreviewBundle } from '../../types/preview';
import { Button, IconButton } from '../ui';
import { ChevronLeft, ChevronRight, CodeXml, Loader2, Lock, Play, RefreshCw, X } from '../ui/icons';
import { buildPreviewDocument, formatMiB, PreviewTooLarge, type PreviewDocument } from './document';
import { PreviewHost } from './host';
import { agoText, fileEventNamesBundle, POLL_MS } from './live';
import type { ContentKind, ContentMessage, HostMessage } from './messages';

/**
 * The Preview tab of the side panel: the bundle of one workspace path, drawn
 * in the sandbox frame (`host.ts`) from one document that the browser makes
 * of its files (`document.ts`).
 *
 * The tab reads the description of the bundle again when a file event of
 * the chat names a file of it, and each POLL_MS while the panel is visible:
 * a file that a shell command wrote sends no event. It builds the document
 * again only when the fingerprint of the bundle changed.
 */

/** What the content page said of itself (`messages.ts`). */
interface ContentState {
  kind: ContentKind;
  slides: number | null;
  slide: number | null;
}

function Updated({ at }: { at: number }) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);
  return <span className="text-xs text-text-dim shrink-0 tabular-nums">Updated {agoText(now - at)}</span>;
}

/** The sentence of a failed read, for the person. */
function problemText(e: unknown): string {
  if (e instanceof PreviewTooLarge) {
    return `This preview needs ${formatMiB(e.bytes)} of files or more. The limit is ${formatMiB(e.limit)}.`;
  }
  const message = apiErrorDetail(e)?.message;
  if (typeof message === 'string' && message) return message;
  return 'The preview cannot be read.';
}

/** The sandbox frame of the preview. A new key starts a new frame. */
function PreviewFrame({ html, onContent, onFail, hostRef }: {
  html: string;
  onContent: (message: ContentMessage) => void;
  onFail: (reason: string) => void;
  hostRef: React.MutableRefObject<PreviewHost | null>;
}) {
  const place = useRef<HTMLDivElement>(null);
  const handlers = useRef({ onContent, onFail });
  useEffect(() => {
    handlers.current = { onContent, onFail };
  }, [onContent, onFail]);

  useEffect(() => {
    const host = new PreviewHost({
      onContent: message => handlers.current.onContent(message),
      onFail: reason => handlers.current.onFail(reason),
    });
    if (place.current) host.start(place.current);
    hostRef.current = host;
    return () => {
      host.dispose();
      if (hostRef.current === host) hostRef.current = null;
    };
  }, [hostRef]);

  useEffect(() => {
    hostRef.current?.show(html);
  }, [html, hostRef]);

  return <div ref={place} className="absolute inset-0" />;
}

export function PreviewTab({ tab }: { tab: PanelTab }) {
  const path = tab.previewPath ?? '';
  const maxBytes = usePreviewStore(s => s.maxBytes);
  const fileChange = usePreviewStore(s => s.fileChange);
  const panelVisible = useChatStore(s => s.panelVisible);
  const togglePanel = useChatStore(s => s.togglePanel);
  const hosted = useHostedStore(s => s.mode === 'external');

  const [bundle, setBundle] = useState<PreviewBundle | null>(null);
  const [doc, setDoc] = useState<PreviewDocument | null>(null);
  const [problem, setProblem] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [showSource, setShowSource] = useState(false);
  const [frameError, setFrameError] = useState<string | null>(null);
  const [frameKey, setFrameKey] = useState(0);
  const [content, setContent] = useState<ContentState | null>(null);
  const [presenting, setPresenting] = useState(false);

  const hostRef = useRef<PreviewHost | null>(null);
  const stageRef = useRef<HTMLDivElement>(null);
  /** The fingerprint of the document on screen. */
  const shown = useRef<string | null>(null);
  const running = useRef(false);
  const again = useRef<'no' | 'read' | 'build'>('no');
  const alive = useRef(true);
  const lastSlide = useRef<number | null>(null);
  const presentingRef = useRef(false);
  useEffect(() => {
    presentingRef.current = presenting;
  }, [presenting]);

  useEffect(() => {
    alive.current = true;
    return () => { alive.current = false; };
  }, []);

  /** Read the description; build the document when the fingerprint changed (or `build`). */
  const refresh = useCallback(async (build: boolean) => {
    if (running.current) {
      if (build || again.current === 'no') again.current = build ? 'build' : 'read';
      return;
    }
    running.current = true;
    setBusy(true);
    try {
      let force = build;
      for (;;) {
        again.current = 'no';
        try {
          const next = await api.getPreviewBundle(path);
          if (!alive.current) return;
          setBundle(next);
          if (force || next.fingerprint !== shown.current) {
            const built = await buildPreviewDocument(
              next, file => api.getPreviewFile(path, file, next.fingerprint), maxBytes,
            );
            if (!alive.current) return;
            shown.current = next.fingerprint;
            setDoc(built);
            setContent(null);
          }
          setProblem(null);
        } catch (e) {
          if (!alive.current) return;
          shown.current = null;
          setDoc(null);
          setProblem(problemText(e));
        }
        if (again.current === 'no') break;
        force = again.current === 'build';
      }
    } finally {
      running.current = false;
      if (alive.current) setBusy(false);
    }
  }, [path, maxBytes]);

  // The first read. The tab is keyed by its path, so the path never changes here.
  useEffect(() => {
    void refresh(true);
  }, [refresh]);

  // Each POLL_MS while the panel and the page are visible.
  useEffect(() => {
    if (!panelVisible) return;
    const tick = () => {
      if (document.visibilityState === 'visible') void refresh(false);
    };
    const timer = setInterval(tick, POLL_MS);
    document.addEventListener('visibilitychange', tick);
    return () => {
      clearInterval(timer);
      document.removeEventListener('visibilitychange', tick);
    };
  }, [panelVisible, refresh]);

  // A file event of the chat that names a file of the bundle.
  const bundleRef = useRef(bundle);
  useEffect(() => {
    bundleRef.current = bundle;
  }, [bundle]);
  useEffect(() => {
    if (!fileChange || !bundleRef.current) return;
    if (fileEventNamesBundle(bundleRef.current, fileChange.path)) void refresh(false);
  }, [fileChange, refresh]);

  const send = useCallback((message: HostMessage) => hostRef.current?.send(message), []);

  const onContent = useCallback((message: ContentMessage) => {
    if (message.type === 'ready') {
      setContent({ kind: message.kind, slides: message.slides ?? null, slide: message.slide ?? null });
      // A new document of the same deck: back to the slide the person was on.
      const wanted = lastSlide.current;
      if (message.slides && wanted && wanted !== message.slide && wanted <= message.slides) {
        send({ type: 'slide', to: wanted });
      }
      if (presentingRef.current) send({ type: 'present', on: true });
    } else {
      lastSlide.current = message.slide;
      setContent(c => (c ? { ...c, slide: message.slide, slides: message.slides } : c));
    }
  }, [send]);

  const onFail = useCallback((reason: string) => setFrameError(reason), []);

  const reload = () => {
    setFrameError(null);
    setFrameKey(k => k + 1);
    void refresh(true);
  };

  // Present: the frame fills the screen.
  useEffect(() => {
    const onChange = () => {
      const on = document.fullscreenElement === stageRef.current && stageRef.current !== null;
      setPresenting(on);
      send({ type: 'present', on });
    };
    document.addEventListener('fullscreenchange', onChange);
    return () => document.removeEventListener('fullscreenchange', onChange);
  }, [send]);

  const present = () => {
    const stage = stageRef.current;
    if (!stage) return;
    setShowSource(false);
    if (typeof stage.requestFullscreen === 'function') {
      stage.requestFullscreen().catch(() => send({ type: 'present', on: true }));
    } else {
      send({ type: 'present', on: true });
    }
    hostRef.current?.focus();
  };

  const isDeck = content?.kind === 'deck' && content.slides !== null;
  const onStageKey = (event: React.KeyboardEvent) => {
    if (!isDeck) return;
    const to = ({
      ArrowRight: 'next', PageDown: 'next', ' ': 'next', ArrowLeft: 'prev', PageUp: 'prev', Home: 'first', End: 'last',
    } as Record<string, 'next' | 'prev' | 'first' | 'last'>)[event.key];
    if (!to) return;
    event.preventDefault();
    send({ type: 'slide', to });
  };

  const shownPath = bundle?.path || path;

  return (
    <div className="flex flex-col flex-1 min-h-0">
      <div className="flex items-center gap-2 px-3 py-2 border-b border-border-subtle bg-bg shrink-0">
        <span className="font-mono text-xs text-text-secondary truncate min-w-0" title={shownPath}>{shownPath}</span>
        {bundle && !problem && (
          <>
            <span aria-hidden="true" className="w-1.5 h-1.5 rounded-full bg-success shrink-0" />
            <Updated at={bundle.modified} />
          </>
        )}
        {busy && <Loader2 size={12} className="motion-safe:animate-spin text-text-faint shrink-0" aria-hidden="true" />}
        <div className="ml-auto flex items-center gap-1 shrink-0">
          {isDeck && (
            <div className="flex items-center gap-0.5 mr-1" role="group" aria-label="Slides">
              <IconButton label="Previous slide" size="xs" onClick={() => send({ type: 'slide', to: 'prev' })}>
                <ChevronLeft size={14} />
              </IconButton>
              <span className="text-xs tabular-nums text-text-dim" aria-live="polite">
                {content?.slide ?? '-'} / {content?.slides}
              </span>
              <IconButton label="Next slide" size="xs" onClick={() => send({ type: 'slide', to: 'next' })}>
                <ChevronRight size={14} />
              </IconButton>
            </div>
          )}
          <IconButton label="Reload the preview" size="xs" onClick={reload}>
            <RefreshCw size={14} />
          </IconButton>
          <IconButton
            label={showSource ? 'Show the preview' : 'Show the source'}
            size="xs"
            active={showSource}
            aria-pressed={showSource}
            disabled={!doc || doc.source === null}
            onClick={() => setShowSource(s => !s)}
          >
            <CodeXml size={14} />
          </IconButton>
          <Button variant="secondary" size="sm" onClick={present} disabled={!doc}>
            <Play size={12} aria-hidden="true" />
            Present
          </Button>
          <IconButton label="Close the panel" size="xs" onClick={togglePanel}>
            <X size={14} />
          </IconButton>
        </div>
      </div>

      <div className="relative flex-1 min-h-0 bg-bg-sunken">
        {problem && (
          <div role="alert" className="absolute inset-0 flex flex-col items-center justify-center gap-3 p-6 text-center">
            <p className="text-sm text-text-muted max-w-md">{problem}</p>
            <Button variant="secondary" size="sm" onClick={reload}>Try again</Button>
          </div>
        )}
        {!problem && frameError && (
          <div role="alert" className="absolute inset-0 flex flex-col items-center justify-center gap-3 p-6 text-center">
            <p className="text-sm text-text-muted max-w-md">{frameError}</p>
            <Button variant="secondary" size="sm" onClick={reload}>Reload</Button>
          </div>
        )}
        {!problem && !doc && !frameError && (
          <div className="absolute inset-0 flex items-center justify-center gap-2 text-sm text-text-dim">
            <Loader2 size={14} className="motion-safe:animate-spin" aria-hidden="true" /> Loading the preview...
          </div>
        )}
        {doc && !problem && !frameError && (
          <>
            {showSource && doc.source !== null && (
              <pre
                aria-label="Source"
                className="absolute inset-0 overflow-auto p-4 text-xs font-mono text-text-muted whitespace-pre-wrap break-words"
              >
                {doc.source}
              </pre>
            )}
            <div
              ref={stageRef}
              hidden={showSource}
              onKeyDown={onStageKey}
              className="absolute inset-0 bg-bg-sunken"
            >
              <PreviewFrame key={frameKey} html={doc.html} onContent={onContent} onFail={onFail} hostRef={hostRef} />
            </div>
          </>
        )}
      </div>

      <div className="flex items-center gap-2 px-3 py-1.5 border-t border-border-subtle bg-bg text-xs text-text-dim shrink-0">
        <Lock size={12} aria-hidden="true" />
        {hosted
          ? 'Preview from the agent. Only the members of this agent can see it.'
          : 'Preview from the agent. It runs in a sandbox with no network access.'}
      </div>
    </div>
  );
}
