import {
  useContext, useEffect, useId, useLayoutEffect, useRef, useState, useSyncExternalStore, type ReactNode,
} from 'react';
import { useAgentStore } from '../../../stores/agentStore';
import { useChatStore } from '../../../stores/chatStore';
import { useMcpAppsStore } from '../../../stores/mcpAppsStore';
import { useThemeStore } from '../../../stores/themeStore';
import type { ToolCallBlockData } from '../../../types/chat';
import type { McpAppRef } from '../../../types/mcpApps';
import { extractResultText } from '../../../utils/extractResultText';
import { formatMcpName } from '../../../utils/formatMcpName';
import { Button, Modal } from '../../ui';
import { AlertTriangle, Loader2, Moon, RotateCw } from '../../ui/icons';
import { MessageChatContext } from '../messageChat';
import { claimHost, hostKey, releaseHost } from './registry';
import type { McpAppHostHandle, McpAppPrompt, McpAppSnapshot } from './types';

/**
 * A tool call that has an interactive view (MCP Apps): the ordinary tool-call
 * card, folded, and under it the view of the tool's MCP server, fed with the
 * call's arguments and result.
 *
 * The view is third-party HTML in a frame that has no origin; `host.ts`
 * runs it and holds every rule about what it may do. This block gives it a
 * place and shows what the person has to see around it: that it is loading,
 * why it failed, and the questions the chat asks before it lets the view call
 * a tool that may change something or open a link.
 *
 * - The view starts when the block comes near the screen and then stays.
 * - A block that goes away hands its view to the block that replaces it (the
 *   streamed message gives way to the stored one when the turn ends), so the
 *   view does not start again. A view that nobody takes over is told to end
 *   and removed (see `registry.ts`).
 */

type HostModule = typeof import('./host');

// The host, with the MCP Apps SDK, loads the first time a chat shows a view;
// a chat without one pays for neither.
let hostModule: Promise<HostModule> | null = null;
function loadHost(): Promise<HostModule> {
  hostModule ??= import('./host').catch((error: unknown) => {
    hostModule = null;
    throw error;
  });
  return hostModule;
}

/** How far outside the screen a block starts its view. */
const NEAR_MARGIN = '600px 0px';

/** The block's part before it has a view to show. */
const WAITING: McpAppSnapshot = {
  status: 'loading', step: null, error: null, height: 0, bordered: true, prompt: null,
};

interface Shown {
  host: McpAppHostHandle | null;
  view: McpAppSnapshot;
}

/**
 * The view a block shows, as a store the block reads: which host it has, and
 * that host's state. One per block, for as long as the block is mounted.
 */
class Slot {
  host: McpAppHostHandle | null = null;
  private shown: Shown = { host: null, view: WAITING };
  private readonly listeners = new Set<() => void>();
  private unsubscribe: (() => void) | null = null;

  private readonly changed = () => {
    this.shown = { host: this.host, view: this.host?.getSnapshot() ?? WAITING };
    for (const listener of [...this.listeners]) listener();
  };

  /** Show `host`; null leaves the block with nothing, without ending the host. */
  take(host: McpAppHostHandle | null): void {
    this.unsubscribe?.();
    this.host = host;
    this.unsubscribe = host ? host.subscribe(this.changed) : null;
    this.changed();
  }

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  getSnapshot = (): Shown => this.shown;
}

/**
 * Why a finished call has no result for the view: the first sentence of its
 * error. No more than that: for a call that the person stopped, the runtime's
 * notice goes on with instructions to the model, and those are not for a view.
 */
function failureOf(block: ToolCallBlockData): string | undefined {
  if (!block.isError || !block.result) return undefined;
  const line = extractResultText(block.result).trim().split('\n')[0];
  const sentence = /^.*?[.!?](?=\s|$)/.exec(line)?.[0] ?? line;
  return sentence.slice(0, 200) || undefined;
}

function composerHasText(): boolean {
  return useAgentStore.getState().composerHasText;
}

function insertIntoComposer(text: string): Promise<boolean> {
  return useAgentStore.getState().offerToComposer(text);
}

export function McpAppBlock({ block, app, chrome }: {
  block: ToolCallBlockData;
  /** `block.mcpApp`: the server, the tool and the address of the view. */
  app: McpAppRef;
  /** The ordinary card of the call (name, input, text result), shown folded above the view. */
  chrome: ReactNode;
}) {
  const enabled = useMcpAppsStore(s => s.enabled);
  const theme = useThemeStore(s => s.resolved);
  // The call belongs to the chat of its message (when the block appeared).
  // Where no message says so, to the chat on screen.
  const chat = useContext(MessageChatContext);
  const [sessionId] = useState(() => chat ?? useChatStore.getState().activeSession);
  const { toolUseId } = block;
  // A call without an id (it should have one) is nobody else's: its view is
  // never handed to another block.
  const blockId = useId();
  const key = hostKey(sessionId, toolUseId || blockId);
  const { server, tool, resourceUri } = app;

  const frameRef = useRef<HTMLDivElement>(null);
  const placeRef = useRef<HTMLDivElement>(null);
  const [slot] = useState(() => new Slot());
  const { host, view } = useSyncExternalStore(slot.subscribe, slot.getSnapshot);
  const [near, setNear] = useState(() => typeof IntersectionObserver === 'undefined');
  // The part of the chat that runs views did not load (the network, an old page).
  const [unavailable, setUnavailable] = useState(false);

  // A view that the block before this one parked is taken over at once, in the
  // commit that mounts this block. When this block goes, its view is parked.
  // A layout effect: the frame must move while both places are in the page.
  useLayoutEffect(() => {
    const place = placeRef.current;
    const parked = place ? claimHost(key) : null;
    if (parked && place) {
      if (parked.attach(place)) slot.take(parked);
      else void parked.dispose();
    }
    return () => {
      const current = slot.host;
      slot.take(null);
      if (current) releaseHost(key, current);
    };
  }, [key, slot]);

  // Near the screen: from then on the view is wanted.
  useEffect(() => {
    const frame = frameRef.current;
    if (near || !frame || typeof IntersectionObserver === 'undefined') return;
    const observer = new IntersectionObserver((entries) => {
      if (entries.some(entry => entry.isIntersecting)) setNear(true);
    }, {
      rootMargin: NEAR_MARGIN,
      // The chat scrolls in an element of its own: the margin counts there too.
      scrollMargin: NEAR_MARGIN,
    } as IntersectionObserverInit);
    observer.observe(frame);
    return () => observer.disconnect();
  }, [near]);

  // Start the view, once it is wanted and nothing was taken over.
  useEffect(() => {
    if (!near || host || !enabled) return;
    let cancelled = false;
    loadHost().then((module) => {
      const place = placeRef.current;
      if (cancelled || slot.host || !place) return;
      const created = module.createHost({
        sessionId,
        toolUseId,
        app: { server, tool, resourceUri },
        theme: useThemeStore.getState().resolved,
        composerHasText,
        insertText: insertIntoComposer,
      });
      if (created.attach(place)) slot.take(created);
      else void created.dispose();
    }, () => {
      if (!cancelled) setUnavailable(true);
    });
    return () => {
      cancelled = true;
    };
  }, [near, host, enabled, slot, sessionId, toolUseId, server, tool, resourceUri]);

  // The view follows the call: its arguments, then its result or its end.
  const { input, mcpAppResult: result } = block;
  const settled = block.status === 'complete';
  const failure = settled && !result ? failureOf(block) : undefined;
  useEffect(() => {
    host?.setToolCall({ input, result, settled, failure });
  }, [host, input, result, settled, failure]);

  useEffect(() => {
    host?.setTheme(theme);
  }, [host, theme]);

  const name = formatMcpName(server);
  const failed = unavailable || view.status === 'error' || view.status === 'asleep';

  return (
    <div className="my-1.5" data-mcp-app={unavailable ? 'error' : view.status}>
      {chrome}
      <div
        ref={frameRef}
        className={`relative overflow-hidden rounded-lg ${
          view.bordered || failed ? 'border border-border bg-surface' : ''
        } ${failed ? '' : 'min-h-[60px]'}`}
      >
        {/* The host puts the frame of the view here. Always in the page: a
            frame that leaves it loads again. */}
        <div ref={placeRef} hidden={failed} />
        {!failed && view.status === 'loading' && (
          <div
            role="status"
            className="absolute inset-0 flex flex-col items-center justify-center gap-2 px-4 text-center text-sm text-text-muted bg-surface"
          >
            <span className="inline-flex items-center gap-2">
              <Loader2 size={14} className="animate-spin text-accent" aria-hidden="true" />
              Loading the {name} app…
            </span>
          </div>
        )}
        {failed && (
          <div role="status" className="flex items-start gap-2 px-3 py-2.5 text-sm text-text-secondary">
            {view.status === 'asleep' ? (
              <>
                <Moon size={14} className="mt-0.5 shrink-0 text-text-muted" aria-hidden="true" />
                <span className="min-w-0 flex-1">
                  The session is asleep, so the {name} app cannot load. Send a message to wake the session, then try again.
                </span>
              </>
            ) : (
              <>
                <AlertTriangle size={14} className="mt-0.5 shrink-0 text-warning" aria-hidden="true" />
                <span className="min-w-0 flex-1 break-words">
                  {unavailable
                    ? 'The app viewer did not load. Reload the page to try again.'
                    : view.stopped
                      ? `The ${name} app was stopped: ${view.error ?? 'unknown reason'}`
                      : `The ${name} app did not load: ${view.error ?? 'unknown error'}`}
                </span>
              </>
            )}
            {host && (
              <Button variant="secondary" size="xs" onClick={() => host.retry()}>
                <RotateCw size={12} aria-hidden="true" />
                Retry
              </Button>
            )}
          </div>
        )}
      </div>
      {/* A dialog of its own for each question: what it knows of one is gone with it. */}
      {host && view.prompt && <PromptDialog key={view.prompt.id} prompt={view.prompt} server={name} host={host} />}
    </div>
  );
}

/**
 * False until a question has been on screen long enough for a click to be
 * meant for it (`prompt.readyAt`). One question takes the place of another
 * in the same spot, so without this the second click of a double click
 * would answer a question nobody has read.
 */
function useAnswerable(readyAt: number): boolean {
  const [answerable, setAnswerable] = useState(false);
  useEffect(() => {
    const timer = setTimeout(() => setAnswerable(true), Math.max(0, readyAt - Date.now()));
    return () => clearTimeout(timer);
  }, [readyAt]);
  return answerable;
}

/**
 * The chat's question before it does what a view asks: call a tool that may
 * change something, open a link, or put text into the message field. Drawn
 * by the chat, over the page, with what the view asked for as plain text,
 * all of it. Its buttons take no click in its first moment on screen.
 * Closing it is a no.
 */
function PromptDialog({ prompt, server, host }: {
  prompt: McpAppPrompt;
  server: string;
  host: McpAppHostHandle;
}) {
  const answerable = useAnswerable(prompt.readyAt);
  const deny = () => host.answer(prompt.id, false);
  const allow = () => host.answer(prompt.id, true);
  if (prompt.kind === 'message') {
    const lines = prompt.text.split('\n').length;
    return (
      <Modal
        open
        onClose={deny}
        title="Put this text into the message field?"
        size="md"
        closeOnBackdrop={false}
        footer={(
          <>
            <Button variant="ghost" size="sm" onClick={deny} disabled={!answerable}>Cancel</Button>
            <Button variant="primary" size="sm" onClick={allow} disabled={!answerable}>Insert text</Button>
          </>
        )}
      >
        <div className="px-5 py-4 space-y-3">
          <p className="text-sm text-text-secondary">
            The {server} app wants to put this text into the message field. Nothing is sent: it goes to the
            agent only when you send it.
          </p>
          <pre
            aria-label="Text"
            className="max-h-60 overflow-auto rounded-lg border border-border-subtle bg-bg p-2 font-mono text-xs text-text whitespace-pre-wrap break-words"
          >
            {prompt.text}
          </pre>
          <p className="text-xs text-text-muted">
            {prompt.text.length} characters{lines > 1 ? `, ${lines} lines` : ''}. This is all of the text.
          </p>
        </div>
      </Modal>
    );
  }
  if (prompt.kind === 'link') {
    return (
      <Modal
        open
        onClose={deny}
        title="Open this link?"
        size="md"
        closeOnBackdrop={false}
        footer={(
          <>
            <Button variant="ghost" size="sm" onClick={deny} disabled={!answerable}>Cancel</Button>
            <Button variant="primary" size="sm" onClick={allow} disabled={!answerable}>Open link</Button>
          </>
        )}
      >
        <div className="px-5 py-4 space-y-3">
          <p className="text-sm text-text-secondary">The {server} app wants to open a link in a new tab:</p>
          <p className="rounded-lg border border-border-subtle bg-bg px-3 py-2 font-mono text-xs text-text break-all">
            {prompt.url}
          </p>
        </div>
      </Modal>
    );
  }
  return (
    <Modal
      open
      onClose={deny}
      title="Allow this tool call?"
      size="md"
      closeOnBackdrop={false}
      footer={(
        <>
          <Button variant="ghost" size="sm" onClick={deny} disabled={!answerable}>Deny</Button>
          <Button variant="primary" size="sm" onClick={allow} disabled={!answerable}>Allow once</Button>
        </>
      )}
    >
      <div className="px-5 py-4 space-y-3">
        <p className="text-sm text-text-secondary">
          The {server} app wants to call the tool{' '}
          <code className="font-mono text-text break-all">{prompt.tool}</code>, which may change something:
        </p>
        <pre
          aria-label="Arguments"
          className="max-h-60 overflow-auto rounded-lg border border-border-subtle bg-bg p-2 font-mono text-xs text-text-muted whitespace-pre-wrap break-words"
        >
          {JSON.stringify(prompt.args, null, 2)}
        </pre>
      </div>
    </Modal>
  );
}
