import {
  AppBridge, RESOURCE_MIME_TYPE,
  type McpUiHostCapabilities, type McpUiHostContext, type McpUiMessageRequest, type McpUiMessageResult,
  type McpUiOpenLinkRequest, type McpUiOpenLinkResult,
} from '@modelcontextprotocol/ext-apps/app-bridge';
import type {
  CallToolRequest, CallToolResult, ReadResourceRequest, ReadResourceResult, Tool,
} from '@modelcontextprotocol/client';
import { api } from '../../../api/client';
import type { ResolvedTheme } from '../../../stores/themeStore';
import type { McpAppResource, McpAppTool } from '../../../types/mcpApps';
import { httpLink } from '../../../utils/httpLink';
import { failureOf } from './failures';
import { hostStyleVariables } from './hostStyles';
import { KeyboardGuard, TAKEN_REASON } from './keyboard';
import { MAX_FRAME_HEIGHT, clampFrameHeight, decideToolCall, messageText } from './policy';
import { hostKey } from './registry';
import {
  PROXY_REFUSED, VIEW_SANDBOX, allowAttribute, grantedPermissions, sandboxUrl, sanitizeCsp,
} from './sandbox';
import { SandboxTransport } from './transport';
import type {
  McpAppHostHandle, McpAppHostOptions, McpAppRequest, McpAppSnapshot, McpAppToolCall,
} from './types';

/**
 * The host of one MCP App view: the chat's side of the conversation with the
 * interactive view of a tool call.
 *
 * It runs once per tool call, in this order:
 *
 * 1. `resource` — read the view's document and the tools of its server from
 *    the gateway.
 * 2. `sandbox` — open the frame of the sandbox proxy, a page of this address
 *    that the gateway serves without an origin, with the document's origins
 *    in its address. The proxy says when it is ready and takes the HTML.
 * 3. `app` — the view starts and shakes hands (`ui/initialize`, then
 *    `ui/notifications/initialized`).
 * 4. `ready` — the view gets the tool call's arguments, then its result (or
 *    word that there is none), and from then on asks the chat for what it
 *    needs.
 *
 * The protocol is the SDK's `AppBridge`, with no MCP client behind it: every
 * request of the view goes through the rules in `policy.ts` and, where it
 * may, to the gateway. The view is third-party content. Nothing it sends is
 * put into the chat's page except as text.
 */

const HOST_INFO = { name: 'nerve', version: '0.0.0' };

/** The frame's height until the view reports one. */
const DEFAULT_FRAME_HEIGHT = 180;

/** After this long without a word from the proxy, the sandbox counts as out of reach. */
export const SANDBOX_WAIT_MS = 8_000;
/** A view that has its HTML but does not finish its start in this time has failed. */
export const APP_START_MS = 20_000;
/** How long a view gets to answer `ui/resource-teardown`. */
const TEARDOWN_MS = 1_000;

/** The largest document the chat hands to a frame; the gateway holds the same limit (5 MiB). */
const MAX_HTML_CHARS = 5 * 1024 * 1024;

const RESOURCE_DEADLINE_MS = 30_000;
const TOOLS_DEADLINE_MS = 15_000;
/** How long a document that is here waits for the tools of its server before the view starts without them. */
const TOOLS_WAIT_MS = 1_500;
/** Longer than the gateway's own deadline for a tool call, which answers 504. */
const TOOL_CALL_DEADLINE_MS = 130_000;

/** Requests of one view that may wait for the gateway at the same time. */
const MAX_IN_FLIGHT = 8;
/** Requests of one view that may wait for the person at the same time: one on screen, the rest behind it. */
const MAX_PROMPTS = 3;
/**
 * How long a question is on screen before a yes to it counts. One question
 * takes the place of another without the dialog moving, so the second click
 * of a double click, or a click meant for the page, would answer a question
 * that the person has not read.
 */
export const PROMPT_SETTLE_MS = 1_000;
/**
 * After this many refusals in a row, a view's requests of that kind are
 * refused without a question, until the view is loaded again: a view that
 * asks again each time it hears no does not get to hold the dialog up.
 */
export const MAX_REFUSALS = 3;
const REFUSED_TOO_OFTEN = 'The person refused three times in a row. Nerve asks no more for this view until it is loaded again.';
/** The server's tools are read again for an unknown tool, but not more often than this. */
const TOOLS_REREAD_MS = 5_000;
/** A view puts text into the composer at most this often; what comes sooner is refused. */
const MESSAGE_INTERVAL_MS = 1_000;
/** The column must hold still this long before the view hears of its new width. */
const RESIZE_DEBOUNCE_MS = 150;

/** A signal that aborts a request after `ms`; undefined where the runtime has none. */
function deadline(ms: number): AbortSignal | undefined {
  return typeof AbortSignal !== 'undefined' && typeof AbortSignal.timeout === 'function'
    ? AbortSignal.timeout(ms)
    : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * The frame height each view last had, so that a view started again for the
 * same call (a chat opened again, a retry) takes the room it had.
 */
const heights = new Map<string, number>();
const MAX_HEIGHTS = 200;

function rememberHeight(key: string, height: number): void {
  heights.delete(key);
  heights.set(key, height);
  if (heights.size > MAX_HEIGHTS) heights.delete(heights.keys().next().value!);
}

/**
 * Put `node` into `parent`. A node that holds a loaded frame moves in one
 * step (`moveBefore`), because a frame that leaves the document, even for a
 * moment, loads again; false where that is not possible.
 */
function moveInto(parent: Element, node: Element, holdsFrame: boolean): boolean {
  if (node.parentNode === parent) return true;
  if (holdsFrame && node.isConnected) {
    if (typeof parent.moveBefore !== 'function' || !parent.isConnected) return false;
    try {
      parent.moveBefore(node, null);
      return true;
    } catch {
      return false;
    }
  }
  parent.appendChild(node);
  return true;
}

interface Waiting {
  id: number;
  request: McpAppRequest;
  settle: (allowed: boolean) => void;
}

type RequestKind = McpAppRequest['kind'];

type Bridge = InstanceType<typeof AppBridge>;

export class McpAppHost implements McpAppHostHandle {
  private readonly options: McpAppHostOptions;
  private readonly key: string;
  /** Holds the frame. It is what moves between a block and the parking place. */
  private readonly root: HTMLDivElement;
  private readonly listeners = new Set<() => void>();
  private snapshot: McpAppSnapshot;

  private theme: ResolvedTheme;
  private call: McpAppToolCall | null = null;

  private started = false;
  private disposed = false;
  /** Counts the starts: what an earlier start still has under way is dropped. */
  private attempt = 0;

  private resource: McpAppResource | null = null;
  private tools: McpAppTool[] | null = null;
  private toolsError = '';
  private toolsReadAt = 0;

  private frame: HTMLIFrameElement | null = null;
  private bridge: Bridge | null = null;
  private keyboard: KeyboardGuard | null = null;
  /** The proxy of this frame has been given the document. */
  private handedOver = false;
  private initialized = false;
  private inputSent = false;
  private resultSent = false;

  private waitTimer: ReturnType<typeof setTimeout> | undefined;
  private startTimer: ReturnType<typeof setTimeout> | undefined;
  private resizeTimer: ReturnType<typeof setTimeout> | undefined;
  private themeTimer: ReturnType<typeof setTimeout> | undefined;
  private resizeObserver: ResizeObserver | null = null;
  private width = 0;

  private inFlight = 0;
  private readonly waiting: Waiting[] = [];
  private promptId = 0;
  /** The person's refusals in a row, by what was asked. */
  private refusals: Record<RequestKind, number> = { tool: 0, link: 0, message: 0 };
  private messageAt = 0;

  constructor(options: McpAppHostOptions) {
    this.options = options;
    this.key = hostKey(options.sessionId, options.toolUseId);
    this.theme = options.theme;
    this.root = document.createElement('div');
    this.root.setAttribute('data-mcp-app-frame', '');
    this.snapshot = {
      status: 'loading',
      step: 'resource',
      error: null,
      height: heights.get(this.key) ?? DEFAULT_FRAME_HEIGHT,
      bordered: true,
      prompt: null,
    };
  }

  // ---- What a block reads and does ----

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  getSnapshot = (): McpAppSnapshot => this.snapshot;

  attach(container: HTMLElement): boolean {
    if (this.disposed || !moveInto(container, this.root, this.frame !== null)) return false;
    // The width it kept while parked.
    this.root.style.width = '';
    if (!this.started) {
      this.started = true;
      void this.load();
    }
    return true;
  }

  park(lot: HTMLElement): boolean {
    if (this.disposed) return false;
    // Parked at the width it has, so the view does not lay itself out again.
    const width = this.root.offsetWidth;
    if (!moveInto(lot, this.root, this.frame !== null)) return false;
    if (width > 0) this.root.style.width = `${width}px`;
    return true;
  }

  setToolCall(call: McpAppToolCall): void {
    this.call = call;
    this.feed();
  }

  setTheme(theme: ResolvedTheme): void {
    if (theme === this.theme) return;
    this.theme = theme;
    // Told in a moment, not now. A block calls this while React applies the
    // new theme, and for that moment the page carries no theme at all (the
    // design system's provider takes its attribute off the page and puts it
    // back). The colours read from the stylesheet then are not the new
    // theme's, and they would stay under its name for as long as the page lives.
    clearTimeout(this.themeTimer);
    this.themeTimer = setTimeout(() => this.pushContext(), 0);
  }

  retry(): void {
    if (this.disposed) return;
    void this.load();
  }

  answer(id: number, allow: boolean): void {
    const first = this.waiting[0];
    const shown = this.snapshot.prompt;
    if (!first || first.id !== id || shown?.id !== id) return;
    // A yes that comes sooner was not given to this question (PROMPT_SETTLE_MS).
    if (allow && Date.now() < shown.readyAt) return;
    this.waiting.shift();
    const { request } = first;
    this.refusals[request.kind] = allow ? 0 : this.refusals[request.kind] + 1;
    // Opened here, in the person's click: a browser lets a page open a tab
    // only then. The view gets no handle to the tab, and the tab none to the chat.
    if (allow && request.kind === 'link') {
      window.open(request.url, '_blank', 'noopener,noreferrer');
    }
    first.settle(allow);
    // One refusal too many: what else of that kind waited behind it goes with it.
    if (this.muted(request.kind)) {
      for (const other of this.waiting.filter(w => w.request.kind === request.kind)) {
        this.waiting.splice(this.waiting.indexOf(other), 1);
        other.settle(false);
      }
    }
    this.publishPrompt();
  }

  async dispose(): Promise<void> {
    if (this.disposed) return;
    this.disposed = true;
    this.attempt += 1;
    // What waits for the person is answered no, while the view can still hear it.
    this.dismissPrompts();
    const bridge = this.bridge;
    if (bridge && this.initialized && this.frame?.isConnected) {
      // The view may save what it has. One that is gone, or silent, is not waited for.
      try {
        await bridge.teardownResource({}, { timeout: TEARDOWN_MS });
      } catch {
        // No answer: it goes all the same.
      }
    }
    this.closeFrame();
    this.root.remove();
    this.listeners.clear();
  }

  // ---- The start ----

  private update(change: Partial<McpAppSnapshot>): void {
    this.snapshot = { ...this.snapshot, ...change };
    for (const listener of [...this.listeners]) listener();
  }

  private fail(error: unknown): void {
    const failure = failureOf(error);
    this.closeFrame();
    this.update(failure.asleep
      ? { status: 'asleep', step: null, error: null, stopped: false }
      : { status: 'error', step: null, error: failure.text, stopped: false });
  }

  /** The view ran, and Nerve ends it: `reason` completes "The app was stopped: …". */
  private stop(reason: string): void {
    this.closeFrame();
    this.update({ status: 'error', step: null, error: reason, stopped: true });
  }

  private async readTools(): Promise<void> {
    this.toolsReadAt = Date.now();
    const { sessionId, app } = this.options;
    try {
      const { tools } = await api.getMcpAppTools(sessionId, app.server, deadline(TOOLS_DEADLINE_MS));
      this.tools = Array.isArray(tools) ? tools : [];
      this.toolsError = '';
    } catch (error) {
      // The view can still show its result; it cannot call a tool until a read works.
      this.toolsError = failureOf(error).text;
    }
  }

  private async load(): Promise<void> {
    const attempt = ++this.attempt;
    this.closeFrame();
    // A view that is loaded again may ask again.
    this.refusals = { tool: 0, link: 0, message: 0 };
    this.update({ status: 'loading', step: 'resource', error: null, stopped: false });
    const { sessionId, app } = this.options;
    const toolsRead = this.readTools();
    let resource: McpAppResource;
    try {
      resource = await api.getMcpAppResource(sessionId, app.server, app.resourceUri, deadline(RESOURCE_DEADLINE_MS));
    } catch (error) {
      if (attempt === this.attempt) this.fail(error);
      return;
    }
    // The tools name what the view may call and describe its own tool. A read
    // that is slow does not hold the view back: it lands when it lands.
    await Promise.race([toolsRead, new Promise<void>((resolve) => { setTimeout(resolve, TOOLS_WAIT_MS); })]);
    if (attempt !== this.attempt) return;
    if (!resource || typeof resource.html !== 'string' || !resource.html) {
      this.fail(new Error('The app has no document.'));
      return;
    }
    if (resource.html.length > MAX_HTML_CHARS) {
      this.fail(new Error("The app's document is too large."));
      return;
    }
    if (resource.mime_type !== RESOURCE_MIME_TYPE) {
      this.fail(new Error(`The app's document is of a kind that Nerve does not show (${String(resource.mime_type)}).`));
      return;
    }
    this.resource = resource;
    this.update({ bordered: resource.prefers_border !== false });
    await this.openFrame(attempt, resource);
  }

  private async openFrame(attempt: number, resource: McpAppResource): Promise<void> {
    const { app } = this.options;
    const csp = sanitizeCsp(resource.csp);
    const granted = grantedPermissions(resource.permissions);

    // No `sandbox` attribute: the gateway's header takes the proxy page's
    // origin away, and the page runs only when it has none (see sandbox.ts).
    const frame = document.createElement('iframe');
    const allow = allowAttribute(granted);
    if (allow) frame.setAttribute('allow', allow);
    // The proxy and the gateway learn which origin embeds the frame, and
    // nothing of the chat's address.
    frame.setAttribute('referrerpolicy', 'origin');
    frame.title = `App of ${app.server}`;
    frame.style.cssText = 'display:block;width:100%;border:0;background:transparent;';
    frame.style.height = `${this.snapshot.height}px`;

    const capabilities: McpUiHostCapabilities = {
      openLinks: {},
      serverTools: {},
      serverResources: {},
      logging: {},
      message: { text: {} },
      ...(granted ? { sandbox: { permissions: granted } } : {}),
    };
    const bridge = new AppBridge(null, HOST_INFO, capabilities, { hostContext: this.hostContext() });
    bridge.addEventListener('sandboxready', () => this.onProxyReady(attempt));
    bridge.addEventListener('initialized', () => this.onInitialized(attempt));
    bridge.addEventListener('sizechange', ({ height }) => this.onSize(attempt, height));
    bridge.addEventListener('loggingmessage', (params) => {
      console.debug(`[mcp-app ${app.server}]`, params.level, params.data);
    });
    // The signal of a request ends when the view stops waiting for the answer.
    bridge.oncalltool = (params, extra) => this.callTool(params, extra.mcpReq.signal);
    bridge.onreadresource = (params) => this.readResource(params);
    bridge.onopenlink = (params, extra) => this.openLink(params, extra.mcpReq.signal);
    bridge.onmessage = (params, extra) => this.message(params, extra.mcpReq.signal);
    bridge.onerror = (error) => {
      console.debug(`[mcp-app ${app.server}]`, error.message);
    };
    // The proxy document tells why it does nothing (`mcp-app-sandbox.html`).
    bridge.fallbackNotificationHandler = async (notification) => {
      if (notification.method === PROXY_REFUSED) this.onProxyRefused(attempt, notification.params);
    };

    this.frame = frame;
    this.bridge = bridge;
    this.root.appendChild(frame);
    const transport = new SandboxTransport(frame);
    // The keyboard stays where the person put it: a view that takes it
    // without the person having pressed in it is stopped (keyboard.ts).
    const keyboard = new KeyboardGuard(frame, this.root, {
      onTaken: () => {
        if (attempt === this.attempt) this.stop(TAKEN_REASON);
      },
      ask: () => transport.askPressed(),
    });
    transport.onpressed = (pressed) => keyboard.answer(pressed);
    this.keyboard = keyboard;
    keyboard.start();
    // Listening before the proxy loads, so its first word is not missed.
    await bridge.connect(transport);
    if (attempt !== this.attempt) return;
    // Only the origins of the view go into the address. The session token never does.
    frame.src = sandboxUrl(csp);
    this.update({ step: 'sandbox' });
    this.waitTimer = setTimeout(() => {
      if (attempt === this.attempt && !this.handedOver) this.fail(new Error('The app sandbox does not answer.'));
    }, SANDBOX_WAIT_MS);
  }

  /**
   * The proxy is ready: give it the view. Once. The chat loads the frame one
   * time, at an address whose `csp` it wrote itself, and that address sets
   * the policy the view runs under. A frame that says it is ready again was
   * loaded again by somebody else, at an address the chat did not choose: a
   * document handed to it would run under a policy of that somebody's making.
   */
  private onProxyReady(attempt: number): void {
    const { bridge, resource } = this;
    if (attempt !== this.attempt || !bridge || !resource) return;
    if (this.handedOver) {
      this.fail(new Error('The frame of the app loaded a second time, which a view may not do.'));
      return;
    }
    this.handedOver = true;
    clearTimeout(this.waitTimer);
    clearTimeout(this.startTimer);
    this.update({ status: 'loading', step: 'app' });
    const csp = sanitizeCsp(resource.csp);
    const permissions = grantedPermissions(resource.permissions);
    void bridge.sendSandboxResourceReady({
      html: resource.html,
      // An origin of its own for the view: see VIEW_SANDBOX.
      sandbox: VIEW_SANDBOX,
      ...(csp ? { csp } : {}),
      ...(permissions ? { permissions } : {}),
    });
    this.startTimer = setTimeout(() => {
      if (attempt === this.attempt && !this.initialized) this.fail(new Error('The app did not start in time.'));
    }, APP_START_MS);
  }

  /** The proxy does not work where it was served: a fault of the setup, shown as it is. */
  private onProxyRefused(attempt: number, params: unknown): void {
    // Only before the proxy is ready: later it is the view that speaks through it.
    if (attempt !== this.attempt || this.snapshot.step !== 'sandbox') return;
    const reason = isRecord(params) && typeof params.reason === 'string' ? params.reason.slice(0, 200) : '';
    this.fail(new Error(`The app sandbox refused to start${reason ? `: ${reason}` : '.'}`));
  }

  private onInitialized(attempt: number): void {
    if (attempt !== this.attempt || !this.bridge) return;
    clearTimeout(this.startTimer);
    this.initialized = true;
    this.update({ status: 'ready', step: null, error: null });
    this.watchWidth();
    // What changed while the view was starting, then the tool call it is for.
    this.pushContext();
    this.feed();
  }

  /** The view reports the size of its content: the frame takes its height, within limits. */
  private onSize(attempt: number, reported: unknown): void {
    if (attempt !== this.attempt || !this.frame) return;
    const height = clampFrameHeight(reported);
    if (height === null || height === this.snapshot.height) return;
    this.frame.style.height = `${height}px`;
    rememberHeight(this.key, height);
    this.update({ height });
  }

  // ---- What the view is told ----

  private toolDefinition(): Tool {
    const { app } = this.options;
    const known = this.tools?.find(t => t.name === app.tool);
    return {
      name: app.tool,
      // The gateway does not pass the schema on; any object is the honest one.
      inputSchema: { type: 'object' },
      ...(known ? { annotations: { readOnlyHint: known.read_only === true } } : {}),
      _meta: { ui: { resourceUri: app.resourceUri, ...(known ? { visibility: known.visibility } : {}) } },
    };
  }

  private hostContext(): McpUiHostContext {
    return {
      toolInfo: { id: this.options.toolUseId, tool: this.toolDefinition() },
      theme: this.theme,
      styles: { variables: hostStyleVariables() },
      displayMode: 'inline',
      availableDisplayModes: ['inline'],
      // The frame is as wide as the column; its height follows the view, up to a limit.
      containerDimensions: this.width > 0
        ? { width: this.width, maxHeight: MAX_FRAME_HEIGHT }
        : { maxHeight: MAX_FRAME_HEIGHT },
      locale: navigator.language,
      timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
      platform: 'web',
      deviceCapabilities: {
        touch: window.matchMedia?.('(pointer: coarse)').matches ?? false,
        hover: window.matchMedia?.('(hover: hover)').matches ?? true,
      },
    };
  }

  /** Tell a started view what changed around it; the bridge sends only the difference. */
  private pushContext(): void {
    if (!this.initialized || !this.bridge) return;
    this.bridge.setHostContext(this.hostContext());
  }

  private watchWidth(): void {
    if (this.resizeObserver || typeof ResizeObserver === 'undefined') return;
    this.resizeObserver = new ResizeObserver((entries) => {
      const width = Math.round(entries[entries.length - 1].contentRect.width);
      // Out of sight (a folded parent) it measures 0; what it measured last stands.
      if (width <= 0 || width === this.width) return;
      this.width = width;
      clearTimeout(this.resizeTimer);
      this.resizeTimer = setTimeout(() => this.pushContext(), RESIZE_DEBOUNCE_MS);
    });
    this.resizeObserver.observe(this.root);
  }

  /** The arguments once, then the result once: or word that the call ended without one. */
  private feed(): void {
    const { bridge, call } = this;
    if (!this.initialized || !bridge || !call) return;
    if (!this.inputSent) {
      this.inputSent = true;
      void bridge.sendToolInput({ arguments: call.input });
    }
    if (this.resultSent) return;
    if (call.result) {
      this.resultSent = true;
      void bridge.sendToolResult(call.result as CallToolResult);
    } else if (call.settled) {
      this.resultSent = true;
      void bridge.sendToolCancelled({ reason: call.failure || 'The tool call ended without a result.' });
    }
  }

  // ---- What the view asks for ----

  private async limited<T>(run: () => Promise<T>): Promise<T> {
    if (this.inFlight >= MAX_IN_FLIGHT) throw new Error('Too many requests of the app are under way.');
    this.inFlight += 1;
    try {
      return await run();
    } catch (error) {
      // The view gets the reason in words; the error itself stays on this side.
      throw new Error(failureOf(error).text, { cause: error });
    } finally {
      this.inFlight -= 1;
    }
  }

  /** `tools/call`: a tool of the view's own server, under the rules of `decideToolCall`. */
  private async callTool(params: CallToolRequest['params'], signal?: AbortSignal): Promise<CallToolResult> {
    const { sessionId, app } = this.options;
    const { attempt } = this;
    const name = params.name;
    const args = isRecord(params.arguments) ? params.arguments : {};
    let decision = decideToolCall(this.tools, name);
    if (decision.action === 'refuse' && !this.tools?.some(t => t.name === name)
      && Date.now() - this.toolsReadAt >= TOOLS_REREAD_MS) {
      // Not read yet, or the server has a tool it did not have then.
      await this.readTools();
      decision = decideToolCall(this.tools, name);
    }
    if (decision.action === 'refuse') {
      throw new Error(this.tools === null
        ? `The tools of ${app.server} could not be read${this.toolsError ? `: ${this.toolsError}` : ' yet.'}`
        : decision.reason);
    }
    if (decision.action === 'confirm') {
      if (this.muted('tool')) throw new Error(REFUSED_TOO_OFTEN);
      if (!(await this.ask({ kind: 'tool', tool: name, args }, signal))) {
        throw new Error('The tool call was not allowed.');
      }
    }
    // The view that asked was replaced or ended while this waited, or it stopped
    // waiting for the answer: nobody to call a tool for.
    if (attempt !== this.attempt) throw new Error('The app is gone.');
    if (signal?.aborted) throw new Error('The app withdrew the tool call.');
    const result = await this.limited(() => api.callMcpAppTool(
      sessionId, { server: app.server, tool: name, arguments: args }, deadline(TOOL_CALL_DEADLINE_MS),
    ));
    return result as CallToolResult;
  }

  /** `resources/read`: a `ui://` document of the view's own server, and nothing else. */
  private async readResource(params: ReadResourceRequest['params']): Promise<ReadResourceResult> {
    const { sessionId, app } = this.options;
    const uri = params.uri;
    if (typeof uri !== 'string' || !uri.startsWith('ui://')) {
      throw new Error('An app can read ui:// resources only.');
    }
    const resource = await this.limited(() => api.getMcpAppResource(
      sessionId, app.server, uri, deadline(RESOURCE_DEADLINE_MS),
    ));
    const ui = {
      ...(resource.csp ? { csp: resource.csp } : {}),
      ...(resource.permissions ? { permissions: resource.permissions } : {}),
      ...(typeof resource.prefers_border === 'boolean' ? { prefersBorder: resource.prefers_border } : {}),
    };
    return {
      contents: [{
        uri: resource.uri,
        mimeType: resource.mime_type,
        text: resource.html,
        ...(Object.keys(ui).length > 0 ? { _meta: { ui } } : {}),
      }],
    };
  }

  /** `ui/open-link`: an http(s) address, shown in full, opened when the person says so. */
  private async openLink(params: McpUiOpenLinkRequest['params'], signal?: AbortSignal): Promise<McpUiOpenLinkResult> {
    const url = httpLink(params.url);
    if (!url || !(await this.ask({ kind: 'link', url }, signal))) return { isError: true };
    return {};
  }

  /**
   * `ui/message`: text for the composer, which the person may then send.
   *
   * What goes out of the composer goes to the agent in the person's name, so
   * a view puts nothing there on its own. The person is shown the whole
   * text and decides. It goes in only when the composer is empty: next to
   * words of the person's own it would go out with them. And the focus
   * stays where it is: a composer that takes the focus while the person
   * types somewhere else gets their next Enter.
   */
  private async message(params: McpUiMessageRequest['params'], signal?: AbortSignal): Promise<McpUiMessageResult> {
    const text = messageText(params.content);
    const now = Date.now();
    if (text === null || now - this.messageAt < MESSAGE_INTERVAL_MS) return { isError: true };
    this.messageAt = now;
    if (this.options.composerHasText()) return { isError: true };
    if (!(await this.ask({ kind: 'message', text }, signal))) return { isError: true };
    // The person may have written something while the question was open.
    if (signal?.aborted || this.options.composerHasText()) return { isError: true };
    return (await this.options.insertText(text)) ? {} : { isError: true };
  }

  /**
   * Ask the person. False at once when too many requests already wait, and
   * as soon as `signal` ends: a view waits for an answer only so long (a
   * minute, unless it says otherwise) and then gives up on its request. Its
   * question goes with it, so that a late yes cannot call a tool or open a
   * link for a request that nobody waits for.
   */
  private ask(request: McpAppRequest, signal?: AbortSignal): Promise<boolean> {
    if (this.disposed || signal?.aborted || this.muted(request.kind) || this.waiting.length >= MAX_PROMPTS) {
      return Promise.resolve(false);
    }
    return new Promise<boolean>((settle) => {
      this.promptId += 1;
      const waiting: Waiting = { id: this.promptId, request, settle };
      this.waiting.push(waiting);
      signal?.addEventListener('abort', () => {
        const at = this.waiting.indexOf(waiting);
        if (at < 0) return;
        this.waiting.splice(at, 1);
        settle(false);
        this.publishPrompt();
      }, { once: true });
      this.publishPrompt();
    });
  }

  /** The person refused this kind of request MAX_REFUSALS times in a row: no more questions of it. */
  private muted(kind: RequestKind): boolean {
    return this.refusals[kind] >= MAX_REFUSALS;
  }

  /** Put the first waiting request on screen, when it is not there yet: one question at a time. */
  private publishPrompt(): void {
    const first = this.waiting[0];
    const shown = this.snapshot.prompt;
    if (!first) {
      if (shown) this.update({ prompt: null });
      return;
    }
    if (shown?.id === first.id) return;
    this.update({ prompt: { ...first.request, id: first.id, readyAt: Date.now() + PROMPT_SETTLE_MS } });
  }

  /** Nobody is left to ask: every waiting request is answered no. */
  private dismissPrompts(): void {
    for (const { settle } of this.waiting.splice(0)) settle(false);
    this.publishPrompt();
  }

  // ---- The end ----

  /** Drop the frame and its channel at once; what waits for the person is answered no. */
  private closeFrame(): void {
    clearTimeout(this.waitTimer);
    clearTimeout(this.startTimer);
    clearTimeout(this.resizeTimer);
    clearTimeout(this.themeTimer);
    this.resizeObserver?.disconnect();
    this.resizeObserver = null;
    this.dismissPrompts();
    this.keyboard?.stop();
    this.keyboard = null;
    const { bridge, frame } = this;
    this.bridge = null;
    this.frame = null;
    this.handedOver = false;
    this.initialized = false;
    this.inputSent = false;
    this.resultSent = false;
    void bridge?.close().catch(() => {});
    frame?.remove();
  }
}

/** Start the host of a view. It does nothing until a block attaches it. */
export function createHost(options: McpAppHostOptions): McpAppHostHandle {
  return new McpAppHost(options);
}
