import type { ResolvedTheme } from '../../../stores/themeStore';
import type { McpAppRef, McpCallToolResult } from '../../../types/mcpApps';

/**
 * What a chat block and the host of its view (`host.ts`) say to each other.
 * Kept apart from the host, so that the block, and a chat that never shows a
 * view, load none of it.
 */

/**
 * What a view asks that the person decides: to call a tool, to open a link,
 * or to put text into the message field.
 */
export type McpAppRequest =
  | { kind: 'tool'; tool: string; args: Record<string, unknown> }
  | { kind: 'link'; url: string }
  | { kind: 'message'; text: string };

/**
 * A request of a view that waits for the person's decision. A yes counts
 * from `readyAt` on (a time in milliseconds, a moment after the question
 * came on screen): a click that comes sooner was aimed at what was there
 * before it.
 */
export type McpAppPrompt = McpAppRequest & { id: number; readyAt: number };

/** What a block shows of its view. A new object for each change. */
export interface McpAppSnapshot {
  /** `loading`: the document, the sandbox or the view's start is under way. */
  status: 'loading' | 'ready' | 'error' | 'asleep';
  /** What `loading` waits for. */
  step: 'resource' | 'sandbox' | 'app' | null;
  /** Why it failed (`error`). */
  error: string | null;
  /** True when the view ran and Nerve stopped it (`error` says why): it did not fail to load. */
  stopped?: boolean;
  /** The height of the frame, in pixels. */
  height: number;
  /** False when the view asks for no border and no background. */
  bordered: boolean;
  /** The request that waits for the person, when there is one. */
  prompt: McpAppPrompt | null;
}

/** The state of the tool call a view was opened for, as its block knows it. */
export interface McpAppToolCall {
  input: Record<string, unknown>;
  /** The result, once the call has one. */
  result?: McpCallToolResult;
  /** The call is over, with or without a result. */
  settled: boolean;
  /** Why a call that is over has no result, in a few words. */
  failure?: string;
}

/** A running view, as a block and the registry (`registry.ts`) use it; `host.ts` implements it. */
export interface McpAppHostHandle {
  subscribe(listener: () => void): () => void;
  getSnapshot(): McpAppSnapshot;
  /** Show the view in `container`. False when it cannot move there. */
  attach(container: HTMLElement): boolean;
  /** Keep the view alive out of sight, in `lot`. False when it cannot. */
  park(lot: HTMLElement): boolean;
  setToolCall(call: McpAppToolCall): void;
  setTheme(theme: ResolvedTheme): void;
  /** Start again from the document. */
  retry(): void;
  /** The person's decision on the prompt `id`. */
  answer(id: number, allow: boolean): void;
  /** Tell the view to end, then remove it. */
  dispose(): Promise<void>;
}

/** What a view is started with. */
export interface McpAppHostOptions {
  sessionId: string;
  toolUseId: string;
  app: McpAppRef;
  theme: ResolvedTheme;
  /** True while the chat's composer holds text. */
  composerHasText: () => boolean;
  /**
   * Put text into the chat's composer, which must be empty, and leave the
   * focus where it is; the person sends it, or not. False when the text did
   * not go in.
   */
  insertText: (text: string) => Promise<boolean>;
}
