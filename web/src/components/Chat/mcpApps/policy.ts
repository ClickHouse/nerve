import type { McpAppTool, McpContentBlock } from '../../../types/mcpApps';

/**
 * What the chat lets a view do, decided here and nowhere else.
 *
 * A view is third-party content that asks the chat for things: to call a
 * tool of its own server, to open a link, to put text into the composer, to
 * be a certain height. Each rule is a function of the request alone, so it
 * can be read and tested without a frame.
 */

/** The frame of a view is never lower or higher than this, whatever the view reports. */
export const MIN_FRAME_HEIGHT = 60;
export const MAX_FRAME_HEIGHT = 800;

/** The height of the frame for a height that the view reports; null for a report that is no height. */
export function clampFrameHeight(reported: unknown): number | null {
  if (typeof reported !== 'number' || !Number.isFinite(reported)) return null;
  return Math.min(MAX_FRAME_HEIGHT, Math.max(MIN_FRAME_HEIGHT, Math.ceil(reported)));
}

export type ToolCallDecision =
  /** Run it at once. */
  | { action: 'run' }
  /** Ask the person first. */
  | { action: 'confirm' }
  /** Never: the reason goes back to the view. */
  | { action: 'refuse'; reason: string };

/**
 * Whether a view may call the tool `name` of its own server, given the
 * server's tools (`GET .../mcp-apps/tools`).
 *
 * - A tool that the server does not have, or does not show to views, is
 *   refused. The gateway refuses it too.
 * - A tool made for views only (hidden from the model), or one the server
 *   marks read-only, runs at once: a Refresh button must not ask each time.
 * - Any other tool is one the model could call, and it may change something:
 *   the person decides, for this one call.
 */
export function decideToolCall(tools: readonly McpAppTool[] | null, name: unknown): ToolCallDecision {
  if (typeof name !== 'string' || !name) return { action: 'refuse', reason: 'The tool call names no tool.' };
  const tool = tools?.find(t => t.name === name);
  if (!tool) return { action: 'refuse', reason: `The server has no tool named ${name}.` };
  const visibility = Array.isArray(tool.visibility) ? tool.visibility : ['model', 'app'];
  if (!visibility.includes('app')) return { action: 'refuse', reason: `The tool ${name} is not available to apps.` };
  if (!visibility.includes('model') || tool.read_only === true) return { action: 'run' };
  return { action: 'confirm' };
}

/**
 * The longest text a view may ask to put into the composer. The person reads
 * all of it in a dialog before it goes there: more than this is more than
 * anybody reads.
 */
export const MAX_MESSAGE_CHARS = 4_000;

/**
 * Characters that show nothing, or change the order in which text shows
 * (control characters, zero-width spaces, direction overrides). In text that
 * a person is asked to read and allow they can only hide something. Line
 * breaks and tabs stay, and so do the two joiners that some scripts and
 * emoji are written with.
 */
const HIDDEN = /(?![\n\t\u200C\u200D])[\p{Cc}\p{Cf}]/gu;

/**
 * The text of a `ui/message` request, as the person is shown it and as it
 * goes into the composer: its text blocks, one paragraph each, without
 * hidden characters, and without runs of empty lines (which push what
 * follows them out of sight). Null when there is none, or more than
 * MAX_MESSAGE_CHARS.
 */
export function messageText(content: unknown): string | null {
  if (!Array.isArray(content)) return null;
  const text = (content as McpContentBlock[])
    .filter(block => block && block.type === 'text' && typeof block.text === 'string')
    .map(block => (block.text as string).replace(/\r\n?/g, '\n').replace(HIDDEN, '').trim())
    .filter(Boolean)
    .join('\n\n')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n');
  return text && text.length <= MAX_MESSAGE_CHARS ? text : null;
}
