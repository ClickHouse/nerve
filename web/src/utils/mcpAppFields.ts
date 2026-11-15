import type { McpAppRef, McpCallToolResult, McpContentBlock } from '../types/mcpApps';

/**
 * The MCP Apps fields of a tool call, as the chat keeps them on its block.
 *
 * The gateway puts `mcp_app` on a `tool_use` event and `mcp_app_result` on
 * its `tool_result` event, and both on the stored `tool_call` block. This
 * reads them from any of the three. A value of another shape gives nothing,
 * so that the call shows as an ordinary tool call.
 */

/** The server of Nerve's own tools. It has no views, and a view never calls it. */
const OWN_SERVER = 'nerve';
const UI_SCHEME = 'ui://';

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** `mcp_app`: `{server, tool, resource_uri}`. */
export function mcpAppRef(raw: unknown): McpAppRef | undefined {
  if (!isRecord(raw)) return undefined;
  const { server, tool, resource_uri: resourceUri } = raw;
  if (typeof server !== 'string' || !server || server === OWN_SERVER) return undefined;
  if (typeof tool !== 'string' || !tool) return undefined;
  if (typeof resourceUri !== 'string' || !resourceUri.startsWith(UI_SCHEME)) return undefined;
  return { server, tool, resourceUri };
}

/** `mcp_app_result`: an MCP `CallToolResult`. Its `content` is always a list here. */
export function mcpAppResult(raw: unknown): McpCallToolResult | undefined {
  if (!isRecord(raw)) return undefined;
  const { content } = raw;
  const blocks: McpContentBlock[] = Array.isArray(content)
    ? content.filter((block): block is McpContentBlock => isRecord(block) && typeof block.type === 'string')
    : typeof content === 'string' ? [{ type: 'text', text: content }] : [];
  return {
    content: blocks,
    ...(isRecord(raw.structuredContent) ? { structuredContent: raw.structuredContent } : {}),
    ...(typeof raw.isError === 'boolean' ? { isError: raw.isError } : {}),
    ...(isRecord(raw._meta) ? { _meta: raw._meta } : {}),
  };
}

/**
 * What a stored block adds to the chat's block; no key for an absent field.
 * A result counts only on a call that has a view.
 */
export function mcpAppFields(raw: { mcp_app?: unknown; mcp_app_result?: unknown }): {
  mcpApp?: McpAppRef;
  mcpAppResult?: McpCallToolResult;
} {
  const mcpApp = mcpAppRef(raw.mcp_app);
  if (!mcpApp) return {};
  const result = mcpAppResult(raw.mcp_app_result);
  return { mcpApp, ...(result ? { mcpAppResult: result } : {}) };
}
