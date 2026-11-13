/**
 * MCP Apps (MCP extension `io.modelcontextprotocol/ui`): what the gateway
 * tells the chat about a tool call that has an interactive view, and the
 * answers of the `/api/.../mcp-apps/` routes.
 *
 * Objects that the gateway passes through from an MCP server keep the names
 * of the protocol (`structuredContent`, `connectDomains`); the gateway's own
 * fields are snake_case.
 */

/** A tool call linked to a `ui://` resource: `mcp_app` on the tool event and on the stored block. */
export interface McpAppRef {
  /** The MCP server, by the name Nerve configured it under. */
  server: string;
  /** The tool's own name, without the `mcp__<server>__` prefix. */
  tool: string;
  /** The `ui://` resource that holds the view. */
  resourceUri: string;
}

/** One content block of an MCP result (`text`, `image`, `resource`, ...). */
export type McpContentBlock = { type: string } & Record<string, unknown>;

/** An MCP `CallToolResult`: `mcp_app_result` on the tool event and block, and the answer of a tool call made for a view. */
export interface McpCallToolResult {
  content: McpContentBlock[];
  structuredContent?: Record<string, unknown>;
  isError?: boolean;
  _meta?: Record<string, unknown>;
}

/** The origins a view may load from (`_meta.ui.csp` of its resource). */
export interface McpUiCsp {
  connectDomains?: string[];
  resourceDomains?: string[];
  frameDomains?: string[];
  baseUriDomains?: string[];
}

/** The browser features a view asks for (`_meta.ui.permissions` of its resource). */
export interface McpUiPermissions {
  camera?: object;
  microphone?: object;
  geolocation?: object;
  clipboardWrite?: object;
}

/** `GET /api/mcp-apps/config`. */
export interface McpAppsConfig {
  enabled: boolean;
}

/** `GET /api/sessions/{id}/mcp-apps/resource`: the document of a view. */
export interface McpAppResource {
  uri: string;
  mime_type: string;
  html: string;
  csp: McpUiCsp | null;
  permissions: McpUiPermissions | null;
  prefers_border: boolean | null;
  /** Served from the gateway's copy, because the session has no live runtime. */
  cached: boolean;
}

/** One tool of `GET /api/sessions/{id}/mcp-apps/tools`. */
export interface McpAppTool {
  name: string;
  /** The server marks the tool as one that changes nothing. */
  read_only: boolean;
  /** Who may call it: the model, a view of the same server, or both. */
  visibility: Array<'model' | 'app'>;
  resource_uri: string | null;
}
