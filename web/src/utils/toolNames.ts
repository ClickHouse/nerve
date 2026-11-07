/**
 * The parts of a tool name as the agent runtime reports it. An MCP tool is
 * `mcp__<server>__<tool>`, for example `mcp__nerve__ask_user`; a built-in tool
 * (`Bash`, `Read`) has no prefix.
 */

const MCP_PREFIX = 'mcp__';
const MCP_SEPARATOR = '__';

/** The end of the server part of an MCP tool name, or -1 for a built-in tool. */
function serverEnd(name: string): number {
  if (!name.startsWith(MCP_PREFIX)) return -1;
  return name.indexOf(MCP_SEPARATOR, MCP_PREFIX.length);
}

/** The tool's own name: `mcp__nerve__ask_user` gives `ask_user`, `Bash` stays `Bash`. */
export function toolBaseName(name: string): string {
  const end = serverEnd(name);
  return end > MCP_PREFIX.length ? name.slice(end + MCP_SEPARATOR.length) : name;
}

/** The MCP server of the tool: `mcp__nerve__ask_user` gives `nerve`; null for a built-in tool. */
export function toolServer(name: string): string | null {
  const end = serverEnd(name);
  return end > MCP_PREFIX.length ? name.slice(MCP_PREFIX.length, end) : null;
}
