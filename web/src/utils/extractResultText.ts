/**
 * Extract readable text from MCP content blocks (used by SubagentToolBlock and chatStore).
 * Claude gives the blocks as a JSON list, Codex as a JSON object with the list in `content`.
 * Other text comes back unchanged.
 */
export function extractResultText(result: string): string {
  try {
    const parsed = JSON.parse(result);
    const blocks = Array.isArray(parsed) ? parsed : Array.isArray(parsed?.content) ? parsed.content : null;
    if (blocks) {
      return blocks
        .filter((b: any) => b.type === 'text' && !b.text.startsWith('agentId:') && !b.text.startsWith('<usage>'))
        .map((b: any) => b.text)
        .join('\n');
    }
  } catch {
    // Not JSON
  }
  return result;
}
