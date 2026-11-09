import { describe, expect, it } from 'vitest';
import { toolBaseName, toolServer } from './toolNames';

describe('toolBaseName', () => {
  it('strips the MCP server prefix', () => {
    expect(toolBaseName('mcp__nerve__ask_user')).toBe('ask_user');
    expect(toolBaseName('mcp__plugin_docs_docs__search_pages')).toBe('search_pages');
  });

  it('keeps a built-in tool name', () => {
    expect(toolBaseName('Bash')).toBe('Bash');
    expect(toolBaseName('ask_user')).toBe('ask_user');
  });

  it('keeps a name that only looks like a prefix', () => {
    expect(toolBaseName('mcp__nerve')).toBe('mcp__nerve');
    expect(toolBaseName('mcp____tool')).toBe('mcp____tool');
  });

  it('keeps a double underscore inside the tool part', () => {
    expect(toolBaseName('mcp__nerve__a__b')).toBe('a__b');
  });
});

describe('toolServer', () => {
  it('gives the MCP server', () => {
    expect(toolServer('mcp__nerve__notify')).toBe('nerve');
    expect(toolServer('mcp__plugin_docs_docs__search_pages')).toBe('plugin_docs_docs');
  });

  it('gives null for a built-in tool', () => {
    expect(toolServer('Read')).toBeNull();
    expect(toolServer('mcp__nerve')).toBeNull();
  });
});
