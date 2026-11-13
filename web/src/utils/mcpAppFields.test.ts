import { describe, expect, it } from 'vitest';
import { mcpAppFields, mcpAppRef, mcpAppResult } from './mcpAppFields';

const APP = { server: 'example-server', tool: 'get-time', resource_uri: 'ui://example/view.html' };

describe('mcpAppRef', () => {
  it('reads the server, the tool and the address of the view', () => {
    expect(mcpAppRef(APP)).toEqual({ server: 'example-server', tool: 'get-time', resourceUri: 'ui://example/view.html' });
  });

  it.each([
    ['nothing', undefined],
    ['null', null],
    ['a string', 'ui://example/view.html'],
    ['a list', [APP]],
    ['no server', { ...APP, server: '' }],
    ['no tool', { ...APP, tool: undefined }],
    ['an address that is not ui://', { ...APP, resource_uri: 'https://example.com/view.html' }],
    ['an address that is not a string', { ...APP, resource_uri: 7 }],
    // Nerve's own tools have no views, and a view never calls them.
    ['the nerve server', { ...APP, server: 'nerve' }],
  ])('gives nothing for %s', (_, raw) => {
    expect(mcpAppRef(raw)).toBeUndefined();
  });
});

describe('mcpAppResult', () => {
  it('passes an MCP CallToolResult through', () => {
    const raw = {
      content: [{ type: 'text', text: '10:00' }],
      structuredContent: { time: '10:00' },
      isError: false,
      _meta: { 'nerve/truncated': true },
    };
    expect(mcpAppResult(raw)).toEqual(raw);
  });

  it('keeps only what is there', () => {
    expect(mcpAppResult({ content: [] })).toEqual({ content: [] });
  });

  it('makes a list of a content that is plain text, and of one that is absent', () => {
    expect(mcpAppResult({ content: 'plain' })).toEqual({ content: [{ type: 'text', text: 'plain' }] });
    expect(mcpAppResult({ structuredContent: { a: 1 } })).toEqual({ content: [], structuredContent: { a: 1 } });
  });

  it('drops content entries that are no blocks, and fields of a wrong type', () => {
    expect(mcpAppResult({ content: [{ type: 'text', text: 'a' }, 'b', null, { text: 'c' }], structuredContent: [1], isError: 'yes' }))
      .toEqual({ content: [{ type: 'text', text: 'a' }] });
  });

  it.each([undefined, null, 'text', 5, [1, 2]])('gives nothing for %j', (raw) => {
    expect(mcpAppResult(raw)).toBeUndefined();
  });
});

describe('mcpAppFields', () => {
  const result = { content: [{ type: 'text', text: '10:00' }], structuredContent: { time: '10:00' } };

  it('gives both fields of a stored block', () => {
    expect(mcpAppFields({ mcp_app: APP, mcp_app_result: result })).toEqual({
      mcpApp: { server: 'example-server', tool: 'get-time', resourceUri: 'ui://example/view.html' },
      mcpAppResult: result,
    });
  });

  it('gives the view alone while the call has no result', () => {
    expect(mcpAppFields({ mcp_app: APP })).toEqual({
      mcpApp: { server: 'example-server', tool: 'get-time', resourceUri: 'ui://example/view.html' },
    });
  });

  it('adds no key to an ordinary call, also when it carries a stray result', () => {
    expect(mcpAppFields({})).toEqual({});
    expect(Object.keys(mcpAppFields({ mcp_app_result: result }))).toEqual([]);
  });
});
