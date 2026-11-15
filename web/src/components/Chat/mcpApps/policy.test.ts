import { describe, expect, it } from 'vitest';
import type { McpAppTool } from '../../../types/mcpApps';
import {
  MAX_FRAME_HEIGHT, MAX_MESSAGE_CHARS, MIN_FRAME_HEIGHT, clampFrameHeight, decideToolCall, messageText,
} from './policy';

function tool(name: string, overrides: Partial<McpAppTool> = {}): McpAppTool {
  return { name, read_only: false, visibility: ['model', 'app'], resource_uri: null, ...overrides };
}

describe('decideToolCall', () => {
  const tools = [
    tool('refresh', { visibility: ['app'] }),
    tool('get-time', { read_only: true }),
    tool('delete-everything'),
    tool('summarize', { visibility: ['model'], read_only: true }),
  ];

  it('runs a tool made for views only at once', () => {
    expect(decideToolCall(tools, 'refresh')).toEqual({ action: 'run' });
  });

  it('runs a read-only tool at once', () => {
    expect(decideToolCall(tools, 'get-time')).toEqual({ action: 'run' });
  });

  it('asks the person for any other tool', () => {
    expect(decideToolCall(tools, 'delete-everything')).toEqual({ action: 'confirm' });
  });

  it('asks for a tool whose read-only mark is not exactly true', () => {
    expect(decideToolCall([tool('odd', { read_only: 'yes' as never })], 'odd')).toEqual({ action: 'confirm' });
  });

  it('refuses a tool that the server does not show to views, read-only or not', () => {
    expect(decideToolCall(tools, 'summarize')).toMatchObject({ action: 'refuse' });
  });

  it('refuses a tool that the server does not have', () => {
    expect(decideToolCall(tools, 'drop-table')).toMatchObject({ action: 'refuse' });
    // Not by a name of the runtime, and not a tool of another server.
    expect(decideToolCall(tools, 'mcp__example-server__get-time')).toMatchObject({ action: 'refuse' });
    expect(decideToolCall(tools, 'mcp__nerve__notify')).toMatchObject({ action: 'refuse' });
  });

  it('refuses every tool while the tools of the server are not known', () => {
    expect(decideToolCall(null, 'get-time')).toMatchObject({ action: 'refuse' });
    expect(decideToolCall([], 'get-time')).toMatchObject({ action: 'refuse' });
  });

  it.each([undefined, null, '', 7, { name: 'get-time' }])('refuses a call that names no tool: %j', (name) => {
    expect(decideToolCall(tools, name)).toMatchObject({ action: 'refuse' });
  });

  it('reads a missing visibility as the default: the model and views', () => {
    expect(decideToolCall([tool('plain', { visibility: undefined as never })], 'plain')).toEqual({ action: 'confirm' });
    expect(decideToolCall([tool('plain', { visibility: undefined as never, read_only: true })], 'plain')).toEqual({ action: 'run' });
  });
});

describe('clampFrameHeight', () => {
  it('keeps a height inside the limits, as whole pixels', () => {
    expect(clampFrameHeight(431)).toBe(431);
    expect(clampFrameHeight(430.2)).toBe(431);
  });

  it('never gives less than the minimum or more than the maximum', () => {
    expect(MIN_FRAME_HEIGHT).toBe(60);
    expect(MAX_FRAME_HEIGHT).toBe(800);
    expect(clampFrameHeight(0)).toBe(60);
    expect(clampFrameHeight(-500)).toBe(60);
    expect(clampFrameHeight(59.9)).toBe(60);
    expect(clampFrameHeight(800)).toBe(800);
    expect(clampFrameHeight(800.1)).toBe(800);
    expect(clampFrameHeight(1e9)).toBe(800);
  });

  it.each([undefined, null, '300', NaN, Infinity, -Infinity, {}])('gives nothing for %j, which is no height', (reported) => {
    expect(clampFrameHeight(reported)).toBeNull();
  });
});

describe('messageText', () => {
  it('joins the text blocks as paragraphs', () => {
    expect(messageText([{ type: 'text', text: 'Plot this.' }, { type: 'text', text: ' And that. ' }]))
      .toBe('Plot this.\n\nAnd that.');
  });

  it('leaves out what is not text', () => {
    expect(messageText([{ type: 'image', data: 'AAAA', mimeType: 'image/png' }, { type: 'text', text: 'Only this.' }]))
      .toBe('Only this.');
  });

  it.each([
    ['no list', { type: 'text', text: 'x' }],
    ['an empty list', []],
    ['no text block', [{ type: 'image', data: 'AAAA' }]],
    ['only blank text', [{ type: 'text', text: '   ' }]],
    ['a text that is not a string', [{ type: 'text', text: 7 }]],
    ['a null entry', [null]],
  ])('gives nothing for %s', (_, content) => {
    expect(messageText(content)).toBeNull();
  });

  it('gives nothing for more text than a person reads before they allow it', () => {
    expect(MAX_MESSAGE_CHARS).toBe(4000);
    expect(messageText([{ type: 'text', text: 'a'.repeat(MAX_MESSAGE_CHARS) }])).toHaveLength(MAX_MESSAGE_CHARS);
    expect(messageText([{ type: 'text', text: 'a'.repeat(MAX_MESSAGE_CHARS + 1) }])).toBeNull();
  });

  it('takes out runs of empty lines, which push what follows them out of sight', () => {
    const text = `Run this in the shell.${'\n'.repeat(30)}thanks`;
    expect(messageText([{ type: 'text', text }])).toBe('Run this in the shell.\n\nthanks');
    expect(messageText([{ type: 'text', text: 'one\r\n\r\n\r\n\r\ntwo\rthree' }])).toBe('one\n\ntwo\nthree');
    expect(messageText([{ type: 'text', text: 'one   \n \t \n   \n\ntwo' }])).toBe('one\n\ntwo');
  });

  it.each([
    ['a zero-width space', 'pay\u200Bload', 'payload'],
    ['a word joiner and a byte order mark', '\uFEFFpay\u2060load', 'payload'],
    ['a soft hyphen', 'pay\u00ADload', 'payload'],
    ['direction overrides, which show text in another order than it has', 'safe \u202Egnihton\u202C \u2066x\u2069', 'safe gnihton x'],
    ['control characters', 'pay\u0000\u0007\u001Bload\u007F', 'payload'],
    ['only hidden characters after the text', `text${'\n'.repeat(30)}\u200B`, 'text'],
  ])('takes out %s', (_, text, shown) => {
    expect(messageText([{ type: 'text', text }])).toBe(shown);
  });

  it('keeps what text is written with: line breaks, tabs, joiners of scripts and emoji', () => {
    const text = 'a\tb\nc\n\nd \u{1F468}\u200D\u{1F469}\u200D\u{1F467} \u0645\u06CC\u200C\u062E\u0648\u0627\u0647\u0645';
    expect(messageText([{ type: 'text', text }])).toBe(text);
  });

  it('gives nothing for text of hidden characters only', () => {
    expect(messageText([{ type: 'text', text: '\u200B\u200B\n\u2060' }])).toBeNull();
  });
});
