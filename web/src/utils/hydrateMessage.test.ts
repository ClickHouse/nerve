import { describe, expect, it } from 'vitest';
import { hydrateMessage } from './hydrateMessage';

describe('hydrateMessage', () => {
  it('keeps the session a stored row is part of, and names none where the row names none', () => {
    expect(hydrateMessage({ id: 1, role: 'assistant', session_id: 'chat-a', blocks: [] }).session_id).toBe('chat-a');
    expect(hydrateMessage({ id: 2, role: 'user', session_id: 'chat-a', content: 'hi' }).session_id).toBe('chat-a');
    // A row this tab made itself, and what is no session id.
    expect('session_id' in hydrateMessage({ role: 'user', content: 'hi' })).toBe(false);
    expect('session_id' in hydrateMessage({ role: 'assistant', session_id: 7, blocks: [] })).toBe(false);
    expect('session_id' in hydrateMessage({ role: 'assistant', session_id: '', blocks: [] })).toBe(false);
  });

  it('maps a spawn block\'s agent_id to agentId', () => {
    const message = hydrateMessage({
      id: 7, role: 'assistant',
      blocks: [
        { type: 'tool_call', tool_use_id: 'toolu_a', tool: 'Agent', input: { description: 'Build the API' }, result: 'launched', agent_id: 'ag_3f9a1c2b7d4e' },
        { type: 'tool_call', tool_use_id: 'toolu_b', tool: 'Read', input: { file_path: 'a.py' } },
      ],
    });

    expect(message.blocks).toEqual([
      { type: 'tool_call', toolUseId: 'toolu_a', tool: 'Agent', input: { description: 'Build the API' }, result: 'launched', isError: undefined, status: 'complete', workflow: undefined, agentId: 'ag_3f9a1c2b7d4e' },
      { type: 'tool_call', toolUseId: 'toolu_b', tool: 'Read', input: { file_path: 'a.py' }, result: undefined, isError: undefined, status: 'complete', workflow: undefined },
    ]);
    expect('agentId' in message.blocks[1]).toBe(false);
  });

  it('maps the MCP Apps fields of a stored tool call', () => {
    const appResult = { content: [{ type: 'text', text: '10:00' }], structuredContent: { time: '10:00' } };
    const message = hydrateMessage({
      id: 8, role: 'assistant',
      blocks: [
        {
          type: 'tool_call', tool_use_id: 'toolu_app', tool: 'mcp__example-server__get-time', input: {}, result: '10:00',
          mcp_app: { server: 'example-server', tool: 'get-time', resource_uri: 'ui://example/view.html' },
          mcp_app_result: appResult,
        },
        { type: 'tool_call', tool_use_id: 'toolu_plain', tool: 'Read', input: { file_path: 'a.py' } },
      ],
    });

    expect(message.blocks[0]).toMatchObject({
      toolUseId: 'toolu_app', result: '10:00', status: 'complete',
      mcpApp: { server: 'example-server', tool: 'get-time', resourceUri: 'ui://example/view.html' },
      mcpAppResult: appResult,
    });
    expect('mcpApp' in message.blocks[1]).toBe(false);
    expect('mcpAppResult' in message.blocks[1]).toBe(false);
  });

  it('keeps the triggers of an auto marker, and reads old markers as before', () => {
    const triggers = [{ agent_id: 'ag_3f9a1c2b7d4e', label: 'build-chart', kind: 'agent', status: 'completed' }];

    expect(hydrateMessage({ role: 'assistant', blocks: [{ type: 'auto', triggers, more: 2 }] }).blocks)
      .toEqual([{ type: 'auto', triggers, more: 2 }]);
    expect(hydrateMessage({ role: 'assistant', blocks: [{ type: 'auto' }] }).blocks).toEqual([{ type: 'auto' }]);
  });

  it('maps nothing new on user rows', () => {
    const message = hydrateMessage({ role: 'user', content: 'hi', actor_id: 'act_owner', client_msg_id: 'c-1' });
    expect(message).toEqual({
      id: undefined, role: 'user', blocks: [{ type: 'text', content: 'hi' }],
      channel: undefined, created_at: undefined, native_turn_id: undefined, actor_id: 'act_owner',
    });
  });
});
