import { describe, expect, it } from 'vitest';
import type { WSMessage } from '../../api/websocket';
import type { MessageBlock } from '../../types/chat';
import type { AutoTurnTrigger } from '../../types/agents';
import { applyStreamEvent, rebuildPanelTabsFromBuffer } from './bufferReplay';

const SID = 's-1234';
const replay = (events: WSMessage[]): MessageBlock[] =>
  events.reduce<MessageBlock[]>((blocks, event) => applyStreamEvent(blocks, event), []);

const spawnInput = { description: 'Build the API', subagent_type: 'Plan', run_in_background: true };
const agentSpawn: WSMessage = { type: 'tool_use', session_id: SID, tool: 'Agent', tool_use_id: 'toolu_a', input: spawnInput };
const childEvents: WSMessage[] = [
  { type: 'thinking', session_id: SID, content: 'child thought', parent_tool_use_id: 'toolu_a' },
  { type: 'token', session_id: SID, content: 'child text', parent_tool_use_id: 'toolu_a' },
  { type: 'tool_use', session_id: SID, tool: 'Bash', input: { command: 'ls' }, tool_use_id: 'toolu_c', parent_tool_use_id: 'toolu_a' },
  { type: 'tool_result', session_id: SID, tool_use_id: 'toolu_c', result: 'out', parent_tool_use_id: 'toolu_a' },
];

describe('buffer replay', () => {
  it('creates no Agent tabs and keeps subagent work out of the main chat', () => {
    const events = [agentSpawn, ...childEvents];

    expect(rebuildPanelTabsFromBuffer(events)).toEqual({ panels: [], activePanelId: null });
    expect(replay(events)).toEqual([
      { type: 'tool_call', toolUseId: 'toolu_a', tool: 'Agent', input: spawnInput, status: 'running' },
    ]);
  });

  it('keeps Workflow tabs and settles them from their progress', () => {
    const events: WSMessage[] = [
      { type: 'tool_use', session_id: SID, tool: 'Workflow', input: { name: 'release-wave' }, tool_use_id: 'toolu_wf' },
      { type: 'tool_use', session_id: SID, tool: 'Workflow', input: { name: 'nested' }, tool_use_id: 'toolu_nested', parent_tool_use_id: 'toolu_a' },
      {
        type: 'workflow_progress', session_id: SID, tool_use_id: 'toolu_wf',
        workflow: { name: 'release-wave', status: 'completed', summary: 'Shipped', phases: [], agents: [] },
      },
    ];

    const { panels, activePanelId } = rebuildPanelTabsFromBuffer(events);

    expect(panels.map(p => [p.id, p.type, p.status, p.content])).toEqual([['toolu_wf', 'workflow', 'complete', 'Shipped']]);
    expect(activePanelId).toBe('toolu_wf');
  });

  it('keeps what triggered an autonomous turn', () => {
    const trigger: AutoTurnTrigger = { agent_id: 'ag_000000000001', label: 'build-chart', kind: 'agent', status: 'completed' };

    const blocks = replay([
      { type: 'token', session_id: SID, content: 'build-chart passed' },
      { type: 'auto_turn', session_id: SID, triggers: [trigger], more: 1 },
    ]);

    expect(blocks[0]).toEqual({ type: 'auto', triggers: [trigger], more: 1 });
    expect(replay([{ type: 'auto_turn', session_id: SID }])).toEqual([{ type: 'auto' }]);
  });
});
