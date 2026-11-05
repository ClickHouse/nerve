// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { WSMessage } from '../../api/websocket';

// The store reads localStorage at module init. See chatStore.test.ts.
function installStorage(): void {
  const data = new Map<string, string>();
  const storage = {
    getItem: (k: string) => (data.has(k) ? data.get(k)! : null),
    setItem: (k: string, v: string) => void data.set(k, String(v)),
    removeItem: (k: string) => void data.delete(k),
    clear: () => data.clear(),
    key: (i: number) => [...data.keys()][i] ?? null,
    get length() { return data.size; },
  };
  for (const target of [globalThis, globalThis.window]) {
    if (target) Object.defineProperty(target, 'localStorage', { value: storage, configurable: true, writable: true });
  }
}
installStorage();

vi.mock('../../api/client', () => ({
  api: {
    getMessages: vi.fn().mockResolvedValue({ messages: [] }),
    // handleDone refreshes the session list.
    listSessions: vi.fn().mockResolvedValue({ sessions: [], archived_count: 0, system_count: 0, has_more: false, next_offset: 0 }),
    // switchSession loads the agent roster.
    getSessionAgents: vi.fn(async (sessionId: string) => ({ session_id: sessionId, agents: [], queue: [] })),
  },
  // chatStore reads these authStore dependencies when it stamps optimistic rows.
  getToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));
vi.mock('../../api/websocket', () => ({
  ws: { switchSession: vi.fn(), send: vi.fn(), connect: vi.fn() },
}));

// Animation frames run only when a test calls runFrame().
const frames = new Map<number, FrameRequestCallback>();
let nextFrame = 1;
vi.stubGlobal('requestAnimationFrame', (cb: FrameRequestCallback) => {
  frames.set(nextFrame, cb);
  return nextFrame++;
});
vi.stubGlobal('cancelAnimationFrame', (id: number) => { frames.delete(id); });
function runFrame(): void {
  const callbacks = [...frames.values()];
  frames.clear();
  callbacks.forEach(cb => cb(0));
}

const { useChatStore } = await import('../chatStore');

const send = (msg: WSMessage) => useChatStore.getState().handleWSMessage(msg);
const token = (content: string): WSMessage => ({ type: 'token', session_id: 's1', content });
const thinking = (content: string): WSMessage => ({ type: 'thinking', session_id: 's1', content });

beforeEach(() => {
  runFrame();
  useChatStore.setState({
    activeSession: 's1',
    isStreaming: true,
    streamingBlocks: [],
    panels: [],
    agentStatus: { state: 'idle' },
  });
});

describe('stream delta batching', () => {
  it('applies the tokens of one frame as one update', () => {
    send(token('Hello'));
    send(token(', '));
    send(token('world'));
    expect(useChatStore.getState().streamingBlocks).toEqual([]);

    runFrame();

    expect(useChatStore.getState().streamingBlocks).toEqual([{ type: 'text', content: 'Hello, world' }]);
    expect(useChatStore.getState().agentStatus).toEqual({ state: 'writing' });
  });

  it('keeps the order of thinking, text, and other events', () => {
    send(thinking('Plan'));
    send(token('Reading'));
    send({ type: 'tool_use', session_id: 's1', tool: 'Read', input: {}, tool_use_id: 't1' });
    send(token('Done'));
    runFrame();

    const blocks = useChatStore.getState().streamingBlocks;
    expect(blocks.map(b => b.type)).toEqual(['thinking', 'text', 'tool_call', 'text']);
    expect(blocks[1]).toEqual({ type: 'text', content: 'Reading' });
    expect(blocks[3]).toEqual({ type: 'text', content: 'Done' });
  });

  it('drops queued deltas when the active session changes before the frame', () => {
    send(token('stale'));
    useChatStore.setState({ activeSession: 's2', streamingBlocks: [] });

    runFrame();

    expect(useChatStore.getState().streamingBlocks).toEqual([]);
  });

  it('does not revive old deltas after switching away and back to an idle session', async () => {
    send(token('stale'));
    const toOther = useChatStore.getState().switchSession('s2');
    const back = useChatStore.getState().switchSession('s1');
    await Promise.all([toOther, back]);

    runFrame();
    send({ type: 'session_status', session_id: 's1', is_running: false });

    expect(useChatStore.getState().streamingBlocks).toEqual([]);
    expect(useChatStore.getState().isStreaming).toBe(false);
    expect(useChatStore.getState().agentStatus).toEqual({ state: 'idle' });
  });

  it('keeps the same agentStatus object while the agent state does not change', () => {
    send(token('a'));
    runFrame();
    const status = useChatStore.getState().agentStatus;

    send(token('b'));
    runFrame();

    expect(useChatStore.getState().agentStatus).toBe(status);
  });
});

describe('agent spawns and subagent events', () => {
  const agentInput = { description: 'Build the API', subagent_type: 'general-purpose', prompt: 'Build it', run_in_background: true };

  it('shows an Agent call as a spawn block and opens no panel', () => {
    send({ type: 'tool_use', session_id: 's1', tool: 'Agent', input: agentInput, tool_use_id: 'toolu_a' });

    expect(useChatStore.getState().streamingBlocks).toEqual([
      { type: 'tool_call', toolUseId: 'toolu_a', tool: 'Agent', input: agentInput, status: 'running' },
    ]);
    expect(useChatStore.getState().panels).toEqual([]);
    expect(useChatStore.getState().panelVisible).toBe(false);

    // The async-launch result completes the card; still no panel.
    send({ type: 'tool_result', session_id: 's1', tool_use_id: 'toolu_a', result: 'Async agent launched', is_error: false });
    expect(useChatStore.getState().streamingBlocks[0]).toMatchObject({ status: 'complete', result: 'Async agent launched' });
    expect(useChatStore.getState().panels).toEqual([]);
  });

  it('drops parent-tagged deltas, tool calls and results', () => {
    send(token('main text'));
    send({ type: 'token', session_id: 's1', content: 'child text', parent_tool_use_id: 'toolu_a' });
    send({ type: 'thinking', session_id: 's1', content: 'child thought', parent_tool_use_id: 'toolu_a' });
    send({ type: 'tool_use', session_id: 's1', tool: 'Bash', input: { command: 'ls' }, tool_use_id: 'toolu_child', parent_tool_use_id: 'toolu_a' });
    send({ type: 'tool_use', session_id: 's1', tool: 'Agent', input: agentInput, tool_use_id: 'toolu_nested', parent_tool_use_id: 'toolu_a' });
    send({ type: 'tool_use', session_id: 's1', tool: 'Workflow', input: { name: 'wf' }, tool_use_id: 'toolu_wf', parent_tool_use_id: 'toolu_a' });
    send({ type: 'tool_result', session_id: 's1', tool_use_id: 'toolu_child', result: 'out', is_error: false, parent_tool_use_id: 'toolu_a' });
    runFrame();

    expect(useChatStore.getState().streamingBlocks).toEqual([{ type: 'text', content: 'main text' }]);
    expect(useChatStore.getState().panels).toEqual([]);
  });

  it('keeps the triggers on the auto-turn marker', () => {
    useChatStore.setState({ isStreaming: false });
    const trigger = { agent_id: 'ag_000000000001', label: 'build-chart', kind: 'agent' as const, status: 'completed' as const };

    send({ type: 'auto_turn', session_id: 's1', triggers: [trigger], more: 2 });

    expect(useChatStore.getState().streamingBlocks).toEqual([{ type: 'auto', triggers: [trigger], more: 2 }]);
    expect(useChatStore.getState().isStreaming).toBe(true);
  });

  it('appends nothing for a turn that only carried its auto marker', () => {
    useChatStore.setState({ messages: [], isStreaming: false });
    send({ type: 'auto_turn', session_id: 's1' });

    send({ type: 'done', session_id: 's1' });

    expect(useChatStore.getState().messages).toEqual([]);
    expect(useChatStore.getState().streamingBlocks).toEqual([]);
    expect(useChatStore.getState().isStreaming).toBe(false);
  });

  it('keeps a background turn that said something', () => {
    useChatStore.setState({ messages: [], isStreaming: false });
    send({ type: 'auto_turn', session_id: 's1' });
    send(token('build-chart passed'));
    send({ type: 'done', session_id: 's1' });

    expect(useChatStore.getState().messages.map(m => m.blocks.map(b => b.type))).toEqual([['auto', 'text']]);
  });

  it('settles only plan tabs when the turn ends', () => {
    const tab = (id: string, type: 'plan' | 'workflow' | 'agents') => ({
      id, type, label: id, subagentType: id, description: '', content: null, prompt: '',
      streaming: true, status: 'running' as const, startedAt: 1, blocks: [],
    });
    useChatStore.setState({ panels: [tab('p', 'plan'), tab('w', 'workflow'), tab('agents-panel', 'agents')] });

    send({ type: 'stopped', session_id: 's1' });

    expect(useChatStore.getState().panels.map(p => [p.id, p.status])).toEqual([
      ['p', 'complete'], ['w', 'running'], ['agents-panel', 'running'],
    ]);
  });
});
