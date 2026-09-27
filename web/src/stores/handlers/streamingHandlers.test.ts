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

vi.mock('../../api/client', () => ({ api: {
  getMessages: vi.fn().mockResolvedValue({ messages: [] }),
} }));
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
