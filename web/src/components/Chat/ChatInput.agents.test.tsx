import { act, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

/**
 * The composer while agents run: it always sends — while a
 * turn streams, Enter queues the message — and background agents alone never
 * put it into the streaming state.
 */

vi.mock('../../api/client', async (orig) => ({
  ...(await orig<typeof import('../../api/client')>()),
  api: new Proxy({} as Record<string, unknown>, {
    get: (target, key: string) => (target[key] ??= vi.fn(async () => ({}))),
  }),
}));
vi.mock('../../api/websocket', () => ({
  ws: {
    sendMessage: vi.fn(() => 'sent'), switchSession: vi.fn(), send: vi.fn(), connect: vi.fn(),
    subscribeAgent: vi.fn(() => 'sent'), unsubscribeAgent: vi.fn(() => 'sent'), onOpen: vi.fn(() => () => {}),
    takePendingMessages: vi.fn(() => []),
  },
}));

const { useAgentStore } = await import('../../stores/agentStore');
const { useChatStore } = await import('../../stores/chatStore');
const { FX_COMPLETED, FX_SESSION_ID, FX_WAVE, makeAgentStoreSeed } = await import('../../test/agentFixtures');
const { ChatInput } = await import('./ChatInput');

const onSend = vi.fn();
const onStop = vi.fn(async () => {});
const openAgentsPanel = vi.fn();

async function renderComposer(isStreaming: boolean) {
  const view = render(<ChatInput onSend={onSend} onStop={onStop} isStreaming={isStreaming} />);
  await act(async () => {});
  return view;
}

const textbox = () => screen.getByRole('textbox') as HTMLTextAreaElement;

beforeEach(() => {
  onSend.mockClear();
  onStop.mockClear();
  openAgentsPanel.mockClear();
  useChatStore.setState({
    activeSession: FX_SESSION_ID, drafts: {}, messages: [{ role: 'user', blocks: [{ type: 'text', content: 'go' }] }],
    sessions: [{ id: FX_SESSION_ID, title: 'Wave', source: 'web', updated_at: '2026-02-27T02:00:00Z', backend: 'claude', model: 'model-a' }],
    virtualSession: null, quotes: [],
    availableModels: [
      { id: 'model-a', provider: 'anthropic', backend: 'claude' },
      { id: 'model-b', provider: 'anthropic', backend: 'claude' },
    ],
    modelDefaults: { claude: 'model-a' },
    loadModels: vi.fn(async () => {}),
    openAgentsPanel,
  });
  useAgentStore.setState(makeAgentStoreSeed(FX_WAVE));
});

describe('ChatInput with agents', () => {
  it('stays a plain composer while only agents run', async () => {
    const user = userEvent.setup();
    await renderComposer(false);
    expect(textbox()).toBeEnabled();
    await user.type(textbox(), 'how is it going?');
    expect(screen.getByRole('button', { name: 'Send' })).toBeEnabled();
    expect(screen.queryByRole('button', { name: 'Queue message' })).toBeNull();
    expect(screen.queryByRole('button', { name: 'Stop the turn' })).toBeNull();
    expect(screen.getByRole('button', { name: 'Attach files' })).toBeEnabled();
  });

  it('queues with Enter while a turn streams', async () => {
    const user = userEvent.setup();
    await renderComposer(true);
    expect(textbox()).toBeEnabled();
    expect(screen.getByRole('button', { name: 'Stop the turn' })).toBeInTheDocument();
    const queue = screen.getByRole('button', { name: 'Queue message' });
    expect(queue).toBeDisabled();

    await user.type(textbox(), 'also run the e2e suite{Enter}');
    expect(onSend).toHaveBeenCalledWith('also run the e2e suite', undefined, undefined);
    expect(textbox()).toHaveValue('');
    // Only the turn-dependent controls wait for the turn.
    expect(screen.getByRole('button', { name: 'Attach files' })).toBeDisabled();
  });

  it('stops the turn first, then sends, on "Stop turn and send"', async () => {
    const user = userEvent.setup();
    const order: string[] = [];
    onStop.mockImplementation(async () => { order.push('stop'); });
    onSend.mockImplementation(() => { order.push('send'); });
    await renderComposer(true);
    await user.type(textbox(), 'now this instead');
    await user.click(screen.getByRole('button', { name: 'Stop options' }));
    await user.click(screen.getByRole('button', { name: /Stop turn and send/ }));
    await act(async () => {});
    expect(order).toEqual(['stop', 'send']);
    expect(onSend).toHaveBeenCalledWith('now this instead', undefined, undefined);
  });

  it('opens the Agents panel on ↓ in an empty composer', async () => {
    const user = userEvent.setup();
    await renderComposer(false);
    await user.click(textbox());
    await user.keyboard('{ArrowDown}');
    expect(openAgentsPanel).toHaveBeenCalledWith(true);
  });

  it('keeps ↓ for the text when the composer has some, or the session has no agents', async () => {
    const user = userEvent.setup();
    await renderComposer(false);
    await user.type(textbox(), 'draft');
    await user.keyboard('{ArrowDown}');
    expect(openAgentsPanel).not.toHaveBeenCalled();

    await user.clear(textbox());
    act(() => { useAgentStore.setState(makeAgentStoreSeed([])); });
    await user.keyboard('{ArrowDown}');
    expect(openAgentsPanel).not.toHaveBeenCalled();
  });

  it('locks the model picker while agents run', async () => {
    await renderComposer(false);
    const picker = screen.getByRole('combobox');
    expect(picker).toBeDisabled();
    expect(picker).toHaveAttribute('title', 'Switching model restarts the runtime and would stop 5 running agents');
  });

  it('frees the model picker once no agent runs', async () => {
    useAgentStore.setState(makeAgentStoreSeed([FX_COMPLETED]));
    await renderComposer(false);
    expect(screen.getByRole('combobox')).toBeEnabled();
  });

  it('applies text other surfaces put into the composer', async () => {
    const user = userEvent.setup();
    await renderComposer(false);
    act(() => { useAgentStore.getState().prefillComposer('Use SendMessage to tell `build-api`: '); });
    expect(textbox()).toHaveValue('Use SendMessage to tell `build-api`: ');
    await act(async () => { await new Promise(r => setTimeout(r, 0)); });
    expect(document.activeElement).toBe(textbox());

    await user.clear(textbox());
    await user.type(textbox(), 'my draft');
    act(() => { useAgentStore.getState().prefillComposer('restored text'); });
    expect(textbox()).toHaveValue('my draft\n\nrestored text');

    act(() => { useAgentStore.getState().prefillComposer('only this', 'replace'); });
    expect(textbox()).toHaveValue('only this');
  });
});
