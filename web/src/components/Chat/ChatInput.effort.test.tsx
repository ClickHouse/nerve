import { act, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

/**
 * The composer's per-chat effort picker. It binds like the model picker: a
 * new (virtual) chat keeps its pick in the store until the session is
 * created, an existing chat PATCHes its own row. Levels the chat's model
 * doesn't take are disabled, and it locks while agents run, because a change
 * restarts the runtime.
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

const { api } = await import('../../api/client');
const { useAgentStore } = await import('../../stores/agentStore');
const { useChatStore } = await import('../../stores/chatStore');
const { FX_COMPLETED, FX_SESSION_ID, FX_WAVE, makeAgentStoreSeed } = await import('../../test/agentFixtures');
const { ChatInput } = await import('./ChatInput');

const VIRTUAL_ID = 'virtual-chat';
const NOW = '2026-11-05T12:00:00Z';

async function renderComposer() {
  const view = render(<ChatInput onSend={vi.fn()} onStop={vi.fn(async () => {})} isStreaming={false} />);
  await act(async () => {});
  return view;
}

const picker = () => screen.getByRole('combobox', { name: 'Effort' }) as HTMLSelectElement;
const option = (name: string) => within(picker()).getByRole('option', { name }) as HTMLOptionElement;

/** Point the active real chat at ``model`` with ``effort`` stored on its row. */
function realChat(model: string, effort: string | null = null) {
  useChatStore.setState({
    activeSession: FX_SESSION_ID,
    virtualSession: null,
    sessions: [{ id: FX_SESSION_ID, title: 'Wave', source: 'web', updated_at: NOW, backend: 'claude', model, effort }],
  });
}

beforeEach(() => {
  vi.mocked(api.updateSession).mockClear();
  useChatStore.setState({
    drafts: {}, messages: [{ role: 'user', blocks: [{ type: 'text', content: 'go' }] }],
    archivedSessions: null, systemSessions: null, quotes: [],
    availableModels: [
      { id: 'model-a', provider: 'anthropic', backend: 'claude' },
      { id: 'model-b', provider: 'anthropic', backend: 'claude' },
      { id: 'llama3', provider: 'ollama', backend: 'claude' },
    ],
    modelDefaults: { claude: 'model-a' },
    newChatModels: {}, newChatEffort: null,
    effortDefault: 'max',
    // model-a is absent: it takes every level.
    effortLevels: { 'model-b': ['low', 'medium', 'high'], llama3: [] },
    loadModels: vi.fn(async () => {}),
  });
  realChat('model-a');
  // A finished agent only: nothing is live.
  useAgentStore.setState(makeAgentStoreSeed([FX_COMPLETED]));
});

describe('ChatInput effort picker', () => {
  it("starts on the server's default and says subagents inherit it", async () => {
    await renderComposer();
    expect(picker()).toBeEnabled();
    expect(picker()).toHaveValue('');
    expect(option('Default (max)').selected).toBe(true);
    expect(picker()).toHaveAttribute('title', 'Reasoning effort for this chat; subagents inherit it (other chats keep theirs)');
    for (const level of ['low', 'medium', 'high', 'xhigh', 'max']) expect(option(level)).toBeEnabled();
  });

  it('keeps a new chat\'s pick in the store until the session is created', async () => {
    const user = userEvent.setup();
    useChatStore.setState({
      virtualSession: { id: VIRTUAL_ID, title: '', source: 'web', status: 'created', updated_at: NOW, is_running: false },
      activeSession: VIRTUAL_ID, messages: [],
    });
    await renderComposer();

    await user.selectOptions(picker(), 'medium');
    expect(useChatStore.getState().newChatEffort).toBe('medium');
    expect(picker()).toHaveValue('medium');

    await user.selectOptions(picker(), 'Default (max)');
    expect(useChatStore.getState().newChatEffort).toBeNull();
    expect(api.updateSession).not.toHaveBeenCalled();
  });

  it('PATCHes an existing chat\'s own row', async () => {
    const user = userEvent.setup();
    await renderComposer();

    await user.selectOptions(picker(), 'high');
    expect(api.updateSession).toHaveBeenLastCalledWith(FX_SESSION_ID, { effort: 'high' });
    expect(useChatStore.getState().sessions[0].effort).toBe('high');
    expect(picker()).toHaveValue('high');

    await user.selectOptions(picker(), 'Default (max)');
    expect(api.updateSession).toHaveBeenLastCalledWith(FX_SESSION_ID, { effort: null });
    expect(useChatStore.getState().sessions[0].effort).toBeNull();
  });

  it('locks while agents run', async () => {
    useAgentStore.setState(makeAgentStoreSeed(FX_WAVE));
    await renderComposer();
    expect(picker()).toBeDisabled();
    expect(picker()).toHaveAttribute('title', 'Switching effort restarts the runtime and would stop 5 running agents');
  });

  it("disables the levels the chat's model doesn't take", async () => {
    realChat('model-b');
    await renderComposer();
    expect(option('low')).toBeEnabled();
    expect(option('medium')).toBeEnabled();
    expect(option('high')).toBeEnabled();
    expect(option('xhigh')).toBeDisabled();
    expect(option('max')).toBeDisabled();
    // The default as this model runs it: max is capped to high.
    expect(option('Default (high)').selected).toBe(true);
  });

  it('keeps a stored level the model does not take, and says what it runs at', async () => {
    realChat('model-b', 'max');
    await renderComposer();
    expect(picker()).toHaveValue('max');
    expect(option('max → high')).toBeEnabled();
    expect(option('max → high').selected).toBe(true);
    expect(option('xhigh')).toBeDisabled();
  });

  it('is off for a model effort does not apply to', async () => {
    realChat('llama3');
    await renderComposer();
    expect(picker()).toBeDisabled();
    expect(picker()).toHaveAttribute('title', "Effort doesn't apply to this model");
    expect(option('Default').selected).toBe(true);
  });

  it('shows a plain Default and every level with an older backend', async () => {
    useChatStore.setState({ effortDefault: null, effortLevels: {} });
    realChat('model-b');
    await renderComposer();
    expect(option('Default').selected).toBe(true);
    for (const level of ['low', 'medium', 'high', 'xhigh', 'max']) expect(option(level)).toBeEnabled();
  });
});
