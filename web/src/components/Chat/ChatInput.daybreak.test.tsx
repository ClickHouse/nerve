import { act, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

/**
 * The composer's Daybreak switch. It is per chat and for Codex only: an
 * existing chat keeps it on its session row (a PATCH), a new chat carries it
 * into the request that creates the session. An account without Daybreak
 * never sees it.
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
const { useChatStore } = await import('../../stores/chatStore');
const { ChatInput } = await import('./ChatInput');

const SESSION = 'session-1';
const VIRTUAL = 'virtual-1';
const SOL = 'gpt-6-sol';
const ASTRA = 'gpt-6-astra';
const WHAT_IT_DOES = 'Daybreak: turns in this chat run with the Daybreak cyber access program of your Codex account';

const updateSession = vi.mocked(api.updateSession);
const createSession = vi.mocked(api.createSession);

/** The backends of `GET /api/models`; the account has Daybreak on these Codex models. */
const backends = (programs: Record<string, string>) => [
  { id: 'claude', label: 'Claude', model: 'claude-opus-5', available: true },
  { id: 'codex', label: 'Codex', model: SOL, available: true, daybreak_programs: programs },
];

const toggle = () => screen.getByRole('button', { name: 'Daybreak' });
const noToggle = () => expect(screen.queryByRole('button', { name: 'Daybreak' })).toBeNull();
/** The text of each line in the open chat. */
const chatLines = () => useChatStore.getState().messages.map((m) => {
  const block = m.blocks[0];
  return block?.type === 'text' ? block.content : null;
});

async function renderComposer(isStreaming = false) {
  render(<ChatInput onSend={vi.fn()} onStop={vi.fn()} isStreaming={isStreaming} />);
  // The composer takes focus on mount, on the next tick.
  await act(async () => { await new Promise(r => setTimeout(r, 0)); });
}

/**
 * An existing chat on this backend and model. The server puts the switch on
 * the rows of Codex chats only; a row without it reads as off.
 */
function existingChat(backend: string, model: string, daybreak?: boolean) {
  useChatStore.setState({
    activeSession: SESSION, virtualSession: null,
    sessions: [{
      id: SESSION, title: 'Chat', source: 'web', updated_at: '2026-01-01T00:00:00Z',
      backend, model, ...(daybreak === undefined ? {} : { codex_daybreak: daybreak }),
    }],
  });
}

/** A new chat that is not created yet, on Codex. */
function newCodexChat() {
  useChatStore.setState({
    activeSession: VIRTUAL, sessions: [], newChatBackend: 'codex',
    virtualSession: { id: VIRTUAL, title: '', source: 'web', status: 'created', updated_at: '2026-01-01T00:00:00Z' },
  });
}

beforeEach(() => {
  updateSession.mockReset();
  createSession.mockReset();
  useChatStore.setState({
    drafts: {}, messages: [], quotes: [], archivedSessions: null, systemSessions: null,
    availableModels: [
      { id: 'claude-opus-5', provider: 'anthropic', backend: 'claude' },
      { id: SOL, provider: 'openai', backend: 'codex' },
      { id: ASTRA, provider: 'openai', backend: 'codex' },
    ],
    modelDefaults: { claude: 'claude-opus-5', codex: SOL },
    backendOptions: backends({ [SOL]: 'daybreakBlue' }), backendDefault: 'claude',
    newChatBackend: null, newChatModels: {}, newChatDaybreak: false, newChatReviewLoop: null,
    loadModels: vi.fn(async () => {}),
  });
});

describe('where the Daybreak switch shows', () => {
  it('is absent when the Codex account has Daybreak on no model', async () => {
    useChatStore.setState({ backendOptions: backends({}) });
    existingChat('codex', SOL);
    await renderComposer();
    noToggle();
  });

  it('is absent when the catalog says nothing about Daybreak', async () => {
    useChatStore.setState({
      backendOptions: [{ id: 'codex', label: 'Codex', model: SOL, available: true }],
    });
    existingChat('codex', SOL);
    await renderComposer();
    noToggle();
  });

  it('is absent in a Claude chat', async () => {
    existingChat('claude', 'claude-opus-5');
    await renderComposer();
    noToggle();
  });

  it('is absent in a new chat on Claude', async () => {
    newCodexChat();
    useChatStore.setState({ newChatBackend: 'claude' });
    await renderComposer();
    noToggle();
  });

  it('is off and ready on a model that offers Daybreak', async () => {
    existingChat('codex', SOL, false);
    await renderComposer();
    expect(toggle()).toBeEnabled();
    expect(toggle()).not.toHaveAttribute('aria-disabled');
    expect(toggle()).toHaveAttribute('aria-pressed', 'false');
    expect(toggle()).toHaveAttribute('title', WHAT_IT_DOES);
  });

  it('reads a row without the switch as off', async () => {
    existingChat('codex', SOL);
    await renderComposer();
    expect(toggle()).toHaveAttribute('aria-pressed', 'false');
    expect(toggle()).not.toHaveAttribute('aria-disabled');
  });

  it('is greyed out on a model without Daybreak, and names the models that offer it', async () => {
    const user = userEvent.setup();
    existingChat('codex', ASTRA);
    await renderComposer();
    expect(toggle()).toHaveAttribute('aria-disabled', 'true');
    expect(toggle()).toHaveAttribute('aria-pressed', 'false');
    expect(toggle()).toHaveAccessibleDescription(
      'Daybreak is not available for this model. Models that offer it: GPT-6 Sol',
    );

    await user.click(toggle());
    expect(updateSession).not.toHaveBeenCalled();
  });

  it('waits for the running turn, like the model menu', async () => {
    existingChat('codex', SOL);
    await renderComposer(true);
    expect(toggle()).toBeDisabled();
  });
});

describe('the Daybreak switch of an existing chat', () => {
  it('asks the server, and shows what the server stored', async () => {
    const user = userEvent.setup();
    existingChat('codex', SOL);
    await renderComposer();

    updateSession.mockResolvedValueOnce({ id: SESSION, codex_daybreak: true });
    await user.click(toggle());
    expect(updateSession).toHaveBeenLastCalledWith(SESSION, { codex_daybreak: true });
    expect(toggle()).toHaveAttribute('aria-pressed', 'true');
    expect(useChatStore.getState().sessions[0].codex_daybreak).toBe(true);

    updateSession.mockResolvedValueOnce({ id: SESSION, codex_daybreak: false });
    await user.click(toggle());
    expect(updateSession).toHaveBeenLastCalledWith(SESSION, { codex_daybreak: false });
    expect(toggle()).toHaveAttribute('aria-pressed', 'false');
    expect(chatLines()).toEqual([]);
  });

  it('stays off and says why when the server refuses', async () => {
    const user = userEvent.setup();
    vi.spyOn(console, 'error').mockImplementationOnce(() => {});
    existingChat('codex', SOL);
    await renderComposer();

    const reason = `Daybreak cannot be turned on: model '${SOL}' has no Daybreak program for the signed-in Codex account`;
    updateSession.mockRejectedValueOnce(new Error(`400: ${JSON.stringify({ detail: reason })}`));
    await user.click(toggle());

    expect(toggle()).toHaveAttribute('aria-pressed', 'false');
    expect(chatLines()).toEqual([`Error: ${reason}`]);
  });

  it('goes off, with the notice of the server, on a move to a model without Daybreak', async () => {
    const user = userEvent.setup();
    existingChat('codex', SOL, true);
    await renderComposer();
    expect(toggle()).toHaveAttribute('aria-pressed', 'true');

    const notice = `Daybreak was turned off: model '${ASTRA}' has no Daybreak program for the signed-in Codex account.`;
    updateSession.mockResolvedValueOnce({
      id: SESSION, model: ASTRA, codex_daybreak: false, codex_daybreak_notice: notice,
    });
    await user.click(screen.getByRole('button', { name: 'Model: GPT-6 Sol' }));
    await user.click(screen.getByRole('button', { name: /^GPT-6 Astra/ }));

    expect(updateSession).toHaveBeenCalledWith(SESSION, { model: ASTRA });
    expect(toggle()).toHaveAttribute('aria-pressed', 'false');
    expect(toggle()).toHaveAttribute('aria-disabled', 'true');
    expect(chatLines()).toEqual([notice]);
  });

  it('stays on, without a notice, on a move to a model that offers Daybreak', async () => {
    const user = userEvent.setup();
    useChatStore.setState({ backendOptions: backends({ [SOL]: 'daybreakBlue', [ASTRA]: 'daybreakRed' }) });
    existingChat('codex', SOL, true);
    await renderComposer();

    updateSession.mockResolvedValueOnce({ id: SESSION, model: ASTRA, codex_daybreak: true });
    await user.click(screen.getByRole('button', { name: 'Model: GPT-6 Sol' }));
    await user.click(screen.getByRole('button', { name: /^GPT-6 Astra/ }));

    expect(toggle()).toHaveAttribute('aria-pressed', 'true');
    expect(chatLines()).toEqual([]);
  });

  it('can be turned off in a chat whose model lost Daybreak', async () => {
    // The account lost Daybreak everywhere, and the chat still has it on:
    // the switch stays, so that the person can turn it off.
    const user = userEvent.setup();
    useChatStore.setState({ backendOptions: backends({}) });
    existingChat('codex', SOL, true);
    await renderComposer();
    expect(toggle()).toHaveAttribute('aria-pressed', 'true');
    expect(toggle()).not.toHaveAttribute('aria-disabled');
    expect(toggle()).toHaveAccessibleDescription(
      'Daybreak is on, but this model does not offer it. Turn it off, or choose a model that offers it',
    );

    updateSession.mockResolvedValueOnce({ id: SESSION, codex_daybreak: false });
    await user.click(toggle());
    expect(updateSession).toHaveBeenCalledWith(SESSION, { codex_daybreak: false });
    noToggle();
  });

  it('is not added to the row of a chat of another backend by a model change', async () => {
    // The server's answer for such a chat has no switch.
    existingChat('claude', 'claude-opus-5');
    await renderComposer();

    updateSession.mockResolvedValueOnce({ id: SESSION, backend: 'claude', model: 'claude-sonnet-5' });
    await act(async () => { await useChatStore.getState().setSessionModel(SESSION, 'claude-sonnet-5'); });

    expect(updateSession).toHaveBeenCalledWith(SESSION, { model: 'claude-sonnet-5' });
    expect(useChatStore.getState().sessions[0]).toEqual({
      id: SESSION, title: 'Chat', source: 'web', updated_at: '2026-01-01T00:00:00Z',
      backend: 'claude', model: 'claude-sonnet-5',
    });
    expect(chatLines()).toEqual([]);
    noToggle();
  });
});

describe('the Daybreak switch of a new chat', () => {
  it('is local until the chat is created, and goes into the request that creates it', async () => {
    const user = userEvent.setup();
    newCodexChat();
    await renderComposer();
    expect(toggle()).toHaveAttribute('aria-pressed', 'false');

    await user.click(toggle());
    expect(toggle()).toHaveAttribute('aria-pressed', 'true');
    expect(updateSession).not.toHaveBeenCalled();
    expect(createSession).not.toHaveBeenCalled();

    createSession.mockResolvedValueOnce({ id: SESSION, source: 'web', backend: 'codex', model: SOL, codex_daybreak: true });
    await act(async () => { await useChatStore.getState().ensureRealSession(); });

    expect(createSession).toHaveBeenCalledWith(undefined, 'codex', undefined, null, undefined, true);
    // The created chat has it on its row, and the next new chat starts off.
    expect(useChatStore.getState().newChatDaybreak).toBe(false);
    expect(toggle()).toHaveAttribute('aria-pressed', 'true');
  });

  it('creates a chat without it when the switch was not touched', async () => {
    newCodexChat();
    await renderComposer();
    createSession.mockResolvedValueOnce({ id: SESSION, source: 'web', backend: 'codex', model: SOL });
    await act(async () => { await useChatStore.getState().ensureRealSession(); });
    expect(createSession).toHaveBeenCalledWith(undefined, 'codex', undefined, null, undefined, false);
  });

  it('is cleared by a pick of a model without Daybreak', async () => {
    const user = userEvent.setup();
    newCodexChat();
    await renderComposer();
    await user.click(toggle());
    expect(useChatStore.getState().newChatDaybreak).toBe(true);

    await user.click(screen.getByRole('button', { name: 'Model: GPT-6 Sol' }));
    await user.click(screen.getByRole('button', { name: /^GPT-6 Astra/ }));
    expect(useChatStore.getState().newChatDaybreak).toBe(false);
    expect(toggle()).toHaveAttribute('aria-pressed', 'false');
    expect(toggle()).toHaveAttribute('aria-disabled', 'true');

    // Back on a model that offers it, the switch is off: it is not remembered.
    await user.click(screen.getByRole('button', { name: 'Model: GPT-6 Astra' }));
    await user.click(screen.getByRole('button', { name: /^GPT-6 Sol/ }));
    expect(toggle()).toHaveAttribute('aria-pressed', 'false');
    expect(toggle()).not.toHaveAttribute('aria-disabled');

    createSession.mockResolvedValueOnce({ id: SESSION, source: 'web', backend: 'codex', model: SOL });
    await act(async () => { await useChatStore.getState().ensureRealSession(); });
    expect(createSession).toHaveBeenCalledWith(undefined, 'codex', undefined, null, undefined, false);
  });

  it('goes with a prompt that is deferred with Run later', async () => {
    const user = userEvent.setup();
    newCodexChat();
    await renderComposer();
    await user.click(toggle());

    createSession.mockResolvedValueOnce({ id: SESSION, source: 'web', backend: 'codex', model: SOL, codex_daybreak: true });
    await act(async () => { await useChatStore.getState().runLater('scan the firmware image', '1h'); });

    expect(createSession).toHaveBeenCalledWith(undefined, 'codex', undefined, null, null, true);
    expect(useChatStore.getState().newChatDaybreak).toBe(false);
  });

  it('does not go with a prompt that is deferred from another chat', async () => {
    // A new Codex chat with Daybreak on and a draft, left unsent: the person
    // opens a chat of another backend and uses Run later there.
    const user = userEvent.setup();
    vi.spyOn(console, 'warn').mockImplementation(() => {});
    newCodexChat();
    useChatStore.setState({
      sessions: [{
        id: SESSION, title: 'Chat', source: 'web', updated_at: '2026-01-01T00:00:00Z',
        backend: 'claude', model: 'claude-opus-5',
      }],
    });
    await renderComposer();
    await user.click(toggle());
    act(() => { useChatStore.getState().setDraft(VIRTUAL, 'scan the firmware image'); });

    await act(async () => { await useChatStore.getState().switchSession(SESSION); });
    // The unsent chat keeps its draft and its switch; the composer is in the other chat.
    expect(useChatStore.getState().activeSession).toBe(SESSION);
    expect(useChatStore.getState().virtualSession?.id).toBe(VIRTUAL);
    expect(useChatStore.getState().newChatDaybreak).toBe(true);
    noToggle();

    createSession.mockResolvedValueOnce({ id: 'session-2', source: 'web' });
    await act(async () => { await useChatStore.getState().runLater('summarize the changelog', '1h'); });

    // The session of the deferred prompt is created without Daybreak. (Its
    // backend and model are not this test's subject.)
    expect(createSession).toHaveBeenCalledTimes(1);
    expect(createSession.mock.calls[0][5]).toBe(false);
  });

  it('does not outlive a new chat that is dropped', async () => {
    const user = userEvent.setup();
    newCodexChat();
    await renderComposer();
    await user.click(toggle());

    act(() => { useChatStore.getState().discardVirtualSession(); });
    expect(useChatStore.getState().newChatDaybreak).toBe(false);
  });

  it('is not sent for a model that stopped offering Daybreak before the chat was created', async () => {
    const user = userEvent.setup();
    newCodexChat();
    await renderComposer();
    await user.click(toggle());
    // The catalog was read again, and the account no longer has Daybreak here.
    act(() => { useChatStore.setState({ backendOptions: backends({ [ASTRA]: 'daybreakBlue' }) }); });
    expect(toggle()).toHaveAttribute('aria-pressed', 'false');

    createSession.mockResolvedValueOnce({ id: SESSION, source: 'web', backend: 'codex', model: SOL });
    await act(async () => { await useChatStore.getState().ensureRealSession(); });
    expect(createSession).toHaveBeenCalledWith(undefined, 'codex', undefined, null, undefined, false);
  });
});
