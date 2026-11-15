import { act, fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

/**
 * The composer is one box that reads as one text field: a press on its blank
 * parts puts the caret in the text, and the focus shortcut shows as a hint
 * while the box is empty and has no focus.
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

const { useChatStore } = await import('../../stores/chatStore');
const { ChatInput } = await import('./ChatInput');

const SESSION = 'session-1';
const textbox = () => screen.getByRole('textbox') as HTMLTextAreaElement;
const hint = () => screen.queryByText(/to focus$/);

async function renderComposer() {
  render(<ChatInput onSend={vi.fn()} onStop={vi.fn()} isStreaming={false} />);
  // The composer takes focus on mount, on the next tick.
  await act(async () => { await new Promise(r => setTimeout(r, 0)); });
}

beforeEach(() => {
  useChatStore.setState({
    activeSession: SESSION, drafts: {}, messages: [], virtualSession: null, quotes: [],
    sessions: [{ id: SESSION, title: 'Chat', source: 'web', updated_at: '2026-01-01T00:00:00Z', backend: 'claude', model: 'model-a' }],
    availableModels: [], loadModels: vi.fn(async () => {}),
  });
});

describe('ChatInput box', () => {
  it('puts the caret in the text on a press on the box, not on a control', async () => {
    await renderComposer();
    act(() => textbox().blur());
    const box = textbox().closest('[data-composer-blank]');
    expect(box).not.toBeNull();

    // A control keeps its own press: nothing cancels it.
    expect(fireEvent.mouseDown(screen.getByRole('button', { name: 'More options' }))).toBe(true);
    expect(document.activeElement).not.toBe(textbox());

    // The blank box cancels the press (so focus does not land on the page)
    // and moves focus to the text.
    expect(fireEvent.mouseDown(box!)).toBe(false);
    expect(document.activeElement).toBe(textbox());
  });

  it('shows the focus shortcut only while the box is empty and has no focus', async () => {
    const user = userEvent.setup();
    await renderComposer();
    expect(document.activeElement).toBe(textbox());
    expect(hint()).toBeNull();

    act(() => textbox().blur());
    expect(hint()).toHaveTextContent('Ctrl+Shift+; to focus');

    await user.type(textbox(), 'draft');
    act(() => textbox().blur());
    expect(hint()).toBeNull();
  });
});
