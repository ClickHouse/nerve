import { act, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

/**
 * What Enter does in the composer. With a mouse and a hardware keyboard,
 * Enter sends and Shift+Enter types a newline. On a touch device the
 * on-screen keyboard has no Shift+Enter, so Enter types a newline and the
 * Send button sends. Cmd/Ctrl+Enter sends in both cases.
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
    takePendingMessages: vi.fn(() => []),
  },
}));

const { useChatStore } = await import('../../stores/chatStore');
const { ChatInput } = await import('./ChatInput');

const onSend = vi.fn();

/** Makes `(pointer: coarse)` match or not; every other query does not match. */
function setTouch(touch: boolean) {
  vi.spyOn(window, 'matchMedia').mockImplementation((query: string) => ({
    matches: touch && query === '(pointer: coarse)',
    media: query,
    onchange: null,
    addListener: () => {},
    removeListener: () => {},
    addEventListener: () => {},
    removeEventListener: () => {},
    dispatchEvent: () => false,
  }));
}

async function renderComposer() {
  render(<ChatInput onSend={onSend} onStop={vi.fn(async () => {})} isStreaming={false} />);
  await act(async () => {});
}

const textbox = () => screen.getByRole('textbox') as HTMLTextAreaElement;

beforeEach(() => {
  onSend.mockClear();
  useChatStore.setState({
    activeSession: 's1', drafts: {}, messages: [], sessions: [], virtualSession: null, quotes: [],
    loadModels: vi.fn(async () => {}),
  });
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe('Enter with a hardware keyboard', () => {
  beforeEach(() => setTouch(false));

  it('sends on Enter and labels the on-screen Enter key "send"', async () => {
    const user = userEvent.setup();
    await renderComposer();
    expect(textbox()).toHaveAttribute('enterkeyhint', 'send');
    await user.type(textbox(), 'hello{Enter}');
    expect(onSend).toHaveBeenCalledTimes(1);
    expect(onSend.mock.calls[0][0]).toBe('hello');
  });

  it('types a newline on Shift+Enter', async () => {
    const user = userEvent.setup();
    await renderComposer();
    await user.type(textbox(), 'one{Shift>}{Enter}{/Shift}two');
    expect(onSend).not.toHaveBeenCalled();
    expect(textbox()).toHaveValue('one\ntwo');
  });
});

describe('Enter on a touch device', () => {
  beforeEach(() => setTouch(true));

  it('types a newline on Enter and labels the on-screen Enter key "enter"', async () => {
    const user = userEvent.setup();
    await renderComposer();
    expect(textbox()).toHaveAttribute('enterkeyhint', 'enter');
    await user.type(textbox(), 'one{Enter}two');
    expect(onSend).not.toHaveBeenCalled();
    expect(textbox()).toHaveValue('one\ntwo');
  });

  it('sends from the Send button', async () => {
    const user = userEvent.setup();
    await renderComposer();
    await user.type(textbox(), 'one{Enter}two');
    await user.click(screen.getByRole('button', { name: 'Send' }));
    expect(onSend).toHaveBeenCalledTimes(1);
    expect(onSend.mock.calls[0][0]).toBe('one\ntwo');
  });

  it.each([['Control'], ['Meta']])('sends on %s+Enter', async (modifier) => {
    const user = userEvent.setup();
    await renderComposer();
    await user.type(textbox(), `hello{${modifier}>}{Enter}{/${modifier}}`);
    expect(onSend).toHaveBeenCalledTimes(1);
    expect(onSend.mock.calls[0][0]).toBe('hello');
  });
});
