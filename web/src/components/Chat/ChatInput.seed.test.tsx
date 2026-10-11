import { act, render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

/**
 * The composer seed: the setup wizard's "Say hello" lands in the chat with
 * a suggested first message, which goes into the composer of the first
 * session that has no draft of its own and is kept as that session's draft.
 */

vi.mock('../../api/client', async () => {
  const actual = await vi.importActual<typeof import('../../api/client')>('../../api/client');
  const generic = () => vi.fn(async () => ({}));
  return {
    ...actual,
    api: new Proxy({} as Record<string, unknown>, {
      get: (target, key: string) => {
        if (!(key in target)) {
          target[key] = key === 'getPromptRewriteStatus'
            ? vi.fn(async () => ({ available: false, enabled: false }))
            : key === 'listActors' ? vi.fn(async () => ({ actors: [] }))
              : generic();
        }
        return target[key];
      },
    }),
    setUnauthorizedHandler: vi.fn(),
  };
});
vi.mock('../../stores/helpers/draftStorage', async (orig) => ({
  ...(await orig<Record<string, unknown>>()), clearAllDrafts: vi.fn(),
}));
vi.mock('../../stores/helpers/readStorage', async (orig) => ({
  ...(await orig<Record<string, unknown>>()), clearAllReads: vi.fn(),
}));
vi.mock('../../api/websocket', () => ({
  ws: {
    sendMessage: vi.fn(() => 'sent'), switchSession: vi.fn(), send: vi.fn(), connect: vi.fn(),
    takePendingMessages: vi.fn(() => []),
  },
}));

const { useChatStore } = await import('../../stores/chatStore');
const { ChatInput } = await import('./ChatInput');

/**
 * Renders and flushes the composer's mount-time fetch of the rewrite
 * status, so that state update lands inside act() rather than after the
 * test has returned.
 */
async function renderComposer(seed: string | null) {
  const view = render(
    <ChatInput onSend={vi.fn()} onStop={vi.fn()} isStreaming={false} seed={seed} />,
  );
  await act(async () => {});
  return view;
}

beforeEach(() => {
  useChatStore.setState({
    activeSession: 's1', drafts: {}, messages: [], sessions: [], virtualSession: null,
    loadModels: vi.fn(async () => {}),
  });
});

describe('composer seed', () => {
  it('fills the composer once for a session with no draft and keeps it as the draft', async () => {
    await renderComposer("Hi, I'm Alice. Tell me what you can do.");
    expect(screen.getByRole('textbox')).toHaveValue("Hi, I'm Alice. Tell me what you can do.");
    expect(useChatStore.getState().drafts.s1).toBe("Hi, I'm Alice. Tell me what you can do.");
  });

  it('never overwrites a draft somebody typed', async () => {
    useChatStore.setState({ drafts: { s1: 'what I was writing' } });
    await renderComposer('Hi.');
    expect(screen.getByRole('textbox')).toHaveValue('what I was writing');
    expect(useChatStore.getState().drafts.s1).toBe('what I was writing');
  });

  it('leaves the composer empty without a seed', async () => {
    await renderComposer(null);
    expect(screen.getByRole('textbox')).toHaveValue('');
    expect(useChatStore.getState().drafts.s1).toBeUndefined();
  });
});
