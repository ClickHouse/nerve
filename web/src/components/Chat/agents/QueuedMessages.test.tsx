import { act, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../../../api/client', async (orig) => ({
  ...(await orig<typeof import('../../../api/client')>()),
  api: new Proxy({} as Record<string, unknown>, {
    get: (target, key: string) => (target[key] ??= vi.fn(async () => ({}))),
  }),
}));
vi.mock('../../../api/websocket', () => ({
  ws: {
    sendMessage: vi.fn(() => 'sent'), switchSession: vi.fn(), send: vi.fn(), connect: vi.fn(),
    subscribeAgent: vi.fn(() => 'sent'), unsubscribeAgent: vi.fn(() => 'sent'), onOpen: vi.fn(() => () => {}),
  },
}));

const { useAgentStore } = await import('../../../stores/agentStore');
const { useAuthStore } = await import('../../../stores/authStore');
const { useChatStore } = await import('../../../stores/chatStore');
const { FX_OWNER_ACTOR, FX_WAVE, makeAgentStoreSeed, makeQueueEntry } = await import('../../../test/agentFixtures');
const { QueuedMessages } = await import('./QueuedMessages');

const cancelQueued = vi.fn(async () => {});

function chips() {
  return within(screen.getByRole('list', { name: 'Queued messages' })).getAllByRole('listitem');
}

beforeEach(() => {
  cancelQueued.mockClear();
  useAuthStore.setState({
    viewer: { id: FX_OWNER_ACTOR, kind: 'human', display_name: 'Owner', username: null } as never,
  });
  useChatStore.setState({ messages: [] });
  useAgentStore.setState({ ...makeAgentStoreSeed(FX_WAVE), cancelQueued });
});

describe('QueuedMessages', () => {
  it('shows nothing with an empty queue', () => {
    render(<QueuedMessages />);
    expect(screen.queryByRole('list', { name: 'Queued messages' })).toBeNull();
  });

  it('shows one chip per queued message, with actions on your own', async () => {
    const mine = makeQueueEntry({ preview: 'also run the e2e suite' });
    const theirs = makeQueueEntry({ preview: 'deploy after', actor_id: 'act_other', source: 'telegram' });
    useAgentStore.setState({ queue: [mine, theirs] });
    render(<QueuedMessages />);

    const [first, second] = chips();
    expect(first).toHaveTextContent('also run the e2e suite');
    expect(within(first).getByRole('button', { name: 'Edit the queued message' })).toBeInTheDocument();
    expect(within(first).getByRole('button', { name: 'Cancel the queued message' })).toBeInTheDocument();
    expect(second).toHaveTextContent('deploy after');
    expect(second).toHaveTextContent('telegram');
    expect(within(second).queryByRole('button')).toBeNull();
  });

  it('mutes internal entries and gives them no actions', () => {
    useAgentStore.setState({ queue: [makeQueueEntry({ preview: 'wake-up check', internal: true, source: 'cron' })] });
    render(<QueuedMessages />);
    expect(chips()[0]).toHaveTextContent('wake-up check');
    expect(within(chips()[0]).queryByRole('button')).toBeNull();
  });

  it('shows an unconfirmed send as Sending…, once', () => {
    const entry = makeQueueEntry({ preview: 'confirmed' });
    useAgentStore.setState({
      queue: [entry],
      pendingSends: {
        [entry.client_msg_id!]: { clientMsgId: entry.client_msg_id!, content: 'confirmed', blocks: [], at: 1 },
        'local-1': { clientMsgId: 'local-1', content: 'still on its way', blocks: [], at: 2 },
      },
    });
    render(<QueuedMessages />);
    expect(chips()).toHaveLength(2);
    expect(chips()[1]).toHaveTextContent('still on its waySending…');
  });

  it('does not repeat a message that is already a bubble here', () => {
    const entry = makeQueueEntry({ preview: 'sent while the lock was held' });
    useAgentStore.setState({ queue: [entry] });
    useChatStore.setState({
      messages: [{ role: 'user', blocks: [{ type: 'text', content: 'sent while the lock was held' }], client_msg_id: entry.client_msg_id }],
    });
    render(<QueuedMessages />);
    expect(screen.queryByRole('list', { name: 'Queued messages' })).toBeNull();
  });

  it('cancels with ×', async () => {
    const entry = makeQueueEntry();
    useAgentStore.setState({ queue: [entry] });
    render(<QueuedMessages />);
    await userEvent.click(screen.getByRole('button', { name: 'Cancel the queued message' }));
    expect(cancelQueued).toHaveBeenCalledWith(entry.queue_id);
  });

  it('takes the text back into the composer with ✎', async () => {
    const entry = makeQueueEntry();
    useAgentStore.setState({ queue: [entry] });
    render(<><textarea id="nerve-chat-input" aria-label="Message" /><QueuedMessages /></>);
    await userEvent.click(screen.getByRole('button', { name: 'Edit the queued message' }));
    await act(async () => {});
    expect(cancelQueued).toHaveBeenCalledWith(entry.queue_id);
    expect(document.activeElement).toBe(document.getElementById('nerve-chat-input'));
  });
});
