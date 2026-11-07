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
const reorderQueue = vi.fn(async () => {});

function chips() {
  return within(screen.getByRole('list', { name: 'Queued messages' })).getAllByRole('listitem');
}

beforeEach(() => {
  cancelQueued.mockClear();
  reorderQueue.mockClear();
  useAuthStore.setState({
    viewer: { id: FX_OWNER_ACTOR, kind: 'human', display_name: 'Owner', username: null } as never,
  });
  useChatStore.setState({ messages: [] });
  useAgentStore.setState({ ...makeAgentStoreSeed(FX_WAVE), cancelQueued, reorderQueue });
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
    expect(within(second).getAllByRole('button').map(b => b.getAttribute('aria-label'))).toEqual([
      'Move up: deploy after', 'Move down: deploy after',
    ]);
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

  it('lists a message sent from another tab, whose echo is a bubble here', async () => {
    const mine = makeQueueEntry({ preview: 'mine' });
    const other = makeQueueEntry({ preview: 'from the other tab' });
    useAgentStore.setState({ queue: [mine, other] });
    useChatStore.setState({
      messages: [{ role: 'user', blocks: [{ type: 'text', content: 'from the other tab' }], client_msg_id: other.client_msg_id }],
    });
    render(<QueuedMessages />);
    expect(chips()).toHaveLength(2);
    await userEvent.click(within(chips()[1]).getByRole('button', { name: 'Move up: from the other tab' }));
    expect(reorderQueue).toHaveBeenLastCalledWith([other.queue_id, mine.queue_id]);
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

  describe('reorder', () => {
    const three = () => ['a', 'b', 'c'].map(n => makeQueueEntry({ queue_id: `q_${n}`, preview: n }));
    // "Move up: a", "Move down: c", ...
    const button = (row: HTMLElement, name: string) => within(row).getByRole('button', { name: new RegExp(`^${name}: `) });

    it('moves a message up or down and sends the whole new order', async () => {
      useAgentStore.setState({ queue: three() });
      render(<QueuedMessages />);
      const [first, , last] = chips();
      expect(button(first, 'Move up')).toBeDisabled();
      expect(button(last, 'Move down')).toBeDisabled();

      await userEvent.click(button(first, 'Move down'));
      expect(reorderQueue).toHaveBeenLastCalledWith(['q_b', 'q_a', 'q_c']);
      await userEvent.click(button(last, 'Move up'));
      expect(reorderQueue).toHaveBeenLastCalledWith(['q_a', 'q_c', 'q_b']);
    });

    it('offers no move for a single message or a send on its way', () => {
      useAgentStore.setState({
        queue: [makeQueueEntry()],
        pendingSends: { 'local-1': { clientMsgId: 'local-1', content: 'on its way', blocks: [], at: 1 } },
      });
      render(<QueuedMessages />);
      expect(screen.queryByRole('button', { name: /^Move up/ })).toBeNull();
    });

    it('names the message in a long move label, cut short', () => {
      const long = 'Then update the changelog with a short note about the flaky login test';
      useAgentStore.setState({ queue: [makeQueueEntry({ preview: 'a' }), makeQueueEntry({ preview: long })] });
      render(<QueuedMessages />);
      expect(within(chips()[1]).getByRole('button', { name: `Move up: ${long.slice(0, 40)}…` })).toBeInTheDocument();
    });

    it('keeps the keyboard focus on the moved message', async () => {
      // The real store action: the order changes at once.
      useAgentStore.setState({ queue: three(), reorderQueue: useAgentStore.getInitialState().reorderQueue });
      render(<QueuedMessages />);
      await userEvent.click(button(chips()[1], 'Move down'));
      expect(chips().map(row => row.textContent)).toEqual(['a', 'c', 'b'].map(t => expect.stringContaining(t)));
      // The moved row is the last row: its "Move down" is off, so focus is on "Move up".
      expect(document.activeElement).toBe(button(chips()[2], 'Move up'));
    });
  });
});
