import { act, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { ToolCallBlockData } from '../../../types/chat';
import type { Notification } from '../../../stores/notificationStore';

vi.mock('../../../api/client', () => ({
  api: {
    getNotification: vi.fn(),
    answerNotification: vi.fn(),
    listNotifications: vi.fn(async () => ({ notifications: [], pending_count: 0 })),
    listActors: vi.fn(async () => ({ actors: [{ id: 'actor-alice', kind: 'human', display_name: 'Alice', username: null }] })),
  },
  // ActorLabel reads the viewer through the auth store, which wires these.
  setToken: vi.fn(), clearToken: vi.fn(), getToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));

const { api } = await import('../../../api/client');
const { useNotificationStore } = await import('../../../stores/notificationStore');
const { ToolCallBlock } = await import('../ToolCallBlock');
const { questionIdFromResult } = await import('../../Notifications/askOptions');
const { formatMessageTime } = await import('../../../utils/messageTime');

const getNotification = vi.mocked(api.getNotification);
const answerNotification = vi.mocked(api.answerNotification);

const ID = 'ask-1a2b3c4d';
/** The server's answer time, from the answer event. */
const ANSWERED_AT = '2026-01-02T03:04:00+00:00';
const OPTIONS = ['Yes, all of these', 'Only the old ones (recommended)', 'No'];

function call(overrides: Partial<ToolCallBlockData> = {}): ToolCallBlockData {
  return {
    type: 'tool_call',
    toolUseId: 'tu-ask',
    tool: 'mcp__nerve__ask_user',
    input: { title: 'Delete the build caches?', body: 'They use **12 GB**.', options: JSON.stringify(OPTIONS) },
    result: JSON.stringify([{ type: 'text', text: `Question sent (${ID}). The user's answer will be automatically injected as a message in this session.` }]),
    status: 'complete',
    ...overrides,
  };
}

function row(overrides: Partial<Notification> = {}): Notification {
  return {
    id: ID, session_id: 's1', session_title: 'Chat', type: 'question', title: 'Delete the build caches?',
    body: 'They use **12 GB**.', priority: 'normal', status: 'pending', options: JSON.stringify(OPTIONS) as unknown as string[],
    answer: null, answered_by: null, answered_by_actor_id: null, answered_at: null,
    created_at: '2026-01-01T00:00:00Z', expires_at: null,
    ...overrides,
  };
}

function renderCall(block = call()) {
  return render(<MemoryRouter><ToolCallBlock block={block} /></MemoryRouter>);
}

const options = async () => within(await screen.findByRole('group', { name: 'Delete the build caches?' }));

beforeEach(() => {
  vi.clearAllMocks();
  useNotificationStore.setState({ notifications: [], toastQueue: [], byId: {}, pendingCount: 0 });
});

describe('questionIdFromResult', () => {
  it('reads the id from the result text', () => {
    expect(questionIdFromResult(call().result)).toBe(ID);
    expect(questionIdFromResult('Failed to ask question: boom')).toBeNull();
    expect(questionIdFromResult(undefined)).toBeNull();
  });
});

describe('AskUserBlock', () => {
  it('shows answerable options for mcp__nerve__ask_user, one per array item', async () => {
    getNotification.mockResolvedValue(row());
    renderCall();
    const rows = (await screen.findByRole('group', { name: 'Delete the build caches?' })).querySelectorAll('button');
    expect([...rows].map(r => r.textContent)).toEqual([
      'AOnly the old onesRecommended', 'BYes, all of these', 'CNo', 'DOther…',
    ]);
    expect(getNotification).toHaveBeenCalledWith(ID);
    expect(screen.getByText('12 GB').tagName).toBe('STRONG');
    expect(screen.queryByText('Nothing happens until you choose.')).not.toBeInTheDocument();
  });

  it('answers the question from the chat', async () => {
    getNotification.mockResolvedValue(row());
    answerNotification.mockResolvedValue({ answered: true });
    renderCall();
    await userEvent.click((await options()).getByRole('button', { name: /Yes, all of these/ }));
    expect(answerNotification).toHaveBeenCalledWith(ID, 'Yes, all of these');
  });

  it('says so when the answer does not go through', async () => {
    getNotification.mockResolvedValue(row());
    answerNotification.mockRejectedValue(new Error('409'));
    vi.spyOn(console, 'error').mockImplementation(() => {});
    renderCall();
    await userEvent.click((await options()).getByRole('button', { name: 'No' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Could not send the answer. Try again.');
    expect((await options()).getByRole('button', { name: 'No' })).toBeEnabled();
  });

  it('folds to the answer from any place, with who, when and the channel', async () => {
    getNotification.mockResolvedValue(row());
    renderCall();
    await screen.findByRole('group', { name: 'Delete the build caches?' });
    act(() => {
      useNotificationStore.getState().handleWSNotificationAnswered({
        notification_id: ID, answer: 'Only the old ones (recommended)', answered_by: 'telegram',
        answered_at: ANSWERED_AT, actor_id: 'actor-alice',
      });
    });
    expect(screen.queryByRole('group')).not.toBeInTheDocument();
    expect(screen.getByText('Answer:').parentElement).toHaveTextContent('Answer: Only the old ones');
    expect(screen.getByText('via telegram')).toBeInTheDocument();
    expect(screen.getByText(formatMessageTime(ANSWERED_AT))).toBeInTheDocument();
    expect(await screen.findByText('Alice')).toBeInTheDocument();
    expect(screen.queryByText('12 GB')).not.toBeInTheDocument();

    await userEvent.click(screen.getByRole('button', { name: 'Show options' }));
    const items = within(screen.getByRole('list', { name: 'Delete the build caches?' })).getAllByRole('listitem');
    expect(items[0]).toHaveTextContent('(your answer)');
    expect(screen.getByRole('button', { name: 'Hide options' })).toHaveAttribute('aria-expanded', 'true');
  });

  it('shows a question that expired', async () => {
    getNotification.mockResolvedValue(row());
    renderCall();
    await screen.findByRole('group', { name: 'Delete the build caches?' });
    act(() => {
      useNotificationStore.getState().handleWSNotificationExpired({ notification_id: ID });
    });
    expect(screen.getByText('Expired without an answer')).toBeInTheDocument();
    expect(screen.queryByRole('group')).not.toBeInTheDocument();
  });

  it('shows the options read-only when the question cannot be read', async () => {
    getNotification.mockRejectedValue(new Error('404'));
    vi.spyOn(console, 'error').mockImplementation(() => {});
    renderCall();
    const list = await screen.findByRole('list', { name: 'Delete the build caches?' });
    expect(within(list).getAllByRole('listitem')).toHaveLength(3);
  });

  it('shows the error of a question that was not sent', () => {
    renderCall(call({ result: 'Failed to ask question: database is locked' }));
    expect(screen.getByText('Failed to ask question: database is locked')).toBeInTheDocument();
    expect(getNotification).not.toHaveBeenCalled();
  });

  it('reads each question once', async () => {
    getNotification.mockResolvedValue(row());
    render(<MemoryRouter><ToolCallBlock block={call()} /><ToolCallBlock block={call()} /></MemoryRouter>);
    expect(await screen.findAllByRole('group', { name: 'Delete the build caches?' })).toHaveLength(2);
    expect(getNotification).toHaveBeenCalledTimes(1);
  });
});

describe('notify in the chat', () => {
  it('is labelled Notify, not as a question', () => {
    renderCall(call({ tool: 'mcp__nerve__notify', input: { title: 'Build finished' }, result: 'Notification sent: n-1' }));
    expect(screen.getByText('Notify')).toBeInTheDocument();
    expect(screen.queryByRole('group')).not.toBeInTheDocument();
  });
});
