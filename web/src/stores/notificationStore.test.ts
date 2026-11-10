import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Notification } from './notificationStore';

vi.mock('../api/client', () => ({
  api: { listNotifications: vi.fn() },
}));

const { api } = await import('../api/client');
const { useNotificationStore } = await import('./notificationStore');
const { tabAttention } = await import('../utils/tabAttention');

function row(id: string, type: Notification['type'], status: string): Notification {
  return {
    id, session_id: 'cron:nightly', session_title: null, type, title: 'Deploy now?', body: '',
    priority: 'normal', status, options: null, answer: null, answered_by: null, answered_at: null,
    created_at: '2026-11-07T10:00:00Z', expires_at: null,
  };
}

function counts() {
  const { pendingCount, pendingAnswerCount } = useNotificationStore.getState();
  return { pendingCount, pendingAnswerCount };
}

/** The tab title prefix with no session that waits. */
function prefix() {
  const { pendingCount, pendingAnswerCount } = useNotificationStore.getState();
  return tabAttention([], pendingAnswerCount, pendingCount).prefix;
}

beforeEach(() => {
  vi.clearAllMocks();
  useNotificationStore.setState({
    notifications: [], toastQueue: [], byId: {}, pendingCount: 0, pendingAnswerCount: 0, filter: 'pending', typeFilter: '',
  });
});

describe('the count of pending questions and approvals', () => {
  it('comes from the server also when the "Answered" filter leaves the pending rows out of the list', async () => {
    vi.mocked(api.listNotifications).mockResolvedValue({
      notifications: [row('ask-old', 'question', 'answered')], pending_count: 2, pending_answer_count: 1,
    });

    useNotificationStore.getState().setFilter('answered');

    await vi.waitFor(() => expect(counts()).toEqual({ pendingCount: 2, pendingAnswerCount: 1 }));
    expect(api.listNotifications).toHaveBeenCalledWith('answered', undefined);
    expect(useNotificationStore.getState().notifications.map((n) => n.status)).toEqual(['answered']);
    expect(prefix()).toBe('! ');
  });

  it('goes up for a question or an approval that arrives, not for a notify, a silenced or a re-delivered row', () => {
    const { handleWSNotification } = useNotificationStore.getState();
    const event = (id: string, type: Notification['type'], extra = {}) =>
      handleWSNotification({ notification_id: id, notification_type: type, session_id: 'cron:nightly', ...extra });

    event('ask-1', 'question');
    event('ok-1', 'approval');
    expect(counts()).toEqual({ pendingCount: 2, pendingAnswerCount: 2 });

    event('note-1', 'notify');
    event('note-2', 'notify', { silenced: true });
    event('ok-1', 'approval', { redelivered: true });
    expect(counts()).toEqual({ pendingCount: 3, pendingAnswerCount: 2 });
  });

  it('goes down for an answer and an expiry, also of a row that the list leaves out', () => {
    useNotificationStore.setState({
      filter: 'answered', notifications: [row('ask-old', 'question', 'answered')], pendingCount: 4, pendingAnswerCount: 3,
    });
    const { handleWSNotificationAnswered, handleWSNotificationExpired } = useNotificationStore.getState();

    // A snoozed approval stays pending.
    handleWSNotificationAnswered({ notification_id: 'ok-1', approval_status: 'snoozed', snooze_until: '2026-11-08T10:00:00Z' });
    expect(counts()).toEqual({ pendingCount: 4, pendingAnswerCount: 3 });

    handleWSNotificationAnswered({ notification_id: 'ask-1', answer: 'Yes', answered_by: 'telegram' });
    expect(counts()).toEqual({ pendingCount: 3, pendingAnswerCount: 2 });

    handleWSNotificationExpired({ notification_id: 'ask-2', notification_type: 'question' });
    expect(counts()).toEqual({ pendingCount: 2, pendingAnswerCount: 1 });

    // The list shows this row as answered: it is not pending.
    handleWSNotificationExpired({ notification_id: 'ask-old', notification_type: 'question' });
    expect(counts()).toEqual({ pendingCount: 2, pendingAnswerCount: 1 });
    expect(prefix()).toBe('! ');

    handleWSNotificationExpired({ notification_id: 'ok-1', notification_type: 'approval' });
    expect(counts()).toEqual({ pendingCount: 1, pendingAnswerCount: 0 });
    expect(prefix()).toBe('(1) ');
  });

  it('goes down for a dismissal, and only a question or an approval lowers the answer count', () => {
    useNotificationStore.setState({
      filter: 'answered',
      notifications: [row('ask-old', 'question', 'answered')],
      byId: { 'ask-chat': row('ask-chat', 'question', 'dismissed') },
      pendingCount: 4, pendingAnswerCount: 2,
    });
    const { handleWSNotificationDismissed } = useNotificationStore.getState();

    handleWSNotificationDismissed({ notification_id: 'note-1', notification_type: 'notify', actor_id: 'actor-alice' });
    expect(counts()).toEqual({ pendingCount: 3, pendingAnswerCount: 2 });

    handleWSNotificationDismissed({ notification_id: 'ask-1', notification_type: 'question', actor_id: 'actor-alice' });
    expect(counts()).toEqual({ pendingCount: 2, pendingAnswerCount: 1 });

    // Rows that are not pending here: in the list, and in the chat only.
    handleWSNotificationDismissed({ notification_id: 'ask-old', notification_type: 'question' });
    handleWSNotificationDismissed({ notification_id: 'ask-chat', notification_type: 'question' });
    expect(counts()).toEqual({ pendingCount: 2, pendingAnswerCount: 1 });

    handleWSNotificationDismissed({ notification_id: 'ok-1', notification_type: 'approval' });
    expect(counts()).toEqual({ pendingCount: 1, pendingAnswerCount: 0 });
  });

  it('marks every copy of a dismissed row with the person who dismissed it', () => {
    const pending = row('ask-1', 'question', 'pending');
    useNotificationStore.setState({
      notifications: [pending], toastQueue: [pending], byId: { 'ask-1': pending }, pendingCount: 1, pendingAnswerCount: 1,
    });

    useNotificationStore.getState().handleWSNotificationDismissed({
      notification_id: 'ask-1', notification_type: 'question', actor_id: 'actor-alice',
    });

    const { notifications, toastQueue, byId } = useNotificationStore.getState();
    for (const copy of [notifications[0], toastQueue[0], byId['ask-1']]) {
      expect(copy).toMatchObject({ status: 'dismissed', dismissed_by_actor_id: 'actor-alice' });
    }
  });
});
