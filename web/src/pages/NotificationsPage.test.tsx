import { act, render, screen, waitFor, within } from '@testing-library/react';
import type { ReactElement } from 'react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { ActorRef } from '../api/client';
import type { Notification, Silence } from '../stores/notificationStore';

vi.mock('../api/client', () => ({
  api: {
    listNotifications: vi.fn(),
    listSilences: vi.fn(),
    listActors: vi.fn(),
    answerNotification: vi.fn(),
    getNotification: vi.fn(),
    dismissAllNotifications: vi.fn(),
  },
  // ActorLabel reads the viewer through the auth store, which wires these.
  setToken: vi.fn(), clearToken: vi.fn(), getToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));
vi.mock('../stores/helpers/draftStorage', () => ({ clearAllDrafts: vi.fn() }));
vi.mock('../stores/helpers/readStorage', () => ({ clearAllReads: vi.fn() }));

const { api } = await import('../api/client');
const { useActorStore } = await import('../stores/actorStore');
const { useNotificationStore } = await import('../stores/notificationStore');
const { NotificationsPage } = await import('./NotificationsPage');
const { NotificationToast } = await import('../components/Notifications/NotificationToast');

const listNotifications = vi.mocked(api.listNotifications);
const listSilences = vi.mocked(api.listSilences);
const listActors = vi.mocked(api.listActors);
const answerNotification = vi.mocked(api.answerNotification);
const getNotification = vi.mocked(api.getNotification);
const dismissAllNotifications = vi.mocked(api.dismissAllNotifications);

const ALICE = 'actor-alice';
const BOB = 'actor-bob';
const SYSTEM = 'actor-system';

function actorRef(id: string, overrides: Partial<ActorRef> = {}): ActorRef {
  return { id, kind: 'human', display_name: null, username: null, ...overrides };
}

let nextId = 1;
function notification(overrides: Partial<Notification>): Notification {
  return {
    id: `notif-${nextId++}`,
    session_id: 's1',
    session_title: 'Chat',
    type: 'question',
    title: 'Pick one',
    body: '',
    priority: 'normal',
    status: 'answered',
    options: null,
    answer: 'yes',
    answered_by: 'web',
    answered_by_actor_id: null,
    dismissed_by_actor_id: null,
    answered_at: '2026-01-01T00:00:00+00:00',
    created_at: '2026-01-01T00:00:00+00:00',
    expires_at: null,
    ...overrides,
  };
}

function silence(overrides: Partial<Silence>): Silence {
  return {
    id: 'sil-1',
    pattern: 'disk usage',
    action: 'silence',
    reason: '',
    created_by: 'web',
    created_by_actor_id: null,
    caused_by_actor_id: null,
    created_at: '2026-01-01T00:00:00+00:00',
    expires_at: null,
    hit_count: 0,
    last_hit_at: null,
    override_count: 0,
    last_override_at: null,
    enabled: 1,
    ...overrides,
  };
}

function attributions(): string[] {
  return [...document.querySelectorAll('[data-attribution="actor"]')]
    .map((el) => el.textContent ?? '');
}

function renderPage() {
  return render(<MemoryRouter><NotificationsPage /></MemoryRouter>);
}

beforeEach(() => {
  vi.clearAllMocks();
  useActorStore.getState().reset();
  useNotificationStore.setState({
    notifications: [], pendingCount: 0, filter: '', typeFilter: '', loading: true,
    toastQueue: [], silences: [],
  });
  listActors.mockResolvedValue({
    actors: [
      actorRef(ALICE, { display_name: 'Alice' }),
      actorRef(BOB, { display_name: 'Bob' }),
      actorRef(SYSTEM, { kind: 'system' }),
    ],
  });
});

describe('NotificationsPage attribution', () => {
  it('names the person who answered and keeps the transport', async () => {
    listNotifications.mockResolvedValue({
      notifications: [notification({ answered_by_actor_id: ALICE })],
      pending_count: 0, pending_answer_count: 0,
    });
    renderPage();
    await waitFor(() => expect(attributions()).toEqual(['Alice']));
    expect(screen.getByText(/via web/)).toBeInTheDocument();
    expect(screen.getByText('Alice').closest('[data-attribution]'))
      .toHaveAttribute('title', 'Answered by Alice');
  });

  it('shows only the transport for a chat-channel answer', async () => {
    listNotifications.mockResolvedValue({
      notifications: [notification({ answered_by: 'telegram' })],
      pending_count: 0, pending_answer_count: 0,
    });
    renderPage();
    await screen.findByText(/via telegram/);
    expect(attributions()).toEqual([]);
    expect(screen.queryByText(/by\s*$/)).not.toBeInTheDocument();
  });

  it('names the person who dismissed a notification', async () => {
    listNotifications.mockResolvedValue({
      notifications: [notification({
        type: 'notify', status: 'dismissed', answer: null, answered_by: null,
        dismissed_by_actor_id: BOB,
      })],
      pending_count: 0, pending_answer_count: 0,
    });
    renderPage();
    await waitFor(() => expect(attributions()).toEqual(['Bob']));
    expect(screen.getByText(/Dismissed by/)).toBeInTheDocument();
  });

  it('names who created a silence, and for whom', async () => {
    listNotifications.mockResolvedValue({ notifications: [], pending_count: 0, pending_answer_count: 0 });
    listSilences.mockResolvedValue({
      silences: [
        silence({ id: 'sil-web', created_by_actor_id: ALICE, caused_by_actor_id: ALICE }),
        silence({
          id: 'sil-tool', created_by: 'chat-1',
          created_by_actor_id: SYSTEM, caused_by_actor_id: BOB,
        }),
        silence({ id: 'sil-old' }),
      ],
    });
    renderPage();
    // The page header renders its actions once per layout; either toggles.
    await userEvent.click((await screen.findAllByRole('button', { name: /Silences/ }))[0]);
    await waitFor(() => expect(attributions()).toEqual(['Alice', 'Nerve for Bob']));
  });

  it('opens the silences manager for a link from Settings', async () => {
    listNotifications.mockResolvedValue({ notifications: [], pending_count: 0, pending_answer_count: 0 });
    listSilences.mockResolvedValue({ silences: [silence({ id: 'sil-web', created_by_actor_id: ALICE })] });
    render(<MemoryRouter initialEntries={['/notifications?silences=1']}><NotificationsPage /></MemoryRouter>);
    expect(await screen.findByRole('heading', { name: 'Silences' })).toBeInTheDocument();
  });

  it('updates the answerer from the answer broadcast', async () => {
    listNotifications.mockResolvedValue({
      notifications: [notification({
        id: 'notif-live', status: 'pending', answer: null, answered_by: null,
        answered_at: null,
      })],
      pending_count: 1, pending_answer_count: 1,
    });
    renderPage();
    await screen.findByText('Pick one');
    act(() => {
      useNotificationStore.getState().handleWSNotificationAnswered({
        notification_id: 'notif-live', answer: 'yes', answered_by: 'web', actor_id: BOB,
      });
    });
    await waitFor(() => expect(attributions()).toEqual(['Bob']));
  });

  it('names who snoozed an approval that is still pending', async () => {
    listNotifications.mockResolvedValue({
      notifications: [notification({
        type: 'approval', status: 'pending', answer: null, answered_by: null,
        answered_at: null, options: ['approve', 'decline', 'snooze_24h'],
        redeliver_at: '2999-01-01T00:00:00+00:00', answered_by_actor_id: BOB,
      })],
      pending_count: 1, pending_answer_count: 1,
    });
    renderPage();
    await waitFor(() => expect(attributions()).toEqual(['Bob']));
    expect(screen.getByText(/Snoozed by/)).toBeInTheDocument();
  });

  it('updates the snoozer from the snooze broadcast', async () => {
    listNotifications.mockResolvedValue({
      notifications: [notification({
        id: 'notif-snooze', type: 'approval', status: 'pending', answer: null,
        answered_by: null, answered_at: null, options: ['approve', 'snooze_24h'],
      })],
      pending_count: 1, pending_answer_count: 1,
    });
    renderPage();
    await screen.findByText('Pick one');
    act(() => {
      useNotificationStore.getState().handleWSNotificationAnswered({
        notification_id: 'notif-snooze', answer: 'snooze_24h', answered_by: 'web',
        approval_status: 'snoozed', snooze_until: '2999-01-01T00:00:00+00:00',
        actor_id: ALICE,
      });
    });
    await waitFor(() => expect(attributions()).toEqual(['Alice']));
  });
});

/** Shows the current path, so a test can see where a click navigated. */
function Where() {
  return <p data-testid="where">{useLocation().pathname}</p>;
}

function renderAt(element: ReactElement) {
  return render(
    <MemoryRouter initialEntries={['/notifications']}>
      <Routes><Route path="*" element={<>{element}<Where /></>} /></Routes>
    </MemoryRouter>,
  );
}

const PLAN_NOTICE = {
  type: 'notify' as const, status: 'pending', answer: null, answered_by: null,
  answered_at: null, title: 'Plan ready for review: Fix the encoder',
  body: 'Version 1 of plan plan-1 waits for your decision.',
  metadata: JSON.stringify({ plan_id: 'plan-1', task_id: 'task-1' }),
};

describe('NotificationsPage links', () => {
  it('opens the plan and the task that a notification names', async () => {
    listNotifications.mockResolvedValue({
      notifications: [notification(PLAN_NOTICE)], pending_count: 1, pending_answer_count: 0,
    });
    renderAt(<NotificationsPage />);
    await userEvent.click(await screen.findByRole('button', { name: 'Open plan' }));
    expect(screen.getByTestId('where')).toHaveTextContent('/plans/plan-1');
    await userEvent.click(screen.getByRole('button', { name: 'Open task' }));
    expect(screen.getByTestId('where')).toHaveTextContent('/tasks/task-1');
  });

  it('shows no plan link on a notification without one', async () => {
    listNotifications.mockResolvedValue({
      notifications: [notification({ type: 'notify', status: 'pending', title: 'Build finished' })],
      pending_count: 1, pending_answer_count: 0,
    });
    renderAt(<NotificationsPage />);
    await screen.findByText('Build finished');
    expect(screen.queryByRole('button', { name: 'Open plan' })).not.toBeInTheDocument();
  });

  it('renders the body as markdown, with app links in place', async () => {
    listNotifications.mockResolvedValue({
      notifications: [notification({
        type: 'notify', status: 'pending', title: 'Done',
        body: 'See **the plan**: [plan-2](/plans/plan-2) or [docs](https://example.com/docs)',
      })],
      pending_count: 1, pending_answer_count: 0,
    });
    renderAt(<NotificationsPage />);
    expect((await screen.findByText('the plan')).tagName).toBe('STRONG');
    expect(screen.getByRole('link', { name: 'docs' })).toHaveAttribute('target', '_blank');
    const planLink = screen.getByRole('link', { name: 'plan-2' });
    expect(planLink).not.toHaveAttribute('target');
    await userEvent.click(planLink);
    expect(screen.getByTestId('where')).toHaveTextContent('/plans/plan-2');
  });
});

describe('NotificationToast links', () => {
  it('opens the plan from a live notification and closes the toast', async () => {
    renderAt(<NotificationToast />);
    act(() => {
      useNotificationStore.getState().handleWSNotification({
        notification_id: 'notif-live-plan', notification_type: 'notify', session_id: 's1',
        title: PLAN_NOTICE.title, body: PLAN_NOTICE.body, priority: 'high', options: null,
        metadata: { plan_id: 'plan-1', task_id: 'task-1' },
      });
    });
    await userEvent.click(await screen.findByRole('button', { name: 'Open plan' }));
    expect(screen.getByTestId('where')).toHaveTextContent('/plans/plan-1');
    expect(useNotificationStore.getState().toastQueue).toEqual([]);
    expect(useNotificationStore.getState().notifications[0].metadata)
      .toEqual({ plan_id: 'plan-1', task_id: 'task-1' });
  });
});

describe('NotificationsPage options', () => {
  it('answers a question from one row per option, with Other… and no wait note', async () => {
    listNotifications.mockResolvedValue({
      notifications: [notification({
        id: 'ask-1', status: 'pending', answer: null, answered_by: null, answered_at: null,
        options: '["Yes, all of these","No"]' as unknown as string[],
      })],
      pending_count: 1, pending_answer_count: 1,
    });
    answerNotification.mockResolvedValue({ answered: true });
    getNotification.mockResolvedValue(notification({ id: 'ask-1', answer: 'Yes, all of these' }));
    renderPage();
    const list = within(await screen.findByRole('group', { name: 'Pick one' }));
    expect(list.getAllByRole('button').map(b => b.textContent)).toEqual(['AYes, all of these', 'BNo', 'COther…']);
    expect(screen.queryByText('Nothing happens until you choose.')).not.toBeInTheDocument();
    await userEvent.click(list.getByRole('button', { name: 'Yes, all of these' }));
    expect(answerNotification).toHaveBeenCalledWith('ask-1', 'Yes, all of these');
  });

  it('says that an approval waits, and offers no Other…', async () => {
    listNotifications.mockResolvedValue({
      notifications: [notification({
        id: 'approval-1', type: 'approval', status: 'pending', answer: null, answered_by: null,
        answered_at: null, options: ['approve', 'decline'],
      })],
      pending_count: 1, pending_answer_count: 1,
    });
    renderPage();
    const list = within(await screen.findByRole('group', { name: 'Pick one' }));
    expect(list.getAllByRole('button').map(b => b.textContent)).toEqual(['Approve', 'Decline']);
    expect(screen.getByText('Nothing happens until you choose.')).toBeInTheDocument();
  });

  it('folds an answered approval to its label', async () => {
    listNotifications.mockResolvedValue({
      notifications: [notification({ type: 'approval', options: ['approve', 'decline'], answer: 'decline' })],
      pending_count: 0, pending_answer_count: 0,
    });
    renderPage();
    expect((await screen.findByText('Answer:')).parentElement).toHaveTextContent('Answer: Decline');
  });
});

describe('NotificationToast options', () => {
  const ASK = {
    notification_id: 'ask-live', notification_type: 'question', session_id: 's1',
    title: 'Which region?', body: '', priority: 'normal',
    options: ['North', 'South', 'East', 'West', 'Nowhere'],
  };

  function push(data: Record<string, unknown>) {
    act(() => { useNotificationStore.getState().handleWSNotification(data); });
  }

  it('shows every option and stays open until the question is answered', () => {
    vi.useFakeTimers();
    try {
      renderAt(<NotificationToast />);
      push(ASK);
      push({ ...ASK, notification_id: 'n-plain', notification_type: 'notify', title: 'Build finished', options: null });
      act(() => { vi.advanceTimersByTime(6000); });
      const list = within(screen.getByRole('group', { name: 'Which region?' }));
      expect(list.getAllByRole('button')).toHaveLength(6);
      expect(screen.queryByText('Build finished')).not.toBeInTheDocument();
    } finally {
      vi.useRealTimers();
    }
  });

  it('closes when answered in the toast', async () => {
    answerNotification.mockResolvedValue({ answered: true });
    getNotification.mockResolvedValue(notification({ id: 'ask-live', title: 'Which region?', answer: 'West' }));
    renderAt(<NotificationToast />);
    push(ASK);
    await userEvent.click(screen.getByRole('button', { name: 'West' }));
    expect(answerNotification).toHaveBeenCalledWith('ask-live', 'West');
    await waitFor(() => expect(useNotificationStore.getState().toastQueue).toEqual([]));
  });

  it('shows the newest toasts when three questions wait', () => {
    renderAt(<NotificationToast />);
    for (const n of [1, 2, 3]) push({ ...ASK, notification_id: `ask-${n}`, title: `Question ${n}` });
    push({ ...ASK, notification_id: 'n-urgent', notification_type: 'notify', title: 'Disk is full', options: null });
    expect(screen.getByText('Disk is full')).toBeInTheDocument();
    expect(screen.queryByText('Question 1')).not.toBeInTheDocument();
    expect(screen.getByText('Question 3')).toBeInTheDocument();
    expect(useNotificationStore.getState().toastQueue).toHaveLength(4);
  });

  it('folds after an answer from the page, without the answer event', async () => {
    listNotifications.mockResolvedValue({ notifications: [], pending_count: 0, pending_answer_count: 0 });
    answerNotification.mockResolvedValue({ answered: true });
    getNotification.mockResolvedValue(notification({
      id: 'ask-live', title: 'Which region?', answer: 'East', answered_by: 'web',
    }));
    renderAt(<NotificationToast />);
    push(ASK);
    await act(async () => { await useNotificationStore.getState().answerNotification('ask-live', 'East'); });
    expect(getNotification).toHaveBeenCalledWith('ask-live');
    expect((await screen.findByText('Answer:')).parentElement).toHaveTextContent('Answer: East');
  });

  it('closes the notify toasts on Dismiss all and keeps the questions', async () => {
    listNotifications.mockResolvedValue({ notifications: [], pending_count: 0, pending_answer_count: 0 });
    dismissAllNotifications.mockResolvedValue({ dismissed: 1 });
    renderAt(<NotificationToast />);
    push(ASK);
    push({ ...ASK, notification_id: 'n-plain', notification_type: 'notify', title: 'Build finished', options: null });
    await act(async () => { await useNotificationStore.getState().dismissAll(); });
    expect(screen.queryByText('Build finished')).not.toBeInTheDocument();
    expect(screen.getByText('Which region?')).toBeInTheDocument();
  });

  it('folds to the answer from another place, then closes', () => {
    vi.useFakeTimers();
    try {
      renderAt(<NotificationToast />);
      push(ASK);
      act(() => {
        useNotificationStore.getState().handleWSNotificationAnswered({
          notification_id: 'ask-live', answer: 'East', answered_by: 'web', actor_id: null,
        });
      });
      expect(screen.queryByRole('group')).not.toBeInTheDocument();
      expect(screen.getByText('Answer:').parentElement).toHaveTextContent('Answer: East');
      act(() => { vi.advanceTimersByTime(6000); });
      expect(screen.queryByText('Which region?')).not.toBeInTheDocument();
    } finally {
      vi.useRealTimers();
    }
  });
});
