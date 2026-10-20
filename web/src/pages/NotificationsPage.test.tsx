import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { ActorRef } from '../api/client';
import type { Notification, Silence } from '../stores/notificationStore';

vi.mock('../api/client', () => ({
  api: {
    listNotifications: vi.fn(),
    listSilences: vi.fn(),
    listActors: vi.fn(),
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

const listNotifications = vi.mocked(api.listNotifications);
const listSilences = vi.mocked(api.listSilences);
const listActors = vi.mocked(api.listActors);

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
      pending_count: 0,
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
      pending_count: 0,
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
      pending_count: 0,
    });
    renderPage();
    await waitFor(() => expect(attributions()).toEqual(['Bob']));
    expect(screen.getByText(/Dismissed by/)).toBeInTheDocument();
  });

  it('names who created a silence, and for whom', async () => {
    listNotifications.mockResolvedValue({ notifications: [], pending_count: 0 });
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

  it('updates the answerer from the answer broadcast', async () => {
    listNotifications.mockResolvedValue({
      notifications: [notification({
        id: 'notif-live', status: 'pending', answer: null, answered_by: null,
        answered_at: null,
      })],
      pending_count: 1,
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
      pending_count: 1,
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
      pending_count: 1,
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
