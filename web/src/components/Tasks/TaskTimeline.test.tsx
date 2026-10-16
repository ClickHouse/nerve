import { render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { ActorRef, TaskEvent } from '../../api/client';

vi.mock('../../api/client', () => ({
  api: {
    listTaskEvents: vi.fn(),
    listActors: vi.fn(),
  },
  // ActorLabel reads the viewer through the auth store, which wires these.
  setToken: vi.fn(), clearToken: vi.fn(), getToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));
vi.mock('../../stores/helpers/draftStorage', () => ({ clearAllDrafts: vi.fn() }));
vi.mock('../../stores/helpers/readStorage', () => ({ clearAllReads: vi.fn() }));

const { api } = await import('../../api/client');
const { useActorStore } = await import('../../stores/actorStore');
const { TaskTimeline } = await import('./TaskTimeline');
const { ActorAttribution } = await import('../Chat/ActorLabel');

const listTaskEvents = vi.mocked(api.listTaskEvents);
const listActors = vi.mocked(api.listActors);

const ALICE = 'actor-alice';
const BOB = 'actor-bob';
const SYSTEM = 'actor-system';

function actorRef(id: string, overrides: Partial<ActorRef> = {}): ActorRef {
  return { id, kind: 'human', display_name: null, username: null, ...overrides };
}

let nextId = 1;
function event(overrides: Partial<TaskEvent>): TaskEvent {
  return {
    id: nextId++,
    task_id: 't1',
    from_status: 'pending',
    to_status: 'in_progress',
    actor: 'web',
    actor_id: null,
    caused_by_actor_id: null,
    created_at: '2026-01-01T00:00:00+00:00',
    ...overrides,
  };
}

function attributions(): string[] {
  return [...document.querySelectorAll('[data-attribution="actor"]')]
    .map((el) => el.textContent ?? '');
}

beforeEach(() => {
  vi.clearAllMocks();
  useActorStore.getState().reset();
  listActors.mockResolvedValue({
    actors: [
      actorRef(ALICE, { display_name: 'Alice' }),
      actorRef(BOB, { display_name: 'Bob' }),
      actorRef(SYSTEM, { kind: 'system' }),
    ],
  });
});

describe('TaskTimeline attribution', () => {
  it('names the person, Nerve for a person, and Nerve on its own', async () => {
    listTaskEvents.mockResolvedValue({
      events: [
        event({ from_status: null, to_status: 'pending', actor_id: ALICE, caused_by_actor_id: ALICE }),
        event({ actor: 'sess-1', actor_id: SYSTEM, caused_by_actor_id: BOB }),
        event({ to_status: 'done', actor: 'cron-1', actor_id: SYSTEM, caused_by_actor_id: SYSTEM }),
      ],
    });
    render(<TaskTimeline taskId="t1" currentStatus="done" />);

    await waitFor(() => expect(attributions()).toEqual(['Alice', 'Nerve for Bob', 'Nerve']));
    expect(screen.getByTitle('Changed by Nerve on behalf of Bob')).toBeInTheDocument();
    expect(screen.getByTitle('Changed by Alice')).toBeInTheDocument();
    // The origin text is not shown once the actor is known.
    expect(screen.queryByText(/sess-1/)).toBeNull();
  });

  it('keeps the origin text for rows without an actor id', async () => {
    listTaskEvents.mockResolvedValue({
      events: [
        event({ from_status: null, to_status: 'pending', actor: 'backfill' }),
        event({ actor: 'web' }),
        event({ to_status: 'deferred', actor: 'system' }),
      ],
    });
    render(<TaskTimeline taskId="t1" currentStatus="deferred" />);

    expect(await screen.findByText(/· backfill/)).toBeInTheDocument();
    expect(screen.getByText(/· web/)).toBeInTheDocument();
    expect(screen.queryByText(/· system/)).toBeNull();
    expect(attributions()).toEqual([]);
  });
});

describe('ActorAttribution', () => {
  it('shows Nerve alone when the cause is unknown', async () => {
    render(<ActorAttribution actorId={SYSTEM} causeId={null} />);
    expect(await screen.findByText('Nerve')).toBeInTheDocument();
    expect(screen.getByTitle('By Nerve itself — scheduled or autonomous work')).toBeInTheDocument();
  });

  it('shows a person acting for themselves once', async () => {
    render(<ActorAttribution actorId={BOB} causeId={BOB} />);
    expect(await screen.findByText('Bob')).toBeInTheDocument();
    expect(attributions()).toEqual(['Bob']);
  });

  it('renders nothing without an actor, so the caller can fall back', () => {
    const { container } = render(<ActorAttribution actorId={null} causeId={ALICE} />);
    expect(container).toBeEmptyDOMElement();
  });
});
