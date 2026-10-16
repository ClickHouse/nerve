import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { ActorRef } from '../api/client';
import type { Plan } from '../stores/planStore';

vi.mock('../api/client', () => ({
  api: {
    getPlan: vi.fn(),
    listActors: vi.fn(),
  },
  // ActorLabel reads the viewer through the auth store, which wires these.
  setToken: vi.fn(), clearToken: vi.fn(), getToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));
vi.mock('../stores/helpers/draftStorage', () => ({ clearAllDrafts: vi.fn() }));
vi.mock('../stores/helpers/readStorage', () => ({ clearAllReads: vi.fn() }));
// The plan body is not under test; keep the markdown pipeline out of it.
vi.mock('../components/Chat/MarkdownContent', () => ({
  MarkdownContent: ({ content }: { content: string }) => <div>{content}</div>,
}));

const { api } = await import('../api/client');
const { useActorStore } = await import('../stores/actorStore');
const { PlanDetailPage } = await import('./PlanDetailPage');

const getPlan = vi.mocked(api.getPlan);
const listActors = vi.mocked(api.listActors);

const ALICE = 'actor-alice';
const SYSTEM = 'actor-system';

function plan(overrides: Partial<Plan>): Plan {
  return {
    id: 'plan-1',
    task_id: 'task-1',
    task_title: 'Write the report',
    session_id: 'cron:task-planner',
    impl_session_id: null,
    status: 'pending',
    content: 'Ship it',
    feedback: null,
    version: 1,
    parent_plan_id: null,
    model: null,
    plan_type: 'generic',
    created_at: '2026-01-01T00:00:00+00:00',
    reviewed_at: null,
    reviewed_by_actor_id: null,
    reviewed_caused_by_actor_id: null,
    created_by_actor_id: null,
    caused_by_actor_id: null,
    ...overrides,
  };
}

function actorRef(id: string, overrides: Partial<ActorRef> = {}): ActorRef {
  return { id, kind: 'human', display_name: null, username: null, ...overrides };
}

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/plans/plan-1']}>
      <Routes><Route path="/plans/:planId" element={<PlanDetailPage />} /></Routes>
    </MemoryRouter>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  useActorStore.getState().reset();
  listActors.mockResolvedValue({
    actors: [
      actorRef(ALICE, { display_name: 'Alice' }),
      actorRef(SYSTEM, { kind: 'system' }),
    ],
  });
});

describe('PlanDetailPage reviewer', () => {
  it('names the person who reviewed the plan', async () => {
    getPlan.mockResolvedValue(plan({
      status: 'declined', reviewed_at: '2026-01-02T10:30:00+00:00',
      reviewed_by_actor_id: ALICE, reviewed_caused_by_actor_id: ALICE,
    }));
    renderPage();
    const reviewer = await screen.findByTestId('plan-reviewer');
    await waitFor(() => expect(reviewer).toHaveTextContent('Reviewed by Alice2026-01-02 10:30'));
  });

  it('shows a plan approved by the agent for a person', async () => {
    getPlan.mockResolvedValue(plan({
      status: 'implementing', impl_session_id: 'impl-1',
      reviewed_at: '2026-01-02T10:30:00+00:00',
      reviewed_by_actor_id: SYSTEM, reviewed_caused_by_actor_id: ALICE,
    }));
    renderPage();
    const reviewer = await screen.findByTestId('plan-reviewer');
    await waitFor(() => expect(reviewer).toHaveTextContent('Nerve for Alice'));
  });

  it('names who proposed the plan, and for whom', async () => {
    getPlan.mockResolvedValue(plan({
      created_by_actor_id: SYSTEM, caused_by_actor_id: ALICE,
    }));
    renderPage();
    const author = await screen.findByTestId('plan-author');
    await waitFor(() => expect(author).toHaveTextContent('Proposed by Nerve for Alice'));
  });

  it('offers no decision while a revision runs', async () => {
    getPlan.mockResolvedValue(plan({
      status: 'revising', feedback: 'add tests',
      reviewed_by_actor_id: ALICE, reviewed_caused_by_actor_id: ALICE,
    }));
    renderPage();
    await screen.findByTestId('plan-revising');
    expect(screen.queryByRole('button', { name: /Approve/ })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Decline/ })).not.toBeInTheDocument();
  });

  it('shows no reviewer for a plan that nobody reviewed', async () => {
    getPlan.mockResolvedValue(plan({}));
    renderPage();
    await screen.findByText('Ship it');
    expect(screen.queryByTestId('plan-reviewer')).not.toBeInTheDocument();
    expect(screen.queryByTestId('plan-author')).not.toBeInTheDocument();
  });
});
