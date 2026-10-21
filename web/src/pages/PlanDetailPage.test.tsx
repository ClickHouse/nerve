import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { ActorRef } from '../api/client';
import type { Plan } from '../stores/planStore';

vi.mock('../api/client', () => ({
  api: {
    getPlan: vi.fn(),
    listActors: vi.fn(),
    listPlans: vi.fn(),
    updatePlan: vi.fn(),
    revisePlan: vi.fn(),
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
const listPlans = vi.mocked(api.listPlans);
const updatePlan = vi.mocked(api.updatePlan);
const revisePlan = vi.mocked(api.revisePlan);

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

  it('keeps every decision on a pending plan that has revision feedback', async () => {
    getPlan.mockResolvedValue(plan({
      status: 'pending', feedback: 'add tests',
      reviewed_by_actor_id: ALICE, reviewed_caused_by_actor_id: ALICE,
    }));
    renderPage();
    await screen.findByText('Revision feedback');
    expect(screen.getByRole('button', { name: /Approve/ })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Decline/ })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Request Revision/ })).toBeInTheDocument();
  });

  it('shows no reviewer for a plan that nobody reviewed', async () => {
    getPlan.mockResolvedValue(plan({}));
    renderPage();
    await screen.findByText('Ship it');
    expect(screen.queryByTestId('plan-reviewer')).not.toBeInTheDocument();
    expect(screen.queryByTestId('plan-author')).not.toBeInTheDocument();
  });

  it('shows the reviewer right after a decline in this tab', async () => {
    getPlan
      .mockResolvedValueOnce(plan({}))
      .mockResolvedValueOnce(plan({
        status: 'declined', feedback: 'not now', reviewed_at: '2026-01-02T10:30:00+00:00',
        reviewed_by_actor_id: ALICE, reviewed_caused_by_actor_id: ALICE,
      }));
    updatePlan.mockResolvedValue({ plan_id: 'plan-1', updated: true });
    listPlans.mockResolvedValue({ plans: [] });
    renderPage();
    await userEvent.click(await screen.findByRole('button', { name: /^Decline$/ }));
    await userEvent.type(screen.getByPlaceholderText(/why is this plan being declined/), 'not now');
    await userEvent.click(screen.getByRole('button', { name: /Confirm Decline/ }));

    const reviewer = await screen.findByTestId('plan-reviewer');
    await waitFor(() => expect(reviewer).toHaveTextContent('Reviewed by Alice'));
    expect(updatePlan).toHaveBeenCalledWith('plan-1', { status: 'declined', feedback: 'not now' });
    expect(getPlan).toHaveBeenCalledTimes(2);
  });

  it('shows the reviewer right after a revision request in this tab', async () => {
    getPlan
      .mockResolvedValueOnce(plan({}))
      .mockResolvedValueOnce(plan({
        feedback: 'add tests',
        reviewed_by_actor_id: ALICE, reviewed_caused_by_actor_id: ALICE,
      }));
    revisePlan.mockResolvedValue({ plan_id: 'plan-1', status: 'revision_requested' });
    renderPage();
    await userEvent.click(await screen.findByRole('button', { name: /Request Revision/ }));
    await userEvent.type(screen.getByPlaceholderText(/Describe what to change/), 'add tests');
    await userEvent.click(screen.getByRole('button', { name: /Send Revision Request/ }));

    const reviewer = await screen.findByTestId('plan-reviewer');
    await waitFor(() => expect(reviewer).toHaveTextContent('Reviewed by Alice'));
    expect(revisePlan).toHaveBeenCalledWith('plan-1', 'add tests');
  });
});
