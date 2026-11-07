import { render, screen, waitFor, within } from '@testing-library/react';
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
    approvePlan: vi.fn(),
  },
  // ActorLabel reads the viewer through the auth store, which wires these.
  setToken: vi.fn(), clearToken: vi.fn(), getToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));
vi.mock('../stores/helpers/draftStorage', async (orig) => ({
  ...(await orig<typeof import('../stores/helpers/draftStorage')>()), clearAllDrafts: vi.fn(),
}));
vi.mock('../stores/helpers/readStorage', async (orig) => ({
  ...(await orig<typeof import('../stores/helpers/readStorage')>()), clearAllReads: vi.fn(),
}));
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
const approvePlan = vi.mocked(api.approvePlan);

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
      <Routes>
        <Route path="/plans/:planId" element={<PlanDetailPage />} />
        <Route path="/chat/:sessionId" element={<div>Chat page</div>} />
      </Routes>
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

describe('PlanDetailPage summary', () => {
  it('shows the summary from the agent under the title', async () => {
    getPlan.mockResolvedValue(plan({ summary: 'Adds a toggle.\nOne PR in example/repo.' }));
    renderPage();
    expect(await screen.findByTestId('plan-summary')).toHaveTextContent('Adds a toggle. One PR in example/repo.');
  });

  it('shows the first heading and lines of a plan without a summary', async () => {
    getPlan.mockResolvedValue(plan({
      summary: null, content: '# Add a toggle\n\nThe settings page gets a switch.\n\n- Step one\n- Step two',
    }));
    renderPage();
    expect(await screen.findByTestId('plan-summary'))
      .toHaveTextContent('Add a toggle The settings page gets a switch. Step one');
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
    const bar = screen.getByRole('region', { name: 'Plan decision' });
    expect(within(bar).getByRole('button', { name: 'Approve & implement' })).toBeInTheDocument();
    expect(within(bar).getByRole('button', { name: 'Decline…' })).toBeInTheDocument();
    expect(within(bar).getByRole('button', { name: 'Request revision…' })).toBeInTheDocument();
  });

  it('shows no reviewer for a plan that nobody reviewed', async () => {
    getPlan.mockResolvedValue(plan({}));
    renderPage();
    await screen.findByTestId('plan-summary');
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
    await userEvent.click(await screen.findByRole('button', { name: 'Decline…' }));
    await userEvent.type(screen.getByRole('textbox', { name: 'Why decline? (optional)' }), 'not now');
    await userEvent.click(screen.getByRole('button', { name: 'Decline plan' }));

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
    await userEvent.click(await screen.findByRole('button', { name: 'Request revision…' }));
    await userEvent.type(screen.getByRole('textbox', { name: 'What should change?' }), 'add tests');
    await userEvent.click(screen.getByRole('button', { name: 'Send' }));

    const reviewer = await screen.findByTestId('plan-reviewer');
    await waitFor(() => expect(reviewer).toHaveTextContent('Reviewed by Alice'));
    expect(revisePlan).toHaveBeenCalledWith('plan-1', 'add tests');
  });
});

describe('PlanDetailPage decision bar', () => {
  it('offers no decision on a plan that is not pending', async () => {
    getPlan.mockResolvedValue(plan({ status: 'declined' }));
    renderPage();
    await screen.findByTestId('plan-summary');
    expect(screen.queryByRole('region', { name: 'Plan decision' })).not.toBeInTheDocument();
  });

  it('declines without a reason, and says that the task closes', async () => {
    getPlan.mockResolvedValue(plan({}));
    updatePlan.mockResolvedValue({ plan_id: 'plan-1', updated: true });
    listPlans.mockResolvedValue({ plans: [] });
    renderPage();
    await userEvent.click(await screen.findByRole('button', { name: 'Decline…' }));
    expect(screen.getByRole('textbox', { name: 'Why decline? (optional)' }))
      .toHaveAccessibleDescription('Declining also closes the task.');
    await userEvent.click(screen.getByRole('button', { name: 'Decline plan' }));
    expect(updatePlan).toHaveBeenCalledWith('plan-1', { status: 'declined', feedback: undefined });
  });

  it('closes a field on Escape and gives the focus back to its button', async () => {
    getPlan.mockResolvedValue(plan({}));
    renderPage();
    const toggle = await screen.findByRole('button', { name: 'Decline…' });
    await userEvent.click(toggle);
    await userEvent.type(screen.getByRole('textbox', { name: 'Why decline? (optional)' }), 'later{Escape}');
    expect(screen.queryByRole('textbox')).not.toBeInTheDocument();
    expect(toggle).toHaveFocus();
    expect(updatePlan).not.toHaveBeenCalled();
  });

  it('closes the revision field once the request is sent', async () => {
    getPlan.mockResolvedValue(plan({}));
    revisePlan.mockResolvedValue({ plan_id: 'plan-1', status: 'revision_requested' });
    renderPage();
    const toggle = await screen.findByRole('button', { name: 'Request revision…' });
    await userEvent.click(toggle);
    expect(toggle).toHaveAttribute('aria-expanded', 'true');
    expect(screen.getByRole('button', { name: 'Send' })).toBeDisabled();
    await userEvent.type(screen.getByRole('textbox', { name: 'What should change?' }), 'split the PR');
    await userEvent.click(screen.getByRole('button', { name: 'Send' }));
    await waitFor(() => expect(screen.queryByRole('textbox')).not.toBeInTheDocument());
    expect(toggle).toHaveAttribute('aria-expanded', 'false');
  });

  it('keeps the field and shows the error when a revision fails', async () => {
    getPlan.mockResolvedValue(plan({}));
    revisePlan.mockRejectedValue(new Error('409: {"detail":"Plan is no longer pending"}'));
    const quiet = vi.spyOn(console, 'error').mockImplementation(() => {});
    renderPage();
    await userEvent.click(await screen.findByRole('button', { name: 'Request revision…' }));
    await userEvent.type(screen.getByRole('textbox', { name: 'What should change?' }), 'split the PR');
    await userEvent.click(screen.getByRole('button', { name: 'Send' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Plan is no longer pending');
    expect(screen.getByRole('textbox', { name: 'What should change?' })).toHaveValue('split the PR');
    quiet.mockRestore();
  });

  it('disables the bar while the approval is sent, then opens the implementation', async () => {
    getPlan.mockResolvedValue(plan({}));
    listPlans.mockResolvedValue({ plans: [] });
    let resolve!: (value: { plan_id: string; impl_session_id: string }) => void;
    approvePlan.mockReturnValue(new Promise(r => { resolve = r; }));
    renderPage();
    await userEvent.click(await screen.findByRole('button', { name: 'Approve & implement' }));
    expect(screen.getByRole('button', { name: 'Decline…' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Approve & implement' })).toBeDisabled();
    resolve({ plan_id: 'plan-1', impl_session_id: 'impl-1' });
    expect(await screen.findByText('Chat page')).toBeInTheDocument();
  });

  it('shows why an approval failed', async () => {
    getPlan.mockResolvedValue(plan({}));
    approvePlan.mockRejectedValue(new Error('409: {"detail":"Plan is \'declined\'"}'));
    const quiet = vi.spyOn(console, 'error').mockImplementation(() => {});
    renderPage();
    await userEvent.click(await screen.findByRole('button', { name: 'Approve & implement' }));
    expect(await screen.findByRole('alert')).toHaveTextContent("Plan is 'declined'");
    quiet.mockRestore();
  });
});

describe('PlanDetailPage implementation', () => {
  it('shows the progress card of an approved plan, and no decision', async () => {
    getPlan.mockResolvedValue(plan({
      status: 'implementing', impl_session_id: 'impl-1', reviewed_at: '2026-01-02T10:30:00+00:00',
      impl_session: {
        id: 'impl-1', title: 'Implement: Write the report', status: 'active',
        updated_at: '2026-01-02T10:30:00+00:00', is_running: true, awaiting_input: false, parked: false,
      },
    }));
    renderPage();
    const card = await screen.findByRole('region', { name: 'Implementation' });
    expect(card).toHaveTextContent('Implement: Write the report');
    expect(card).toHaveTextContent('Running');
    expect(screen.queryByRole('region', { name: 'Plan decision' })).not.toBeInTheDocument();
    await userEvent.click(within(card).getByRole('button', { name: 'Open session' }));
    expect(await screen.findByText('Chat page')).toBeInTheDocument();
  });
});
