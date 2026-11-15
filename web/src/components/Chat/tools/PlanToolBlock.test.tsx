import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Plan } from '../../../stores/planStore';
import type { ToolCallBlockData } from '../../../types/chat';

vi.mock('../../../api/client', () => ({
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
vi.mock('../../../stores/helpers/draftStorage', async (orig) => ({
  ...(await orig<typeof import('../../../stores/helpers/draftStorage')>()), clearAllDrafts: vi.fn(),
}));
vi.mock('../../../stores/helpers/readStorage', async (orig) => ({
  ...(await orig<typeof import('../../../stores/helpers/readStorage')>()), clearAllReads: vi.fn(),
}));
// The plan text is not under test; keep the markdown pipeline out of it.
vi.mock('../MarkdownContent', () => ({
  MarkdownContent: ({ content }: { content: string }) => <div>{content}</div>,
}));

const { api } = await import('../../../api/client');
const { useActorStore } = await import('../../../stores/actorStore');
const { usePlanStore } = await import('../../../stores/planStore');
const { PlanToolBlock } = await import('./PlanToolBlock');

const getPlan = vi.mocked(api.getPlan);
const listActors = vi.mocked(api.listActors);

function plan(overrides: Partial<Plan> = {}): Plan {
  return {
    id: 'plan-1',
    task_id: 'task-1',
    task_title: 'Write the report',
    session_id: 'cron:task-planner',
    impl_session_id: null,
    status: 'pending',
    content: 'Ship the report',
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

function propose(): ToolCallBlockData {
  return {
    type: 'tool_call',
    toolUseId: 'tu-1',
    tool: 'mcp__nerve__plan_propose',
    input: { task_id: 'task-1', content: '# Plan\nShip the report' },
    result: "Plan proposed: plan-1 (v1) for task 'Write the report'. Awaiting human review.",
    status: 'complete',
  };
}

function Location() {
  return <div data-testid="location">{useLocation().pathname}</div>;
}

function renderBlock(block: ToolCallBlockData) {
  return render(
    <MemoryRouter initialEntries={['/chat/abc']}>
      <Location />
      <Routes>
        <Route path="/chat/:sessionId" element={<PlanToolBlock block={block} />} />
        <Route path="*" element={<div>Elsewhere</div>} />
      </Routes>
    </MemoryRouter>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  useActorStore.getState().reset();
  usePlanStore.setState({ selectedPlan: null, detailLoading: false, actionError: null });
  listActors.mockResolvedValue({ actors: [] });
});

describe('PlanToolBlock review dialog', () => {
  it('opens the plan in a dialog from the Review plan button, without leaving the chat', async () => {
    getPlan.mockResolvedValue(plan());
    const user = userEvent.setup();
    renderBlock(propose());

    await user.click(screen.getByRole('button', { name: /propose plan/i }));
    await user.click(screen.getByRole('button', { name: /review plan/i }));

    const dialog = await screen.findByRole('dialog');
    expect(await screen.findByText('Ship the report', { selector: 'div' })).toBeInTheDocument();
    expect(dialog).toHaveTextContent('Write the report');
    expect(getPlan).toHaveBeenCalledWith('plan-1');
    expect(screen.getByTestId('location')).toHaveTextContent('/chat/abc');
    expect(screen.getByRole('button', { name: 'Approve & implement' })).toBeInTheDocument();
  });

  it('closes the dialog and clears the selected plan', async () => {
    getPlan.mockResolvedValue(plan());
    const user = userEvent.setup();
    renderBlock(propose());

    await user.click(screen.getByRole('button', { name: /propose plan/i }));
    await user.click(screen.getByRole('button', { name: /review plan/i }));
    await screen.findByRole('dialog');
    await user.click(screen.getByRole('button', { name: 'Close dialog' }));

    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
    expect(usePlanStore.getState().selectedPlan).toBeNull();
  });

  it('shows no decision bar for a plan that was decided', async () => {
    getPlan.mockResolvedValue(plan({ status: 'declined' }));
    const user = userEvent.setup();
    renderBlock(propose());

    await user.click(screen.getByRole('button', { name: /propose plan/i }));
    await user.click(screen.getByRole('button', { name: /review plan/i }));
    await screen.findByRole('dialog');

    await screen.findByText('Ship the report', { selector: 'div' });
    expect(screen.queryByRole('region', { name: 'Plan decision' })).not.toBeInTheDocument();
  });

  it('opens the full page from the button in the dialog title', async () => {
    getPlan.mockResolvedValue(plan());
    const user = userEvent.setup();
    renderBlock(propose());

    await user.click(screen.getByRole('button', { name: /propose plan/i }));
    await user.click(screen.getByRole('button', { name: /review plan/i }));
    await screen.findByRole('dialog');
    await user.click(screen.getByRole('button', { name: 'Open as full page' }));

    expect(screen.getByTestId('location')).toHaveTextContent('/plans/plan-1');
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });
});
