import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { ActorRef, WorkflowRun } from '../api/client';

vi.mock('../api/client', () => ({
  api: {
    listWorkflowRuns: vi.fn(),
    getWorkflowRunJournal: vi.fn(),
    killWorkflowRun: vi.fn(),
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
const { useWorkflowRunStore } = await import('../stores/workflowRunStore');
const { WorkflowRunsPage } = await import('./WorkflowRunsPage');

const listWorkflowRuns = vi.mocked(api.listWorkflowRuns);
const getWorkflowRunJournal = vi.mocked(api.getWorkflowRunJournal);
const listActors = vi.mocked(api.listActors);

const ALICE = 'actor-alice';
const BOB = 'actor-bob';
const SYSTEM = 'actor-system';

function run(overrides: Partial<WorkflowRun>): WorkflowRun {
  return {
    id: 'wfr-1',
    engine: 'claude-workflow',
    title: 'Summarize the logs',
    spec: { prompt: 'Summarize the logs' },
    status: 'done',
    budget_usd: 2,
    spent_usd: 0.5,
    warned_at: null,
    session_id: 'workflow:wfr-1',
    journal_dir: null,
    created_by: 'api',
    created_by_actor_id: null,
    caused_by_actor_id: null,
    killed_by_actor_id: null,
    killed_caused_by_actor_id: null,
    error: null,
    result: null,
    created_at: '2026-01-01T00:00:00+00:00',
    started_at: null,
    finished_at: null,
    updated_at: '2026-01-01T00:00:00+00:00',
    ...overrides,
  };
}

function actorRef(id: string, overrides: Partial<ActorRef> = {}): ActorRef {
  return { id, kind: 'human', display_name: null, username: null, ...overrides };
}

async function renderWith(runs: WorkflowRun[]) {
  listWorkflowRuns.mockResolvedValue({ runs, total: runs.length });
  render(<MemoryRouter><WorkflowRunsPage /></MemoryRouter>);
  await screen.findByText(runs[0].title);
}

function expand(title: string) {
  fireEvent.click(screen.getByText(title));
}

beforeEach(() => {
  vi.clearAllMocks();
  useActorStore.getState().reset();
  useWorkflowRunStore.setState({ runs: [], total: 0, loading: true, fetching: false, error: null });
  getWorkflowRunJournal.mockResolvedValue({
    run_json: null, events: [], has_result: false, result: '',
  });
  listActors.mockResolvedValue({
    actors: [
      actorRef(ALICE, { display_name: 'Alice' }),
      actorRef(BOB, { display_name: 'Bob' }),
      actorRef(SYSTEM, { kind: 'system' }),
    ],
  });
});

describe('WorkflowRunsPage attribution', () => {
  it('names the person who created the run', async () => {
    await renderWith([run({ created_by_actor_id: ALICE, caused_by_actor_id: ALICE })]);
    const creator = screen.getByTestId('run-creator');
    await waitFor(() => expect(creator).toHaveTextContent('by Alice'));

    expand('Summarize the logs');
    const detail = await screen.findByTestId('run-created-by');
    await waitFor(() => expect(detail).toHaveTextContent('Created byAlice'));
    // The origin text stays available as the tooltip.
    expect(detail.querySelector('[title="api"]')).not.toBeNull();
  });

  it('shows a run that the agent started for a person', async () => {
    await renderWith([run({
      created_by: 'session:alices-chat',
      created_by_actor_id: SYSTEM, caused_by_actor_id: ALICE,
    })]);
    const creator = screen.getByTestId('run-creator');
    await waitFor(() => expect(creator).toHaveTextContent('by Nerve for Alice'));
  });

  it('falls back to the origin text for an older run', async () => {
    await renderWith([run({ created_by: 'session:main' })]);
    expect(screen.queryByTestId('run-creator')).not.toBeInTheDocument();

    expand('Summarize the logs');
    const detail = await screen.findByTestId('run-created-by');
    expect(detail).toHaveTextContent('Created bysession:main');
    expect(screen.queryByTestId('run-killed-by')).not.toBeInTheDocument();
  });

  it('names the person who killed the run', async () => {
    await renderWith([run({
      status: 'killed', error: 'not needed (by api)',
      created_by_actor_id: ALICE, caused_by_actor_id: ALICE,
      killed_by_actor_id: BOB, killed_caused_by_actor_id: BOB,
    })]);
    expand('Summarize the logs');
    const killer = await screen.findByTestId('run-killed-by');
    await waitFor(() => expect(killer).toHaveTextContent('Killed byBob'));
  });
});
