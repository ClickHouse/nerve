import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { CronSchedule } from '../../api/client';
import type { CronJob } from '../../stores/cronStore';

vi.mock('../../api/client', () => ({
  api: {
    listCronJobs: vi.fn(),
    getCronSchedule: vi.fn(),
    getCronLogs: vi.fn(),
    triggerCronJob: vi.fn(),
    rotateCronJob: vi.fn(),
    setCronJobEnabled: vi.fn(),
  },
}));

const { api } = await import('../../api/client');
const { FORECAST_TIMEOUT_MS, useCronStore } = await import('../../stores/cronStore');
const { JobSheet } = await import('./JobSheet');

const listCronJobs = vi.mocked(api.listCronJobs);
const getCronSchedule = vi.mocked(api.getCronSchedule);
const getCronLogs = vi.mocked(api.getCronLogs);
const setCronJobEnabled = vi.mocked(api.setCronJobEnabled);

const SKIP_REASON = "gate 'tasks' not satisfied (pending tasks exist)";

function job(overrides: Partial<CronJob> = {}): CronJob {
  return {
    id: 'planner',
    type: 'cron',
    kind: 'agent',
    schedule: '0 * * * *',
    description: '',
    enabled: true,
    next_run: '2030-01-01T09:00:00+00:00',
    last_run: null,
    stats_7d: { runs: 0, success: 0, error: 0, missed: 0 },
    ...overrides,
  };
}

function renderSheet(overrides: Partial<CronJob> = {}) {
  return render(
    <MemoryRouter>
      <JobSheet job={job(overrides)} />
    </MemoryRouter>,
  );
}

function fact(label: string): HTMLElement {
  return screen.getByText(label, { selector: 'dt' }).parentElement!;
}

describe('the job sheet', () => {
  beforeEach(() => {
    useCronStore.setState({ toggling: null, toggleError: null });
  });

  it('says the schedule in words, with the expression beside it', () => {
    renderSheet({ schedule: '0 8 * * *' });
    expect(within(fact('Schedule')).getByText('At 08:00')).toBeInTheDocument();
    expect(within(fact('Schedule')).getByText('0 8 * * *')).toBeInTheDocument();
  });

  it('says an interval in words', () => {
    renderSheet({ schedule: '1h30m' });
    expect(fact('Schedule').textContent).toBe('ScheduleEvery 1h 30m');
  });

  it('shows a schedule that it cannot read as written', () => {
    renderSheet({ schedule: 'hourly' });
    expect(fact('Schedule').textContent).toBe('Schedulehourly');
  });

  it('shows the last run with its result', () => {
    renderSheet({
      last_run: { status: 'error', started_at: '2030-01-01 08:00:00', finished_at: '2030-01-01T08:00:12+00:00' },
    });
    const last = within(fact('Last run'));
    expect(last.getByRole('img', { name: 'Failed' })).toBeInTheDocument();
    expect(last.getByText('Took 12.0s')).toBeInTheDocument();
  });

  it('says when a job never ran', () => {
    renderSheet();
    expect(within(fact('Last run')).getByText('No runs yet')).toBeInTheDocument();
    expect(screen.getByTestId('cron-success').textContent).toBe('No runs');
  });

  it('counts the runs of the last 7 days that succeeded', () => {
    renderSheet({ stats_7d: { runs: 8, success: 6, error: 1, missed: 0 } });
    expect(screen.getByTestId('cron-success').textContent).toBe('6 of 7 succeeded');
    expect(within(fact('Last 7 days')).getByText('1 failed')).toBeInTheDocument();
  });

  it('says that the workflow run of a workflow job started, not that it succeeded', () => {
    renderSheet({
      kind: 'workflow',
      last_run: { status: 'success', started_at: '2030-01-01 08:00:00', finished_at: '2030-01-01T08:00:01+00:00' },
      stats_7d: { runs: 6, success: 5, error: 1, missed: 0 },
    });
    const last = within(fact('Last run'));
    expect(last.queryByRole('img')).toBeNull();
    expect(screen.getByTestId('cron-last-run').textContent).toMatch(/^Started \S/);
    expect(last.queryByText(/^Took/)).toBeNull();
    expect(screen.getByTestId('cron-success').textContent).toBe('Started 5 times');
    expect(within(fact('Last 7 days')).getByText('1 failed')).toBeInTheDocument();
  });

  it.each([
    [1, 0, 'Started once'],
    [0, 2, 'No starts'],
  ])('counts %i started and %i failed workflow runs', (success, error, text) => {
    renderSheet({ kind: 'workflow', stats_7d: { runs: success + error, success, error, missed: 0 } });
    expect(screen.getByTestId('cron-success').textContent).toBe(text);
  });

  it('shows a workflow run that failed to start as failed', () => {
    renderSheet({
      kind: 'workflow',
      last_run: { status: 'error', started_at: '2030-01-01 08:00:00', finished_at: '2030-01-01T08:00:01+00:00' },
    });
    expect(within(fact('Last run')).getByRole('img', { name: 'Failed' })).toBeInTheDocument();
    expect(screen.getByTestId('cron-last-run').textContent).not.toMatch(/Started/);
  });

  it.each(['agent', 'source', 'other'])('shows a run of a job of kind %s that succeeded with its result', (kind) => {
    renderSheet({
      kind: kind as CronJob['kind'],
      last_run: { status: 'success', started_at: '2030-01-01 08:00:00', finished_at: '2030-01-01T08:00:12+00:00' },
      stats_7d: { runs: 2, success: 2, error: 0, missed: 0 },
    });
    const last = within(fact('Last run'));
    expect(last.getByRole('img', { name: 'Succeeded' })).toBeInTheDocument();
    expect(last.getByText('Took 12.0s')).toBeInTheDocument();
    expect(screen.getByTestId('cron-success').textContent).toBe('2 of 2 succeeded');
  });

  it('shows a forecast skip with its reason, and a run', () => {
    renderSheet({ forecast: 'skip', forecast_reason: SKIP_REASON });
    expect(screen.getByTestId('cron-forecast').textContent).toBe(`Will skip: ${SKIP_REASON}`);
  });

  it('shows a forecast run of a job with gates, and none for a job without', () => {
    renderSheet({ forecast: 'run', forecast_reason: null, gates: ['pending tasks'] });
    expect(screen.getByTestId('cron-forecast').textContent).toBe('Will run');
    renderSheet({ id: 'digest', forecast: 'run', forecast_reason: null });
    expect(screen.getAllByTestId('cron-forecast')).toHaveLength(1);
  });

  it('shows no forecast when none was loaded, and none for a paused job', () => {
    renderSheet();
    expect(screen.queryByTestId('cron-forecast')).toBeNull();
    expect(within(fact('Next run')).getByText(/^in /)).toBeInTheDocument();
  });

  it('says a disabled job is paused', () => {
    renderSheet({ enabled: false, next_run: null, forecast: 'run' });
    expect(within(fact('Next run')).getByText('Paused')).toBeInTheDocument();
    expect(screen.queryByTestId('cron-forecast')).toBeNull();
    expect(screen.queryByRole('button', { name: /Run now/ })).toBeNull();
  });
});

describe('the Active switch', () => {
  beforeEach(() => {
    setCronJobEnabled.mockReset();
    listCronJobs.mockReset();
    getCronSchedule.mockReset();
    getCronSchedule.mockResolvedValue({ next: null, jobs: [] });
    useCronStore.setState({ jobs: [job()], toggling: null, toggleError: null });
  });

  it('switches the job off and reloads the jobs', async () => {
    setCronJobEnabled.mockResolvedValue({ job_id: 'planner', enabled: false });
    listCronJobs.mockResolvedValue({ jobs: [job({ enabled: false })] });
    renderSheet();

    const toggle = screen.getByRole('switch', { name: 'Active' });
    expect(toggle).toHaveAttribute('aria-checked', 'true');
    await userEvent.click(toggle);

    expect(setCronJobEnabled).toHaveBeenCalledWith('planner', false);
    await vi.waitFor(() => expect(useCronStore.getState().jobs[0].enabled).toBe(false));
  });

  it('shows why a switch failed', async () => {
    setCronJobEnabled.mockRejectedValue(new Error('409: {"detail": "This instance is in lockdown."}'));
    renderSheet();

    await userEvent.click(screen.getByRole('switch', { name: 'Active' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('This instance is in lockdown.');
    expect(listCronJobs).not.toHaveBeenCalled();
  });

  it('is not there for a source', () => {
    renderSheet({ id: 'source:mail', type: 'source', kind: 'source' });
    expect(screen.queryByRole('switch')).toBeNull();
  });
});

describe('opening the page', () => {
  beforeEach(() => {
    listCronJobs.mockReset();
    getCronLogs.mockReset();
    getCronSchedule.mockResolvedValue({ next: null, jobs: [] });
    getCronLogs.mockResolvedValue({ logs: [], total: 0, empty: 0, limit: 50, offset: 0 });
    useCronStore.setState({ jobs: [], selectedJobId: null, showEmpty: false });
  });

  it("opens on the first job's sheet, with the empty runs hidden", async () => {
    listCronJobs.mockResolvedValue({ jobs: [job({ id: 'digest' }), job({ id: 'triage' })] });

    await useCronStore.getState().open();

    expect(useCronStore.getState().selectedJobId).toBe('digest');
    await vi.waitFor(() => expect(getCronLogs).toHaveBeenCalledWith('digest', 50, 0, true));
  });

  it('selects the first job when the selected one is gone, on open and on refresh', async () => {
    listCronJobs.mockResolvedValue({ jobs: [job({ id: 'digest' }), job({ id: 'triage' })] });
    useCronStore.setState({ selectedJobId: 'removed' });

    await useCronStore.getState().open();

    expect(useCronStore.getState().selectedJobId).toBe('digest');
    expect(getCronLogs.mock.calls).toEqual([['digest', 50, 0, true]]);

    useCronStore.setState({ selectedJobId: 'triage' });
    listCronJobs.mockResolvedValue({ jobs: [job({ id: 'digest' })] });
    await useCronStore.getState().refresh();

    expect(useCronStore.getState().selectedJobId).toBe('digest');
    expect(getCronLogs).toHaveBeenLastCalledWith('digest', 50, 0, true);
  });

  it('keeps the list of all jobs on a refresh', async () => {
    listCronJobs.mockResolvedValue({ jobs: [job({ id: 'digest' })] });

    await useCronStore.getState().refresh();

    expect(useCronStore.getState().selectedJobId).toBeNull();
    expect(getCronLogs).toHaveBeenLastCalledWith(undefined, 50, 0, true);
  });

  it('keeps a job that was selected before', async () => {
    useCronStore.setState({ selectedJobId: 'triage' });
    listCronJobs.mockResolvedValue({ jobs: [job({ id: 'digest' }), job({ id: 'triage' })] });

    await useCronStore.getState().open();

    expect(useCronStore.getState().selectedJobId).toBe('triage');
    expect(getCronLogs).toHaveBeenCalledWith('triage', 50, 0, true);
  });
});

describe('loading the jobs', () => {
  beforeEach(() => {
    listCronJobs.mockReset();
    getCronSchedule.mockReset();
    useCronStore.setState({ jobs: [] });
  });

  it('puts each job next to its forecast', async () => {
    listCronJobs.mockResolvedValue({
      jobs: [job(), job({ id: 'digest' }), job({ id: 'source:mail', type: 'source' })],
    });
    getCronSchedule.mockResolvedValue({
      next: { at: '2030-01-01T10:00:00+00:00', name: 'digest', kind: 'cron' },
      jobs: [
        { id: 'planner', next_run: '2030-01-01T09:00:00+00:00', forecast: 'skip', reason: SKIP_REASON },
        { id: 'digest', next_run: '2030-01-01T10:00:00+00:00', forecast: 'run', reason: null },
      ],
    });

    await useCronStore.getState().loadJobs();

    await vi.waitFor(() =>
      expect(useCronStore.getState().jobs.map(j => [j.id, j.forecast, j.forecast_reason])).toEqual([
        ['planner', 'skip', SKIP_REASON],
        ['digest', 'run', null],
        ['source:mail', null, null],
      ]),
    );
  });

  it('still lists the jobs when the schedule cannot be read', async () => {
    const quiet = vi.spyOn(console, 'error').mockImplementation(() => {});
    listCronJobs.mockResolvedValue({ jobs: [job()] });
    getCronSchedule.mockRejectedValue(new Error('unreachable'));

    await useCronStore.getState().loadJobs();

    await vi.waitFor(() => expect(quiet).toHaveBeenCalled());
    expect(forecasts()).toEqual([['planner', null]]);
    quiet.mockRestore();
  });

  it('shows the jobs before their forecasts arrive', async () => {
    const pending = deferred<CronSchedule>();
    listCronJobs.mockResolvedValue({ jobs: [job()] });
    getCronSchedule.mockReturnValue(pending.promise);

    await useCronStore.getState().loadJobs();
    expect(forecasts()).toEqual([['planner', null]]);

    pending.resolve(scheduleSaying('skip'));
    await vi.waitFor(() => expect(forecasts()).toEqual([['planner', 'skip']]));
  });

  it('goes on without forecasts that take too long, keeping the last ones meanwhile', async () => {
    vi.useFakeTimers();
    const quiet = vi.spyOn(console, 'error').mockImplementation(() => {});
    try {
      useCronStore.setState({ jobs: [job({ forecast: 'run' })] });
      listCronJobs.mockResolvedValue({ jobs: [job()] });
      let signal: AbortSignal | undefined;
      getCronSchedule.mockImplementation((given) => {
        signal = given;
        return new Promise<CronSchedule>(() => {});
      });

      await useCronStore.getState().loadJobs();
      expect(forecasts()).toEqual([['planner', 'run']]);

      await vi.advanceTimersByTimeAsync(FORECAST_TIMEOUT_MS);
      await vi.waitFor(() => expect(forecasts()).toEqual([['planner', null]]));
      expect(signal?.aborted).toBe(true);
    } finally {
      quiet.mockRestore();
      vi.useRealTimers();
    }
  });

  it("never lets an older load's forecasts replace a newer one's", async () => {
    const older = deferred<CronSchedule>();
    listCronJobs.mockResolvedValue({ jobs: [job()] });
    getCronSchedule.mockReturnValueOnce(older.promise).mockResolvedValueOnce(scheduleSaying('run'));

    await useCronStore.getState().loadJobs();
    await useCronStore.getState().loadJobs();
    await vi.waitFor(() => expect(forecasts()).toEqual([['planner', 'run']]));

    older.resolve(scheduleSaying('skip'));
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(forecasts()).toEqual([['planner', 'run']]);
  });
});

function forecasts() {
  return useCronStore.getState().jobs.map(j => [j.id, j.forecast]);
}

function scheduleSaying(forecast: 'run' | 'skip'): CronSchedule {
  return {
    next: null,
    jobs: [{
      id: 'planner',
      next_run: '2030-01-01T09:00:00+00:00',
      forecast,
      reason: forecast === 'skip' ? SKIP_REASON : null,
    }],
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(settle => { resolve = settle; });
  return { promise, resolve };
}
