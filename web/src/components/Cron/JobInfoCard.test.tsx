import { render, screen } from '@testing-library/react';
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
  },
}));

const { api } = await import('../../api/client');
const { FORECAST_TIMEOUT_MS, useCronStore } = await import('../../stores/cronStore');
const { JobInfoCard } = await import('./JobInfoCard');

const listCronJobs = vi.mocked(api.listCronJobs);
const getCronSchedule = vi.mocked(api.getCronSchedule);

const SKIP_REASON = "gate 'tasks' not satisfied (pending tasks exist)";

function job(overrides: Partial<CronJob> = {}): CronJob {
  return {
    id: 'planner',
    type: 'cron',
    schedule: '0 * * * *',
    description: '',
    enabled: true,
    next_run: '2030-01-01T09:00:00+00:00',
    ...overrides,
  };
}

function renderCard(overrides: Partial<CronJob>) {
  render(
    <MemoryRouter>
      <JobInfoCard job={job(overrides)} />
    </MemoryRouter>,
  );
  return screen.getByTestId('cron-forecast').textContent;
}

describe('the Forecast column', () => {
  it('shows a skip with its reason', () => {
    expect(renderCard({ forecast: 'skip', forecast_reason: SKIP_REASON }))
      .toBe(`skip — ${SKIP_REASON}`);
    expect(screen.getByText('Forecast')).toBeInTheDocument();
  });

  it('shows a run', () => {
    expect(renderCard({ forecast: 'run', forecast_reason: null })).toBe('run');
  });

  it('shows a dash when the job has no next run to forecast', () => {
    expect(renderCard({ forecast: null, next_run: null, enabled: false })).toBe('—');
  });

  it('shows a dash when no forecast was loaded', () => {
    expect(renderCard({})).toBe('—');
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
