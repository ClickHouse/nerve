import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { CronLog } from '../../stores/cronStore';

vi.mock('../../api/client', () => ({
  api: {
    getCronLogs: vi.fn(),
  },
}));

const { api } = await import('../../api/client');
const { useCronStore } = await import('../../stores/cronStore');
const { RunHistory } = await import('./RunHistory');

const getCronLogs = vi.mocked(api.getCronLogs);

function log(overrides: Partial<CronLog> = {}): CronLog {
  return {
    id: 1,
    job_id: 'source:mail',
    started_at: '2030-01-01 08:00:00',
    finished_at: '2030-01-01T08:00:02+00:00',
    status: 'success',
    output: '2 ingested',
    error: null,
    session_id: null,
    records: 2,
    ...overrides,
  };
}

function renderHistory(showJob = false) {
  return render(
    <MemoryRouter>
      <RunHistory showJob={showJob} />
    </MemoryRouter>,
  );
}

describe('the run history', () => {
  beforeEach(() => {
    getCronLogs.mockReset();
    useCronStore.setState({
      selectedJobId: 'source:mail', logs: [], logsTotal: 0, logsEmpty: 0, logsOffset: 0,
      showEmpty: false, loading: false,
    });
  });

  it('lists each run with its result, duration and session', () => {
    useCronStore.setState({
      logs: [
        log({ id: 3, status: null, finished_at: null, output: null }),
        log({ id: 2, status: 'error', error: 'timeout\nmore', output: null, session_id: 'cron:mail:1' }),
        log({ id: 1 }),
        log({ id: 0, status: 'missed', output: null, error: 'run dropped' }),
      ],
      logsTotal: 4,
    });
    renderHistory();

    const rows = screen.getAllByRole('listitem');
    expect(within(rows[0]).getByRole('img', { name: 'Running' })).toBeInTheDocument();
    expect(within(rows[1]).getByRole('img', { name: 'Failed' })).toBeInTheDocument();
    expect(within(rows[1]).getByText('timeout')).toBeInTheDocument();
    expect(within(rows[1]).getByRole('link', { name: 'Open chat' }))
      .toHaveAttribute('href', '/chat/cron%3Amail%3A1');
    expect(within(rows[2]).getByRole('img', { name: 'Succeeded' })).toBeInTheDocument();
    expect(within(rows[2]).getByText('2.0s')).toBeInTheDocument();
    expect(within(rows[3]).getByRole('img', { name: 'Missed' })).toBeInTheDocument();
  });

  it('opens a run to show all of its error', async () => {
    useCronStore.setState({ logs: [log({ status: 'error', error: 'timeout\nat line 2', output: null })] });
    renderHistory();

    await userEvent.click(screen.getByRole('button', { expanded: false }));

    expect(screen.getByText(/at line 2/)).toBeInTheDocument();
  });

  it('has no filter when no run is empty', () => {
    useCronStore.setState({ logs: [log()], logsEmpty: 0 });
    renderHistory();
    expect(screen.queryByRole('checkbox')).toBeNull();
  });

  it('shows the runs with no new items on request', async () => {
    getCronLogs.mockResolvedValue({
      logs: [log({ id: 2, records: 0, output: '0 ingested' }), log()],
      total: 2, empty: 1, limit: 50, offset: 0,
    });
    useCronStore.setState({ logs: [log()], logsTotal: 1, logsEmpty: 1 });
    renderHistory();

    const filter = screen.getByRole('checkbox', { name: 'Show runs with no new items (1)' });
    expect(filter).not.toBeChecked();
    await userEvent.click(filter);

    expect(getCronLogs).toHaveBeenCalledWith('source:mail', 50, 0, false);
    await vi.waitFor(() => expect(screen.getAllByRole('listitem')).toHaveLength(2));
    expect(screen.getByRole('checkbox')).toBeChecked();
  });

  it('says when every run was empty', () => {
    useCronStore.setState({ logs: [], logsEmpty: 12 });
    renderHistory();
    expect(screen.getByText('No runs with new items.')).toBeInTheDocument();
  });

  it('names the job of each run in the list of all jobs only', () => {
    useCronStore.setState({ selectedJobId: null, logs: [log()] });
    const { unmount } = renderHistory(true);
    // Two copies: one for wide screens and one for phones, CSS shows one.
    expect(screen.getAllByText('source:mail')).toHaveLength(2);
    unmount();
    renderHistory(false);
    expect(screen.queryByText('source:mail')).toBeNull();
  });
});
