import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { MemoryRouter } from 'react-router-dom';
import { CodeReviewHistory } from './CodeReviewDetailPage';
import { codeReview } from '../api/codeReview';

vi.mock('../api/codeReview', () => ({ codeReview: { detail: vi.fn(), version: vi.fn() } }));
afterEach(cleanup);

it('labels an old report by SHA and keeps current unreviewed state visible', async () => {
  vi.mocked(codeReview.detail).mockResolvedValue({
    pr: { id: 'cr-1', task_id: 'cr-1', profile: 'example', repository: 'alice/example', number: 1,
      title: 'Example PR', url: 'https://example.com', state: 'open', draft: 0, head: 'new-sha', base: 'base',
      current_version: 'new-version', reviewed_version: 'old-version', review_state: 'queued', severity: 'none',
      ci: '{}', content_updated_at: '', last_polled_at: '', error: '' },
    versions: [{ id: 'old-version', head: 'old-sha', base: 'base', status: 'complete', created_at: '', updated_at: '', error: '', spent_usd: 1 }], total: 1,
  });
  vi.mocked(codeReview.version).mockResolvedValue({
    version: { id: 'old-version', head: 'old-sha', base: 'base', status: 'complete', created_at: '', updated_at: '', error: '', spent_usd: 1, snapshot: {} }, attempts: [],
    reports: [{ version: 'old-version', run: 'run-1', admission: 'complete', error: '', spent_usd: 1,
      output: { 'description.md': 'Old description', 'diagrams.md': 'Diagram explanation', 'review.md': 'Checked scope' } }],
  });
  render(<MemoryRouter><CodeReviewHistory prId="cr-1" /></MemoryRouter>);
  expect(await screen.findByRole('status')).toHaveTextContent('Current input version has no completed review');
  fireEvent.change(await screen.findByRole('combobox'), { target: { value: 'old-version' } });
  expect(await screen.findByText('Old description')).toBeInTheDocument();
  expect(screen.getByText('old-sha', { selector: 'code' })).toBeInTheDocument();
});
