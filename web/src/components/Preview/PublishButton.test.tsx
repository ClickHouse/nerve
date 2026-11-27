import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const publishArtifact = vi.fn();
vi.mock('../../api/client', async (orig) => ({
  ...(await orig<typeof import('../../api/client')>()),
  api: { publishArtifact: (...args: unknown[]) => publishArtifact(...args) },
}));

const { useHostedStore } = await import('../../api/hosted');
const { useArtifactStore, resetArtifactConfig, applyViewerArtifacts } = await import('../../stores/artifactStore');
const { PublishButton } = await import('./PublishButton');
const { defaultTitle } = await import('./title');
const { ApiError } = await import('../../api/hosted');

const VIEWER = 'https://artifacts.example.test';
const PUBLISHED = {
  artifact_id: '7d1c2b3a-4e5f-4a6b-8c7d-9e0f1a2b3c4d', version: 1,
  viewer_url: `${VIEWER}/t/0b9d6c1e-5c3f-4e43-9f0a-1d2c3b4a5f60/a/7d1c2b3a-4e5f-4a6b-8c7d-9e0f1a2b3c4d`,
  expires_at: '2026-12-25T10:00:00Z', title: 'q3',
};

beforeEach(() => {
  publishArtifact.mockReset();
});
afterEach(() => {
  resetArtifactConfig();
  useHostedStore.setState({ mode: 'local' });
});

function on() {
  useHostedStore.setState({ mode: 'external' });
  useArtifactStore.setState({ publish: true, viewerOrigin: VIEWER });
}

describe('the Publish button of the Preview tab', () => {
  it('is absent in local mode and with the switch off', () => {
    useArtifactStore.setState({ publish: true });
    const { rerender } = render(<PublishButton path="decks/q3" disabled={false} />);
    expect(screen.queryByRole('button', { name: 'Publish' })).toBeNull();
    useArtifactStore.setState({ publish: false });
    useHostedStore.setState({ mode: 'external' });
    rerender(<PublishButton path="decks/q3" disabled={false} />);
    expect(screen.queryByRole('button', { name: 'Publish' })).toBeNull();
  });

  it('publishes the path with the title, then the next publish is a new version', async () => {
    on();
    publishArtifact.mockResolvedValueOnce(PUBLISHED).mockResolvedValueOnce({ ...PUBLISHED, version: 2, changed_after: true });
    render(<PublishButton path="decks/q3/index.html" disabled={false} />);
    await userEvent.click(screen.getByRole('button', { name: 'Publish' }));
    const dialog = screen.getByRole('dialog');
    const field = screen.getByLabelText('Title');
    expect(field).toHaveValue('q3');
    await userEvent.clear(field);
    await userEvent.type(field, 'Q3 review');
    await userEvent.click(screen.getAllByRole('button', { name: 'Publish' }).at(-1)!);
    expect(publishArtifact).toHaveBeenLastCalledWith('decks/q3/index.html', 'Q3 review', undefined);
    expect(dialog).toHaveTextContent('as version 1');
    expect(dialog).not.toHaveTextContent('earlier state');
    expect(screen.getByRole('link', { name: /Open/ })).toHaveAttribute('href', PUBLISHED.viewer_url);
    await userEvent.click(screen.getByRole('button', { name: 'Close' }));

    await userEvent.click(screen.getByRole('button', { name: 'Publish' }));
    expect(screen.getByRole('dialog')).toHaveTextContent('a new version of q3');
    await userEvent.click(screen.getAllByRole('button', { name: 'Publish' }).at(-1)!);
    expect(publishArtifact).toHaveBeenLastCalledWith('decks/q3/index.html', 'Q3 review', PUBLISHED.artifact_id);
    expect(await screen.findByText(/This version has the earlier state/)).toBeInTheDocument();
  });

  it('says the sentence of a refusal', async () => {
    on();
    const error = new ApiError(422, JSON.stringify({
      detail: { code: 'restricted_egress', message: 'Agents with restricted internet access cannot publish artifacts.' },
    }));
    publishArtifact.mockRejectedValueOnce(error);
    render(<PublishButton path="decks/q3" disabled={false} />);
    await userEvent.click(screen.getByRole('button', { name: 'Publish' }));
    await userEvent.click(screen.getAllByRole('button', { name: 'Publish' }).at(-1)!);
    expect(await screen.findByRole('alert')).toHaveTextContent(
      'Agents with restricted internet access cannot publish artifacts.',
    );
  });

  it('is disabled while the preview has no bundle', () => {
    on();
    render(<PublishButton path="decks/q3" disabled />);
    expect(screen.getByRole('button', { name: 'Publish' })).toBeDisabled();
  });

  it('starts from the name of the folder or the file', () => {
    expect(defaultTitle('decks/q3-infra/index.html')).toBe('q3-infra');
    expect(defaultTitle('reports/cost.csv')).toBe('cost');
    expect(defaultTitle('decks/q3/')).toBe('q3');
  });
});

describe('the settings of the web UI', () => {
  it('take only an origin', () => {
    applyViewerArtifacts({ publish: true, viewer_origin: VIEWER });
    expect(useArtifactStore.getState()).toMatchObject({ publish: true, viewerOrigin: VIEWER });
    for (const value of [`${VIEWER}/t/x`, `${VIEWER}?a=1`, 'javascript:alert(1)', '', null]) {
      applyViewerArtifacts({ publish: true, viewer_origin: value });
      expect(useArtifactStore.getState().viewerOrigin).toBeNull();
    }
    applyViewerArtifacts(undefined);
    expect(useArtifactStore.getState()).toMatchObject({ publish: false, viewerOrigin: null });
  });
});
