import { act, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi, type MockInstance } from 'vitest';
import type { PreviewBundle } from '../../types/preview';

vi.mock('../../api/client', async (orig) => ({
  ...(await orig<typeof import('../../api/client')>()),
  api: new Proxy({} as Record<string, unknown>, {
    get: (target, key: string) => (target[key] ??= vi.fn(async () => ({}))),
  }),
}));
vi.mock('../../api/websocket', () => ({
  ws: {
    sendMessage: vi.fn(() => 'sent'), switchSession: vi.fn(), send: vi.fn(), connect: vi.fn(),
    subscribeAgent: vi.fn(() => 'sent'), unsubscribeAgent: vi.fn(() => 'sent'), onOpen: vi.fn(() => () => {}),
    answerInteraction: vi.fn(),
  },
}));

const { api } = await import('../../api/client');
const { useChatStore } = await import('../../stores/chatStore');
const { usePreviewStore, resetPreviewConfig } = await import('../../stores/previewStore');
const { SidePanel } = await import('../Chat/SidePanel');
const { openPreview, previewTabId, canPreview } = await import('./open');
const { POLL_MS, agoText, fileEventNamesBundle } = await import('./live');
const { MAX_RESTARTS } = await import('./PreviewTab');
const { ApiError } = await import('../../api/hosted');
const { MOBILE_QUERY } = await import('../../hooks/useMediaQuery');

const PATH = 'decks/demo/index.html';
const encoder = new TextEncoder();

let entry = '<!doctype html><html><body><h1>Slide one</h1></body></html>';
let fingerprint = 'f1';

function description(): PreviewBundle {
  return {
    path: PATH, root: 'decks/demo', root_path: '/ws/decks/demo', kind: 'folder', entry: 'index.html',
    files: [{ path: 'index.html', size: encoder.encode(entry).length, type: 'text/html' }],
    size: 0, fingerprint, modified: Date.now(),
  };
}

const getBundle = () => vi.mocked(api.getPreviewBundle);
const getFile = () => vi.mocked(api.getPreviewFile);

async function settle() {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(0);
  });
}

async function openTab() {
  useChatStore.setState({ activeSession: 'session-1', panels: [], activePanelId: null, panelVisible: false });
  act(() => openPreview(PATH));
  const view = render(<SidePanel />);
  await settle();
  return view;
}

beforeEach(() => {
  vi.useFakeTimers({ shouldAdvanceTime: true });
  entry = '<!doctype html><html><body><h1>Slide one</h1></body></html>';
  fingerprint = 'f1';
  getBundle().mockImplementation(async () => description());
  getFile().mockImplementation(async () => encoder.encode(entry).slice().buffer);
  usePreviewStore.setState({ enabled: true, maxBytes: 10 * 1024 * 1024 });
});

afterEach(() => {
  vi.useRealTimers();
  getBundle().mockReset();
  getFile().mockReset();
  resetPreviewConfig();
  document.body.innerHTML = '';
});

describe('opening a preview', () => {
  it('opens one tab for a path and focuses it again', () => {
    useChatStore.setState({ panels: [], activePanelId: null, panelVisible: false });
    openPreview(PATH);
    openPreview(PATH);
    const { panels, activePanelId, panelVisible } = useChatStore.getState();
    expect(panels.filter(p => p.type === 'preview')).toHaveLength(1);
    expect(activePanelId).toBe(previewTabId(PATH));
    expect(panelVisible).toBe(true);
  });

  it.each([
    ['deck/index.html', true], ['page.HTML', true], ['logo.svg', true], ['photo.jpeg', true],
    ['notes.md', false], ['data.csv', false], ['app.js', false], ['', false],
  ])('offers a preview for %s: %s', (path, offered) => {
    expect(canPreview(path)).toBe(offered);
  });
});

describe('the Preview tab', () => {
  it('shows the path, the update time and the controls', async () => {
    await openTab();
    expect(screen.getByText(PATH)).toBeInTheDocument();
    expect(screen.getByText(/^Updated \d+ s ago$/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Reload the preview' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Show the source' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Present' })).toBeEnabled();
    expect(screen.getByTitle('Preview')).toBeInTheDocument();
  });

  it('shows the source of the entry as text', async () => {
    entry = '<!doctype html><html><body><img src=x onerror="window.bad=1"><h1>Slide one</h1></body></html>';
    await openTab();
    await userEvent.click(screen.getByRole('button', { name: 'Show the source' }));
    const source = screen.getByLabelText('Source');
    expect(source.textContent).toBe(entry);
    expect(source.querySelector('img')).toBeNull();
    expect(screen.getByRole('button', { name: 'Show the preview' })).toHaveAttribute('aria-pressed', 'true');
  });

  it('reads the description each poll and builds again only when the fingerprint changed', async () => {
    await openTab();
    expect(getBundle()).toHaveBeenCalledTimes(1);
    expect(getFile()).toHaveBeenCalledTimes(1);
    // The file is read for the description that was just read.
    expect(getFile()).toHaveBeenCalledWith(PATH, 'index.html', 'f1');

    // Nothing changed.
    await act(async () => { await vi.advanceTimersByTimeAsync(POLL_MS); });
    expect(getBundle()).toHaveBeenCalledTimes(2);
    expect(getFile()).toHaveBeenCalledTimes(1);

    // A shell command wrote the file: no event, the poll sees it.
    entry = '<!doctype html><html><body><h1>Slide two</h1></body></html>';
    fingerprint = 'f2';
    await act(async () => { await vi.advanceTimersByTimeAsync(POLL_MS); });
    expect(getFile()).toHaveBeenCalledTimes(2);
    await userEvent.click(screen.getByRole('button', { name: 'Show the source' }));
    expect(screen.getByLabelText('Source').textContent).toContain('Slide two');
  });

  it('reads the description when a file event names a file of the bundle', async () => {
    await openTab();
    expect(getBundle()).toHaveBeenCalledTimes(1);
    fingerprint = 'f2';
    act(() => usePreviewStore.getState().noteFileChange('/ws/decks/demo/index.html'));
    await settle();
    expect(getBundle()).toHaveBeenCalledTimes(2);
    expect(getFile()).toHaveBeenCalledTimes(2);

    act(() => usePreviewStore.getState().noteFileChange('/ws/other/file.html'));
    await settle();
    expect(getBundle()).toHaveBeenCalledTimes(2);
  });

  it('starts again from a new description when a file changed after the description', async () => {
    const stale = new ApiError(409, JSON.stringify({ detail: { code: 'stale_description', message: 'changed' } }));
    getFile().mockRejectedValueOnce(stale);
    await openTab();
    expect(getBundle()).toHaveBeenCalledTimes(2);
    expect(getFile()).toHaveBeenCalledTimes(2);
    expect(screen.queryByRole('alert')).toBeNull();
    expect(screen.getByTitle('Preview')).toBeInTheDocument();
  });

  it('stops after a few restarts for files that change all the time, and tries at the next poll', async () => {
    const stale = new ApiError(409, JSON.stringify({ detail: { code: 'stale_description', message: 'changed' } }));
    getFile().mockRejectedValue(stale);
    await openTab();
    expect(getBundle()).toHaveBeenCalledTimes(MAX_RESTARTS + 1);
    expect(screen.getByRole('alert')).toHaveTextContent('The files change while the preview is read.');
    getFile().mockImplementation(async () => encoder.encode(entry).slice().buffer);
    await act(async () => { await vi.advanceTimersByTimeAsync(POLL_MS); });
    expect(screen.queryByRole('alert')).toBeNull();
    expect(screen.getByTitle('Preview')).toBeInTheDocument();
  });

  it('does not poll while the panel is hidden', async () => {
    await openTab();
    act(() => useChatStore.setState({ panelVisible: false }));
    await act(async () => { await vi.advanceTimersByTimeAsync(POLL_MS * 3); });
    expect(getBundle()).toHaveBeenCalledTimes(1);
  });

  it('shows the reason of a refused path', async () => {
    const { ApiError } = await import('../../api/hosted');
    getBundle().mockRejectedValue(new ApiError(422, JSON.stringify({
      detail: { code: 'no_entry', message: 'The folder has no index.html.' },
    })));
    await openTab();
    expect(screen.getByRole('alert')).toHaveTextContent('The folder has no index.html.');
    expect(screen.getByRole('button', { name: 'Try again' })).toBeInTheDocument();
  });

  it('says so when the files are larger than the limit', async () => {
    usePreviewStore.setState({ maxBytes: 10 });
    await openTab();
    expect(screen.getByRole('alert')).toHaveTextContent(/This preview needs .+ of files or more\. The limit is .+ MiB\./);
    expect(getFile()).not.toHaveBeenCalled();
  });
});

describe('the Preview tab on a phone', () => {
  let matchMedia: MockInstance<typeof window.matchMedia>;
  beforeEach(() => {
    const stub = window.matchMedia;
    matchMedia = vi.spyOn(window, 'matchMedia').mockImplementation(
      (query: string) => ({ ...stub(query), matches: query === MOBILE_QUERY }),
    );
  });
  afterEach(() => matchMedia.mockRestore());

  it('covers the chat as a named region and closes with its own button', async () => {
    await openTab();
    const region = screen.getByRole('region', { name: 'Preview' });
    expect(region).toBeVisible();
    expect(screen.getByTitle('Preview')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Close the panel' }));
    expect(useChatStore.getState().panelVisible).toBe(false);
  });
});

describe('helpers', () => {
  it('matches a file event to the bundle', () => {
    const folder = description();
    expect(fileEventNamesBundle(folder, '/ws/decks/demo/index.html')).toBe(true);
    expect(fileEventNamesBundle(folder, '/ws/decks/demo/new.css')).toBe(true);
    expect(fileEventNamesBundle(folder, '/ws/decks/demo-2/index.html')).toBe(false);
    const page = { ...folder, kind: 'page' as const, root_path: '/ws', entry: 'page.html', files: [{ path: 'page.html', size: 1, type: 'text/html' }] };
    expect(fileEventNamesBundle(page, '/ws/page.html')).toBe(true);
    expect(fileEventNamesBundle(page, '/ws/other.html')).toBe(false);
  });

  it('says how long ago', () => {
    expect(agoText(4_000)).toBe('4 s ago');
    expect(agoText(-5)).toBe('0 s ago');
    expect(agoText(3 * 60_000)).toBe('3 min ago');
    expect(agoText(2 * 3_600_000)).toBe('2 h ago');
    expect(agoText(5 * 86_400_000)).toBe('5 d ago');
  });
});
