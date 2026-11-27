import { act, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { PreviewBundle, PreviewBundleKind } from '../../types/preview';
import type { ContentMessage } from './messages';

/**
 * The `download` message of a table or Markdown page: the Preview tab
 * downloads the source file of the bundle with the fixed name of the kind,
 * and nothing for any other kind or for a page that said no `ready`.
 */

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
const hosts: Array<{ onContent: (m: ContentMessage) => void; onHandOver?: () => void }> = [];
const shows: string[] = [];
vi.mock('./host', () => ({
  PreviewHost: class {
    constructor(options: { onContent: (m: ContentMessage) => void; onHandOver?: () => void }) { hosts.push(options); }
    start() {}
    show(html: string) { shows.push(html); }
    send() {}
    focus() {}
    dispose() {}
  },
}));
const downloads: Array<{ name: string; type: string }> = [];
vi.mock('../Chat/rich/export', () => ({
  downloadBlob: (blob: Blob, name: string) => downloads.push({ name, type: blob.type }),
}));

const { api } = await import('../../api/client');
const { useChatStore } = await import('../../stores/chatStore');
const { usePreviewStore, resetPreviewConfig } = await import('../../stores/previewStore');
const { SidePanel } = await import('../Chat/SidePanel');
const { openPreview } = await import('./open');

const encoder = new TextEncoder();
const PAGE = '<!doctype html><html><body><main id="nerve-kit-page"></main></body></html>';

function description(path: string, kind: PreviewBundleKind, extra: string[]): PreviewBundle {
  return {
    path, root: 'reports', root_path: '/ws/reports', kind, entry: 'index.html',
    files: [
      { path: 'index.html', size: PAGE.length, type: 'text/html' },
      ...extra.map(p => ({ path: p, size: 4, type: p.endsWith('.csv') ? 'text/csv' : 'text/markdown' })),
    ],
    size: 0, fingerprint: 'f1', modified: Date.now(),
  };
}

const getBundle = () => vi.mocked(api.getPreviewBundle);
const getFile = () => vi.mocked(api.getPreviewFile);

async function settle() {
  await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); });
}

async function open(bundle: PreviewBundle) {
  getBundle().mockImplementation(async () => bundle);
  useChatStore.setState({ activeSession: 'session-1', panels: [], activePanelId: null, panelVisible: false });
  act(() => openPreview(bundle.path));
  render(<SidePanel />);
  await settle();
  return (message: ContentMessage) => act(() => hosts.at(-1)!.onContent(message));
}

beforeEach(() => {
  hosts.length = 0;
  shows.length = 0;
  downloads.length = 0;
  getFile().mockImplementation(async (_path: string, file: string) => encoder.encode(file === 'index.html' ? PAGE : 'a,b\n').slice().buffer);
  usePreviewStore.setState({ enabled: true, maxBytes: 10 * 1024 * 1024 });
});

afterEach(() => {
  getBundle().mockReset();
  getFile().mockReset();
  resetPreviewConfig();
  document.body.innerHTML = '';
});

const sourceReads = () => getFile().mock.calls.filter(([, file]) => file !== 'index.html');

describe('the download of a kit page', () => {
  it.each([
    ['table', 'reports/cost.csv', 'csv', 'data.csv', 'cost.csv', 'text/csv'],
    ['markdown', 'reports/notes.md', 'markdown', 'source.md', 'notes.md', 'text/markdown'],
  ] as const)('a %s page gets the fixed file of its bundle', async (kind, path, bundleKind, file, name, type) => {
    const say = await open(description(path, bundleKind, [file]));
    say({ type: 'ready', kind, canPrint: kind === 'markdown' });
    say({ type: 'download' });
    await settle();
    expect(sourceReads()).toEqual([[path, file, 'f1']]);
    expect(downloads).toEqual([{ name, type }]);
  });

  it('keeps the page when the download fails, and says so in one line', async () => {
    const { ApiError } = await import('../../api/hosted');
    const say = await open(description('reports/cost.csv', 'csv', ['data.csv']));
    const shown = shows.length;
    getFile().mockImplementation(async (_path: string, file: string) => {
      if (file === 'data.csv') {
        throw new ApiError(409, JSON.stringify({ detail: { code: 'stale_description', message: 'The files changed.' } }));
      }
      return encoder.encode(PAGE).slice().buffer;
    });
    say({ type: 'ready', kind: 'table', canPrint: false });
    say({ type: 'download' });
    await settle();
    expect(downloads).toEqual([]);
    expect(screen.getByRole('status')).toHaveTextContent('The download failed: The files changed.');
    // The page stays: no error in its place, and no new document.
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Show the source' })).toBeEnabled();
    expect(shows.length).toBe(shown);
    // The frame gets the next document: the line goes.
    act(() => hosts.at(-1)!.onHandOver?.());
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
  });

  it('shows no line for a download that fails after the next document or after a newer download', async () => {
    const { ApiError } = await import('../../api/hosted');
    const say = await open(description('reports/cost.csv', 'csv', ['data.csv']));
    const refusals: Array<(e: unknown) => void> = [];
    getFile().mockImplementation((_path: string, file: string) => (file === 'data.csv'
      ? new Promise<ArrayBuffer>((_resolve, reject) => { refusals.push(reject); })
      : Promise.resolve(encoder.encode(PAGE).slice().buffer)));
    const stale = () => new ApiError(409, JSON.stringify({ detail: { code: 'stale_description', message: 'The files changed.' } }));
    say({ type: 'ready', kind: 'table', canPrint: false });

    say({ type: 'download' });
    act(() => hosts.at(-1)!.onHandOver?.());
    refusals[0](stale());
    await settle();
    expect(screen.queryByRole('status')).not.toBeInTheDocument();

    say({ type: 'ready', kind: 'table', canPrint: false });
    say({ type: 'download' });
    say({ type: 'download' });
    refusals[1](stale());
    await settle();
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
    refusals[2](stale());
    await settle();
    expect(screen.getByRole('status')).toHaveTextContent('The download failed');
  });

  it('gets nothing for a deck', async () => {
    const say = await open(description('reports/index.html', 'folder', ['data.csv', 'source.md']));
    say({ type: 'ready', kind: 'deck', canPrint: true, slides: 2, slide: 1 });
    say({ type: 'download' });
    await settle();
    expect(sourceReads()).toEqual([]);
    expect(downloads).toEqual([]);
  });

  it.each([
    ['table', 'data.csv'],
    ['markdown', 'source.md'],
  ] as const)('gets nothing for an HTML folder whose page says it is a %s page', async (kind, file) => {
    // The folder holds a file of the fixed name, and its page claims the kind.
    const say = await open(description('reports/index.html', 'folder', [file]));
    say({ type: 'ready', kind, canPrint: false });
    say({ type: 'download' });
    say({ type: 'download' });
    await settle();
    expect(sourceReads()).toEqual([]);
    expect(downloads).toEqual([]);
  });

  it('gets nothing for the old document while the next description is built', async () => {
    // A folder (a page that claims a table) becomes a CSV bundle at the same path.
    const say = await open(description('reports/cost.csv', 'folder', ['data.csv']));
    say({ type: 'ready', kind: 'table', canPrint: false });
    let finish: (value: ArrayBuffer) => void = () => {};
    getFile().mockImplementation((_p: string, file: string) => (file === 'index.html'
      ? new Promise<ArrayBuffer>(resolve => { finish = resolve; })
      : Promise.resolve(encoder.encode('a,b\n').slice().buffer)));
    getBundle().mockImplementation(async () => ({ ...description('reports/cost.csv', 'csv', ['data.csv']), fingerprint: 'f2' }));
    act(() => usePreviewStore.getState().noteFileChange('/ws/reports/cost.csv'));
    await settle();
    // The new description is there, its document is not built yet.
    say({ type: 'ready', kind: 'table', canPrint: false });
    say({ type: 'download' });
    await settle();
    expect(sourceReads()).toEqual([]);
    expect(downloads).toEqual([]);

    // The new document is shown and says ready: now the download is its own.
    finish(encoder.encode(PAGE).slice().buffer);
    await settle();
    say({ type: 'download' });
    await settle();
    expect(sourceReads()).toEqual([]);
    say({ type: 'ready', kind: 'table', canPrint: false });
    say({ type: 'download' });
    await settle();
    expect(sourceReads()).toEqual([['reports/cost.csv', 'data.csv', 'f2']]);
    expect(downloads).toEqual([{ name: 'cost.csv', type: 'text/csv' }]);
  });

  it('hands a rebuild with the same text over again, and the page is ready again', async () => {
    const say = await open(description('reports/cost.csv', 'csv', ['data.csv']));
    expect(shows).toHaveLength(1);
    say({ type: 'ready', kind: 'table', canPrint: false });
    // A save without a change of bytes: a new fingerprint, the same document text.
    getBundle().mockImplementation(async () => ({ ...description('reports/cost.csv', 'csv', ['data.csv']), fingerprint: 'f2' }));
    act(() => usePreviewStore.getState().noteFileChange('/ws/reports/cost.csv'));
    await settle();
    expect(shows).toHaveLength(2);
    expect(shows[1]).toBe(shows[0]);
    // The ready of before does not count for the new document.
    say({ type: 'download' });
    await settle();
    expect(sourceReads()).toEqual([]);
    say({ type: 'ready', kind: 'table', canPrint: false });
    say({ type: 'download' });
    await settle();
    expect(sourceReads()).toEqual([['reports/cost.csv', 'data.csv', 'f2']]);
  });

  it('does not hand the same description over twice', async () => {
    await open(description('reports/cost.csv', 'csv', ['data.csv']));
    act(() => usePreviewStore.getState().noteFileChange('/ws/reports/cost.csv'));
    await settle();
    expect(getBundle()).toHaveBeenCalledTimes(2);
    expect(shows).toHaveLength(1);
  });

  it('shows the deck controls again after a rebuild with the same text', async () => {
    const say = await open(description('reports/index.html', 'folder', []));
    say({ type: 'ready', kind: 'deck', canPrint: true, slides: 3, slide: 1 });
    expect(screen.getByRole('group', { name: 'Slides' })).toBeInTheDocument();
    getBundle().mockImplementation(async () => ({ ...description('reports/index.html', 'folder', []), fingerprint: 'f2' }));
    act(() => usePreviewStore.getState().noteFileChange('/ws/reports/index.html'));
    await settle();
    expect(shows).toHaveLength(2);
    expect(screen.queryByRole('group', { name: 'Slides' })).toBeNull();
    say({ type: 'ready', kind: 'deck', canPrint: true, slides: 3, slide: 1 });
    expect(screen.getByRole('group', { name: 'Slides' })).toBeInTheDocument();
  });

  it('shows no deck controls and gets no download after a build that failed', async () => {
    const say = await open(description('reports/index.html', 'folder', ['data.csv']));
    say({ type: 'ready', kind: 'deck', canPrint: true, slides: 3, slide: 1 });
    expect(screen.getByRole('group', { name: 'Slides' })).toBeInTheDocument();
    const { ApiError } = await import('../../api/hosted');
    getBundle().mockRejectedValue(new ApiError(422, JSON.stringify({ detail: { code: 'no_entry', message: 'The folder has no index.html.' } })));
    act(() => usePreviewStore.getState().noteFileChange('/ws/reports/index.html'));
    await settle();
    expect(screen.queryByRole('group', { name: 'Slides' })).toBeNull();
    say({ type: 'download' });
    await settle();
    expect(sourceReads()).toEqual([]);
  });

  it('gets nothing for a page that said no ready', async () => {
    const say = await open(description('reports/cost.csv', 'csv', ['data.csv']));
    say({ type: 'download' });
    await settle();
    expect(sourceReads()).toEqual([]);
    expect(downloads).toEqual([]);
  });

  it('gets nothing when the bundle has no such file', async () => {
    const say = await open(description('reports/page.html', 'page', []));
    say({ type: 'ready', kind: 'table', canPrint: false });
    say({ type: 'download' });
    await settle();
    expect(sourceReads()).toEqual([]);
    expect(downloads).toEqual([]);
  });
});
