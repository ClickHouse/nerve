import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi, type MockInstance } from 'vitest';

vi.mock('../api/client', () => ({
  api: {
    listWorkspaceDir: vi.fn(),
    readWorkspaceFile: vi.fn(),
    writeMemoryFile: vi.fn(),
  },
  setToken: vi.fn(), clearToken: vi.fn(), getToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));

const { api } = await import('../api/client');
const { MOBILE_QUERY } = await import('../hooks/useMediaQuery');
const { useFilesStore } = await import('../stores/filesStore');
const { FilesPage } = await import('./FilesPage');

const listWorkspaceDir = vi.mocked(api.listWorkspaceDir);
const readWorkspaceFile = vi.mocked(api.readWorkspaceFile);
const writeMemoryFile = vi.mocked(api.writeMemoryFile);

const file = (path: string, extra: Record<string, unknown> = {}) => ({
  name: path.split('/').pop()!, path, type: 'file' as const, size: 10,
  read_only: false, read_only_reason: null, ...extra,
});
const dir = (path: string) => ({ name: path.split('/').pop()!, path, type: 'directory' as const });
const LISTINGS: Record<string, ReturnType<typeof file | typeof dir>[]> = {
  '': [dir('notes'), file('README.md')],
  notes: [file('notes/plan.md', { read_only: true, read_only_reason: 'not_memory' }), file('notes/run.log', { read_only: true, read_only_reason: 'not_memory' }),
    file('notes/shot.png', { read_only: true, read_only_reason: 'not_memory' }), file('notes/chart.svg', { read_only: true, read_only_reason: 'not_memory' })],
};

beforeEach(() => {
  vi.clearAllMocks();
  useFilesStore.setState({
    dirs: {}, dirErrors: {}, openFiles: [], activeFile: null, loading: false, saving: false,
  });
  listWorkspaceDir.mockImplementation(async (path: string) => {
    const entries = LISTINGS[path] ?? [];
    return { path, entries, total: entries.length, truncated: false };
  });
  readWorkspaceFile.mockImplementation(async (path: string) => {
    const entry = Object.values(LISTINGS).flat().find(e => e.path === path) as ReturnType<typeof file>;
    return {
      path, name: entry.name, size: 10, content: `# ${path}`, binary: false, too_large: false,
      read_only: entry.read_only, read_only_reason: entry.read_only_reason,
    };
  });
});

/** Render the page and wait for the tree to load. */
async function renderPage() {
  render(<FilesPage />);
  await screen.findByRole('button', { name: 'README.md' });
}

/** The drawer stays mounted while closed, parked and inert. */
const drawer = () => screen.getByRole('dialog', { name: 'Workspace files' });

describe('FilesPage on a phone', () => {
  let matchMedia: MockInstance<typeof window.matchMedia>;
  beforeEach(() => {
    const stub = window.matchMedia;
    matchMedia = vi.spyOn(window, 'matchMedia').mockImplementation(
      (query: string) => ({ ...stub(query), matches: query === MOBILE_QUERY }),
    );
  });
  afterEach(() => matchMedia.mockRestore());

  it('opens the file list when no file is open', async () => {
    await renderPage();
    expect(drawer()).not.toHaveAttribute('inert');
  });

  it('keeps the file list open when a directory is tapped', async () => {
    await renderPage();
    await userEvent.click(screen.getByRole('button', { name: 'notes' }));
    expect(await screen.findByRole('button', { name: 'plan.md' })).toBeInTheDocument();
    expect(drawer()).not.toHaveAttribute('inert');
  });

  it('closes the file list when a file is tapped, and opens the file', async () => {
    await renderPage();
    await userEvent.click(screen.getByRole('button', { name: 'README.md' }));
    expect(drawer()).toHaveAttribute('inert');
    expect(readWorkspaceFile).toHaveBeenCalledWith('README.md');
    expect(await screen.findByDisplayValue('# README.md')).toBeInTheDocument();
  });

  it('opens with the header toggle and closes with the close button', async () => {
    useFilesStore.setState({
      openFiles: [{
        path: 'README.md', name: 'README.md', content: '# README.md',
        originalContent: '# README.md', modified: false, readOnly: false,
      }],
      activeFile: 'README.md',
    });
    await renderPage();
    expect(drawer()).toHaveAttribute('inert');

    await userEvent.click(screen.getByRole('button', { name: 'Show file list' }));
    expect(drawer()).not.toHaveAttribute('inert');

    await userEvent.click(screen.getByRole('button', { name: 'Close file list' }));
    expect(drawer()).toHaveAttribute('inert');
  });
});

describe('FilesPage on a desktop', () => {
  it('shows the tree beside the editor, with no toggle', async () => {
    await renderPage();
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Show file list' })).not.toBeInTheDocument();

    await userEvent.click(screen.getByRole('button', { name: 'README.md' }));
    expect(await screen.findByDisplayValue('# README.md')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'README.md' })).toBeVisible();
  });
});

describe('FilesPage workspace tree', () => {
  it('loads a directory only when it is expanded, and once', async () => {
    await renderPage();
    expect(listWorkspaceDir).toHaveBeenCalledTimes(1);
    expect(listWorkspaceDir).toHaveBeenCalledWith('');
    const notes = screen.getByRole('button', { name: 'notes' });
    await userEvent.click(notes);
    expect(await screen.findByRole('button', { name: 'run.log' })).toBeInTheDocument();
    expect(listWorkspaceDir).toHaveBeenLastCalledWith('notes');
    await userEvent.click(notes);  // collapse
    await userEvent.click(notes);  // expand again: cached
    expect(listWorkspaceDir).toHaveBeenCalledTimes(2);
  });

  it('opens a non-memory file read-only, with no save and no preview toggle', async () => {
    await renderPage();
    await userEvent.click(screen.getByRole('button', { name: 'notes' }));
    await userEvent.click(await screen.findByRole('button', { name: 'run.log' }));
    const text = await screen.findByDisplayValue('# notes/run.log');
    expect(text).toHaveAttribute('readonly');
    expect(screen.getByText('Read-only')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Preview rendered' })).not.toBeInTheDocument();
    await userEvent.type(text, 'x');
    expect(screen.queryByRole('button', { name: /Save/ })).not.toBeInTheDocument();
    expect(writeMemoryFile).not.toHaveBeenCalled();
  });

  it('keeps the preview toggle for markdown', async () => {
    await renderPage();
    await userEvent.click(screen.getByRole('button', { name: 'README.md' }));
    await screen.findByDisplayValue('# README.md');
    expect(screen.getByRole('button', { name: 'Preview rendered' })).toBeInTheDocument();
  });

  it('shows a notice instead of a binary file', async () => {
    readWorkspaceFile.mockResolvedValueOnce({
      path: 'README.md', name: 'README.md', size: 3, content: '', binary: true, too_large: false,
      read_only: false, read_only_reason: null,
    });
    await renderPage();
    await userEvent.click(screen.getByRole('button', { name: 'README.md' }));
    expect(await screen.findByText('Binary file: not shown.')).toBeInTheDocument();
    expect(screen.queryByRole('textbox')).not.toBeInTheDocument();
  });

  it.each([
    ['binary', { binary: true, too_large: false, size: 3 }, 'Download (3 B)'],
    ['too large', { binary: false, too_large: true, size: 5 * 1024 * 1024 }, 'Download (5.0 MB)'],
  ])('offers a download for a %s file', async (_kind, flags, label) => {
    readWorkspaceFile.mockResolvedValueOnce({
      path: 'notes/run.log', name: 'run.log', content: '',
      read_only: true, read_only_reason: 'not_memory', ...flags,
    });
    await renderPage();
    await userEvent.click(screen.getByRole('button', { name: 'notes' }));
    await userEvent.click(await screen.findByRole('button', { name: 'run.log' }));
    const link = await screen.findByRole('link', { name: label });
    expect(link).toHaveAttribute('href', '/api/workspace/download?path=notes%2Frun.log');
    expect(link).toHaveAttribute('download', 'run.log');
  });

  it('previews a raster image instead of the binary notice', async () => {
    readWorkspaceFile.mockResolvedValueOnce({
      path: 'notes/shot.png', name: 'shot.png', size: 2048, content: '', binary: true, too_large: false,
      read_only: true, read_only_reason: 'not_memory',
    });
    await renderPage();
    await userEvent.click(screen.getByRole('button', { name: 'notes' }));
    await userEvent.click(await screen.findByRole('button', { name: 'shot.png' }));
    const img = await screen.findByRole('img', { name: 'shot.png' });
    expect(img).toHaveAttribute('src', '/api/workspace/download?path=notes%2Fshot.png&inline=1');
    expect(screen.queryByText('Binary file: not shown.')).not.toBeInTheDocument();
    // The image is a viewer link: a click opens the zoomable viewer, the
    // viewer's Download fetches the attachment.
    const viewerLink = img.closest('a')!;
    expect(viewerLink).toHaveAttribute('data-lightbox', '/api/workspace/download?path=notes%2Fshot.png&inline=1');
    expect(viewerLink).toHaveAttribute('data-lightbox-download', '/api/workspace/download?path=notes%2Fshot.png');
    const download = screen.getByRole('link', { name: 'Download (2.0 KB)' });
    expect(download).toHaveAttribute('href', '/api/workspace/download?path=notes%2Fshot.png');
  });

  it('keeps the notice for an SVG', async () => {
    readWorkspaceFile.mockResolvedValueOnce({
      path: 'notes/chart.svg', name: 'chart.svg', size: 10, content: '', binary: true, too_large: false,
      read_only: true, read_only_reason: 'not_memory',
    });
    await renderPage();
    await userEvent.click(screen.getByRole('button', { name: 'notes' }));
    await userEvent.click(await screen.findByRole('button', { name: 'chart.svg' }));
    expect(await screen.findByText('Binary file: not shown.')).toBeInTheDocument();
    expect(screen.queryByRole('img')).not.toBeInTheDocument();
  });

  it('offers no download for a file it shows', async () => {
    await renderPage();
    await userEvent.click(screen.getByRole('button', { name: 'README.md' }));
    await screen.findByDisplayValue('# README.md');
    expect(screen.queryByRole('link', { name: /Download/ })).not.toBeInTheDocument();
  });

  it('says how many entries a capped directory left out', async () => {
    listWorkspaceDir.mockImplementation(async (path: string) => {
      const entries = LISTINGS[path] ?? [];
      return { path, entries, total: entries.length + 5, truncated: true };
    });
    await renderPage();
    expect(screen.getByText('5 more not shown')).toBeInTheDocument();
  });

  it('refresh reloads the root and every open directory', async () => {
    await renderPage();
    await userEvent.click(screen.getByRole('button', { name: 'notes' }));
    await screen.findByRole('button', { name: 'plan.md' });
    listWorkspaceDir.mockClear();
    await userEvent.click(screen.getByRole('button', { name: 'Refresh file list' }));
    expect(listWorkspaceDir.mock.calls.map(c => c[0]).sort()).toEqual(['', 'notes']);
  });
});
