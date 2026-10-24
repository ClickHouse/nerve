import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi, type MockInstance } from 'vitest';

vi.mock('../api/client', () => ({
  api: {
    listMemoryFiles: vi.fn(),
    readMemoryFile: vi.fn(),
    writeMemoryFile: vi.fn(),
  },
  setToken: vi.fn(), clearToken: vi.fn(), getToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));

const { api } = await import('../api/client');
const { MOBILE_QUERY } = await import('../hooks/useMediaQuery');
const { useFilesStore } = await import('../stores/filesStore');
const { FilesPage } = await import('./FilesPage');

const listMemoryFiles = vi.mocked(api.listMemoryFiles);
const readMemoryFile = vi.mocked(api.readMemoryFile);

beforeEach(() => {
  vi.clearAllMocks();
  useFilesStore.setState({ tree: [], openFiles: [], activeFile: null, loading: false, saving: false });
  listMemoryFiles.mockResolvedValue({
    files: [
      { path: 'notes/plan.md', name: 'plan.md', size: 10 },
      { path: 'README.md', name: 'README.md', size: 10 },
    ],
  });
  readMemoryFile.mockImplementation(async (path: string) => ({ path, content: `# ${path}` }));
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
    expect(screen.queryByRole('button', { name: 'plan.md' })).not.toBeInTheDocument();
    expect(drawer()).not.toHaveAttribute('inert');
  });

  it('closes the file list when a file is tapped, and opens the file', async () => {
    await renderPage();
    await userEvent.click(screen.getByRole('button', { name: 'README.md' }));
    expect(drawer()).toHaveAttribute('inert');
    expect(readMemoryFile).toHaveBeenCalledWith('README.md');
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
