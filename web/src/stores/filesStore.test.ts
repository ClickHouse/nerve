import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../api/hosted';

vi.mock('../api/client', () => ({
  api: {
    readMemoryFile: vi.fn(),
    writeMemoryFile: vi.fn(),
    listMemoryFiles: vi.fn(),
  },
}));
const { api } = await import('../api/client');
const { useFilesStore } = await import('./filesStore');

const read = vi.mocked(api.readMemoryFile);
const write = vi.mocked(api.writeMemoryFile);

beforeEach(() => {
  vi.clearAllMocks();
  useFilesStore.setState({
    openFiles: [], tree: [], activeFile: null, loading: false, saving: false,
  });
  read.mockResolvedValue({
    path: 'MEMORY.md', content: 'initial', revision: 'r1', read_only: false,
  });
});

describe('revision-aware file saves', () => {
  it('sends the read revision and retains unsaved edits on a 409 conflict', async () => {
    await useFilesStore.getState().openFile('MEMORY.md');
    useFilesStore.getState().updateContent('MEMORY.md', 'draft');
    write.mockRejectedValue(new ApiError(409, '{"detail":"stale revision"}'));

    await useFilesStore.getState().saveFile('MEMORY.md');

    expect(write).toHaveBeenCalledWith('MEMORY.md', 'draft', 'r1');
    const file = useFilesStore.getState().openFiles[0];
    expect(file.content).toBe('draft');
    expect(file.originalContent).toBe('initial');
    expect(file.modified).toBe(true);
    expect(file.saveError).toMatch(/changed on disk/i);
  });

  it('keeps edits typed while a save was in flight', async () => {
    await useFilesStore.getState().openFile('MEMORY.md');
    useFilesStore.getState().updateContent('MEMORY.md', 'first draft');
    let complete!: (value: { path: string; saved: boolean; revision: string }) => void;
    write.mockImplementation(() => new Promise<{ path: string; saved: boolean; revision: string }>(resolve => { complete = resolve; }));
    const saving = useFilesStore.getState().saveFile('MEMORY.md');
    useFilesStore.getState().updateContent('MEMORY.md', 'second draft');
    complete({ path: 'MEMORY.md', saved: true, revision: 'r2' });
    await saving;

    const file = useFilesStore.getState().openFiles[0];
    expect(file.originalContent).toBe('first draft');
    expect(file.content).toBe('second draft');
    expect(file.modified).toBe(true);
    expect(file.revision).toBe('r2');
  });

  it('updates the revision after successful save', async () => {
    await useFilesStore.getState().openFile('MEMORY.md');
    useFilesStore.getState().updateContent('MEMORY.md', 'saved');
    write.mockResolvedValue({ path: 'MEMORY.md', saved: true, revision: 'r2' });
    await useFilesStore.getState().saveFile('MEMORY.md');
    expect(useFilesStore.getState().openFiles[0]).toMatchObject({
      content: 'saved', originalContent: 'saved', modified: false,
      revision: 'r2', saveError: null,
    });
  });
  it('refreshes an unmodified cached file when it is reopened', async () => {
    await useFilesStore.getState().openFile('MEMORY.md');
    read.mockResolvedValueOnce({
      path: 'MEMORY.md', content: 'agent changed this', revision: 'r2', read_only: false,
    });
    await useFilesStore.getState().openFile('MEMORY.md');
    expect(useFilesStore.getState().openFiles[0]).toMatchObject({
      content: 'agent changed this', originalContent: 'agent changed this',
      modified: false, revision: 'r2',
    });
  });


  it('refreshes a clean tab on tab selection, not only tree selection', async () => {
    await useFilesStore.getState().openFile('MEMORY.md');
    read.mockResolvedValueOnce({
      path: 'MEMORY.md', content: 'agent wrote this', revision: 'r2', read_only: false,
    });

    useFilesStore.getState().setActiveFile('MEMORY.md');
    await vi.waitFor(() => {
      expect(useFilesStore.getState().openFiles[0]).toMatchObject({
        content: 'agent wrote this', originalContent: 'agent wrote this',
        revision: 'r2', modified: false,
      });
    });
    expect(read).toHaveBeenCalledTimes(2);
  });

  it('does not refresh a dirty tab when it is selected', async () => {
    await useFilesStore.getState().openFile('MEMORY.md');
    useFilesStore.getState().updateContent('MEMORY.md', 'my draft');
    useFilesStore.getState().setActiveFile('MEMORY.md');

    expect(read).toHaveBeenCalledTimes(1);
    expect(useFilesStore.getState().openFiles[0]).toMatchObject({
      content: 'my draft', revision: 'r1', modified: true,
    });
  });

  it('does not overwrite local edits made while a cached-file refresh is pending', async () => {
    await useFilesStore.getState().openFile('MEMORY.md');
    let finish!: (value: { path: string; content: string; revision: string; read_only: boolean }) => void;
    read.mockImplementationOnce(() => new Promise<{ path: string; content: string; revision: string; read_only: boolean }>(resolve => { finish = resolve; }));
    const refreshing = useFilesStore.getState().openFile('MEMORY.md');
    useFilesStore.getState().updateContent('MEMORY.md', 'my newer draft');
    finish({ path: 'MEMORY.md', content: 'agent update', revision: 'r2', read_only: false });
    await refreshing;
    expect(useFilesStore.getState().openFiles[0]).toMatchObject({
      content: 'my newer draft', originalContent: 'initial',
      modified: true, revision: 'r1',
    });
  });

  it('keeps a tab open until its in-flight save finishes', async () => {
    await useFilesStore.getState().openFile('MEMORY.md');
    useFilesStore.getState().updateContent('MEMORY.md', 'new contents');
    let finish!: (value: { path: string; saved: boolean; revision: string }) => void;
    write.mockImplementationOnce(() => new Promise<{ path: string; saved: boolean; revision: string }>(resolve => { finish = resolve; }));
    const saving = useFilesStore.getState().saveFile('MEMORY.md');
    useFilesStore.getState().closeFile('MEMORY.md');
    expect(useFilesStore.getState().openFiles).toHaveLength(1);
    finish({ path: 'MEMORY.md', saved: true, revision: 'r2' });
    await saving;
    expect(useFilesStore.getState().openFiles[0]).toMatchObject({
      content: 'new contents', modified: false, revision: 'r2',
    });
  });

});
