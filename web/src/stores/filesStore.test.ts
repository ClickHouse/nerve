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
});
