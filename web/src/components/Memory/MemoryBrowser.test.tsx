import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '../../api/hosted';

vi.mock('../../api/client', () => ({
  api: {
    listMemoryFiles: vi.fn(),
    readMemoryFile: vi.fn(),
    writeMemoryFile: vi.fn(),
  },
}));

const { api } = await import('../../api/client');
const { MemoryBrowser } = await import('./MemoryBrowser');
const list = vi.mocked(api.listMemoryFiles);
const read = vi.mocked(api.readMemoryFile);
const write = vi.mocked(api.writeMemoryFile);

beforeEach(() => {
  vi.clearAllMocks();
  list.mockResolvedValue({
    files: [
      { path: 'memory/one.md', name: 'one.md', size: 10, modified: '' },
      { path: 'memory/two.md', name: 'two.md', size: 10, modified: '' },
    ],
  });
  read.mockImplementation(async path => ({
    path, content: path.includes('one') ? 'first' : 'second',
    revision: path.includes('one') ? 'r1' : 'r2',
  }));
});

describe('memory-file editor conflict protection', () => {
  it('retains unsaved text and shows a conflict after a stale save', async () => {
    const user = userEvent.setup();
    render(<MemoryBrowser />);
    await user.click(await screen.findByText('one.md'));
    await user.click(screen.getByRole('button', { name: 'Edit' }));
    const editor = screen.getByRole('textbox');
    await user.clear(editor);
    await user.type(editor, 'my unsaved note');

    write.mockRejectedValue(new ApiError(409, '{"detail":"stale revision"}'));
    await user.click(screen.getByRole('button', { name: 'Save' }));
    expect(await screen.findByRole('alert')).toHaveTextContent(/changed on disk/i);
    expect(editor).toHaveValue('my unsaved note');
    expect(write).toHaveBeenCalledWith('memory/one.md', 'my unsaved note', 'r1');
  });

  it('does not switch files when the user refuses to discard a draft', async () => {
    const user = userEvent.setup();
    render(<MemoryBrowser />);
    await user.click(await screen.findByText('one.md'));
    await user.click(screen.getByRole('button', { name: 'Edit' }));
    const editor = screen.getByRole('textbox');
    await user.clear(editor);
    await user.type(editor, 'my draft');

    const confirm = vi.spyOn(window, 'confirm').mockReturnValue(false);
    try {
      await user.click(screen.getByText('two.md'));
      expect(confirm).toHaveBeenCalled();
      expect(editor).toHaveValue('my draft');
      expect(read).toHaveBeenCalledTimes(1);
    } finally {
      confirm.mockRestore();
    }
  });
});
