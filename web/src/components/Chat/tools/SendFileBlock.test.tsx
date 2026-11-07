import { fireEvent, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it } from 'vitest';
import type { ToolCallBlockData } from '../../../types/chat';
import { SendFileBlock } from './SendFileBlock';

function call(filePath: string, overrides: Partial<ToolCallBlockData> = {}): ToolCallBlockData {
  return {
    type: 'tool_call', toolUseId: 'tu-1', tool: 'mcp__nerve__send_file', input: { file_path: filePath },
    result: 'File ready: x (1 bytes).', status: 'complete', ...overrides,
  };
}

const DOWNLOAD = '/api/files/download?path=%2Fws%2Fshots%2Fchart%20one.png';

describe('SendFileBlock', () => {
  it('shows an image file in the chat, with its name and a download link', () => {
    render(<SendFileBlock block={call('/ws/shots/chart one.png')} />);
    expect(screen.getByRole('img', { name: 'chart one.png' })).toHaveAttribute('src', DOWNLOAD);
    expect(screen.getByRole('link', { name: 'Download chart one.png' })).toHaveAttribute('href', DOWNLOAD);
  });

  it('opens the image at full size, with a download card', async () => {
    render(<SendFileBlock block={call('/ws/shots/chart one.png')} />);
    await userEvent.click(screen.getByRole('button', { name: 'chart one.png' }));
    const dialog = within(screen.getByRole('dialog', { name: 'chart one.png' }));
    expect(dialog.getByRole('img', { name: 'chart one.png' })).toHaveAttribute('src', DOWNLOAD);
    expect(dialog.getByRole('link', { name: /chart one\.png/ })).toHaveAttribute('download', 'chart one.png');
  });

  it.each(['report.csv', 'notes.md', 'archive.tar.zst'])('shows %s as a download card', (name) => {
    render(<SendFileBlock block={call(`/ws/${name}`)} />);
    expect(screen.queryByRole('img')).not.toBeInTheDocument();
    expect(screen.getByRole('link', { name: new RegExp(name.replace('.', '\\.')) })).toHaveAttribute('download', name);
  });

  it('shows a download card when the image does not load', () => {
    render(<SendFileBlock block={call('/ws/gone.webp')} />);
    fireEvent.error(screen.getByRole('img', { name: 'gone.webp' }));
    expect(screen.queryByRole('img')).not.toBeInTheDocument();
    expect(screen.getByRole('link', { name: /gone\.webp/ })).toHaveAttribute('download', 'gone.webp');
  });

  it('says why the file was not sent, with no link', () => {
    render(<SendFileBlock block={call('/tmp/a.png', { result: 'Error: file must be within the workspace.' })} />);
    expect(screen.getByRole('status')).toHaveTextContent('Could not send a.png: file must be within the workspace.');
    expect(screen.queryByRole('link')).not.toBeInTheDocument();
    expect(screen.queryByRole('img')).not.toBeInTheDocument();
  });

  it('says that a failed call sent nothing', () => {
    render(<SendFileBlock block={call('/ws/a.png', { result: 'tool timed out', isError: true })} />);
    expect(screen.getByRole('status')).toHaveTextContent('Could not send a.png: tool timed out');
  });

  it('shows the progress while the call runs', () => {
    render(<SendFileBlock block={call('/ws/a.png', { status: 'running', result: undefined })} />);
    expect(screen.getByText(/Sending file/)).toBeInTheDocument();
  });
});
