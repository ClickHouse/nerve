import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import type { ToolCallBlockData } from '../../../types/chat';
import { SendImageBlock } from './SendImageBlock';
import { ToolCallBlock } from '../ToolCallBlock';

/** The result of a Nerve MCP tool as the chat gets it from each backend. */
const claude = (text: string) => JSON.stringify([{ type: 'text', text }]);
const codex = (text: string) => JSON.stringify({ content: [{ type: 'text', text }] });
const BACKENDS: [string, (text: string) => string][] = [['Claude', claude], ['Codex', codex]];

function call(input: Record<string, unknown>, overrides: Partial<ToolCallBlockData> = {}): ToolCallBlockData {
  return {
    type: 'tool_call', toolUseId: 'tu-1', tool: 'mcp__nerve__send_image', input,
    result: claude('Image sent: x'), status: 'complete', ...overrides,
  };
}

const COPY = '/api/files/uploads/0123abcd4567ef89';

describe('SendImageBlock', () => {
  it.each(BACKENDS)('shows the copy the handler kept, as a viewer link (%s)', (_, result) => {
    render(<SendImageBlock block={call(
      { file_path: '/ws/tmp/chart.png' },
      { result: result(`Image sent: chart.png\nweb_url: ${COPY}`) },
    )} />);
    const img = screen.getByRole('img', { name: 'chart.png' });
    expect(img).toHaveAttribute('src', COPY);
    const link = img.closest('a')!;
    expect(link).toHaveAttribute('href', COPY);
    expect(link).toHaveAttribute('target', '_blank');
    expect(link).toHaveAttribute('data-lightbox', COPY);
    expect(link).toHaveAttribute('data-lightbox-title', 'chart.png');
    expect(link).toHaveAttribute('data-lightbox-download', `${COPY}?download=1`);
    expect(screen.getByRole('link', { name: 'Download chart.png' })).toHaveAttribute('href', `${COPY}?download=1`);
  });

  it('shows it when no channel took it but the web UI has the copy', () => {
    render(<SendImageBlock block={call(
      { file_path: '/ws/a.png' }, { result: claude(`Image shown in the web UI only: a.png\nweb_url: ${COPY}`) },
    )} />);
    expect(screen.getByRole('img', { name: 'a.png' })).toHaveAttribute('src', COPY);
  });

  it('renders the caption as markdown, and names the image by it', () => {
    render(<SendImageBlock block={call(
      { file_path: '/ws/a.png', caption: 'Memory **after** the fix' }, { result: claude(`Image sent: a.png\nweb_url: ${COPY}`) },
    )} />);
    expect(screen.getByRole('img', { name: 'Memory **after** the fix' })).toBeInTheDocument();
    expect(screen.getByText('after').tagName).toBe('STRONG');
  });

  it('shows a remote image directly, sending no referrer', () => {
    render(<SendImageBlock block={call({ url: 'https://example.com/p/plot.png?x=1' }, { result: claude('Image sent: https://example.com/p/plot.png?x=1') })} />);
    const img = screen.getByRole('img', { name: 'plot.png' });
    expect(img).toHaveAttribute('src', 'https://example.com/p/plot.png?x=1');
    expect(img).toHaveAttribute('referrerpolicy', 'no-referrer');
  });

  it('shows an older call (no copy) from its path while the file is there', () => {
    render(<SendImageBlock block={call({ file_path: '/ws/old shot.png' }, { result: claude('Image sent: old shot.png') })} />);
    expect(screen.getByRole('img', { name: 'old shot.png' }))
      .toHaveAttribute('src', '/api/files/download?path=%2Fws%2Fold%20shot.png');
  });

  it('says when the image is gone', () => {
    render(<SendImageBlock block={call({ file_path: '/ws/a.png' }, { result: claude(`Image sent: a.png\nweb_url: ${COPY}`) })} />);
    fireEvent.error(screen.getByRole('img', { name: 'a.png' }));
    expect(screen.queryByRole('img')).not.toBeInTheDocument();
    expect(screen.getByText(/no longer available/)).toBeInTheDocument();
  });

  it.each(BACKENDS)('says why the image was not sent (%s)', (_, result) => {
    render(<SendImageBlock block={call({ file_path: '/tmp/a.png' }, { result: result('Error: file must be within the workspace.') })} />);
    expect(screen.getByRole('status')).toHaveTextContent('Could not send the image: file must be within the workspace.');
    expect(screen.queryByRole('img')).not.toBeInTheDocument();
  });

  it('shows nothing to load for a non-image with no copy', () => {
    render(<SendImageBlock block={call({ file_path: '/ws/a.txt' }, { result: claude('Image delivery not available on this channel. Try send_file as a fallback, or open the web panel.') })} />);
    expect(screen.getByRole('status')).toHaveTextContent('Could not send the image: Image delivery not available');
  });

  it('does not use a web_url line that is not an upload', () => {
    render(<SendImageBlock block={call({ file_path: '/ws/a.txt' }, { result: claude('Image sent: a.txt\nweb_url: https://evil.example/x') })} />);
    expect(screen.queryByRole('img')).not.toBeInTheDocument();
  });

  it('says it is sending while the call runs', () => {
    render(<SendImageBlock block={call({ file_path: '/ws/a.png' }, { status: 'running', result: undefined })} />);
    expect(screen.getByText('Sending image…')).toBeInTheDocument();
  });

  it('is what the chat shows for a send_image call', () => {
    render(<ToolCallBlock block={call({ file_path: '/ws/a.png' }, { result: claude(`Image sent: a.png\nweb_url: ${COPY}`) })} />);
    expect(screen.getByRole('img', { name: 'a.png' })).toHaveAttribute('src', COPY);
  });
});
