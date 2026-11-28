import { StrictMode } from 'react';
import { act, fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { ToolCallBlockData } from '../../../types/chat';
import { ImageViewerHost } from '../../ImageViewer/ImageViewerHost';
import { FileToolBlock } from './FileToolBlock';

// "PNG" in base64; the block only decodes it into a blob.
const DATA = btoa('\x89PNG\r\n');

const image = (extra: Record<string, unknown> = {}) =>
  ({ type: 'image', source: { type: 'base64', media_type: 'image/png', data: DATA, ...extra } });

function read(filePath: string, result: unknown, overrides: Partial<ToolCallBlockData> = {}): ToolCallBlockData {
  return {
    type: 'tool_call', toolUseId: 'tu-1', tool: 'Read', input: { file_path: filePath },
    result: typeof result === 'string' ? result : JSON.stringify(result), status: 'complete', ...overrides,
  };
}

let made: Blob[] = [];
let revoked: string[] = [];

beforeEach(() => {
  made = [];
  revoked = [];
  URL.createObjectURL = vi.fn((b: Blob) => {
    made.push(b);
    return `blob:test/${made.length}`;
  }) as typeof URL.createObjectURL;
  URL.revokeObjectURL = vi.fn((u: string) => { revoked.push(u); }) as typeof URL.revokeObjectURL;
});

afterEach(() => vi.restoreAllMocks());

describe('FileToolBlock', () => {
  it('shows a Read image as a picture that opens in the viewer, not as base64', () => {
    render(<FileToolBlock block={read('/tmp/work/snow/bp_try.png', [image()])} />);
    const img = screen.getByRole('img', { name: 'bp_try.png' });
    expect(img).toHaveAttribute('src', 'blob:test/1');
    const link = img.closest('a')!;
    expect(link).toHaveAttribute('data-lightbox', 'blob:test/1');
    expect(link).toHaveAttribute('data-lightbox-title', 'bp_try.png');
    expect(link.closest('[data-lightbox-group]')).not.toBeNull();
    expect(made[0].type).toBe('image/png');
    expect(screen.getByText('image')).toBeInTheDocument();
    expect(screen.queryByText(new RegExp(DATA.slice(0, 6)))).not.toBeInTheDocument();
  });

  it('takes the media type from the extension when the result lacks one', () => {
    render(<FileToolBlock block={read('/ws/a.JPG', [image({ media_type: undefined })])} />);
    expect(made[0].type).toBe('image/jpeg');
  });

  it('numbers several images and accepts the {content: [...]} shape', () => {
    render(<FileToolBlock block={read('/ws/s.png', { content: [image(), image()] })} />);
    expect(screen.getByRole('img', { name: 's.png (1)' })).toBeInTheDocument();
    expect(screen.getByRole('img', { name: 's.png (2)' })).toBeInTheDocument();
    expect(screen.getByText('2 images')).toBeInTheDocument();
  });

  it('frees the blob URLs when it goes', () => {
    const { unmount } = render(<FileToolBlock block={read('/ws/s.png', [image()])} />);
    unmount();
    expect(revoked).toEqual(['blob:test/1']);
  });

  it('shows a blob URL that is still valid when React runs the effects twice (StrictMode)', () => {
    render(<StrictMode><FileToolBlock block={read('/ws/s.png', [image()])} /></StrictMode>);
    const src = screen.getByRole('img', { name: 's.png' }).getAttribute('src');
    expect(src).toMatch(/^blob:test\//);
    expect(revoked).not.toContain(src);
  });

  it('makes a raster blob of an image whose result names another type, such as SVG', () => {
    render(<FileToolBlock block={read('/ws/a.png', [image({ media_type: 'image/svg+xml' })])} />);
    expect(made[0].type).toBe('image/png');
  });

  it('shows the result as text when no image of it decodes', async () => {
    render(<FileToolBlock block={read('/ws/x.png', [image({ data: '%%%' })])} />);
    expect(screen.queryByRole('img')).not.toBeInTheDocument();
    expect(screen.queryByText('image')).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole('button'));
    expect(screen.getByText(/%%%/)).toBeInTheDocument();
  });

  it('keeps the blob URL of a picture that the viewer shows until the viewer closes', async () => {
    class LoadedImage {
      naturalWidth = 640;
      naturalHeight = 480;
      onload: (() => void) | null = null;
      set src(_: string) {
        queueMicrotask(() => this.onload?.());
      }
    }
    vi.stubGlobal('Image', LoadedImage);
    try {
      const block = read('/ws/s.png', [image()]);
      const { rerender } = render(<><ImageViewerHost /><FileToolBlock block={block} /></>);
      await act(async () => {
        fireEvent.click(screen.getByRole('link', { name: 's.png' }), { button: 0 });
      });
      await screen.findByRole('dialog', { name: 's.png' });
      // The card goes (a tool burst folds); the viewer still shows its picture.
      rerender(<><ImageViewerHost /></>);
      expect(revoked).toEqual([]);
      await userEvent.keyboard('{Escape}');
      expect(revoked).toEqual(['blob:test/1']);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('labels a PDF instead of dumping it', async () => {
    const pdf = { type: 'document', source: { type: 'base64', media_type: 'application/pdf', data: DATA } };
    render(<FileToolBlock block={read('/ws/r.pdf', [pdf])} />);
    expect(screen.getByText('document')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button'));
    expect(screen.queryByText(new RegExp(DATA.slice(0, 6)))).not.toBeInTheDocument();
  });

  it('keeps a text Read as before: line count, expand to see it', async () => {
    render(<FileToolBlock block={read('/ws/n.md', '1\tone\n2\ttwo')} />);
    expect(screen.getByText('2 lines')).toBeInTheDocument();
    expect(screen.queryByRole('img')).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole('button'));
    expect(document.querySelector('pre')?.textContent).toBe('1\tone\n2\ttwo');
  });

  it.each([
    ['bad JSON', '[{"type": "image", "source": {"type": "base64", "data": "x"'],
    ['undecodable data', JSON.stringify([image({ data: '!!not base64!!' })])],
  ])('falls back to text on %s', async (_, result) => {
    render(<FileToolBlock block={read('/ws/x.png', result)} />);
    expect(screen.queryByRole('img')).not.toBeInTheDocument();
  });

  it('shows an error result as text', async () => {
    render(<FileToolBlock block={read('/ws/x.png', [image()], { isError: true })} />);
    expect(screen.queryByRole('img')).not.toBeInTheDocument();
  });
});
