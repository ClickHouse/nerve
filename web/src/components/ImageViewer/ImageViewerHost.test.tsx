import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ImageViewerHost, collectGroup } from './ImageViewerHost';
import { imageLinkProps, isRasterImagePath } from '../../utils/imageLinks';

/** jsdom loads no images: this one "loads" at once with a fixed size, or fails for URLs with `broken`. */
class FakeImage {
  naturalWidth = 640;
  naturalHeight = 480;
  onload: (() => void) | null = null;
  onerror: (() => void) | null = null;
  set src(value: string) {
    queueMicrotask(() => (value.includes('broken') ? this.onerror?.() : this.onload?.()));
  }
}

beforeEach(() => {
  vi.stubGlobal('Image', FakeImage);
});
afterEach(() => {
  vi.unstubAllGlobals();
});

function Message({ urls, role = 'user' }: { urls: string[]; role?: string }) {
  return (
    <div data-role={role}>
      {urls.map(u => (
        <a key={u} {...imageLinkProps(u, u.split('/').pop()!)}>
          <img src={u} alt={u.split('/').pop()} />
        </a>
      ))}
    </div>
  );
}

async function clickImage(name: string, init?: MouseEventInit) {
  const link = screen.getByRole('img', { name }).closest('a')!;
  await act(async () => {
    fireEvent.click(link, { button: 0, ...init });
  });
  return link;
}

describe('imageLinkProps', () => {
  it('links to the inline image and to an attachment for Download', () => {
    expect(imageLinkProps('/api/files/uploads/a1', 'a.png')).toMatchObject({
      href: '/api/files/uploads/a1',
      target: '_blank',
      'data-lightbox': '/api/files/uploads/a1',
      'data-lightbox-title': 'a.png',
      'data-lightbox-download': '/api/files/uploads/a1?download=1',
    });
    expect(imageLinkProps('/x?path=a.png&inline=1', 'a.png', '/x?path=a.png')['data-lightbox-download'])
      .toBe('/x?path=a.png');
  });

  it('knows raster images by extension, and SVG is not one', () => {
    expect(['a.png', 'b.JPG', 'c.jpeg', 'd.webp', 'e.gif'].every(isRasterImagePath)).toBe(true);
    expect(['a.svg', 'b.pdf', 'png', 'x.png.txt'].some(isRasterImagePath)).toBe(false);
  });
});

describe('ImageViewerHost', () => {
  it('opens a clicked chat image in the zoomable viewer instead of following the link', async () => {
    render(<><ImageViewerHost /><Message urls={['/api/files/uploads/a/shot.png']} /></>);
    const link = screen.getByRole('img', { name: 'shot.png' }).closest('a')!;
    let defaultPrevented = false;
    await act(async () => {
      defaultPrevented = !fireEvent.click(link, { button: 0 });
    });
    expect(defaultPrevented).toBe(true);
    const dialog = await screen.findByRole('dialog', { name: 'shot.png' });
    expect(dialog.querySelector('img')).toHaveAttribute('src', '/api/files/uploads/a/shot.png');
    expect(screen.getByRole('link', { name: 'Open original' })).toHaveAttribute('href', '/api/files/uploads/a/shot.png');
    expect(screen.getByRole('link', { name: 'Download' })).toHaveAttribute('href', '/api/files/uploads/a/shot.png?download=1');
    // A single image has nothing to step through.
    expect(screen.queryByRole('button', { name: 'Next picture' })).not.toBeInTheDocument();
  });

  it.each([['metaKey'], ['ctrlKey'], ['shiftKey']])('leaves a %s click to the browser (new tab)', async key => {
    render(<><ImageViewerHost /><Message urls={['/u/a.png']} /></>);
    await clickImage('a.png', { [key]: true });
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('steps through the images of the same message, not of others', async () => {
    render(
      <>
        <ImageViewerHost />
        <Message urls={['/u/1.png', '/u/2.png', '/u/3.png']} />
        <Message urls={['/u/other.png']} role="assistant" />
      </>,
    );
    await clickImage('2.png');
    await screen.findByRole('dialog', { name: '2.png' });
    expect(screen.getByText('2 / 3')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Next picture' }));
    await screen.findByRole('dialog', { name: '3.png' });
    await userEvent.click(screen.getByRole('button', { name: 'Next picture' }));
    // Wraps around.
    const dialog = await screen.findByRole('dialog', { name: '1.png' });
    // The new picture takes focus (a frame later), so the keys work at once.
    await waitFor(() => expect(within(dialog).getByRole('img', { name: '1.png' })).toHaveFocus());
    await userEvent.keyboard('p');
    await screen.findByRole('dialog', { name: '3.png' });
  });

  it('closes on Escape', async () => {
    render(<><ImageViewerHost /><Message urls={['/u/a.png']} /></>);
    await clickImage('a.png');
    await screen.findByRole('dialog');
    await userEvent.keyboard('{Escape}');
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('opens a file that does not load as an image in a tab instead', async () => {
    const open = vi.spyOn(window, 'open').mockImplementation(() => null);
    render(<><ImageViewerHost /><Message urls={['/u/broken.png']} /></>);
    await clickImage('broken.png');
    expect(open).toHaveBeenCalledWith('/u/broken.png', '_blank', 'noopener,noreferrer');
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    open.mockRestore();
  });

  it('ignores clicks on ordinary links', async () => {
    render(<><ImageViewerHost /><a href="/x">plain</a></>);
    const notPrevented = fireEvent.click(screen.getByRole('link', { name: 'plain' }), { button: 0 });
    expect(notPrevented).toBe(true);
  });
});

describe('collectGroup', () => {
  it('uses an explicit data-lightbox-group over the message', () => {
    const { container } = render(
      <div data-role="assistant">
        <div data-lightbox-group="">
          <a data-lightbox="/a" href="/a"><img alt="a" /></a>
        </div>
        <a data-lightbox="/b" href="/b"><img alt="b" /></a>
      </div>,
    );
    const link = container.querySelector<HTMLAnchorElement>('a[data-lightbox="/a"]')!;
    expect(collectGroup(link)).toEqual({ images: [{ src: '/a', title: 'a', download: link.href }], index: 0 });
  });
});
