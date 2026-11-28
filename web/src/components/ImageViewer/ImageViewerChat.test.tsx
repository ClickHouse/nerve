import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { ChatMessage, ToolCallBlockData } from '../../types/chat';
import { BlockRenderer } from '../Chat/BlockRenderer';
import { SendFileBlock } from '../Chat/tools/SendFileBlock';
import { UserMessage } from '../Chat/UserMessage';
import { ImageViewerHost } from './ImageViewerHost';

/**
 * jsdom loads no image: each one loads at once at 640 × 480, or when the
 * test calls `release` for a source in `held`. The sources asked for are kept.
 */
const loaded: string[] = [];
const held = new Set<string>();
const waiting: (() => void)[] = [];
const release = () => act(async () => waiting.splice(0).forEach(go => go()));
class FakeImage {
  naturalWidth = 640;
  naturalHeight = 480;
  onload: (() => void) | null = null;
  set src(value: string) {
    loaded.push(value);
    const go = () => this.onload?.();
    if (held.has(value)) waiting.push(go);
    else queueMicrotask(go);
  }
}

/** jsdom lays nothing out: every box is 1000 × 800, so a 640 × 480 picture fits. */
const BOX = { left: 0, top: 0, right: 1000, bottom: 800, x: 0, y: 0, width: 1000, height: 800 };

beforeEach(() => {
  loaded.length = 0;
  held.clear();
  waiting.length = 0;
  vi.stubGlobal('Image', FakeImage);
  vi.spyOn(HTMLElement.prototype, 'getBoundingClientRect').mockReturnValue({ ...BOX, toJSON: () => BOX } as DOMRect);
});
afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

function sent(path: string): ToolCallBlockData {
  return {
    type: 'tool_call', toolUseId: `tu-${path}`, tool: 'mcp__nerve__send_file', input: { file_path: path },
    result: JSON.stringify([{ type: 'text', text: 'Sent file' }]), status: 'complete',
  };
}

function attached(filename: string): ChatMessage {
  return {
    role: 'user',
    blocks: [{ type: 'text', content: 'Here is the screenshot' },
      { type: 'image', url: `/api/uploads/${filename}`, filename, media_type: 'image/png' }],
  };
}

const ONE = `/api/files/download?path=${encodeURIComponent('/ws/one.png')}`;

/** Three images in one chat, each in a message of its own: sent, attached, an image block. */
function renderChat() {
  return render(
    <>
      <ImageViewerHost />
      <div data-lightbox-group="">
        <div data-role="assistant"><SendFileBlock block={sent('/ws/one.png')} /></div>
        <UserMessage message={attached('two.png')} />
        <div data-role="assistant">
          <BlockRenderer blocks={[{ type: 'image', url: '/api/uploads/three.png', filename: 'three.png', media_type: 'image/png' }]} />
        </div>
      </div>
    </>,
  );
}

async function open(name: string) {
  await act(async () => {
    fireEvent.click(screen.getByRole('link', { name }), { button: 0 });
  });
  return screen.findByRole('dialog', { name });
}

const shownSrc = () => screen.getByRole('dialog').querySelector('.rich-lightbox-content img')?.getAttribute('src');
const viewport = () => within(screen.getByRole('dialog')).getByRole('img');
const content = () => viewport().firstElementChild as HTMLElement;
const button = (name: string) => within(screen.getByRole('dialog')).queryByRole('button', { name });

/** The keys go to the picture, which takes focus a frame after it shows. */
async function keys(text: string) {
  await waitFor(() => expect(viewport()).toHaveFocus());
  await userEvent.keyboard(text);
}

describe('the image viewer in a chat', () => {
  it('opens an image at its place among all the images of the chat', async () => {
    renderChat();
    await open('two.png');
    expect(shownSrc()).toBe('/api/uploads/two.png');
    expect(screen.getByText('2 / 3')).toBeInTheDocument();
  });

  it('steps with Left and Right while the picture fits, and stops at the first and the last', async () => {
    renderChat();
    await open('two.png');
    await keys('{ArrowRight}');
    await screen.findByRole('dialog', { name: 'three.png' });
    expect(screen.getByText('3 / 3')).toBeInTheDocument();
    expect(button('Next picture')).not.toBeInTheDocument();
    await keys('{ArrowRight}');
    expect(screen.getByRole('dialog')).toHaveAccessibleName('three.png');

    await keys('{ArrowLeft}');
    await screen.findByRole('dialog', { name: 'two.png' });
    await keys('{ArrowLeft}');
    await screen.findByRole('dialog', { name: 'one.png' });
    expect(shownSrc()).toBe(ONE);
    expect(button('Previous picture')).not.toBeInTheDocument();
    await keys('{ArrowLeft}');
    expect(screen.getByRole('dialog')).toHaveAccessibleName('one.png');
  });

  it('moves the picture with Left and Right when it is zoomed in wider than the window', async () => {
    renderChat();
    await open('two.png');
    // 640 px at 156% is 1000 px; at 195% it is wider than the window.
    await keys('+++');
    const before = content().style.transform;
    await keys('{ArrowRight}');
    expect(screen.getByRole('dialog')).toHaveAccessibleName('two.png');
    expect(content().style.transform).not.toBe(before);
  });

  it('moves the picture with Shift and an arrow key, also while it fits', async () => {
    renderChat();
    await open('two.png');
    await keys('{Shift>}{ArrowRight}{/Shift}');
    expect(screen.getByRole('dialog')).toHaveAccessibleName('two.png');
    expect(content().style.transform).toMatch(/^translate\(-192px/);
  });

  it('steps with the arrow buttons over the picture', async () => {
    renderChat();
    await open('one.png');
    expect(button('Previous picture')).not.toBeInTheDocument();
    await userEvent.click(button('Next picture')!);
    await screen.findByRole('dialog', { name: 'two.png' });
    await userEvent.click(button('Previous picture')!);
    await screen.findByRole('dialog', { name: 'one.png' });
  });

  it('keeps focus on the arrow button that was used, or on the other one at an end', async () => {
    renderChat();
    await open('one.png');
    await userEvent.click(button('Next picture')!);
    await screen.findByRole('dialog', { name: 'two.png' });
    await waitFor(() => expect(button('Next picture')).toHaveFocus());
    await userEvent.keyboard('{Enter}');
    await screen.findByRole('dialog', { name: 'three.png' });
    await waitFor(() => expect(button('Previous picture')).toHaveFocus());
  });

  it('keeps the title, the count and the arrows of the picture that shows while the next one loads', async () => {
    held.add('/api/uploads/three.png');
    renderChat();
    await open('two.png');
    await keys('{ArrowRight}');
    expect(screen.getByRole('dialog')).toHaveAccessibleName('two.png');
    expect(screen.getByText('2 / 3')).toBeInTheDocument();
    expect(button('Next picture')).toBeInTheDocument();
    await release();
    await screen.findByRole('dialog', { name: 'three.png' });
    expect(screen.getByText('3 / 3')).toBeInTheDocument();
    expect(button('Next picture')).not.toBeInTheDocument();
  });

  it('says which picture shows, for a screen reader', async () => {
    renderChat();
    await open('two.png');
    await keys('{ArrowRight}');
    await screen.findByRole('dialog', { name: 'three.png' });
    expect(screen.getByText('three.png, picture 3 of 3')).toHaveAttribute('aria-live', 'polite');
  });

  it('loads the pictures before and after the one that shows', async () => {
    renderChat();
    await open('two.png');
    await waitFor(() => expect(loaded).toEqual(expect.arrayContaining([ONE, '/api/uploads/three.png'])));
  });

  it('gives focus to the image that showed last when it closes', async () => {
    renderChat();
    await open('one.png');
    await keys('{ArrowRight}');
    await screen.findByRole('dialog', { name: 'two.png' });
    await keys('{ArrowRight}');
    await screen.findByRole('dialog', { name: 'three.png' });
    await keys('{Escape}');
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'three.png' })).toHaveFocus();
  });

  it('shows one image with no arrows and no count outside a set, and Right moves it', async () => {
    render(<><ImageViewerHost /><SendFileBlock block={sent('/ws/one.png')} /></>);
    await open('one.png');
    expect(button('Next picture')).not.toBeInTheDocument();
    expect(screen.queryByText(/\d \/ \d/)).not.toBeInTheDocument();
    await keys('{ArrowRight}');
    expect(content().style.transform).toMatch(/^translate\(-48px/);
  });

  describe('on a touch screen', () => {
    const at = (x: number, y = 300, pointerId = 1, pointerType = 'touch') => ({ pointerId, pointerType, clientX: x, clientY: y });
    function swipe(dx: number, dy = 0, pointerType = 'touch') {
      fireEvent.pointerDown(viewport(), at(200, 300, 1, pointerType));
      fireEvent.pointerMove(viewport(), at(200 + dx / 2, 300 + dy / 2, 1, pointerType));
      fireEvent.pointerUp(viewport(), at(200 + dx, 300 + dy, 1, pointerType));
    }

    it('shows the next picture after a swipe to the left, the previous one after a swipe to the right', async () => {
      renderChat();
      await open('two.png');
      swipe(-120);
      await screen.findByRole('dialog', { name: 'three.png' });
      swipe(120);
      await screen.findByRole('dialog', { name: 'two.png' });
      swipe(120);
      await screen.findByRole('dialog', { name: 'one.png' });
      swipe(120);
      expect(screen.getByRole('dialog')).toHaveAccessibleName('one.png');
    });

    it('takes no short move, no move up or down, and no mouse drag as a swipe', async () => {
      renderChat();
      await open('two.png');
      swipe(-30);
      swipe(-80, 120);
      swipe(-120, 0, 'mouse');
      await act(async () => {});
      expect(screen.getByRole('dialog')).toHaveAccessibleName('two.png');
    });

    it('takes a second finger as a pinch, not a swipe', async () => {
      renderChat();
      await open('two.png');
      fireEvent.pointerDown(viewport(), at(200));
      fireEvent.pointerDown(viewport(), at(260, 300, 2));
      fireEvent.pointerMove(viewport(), at(40, 300, 2));
      fireEvent.pointerUp(viewport(), at(40, 300, 2));
      fireEvent.pointerUp(viewport(), at(30));
      await act(async () => {});
      expect(screen.getByRole('dialog')).toHaveAccessibleName('two.png');
    });

    it('moves the picture with the finger, a third as far past the last one, and puts it back', async () => {
      renderChat();
      await open('two.png');
      fireEvent.pointerDown(viewport(), at(200));
      fireEvent.pointerMove(viewport(), at(170));
      expect(content().style.transform).toMatch(/^translate\(-30px/);
      fireEvent.pointerCancel(viewport(), at(170));
      expect(content().style.transform).toMatch(/^translate\(0px/);
      await keys('{ArrowRight}');
      await screen.findByRole('dialog', { name: 'three.png' });
      fireEvent.pointerDown(viewport(), at(200));
      fireEvent.pointerMove(viewport(), at(170));
      expect(content().style.transform).toMatch(/^translate\(-10px/);
    });

    it('moves a picture that is zoomed in wider than the window, and does not step', async () => {
      renderChat();
      await open('two.png');
      await keys('+++');
      swipe(-120);
      await act(async () => {});
      expect(screen.getByRole('dialog')).toHaveAccessibleName('two.png');
    });
  });
});
