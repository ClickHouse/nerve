import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { Lightbox } from './Lightbox';

const MARKUP = '<svg xmlns="http://www.w3.org/2000/svg" width="400" height="200" viewBox="0 0 400 200">'
  + '<text x="10" y="20">graph A</text></svg>';

/** jsdom lays nothing out: the viewport is 1000 × 800 at the page's corner. */
const VIEWPORT = { left: 0, top: 0, right: 1000, bottom: 800, x: 0, y: 0, width: 1000, height: 800 };

function open({ size = { width: 400, height: 200 }, markup = MARKUP } = {}) {
  const onClose = vi.fn();
  render(
    <Lightbox open onClose={onClose} title="Diagram" label="mermaid" markup={markup} size={size} />,
  );
  const viewport = screen.getByRole('img', { name: 'Diagram' });
  viewport.getBoundingClientRect = () => ({ ...VIEWPORT, toJSON: () => VIEWPORT });
  const content = viewport.firstElementChild as HTMLElement;
  const host = content.firstElementChild as HTMLElement;
  return { onClose, viewport, content, host };
}

/** The picture's place: its zoom, and how far it was moved from where 1:1 puts it. */
function placeOf(content: HTMLElement) {
  const match = /translate\((-?[\d.e-]+)px, (-?[\d.e-]+)px\) scale\(([\d.e-]+)\)/.exec(content.style.transform);
  if (!match) throw new Error(`unexpected transform: ${content.style.transform}`);
  return { x: Number(match[1]), y: Number(match[2]), zoom: Number(match[3]) };
}

const zoomLevel = () => screen.getByLabelText('Zoom level').textContent;

/** The corner's CSS offset, compacted: jsdom serializes `max()` with stray `*`s. */
const offset = (value: string) => value.replace(/[\s*]+/g, '');

describe('Lightbox', () => {
  it('shows the picture at 1:1 in a dialog of its own', async () => {
    const { viewport, content, host } = open();
    expect(screen.getByRole('dialog', { name: 'Diagram' })).toBeInTheDocument();
    // In a shadow root, at its natural size, centred while it fits.
    const svg = host.shadowRoot!.querySelector('svg')!;
    expect(svg.querySelector('text')?.textContent).toBe('graph A');
    expect([svg.style.width, svg.style.height]).toEqual(['400px', '200px']);
    expect(offset(content.style.left)).toBe('max(24px,calc(50%-200px))');
    expect(placeOf(content)).toEqual({ x: 0, y: 0, zoom: 1 });
    expect(zoomLevel()).toBe('100%');
    // Focus starts on the picture, so the keys work at once.
    await waitFor(() => expect(viewport).toHaveFocus());
  });

  it('zooms with its buttons and keys, about the middle of the view', async () => {
    const { viewport, content } = open();
    await userEvent.click(screen.getByRole('button', { name: 'Zoom in' }));
    expect(zoomLevel()).toBe('125%');
    // At 1:1 the 400 × 200 picture sits at (300, 300); zooming about (500, 400)
    // keeps its centre there.
    expect(placeOf(content)).toEqual({ x: -50, y: -25, zoom: 1.25 });

    viewport.focus();
    await userEvent.keyboard('-');
    expect(zoomLevel()).toBe('100%');
    await userEvent.keyboard('+');
    await userEvent.keyboard('+');
    expect(zoomLevel()).toBe('156%');
    await userEvent.keyboard('0');
    expect(placeOf(content)).toEqual({ x: 0, y: 0, zoom: 1 });
    await userEvent.click(screen.getByRole('button', { name: 'Zoom out' }));
    expect(zoomLevel()).toBe('80%');
    await userEvent.click(screen.getByRole('button', { name: 'Actual size (1:1)' }));
    expect(zoomLevel()).toBe('100%');
  });

  it('zooms about the cursor with the wheel, instead of scrolling the page', () => {
    const { viewport, content } = open();
    // (340, 320) is the picture's point (40, 20).
    const notCancelled = fireEvent.wheel(viewport, { deltaY: -200, clientX: 340, clientY: 320 });
    expect(notCancelled).toBe(false);
    const place = placeOf(content);
    expect(place.zoom).toBeCloseTo(Math.exp(0.3), 5);
    expect((340 - 300 - place.x) / place.zoom).toBeCloseTo(40, 5);
    expect((320 - 300 - place.y) / place.zoom).toBeCloseTo(20, 5);
    expect(zoomLevel()).toBe('135%');
  });

  it('moves the picture by dragging, and with the arrow keys', async () => {
    const { viewport, content } = open();
    fireEvent.pointerDown(viewport, { pointerId: 1, pointerType: 'mouse', button: 0, clientX: 100, clientY: 100 });
    fireEvent.pointerMove(viewport, { pointerId: 1, pointerType: 'mouse', clientX: 130, clientY: 110 });
    fireEvent.pointerMove(viewport, { pointerId: 1, pointerType: 'mouse', clientX: 160, clientY: 130 });
    fireEvent.pointerUp(viewport, { pointerId: 1, pointerType: 'mouse', clientX: 160, clientY: 130 });
    expect(placeOf(content)).toEqual({ x: 60, y: 30, zoom: 1 });
    // A move after the button is up drags nothing.
    fireEvent.pointerMove(viewport, { pointerId: 1, pointerType: 'mouse', clientX: 400, clientY: 400 });
    expect(placeOf(content)).toEqual({ x: 60, y: 30, zoom: 1 });

    viewport.focus();
    await userEvent.keyboard('{ArrowRight}{ArrowDown}');
    expect(placeOf(content)).toEqual({ x: 12, y: -18, zoom: 1 });
  });

  it('zooms when two fingers pinch', () => {
    const { viewport } = open();
    fireEvent.pointerDown(viewport, { pointerId: 1, pointerType: 'touch', clientX: 400, clientY: 400 });
    fireEvent.pointerDown(viewport, { pointerId: 2, pointerType: 'touch', clientX: 600, clientY: 400 });
    fireEvent.pointerMove(viewport, { pointerId: 2, pointerType: 'touch', clientX: 700, clientY: 400 });
    expect(zoomLevel()).toBe('150%');
  });

  it('fits a large picture into the window, and back to 1:1 with 0', async () => {
    const { viewport, content } = open({ size: { width: 2000, height: 500 } });
    // Too wide to fit at 1:1: it opens at its start, not at its middle.
    expect(offset(content.style.left)).toBe('max(24px,calc(50%-1000px))');
    await userEvent.click(screen.getByRole('button', { name: 'Fit to window' }));
    const place = placeOf(content);
    expect(place.zoom).toBeCloseTo(952 / 2000, 5);
    expect(zoomLevel()).toBe('48%');
    // Centred: the corner at 1:1 is (24, 150), the fitted picture 952 × 238.
    expect(24 + place.x).toBeCloseTo((1000 - 952) / 2, 5);
    expect(150 + place.y).toBeCloseTo((800 - 238) / 2, 5);
    viewport.focus();
    await userEvent.keyboard('0');
    expect(placeOf(content)).toEqual({ x: 0, y: 0, zoom: 1 });
  });

  it('keeps the keys it handles from the shortcuts behind it', async () => {
    const appHandler = vi.fn();
    document.addEventListener('keydown', appHandler);
    try {
      const { viewport } = open();
      viewport.focus();
      await userEvent.keyboard('+{ArrowUp}0');
      expect(appHandler).not.toHaveBeenCalled();
    } finally {
      document.removeEventListener('keydown', appHandler);
    }
  });

  it('closes on Escape and from its close button', async () => {
    const { onClose } = open();
    await userEvent.keyboard('{Escape}');
    expect(onClose).toHaveBeenCalledTimes(1);
    await userEvent.click(screen.getByRole('button', { name: 'Close dialog' }));
    expect(onClose).toHaveBeenCalledTimes(2);
  });
});
