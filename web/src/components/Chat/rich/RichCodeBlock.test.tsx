import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useThemeStore } from '../../../stores/themeStore';
import { MarkdownStreamingContext } from '../streamingContext';
import { RichCodeBlock } from './RichCodeBlock';

const { mountMermaid } = vi.hoisted(() => ({
  mountMermaid: vi.fn<(stage: HTMLElement, source: string, theme: string) => Promise<() => void>>(),
}));
vi.mock('./mermaid', () => ({ mountMermaid }));

function draw(stage: HTMLElement, source: string) {
  stage.innerHTML = `<svg><text>${source}</text></svg>`;
  return () => { stage.innerHTML = ''; };
}

beforeEach(() => {
  mountMermaid.mockReset();
  mountMermaid.mockImplementation(async (stage, source) => {
    if (source.includes('BAD')) throw new Error('boom on line 1');
    return draw(stage, source);
  });
});

afterEach(() => {
  vi.useRealTimers();
});

function renderBlock(source: string, streaming: boolean) {
  const ui = (text: string, live: boolean) => (
    <MarkdownStreamingContext.Provider value={live}>
      <RichCodeBlock kind="mermaid" source={text} />
    </MarkdownStreamingContext.Provider>
  );
  const result = render(ui(source, streaming));
  return { update: (text: string, live: boolean) => result.rerender(ui(text, live)) };
}

const figure = () => document.querySelector<HTMLElement>('[aria-label="Rendered mermaid"]')!;
const shown = () => figure().querySelector('text')?.textContent;

describe('RichCodeBlock', () => {
  it('shows the source until the picture is ready, then the picture', async () => {
    let finish!: () => void;
    mountMermaid.mockImplementationOnce((stage, source) => new Promise(resolve => {
      finish = () => resolve(draw(stage, source));
    }));
    renderBlock('graph A', false);
    // The renderer module loads first; the mount itself is a microtask later.
    await act(async () => {});
    expect(mountMermaid).toHaveBeenCalledTimes(1);
    expect(figure().hidden).toBe(true);
    expect(document.querySelector('pre code')?.textContent).toBe('graph A');
    expect(screen.getByText('rendering…')).toBeInTheDocument();
    await act(async () => { finish(); });
    expect(figure().hidden).toBe(false);
    expect(shown()).toBe('graph A');
    expect(document.querySelector('pre')).toBeNull();
    expect(screen.queryByText('rendering…')).toBeNull();
  });

  it('keeps the previous picture on screen until the next one is ready', async () => {
    const { update } = renderBlock('graph A', false);
    await screen.findByRole('img', { name: 'Rendered mermaid' });
    expect(shown()).toBe('graph A');

    let finish!: () => void;
    mountMermaid.mockImplementationOnce((stage, source) => new Promise(resolve => {
      finish = () => resolve(draw(stage, source));
    }));
    update('graph B', false);
    await act(async () => {});
    expect(mountMermaid).toHaveBeenCalledTimes(2);
    expect(figure().hidden).toBe(false);
    expect(shown()).toBe('graph A');
    await act(async () => { finish(); });
    expect(shown()).toBe('graph B');
    expect(figure().querySelectorAll('svg')).toHaveLength(1);
  });

  it('while streaming, renders again once the text stops changing', async () => {
    vi.useFakeTimers();
    const { update } = renderBlock('gra', true);
    // The text at mount is tried once; later text waits for a pause.
    await act(async () => {});
    expect(mountMermaid).toHaveBeenCalledTimes(1);
    update('graph', true);
    update('graph TD', true);
    await act(async () => { vi.advanceTimersByTime(400); });
    expect(mountMermaid).toHaveBeenCalledTimes(1);
    update('graph TD\n  A-->B', true);
    await act(async () => { vi.advanceTimersByTime(400); });
    expect(mountMermaid).toHaveBeenCalledTimes(1);
    await act(async () => { vi.advanceTimersByTime(300); });
    expect(mountMermaid).toHaveBeenCalledTimes(2);
    expect(mountMermaid.mock.calls[1][1]).toBe('graph TD\n  A-->B');
  });

  it('keeps quiet about a failure while streaming, and explains it after', async () => {
    const { update } = renderBlock('BAD', true);
    await act(async () => {});
    expect(screen.queryByRole('status')).toBeNull();
    expect(screen.getByText('renders when complete')).toBeInTheDocument();
    expect(document.querySelector('pre code')?.textContent).toBe('BAD');

    update('BAD', false);
    const note = await screen.findByRole('status');
    expect(note.textContent).toContain("Couldn't render this mermaid: boom on line 1");
    expect(screen.getByText("couldn't render")).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /^(Source|Rendered)$/ })).toBeNull();
    expect(document.querySelector('pre code')?.textContent).toBe('BAD');
  });

  it('renders again when the theme changes', async () => {
    renderBlock('graph A', false);
    await screen.findByRole('img', { name: 'Rendered mermaid' });
    const before = useThemeStore.getState().resolved;
    expect(mountMermaid.mock.calls[0][2]).toBe(before);
    const other = before === 'dark' ? 'light' : 'dark';
    await act(async () => { useThemeStore.setState({ resolved: other }); });
    expect(mountMermaid).toHaveBeenCalledTimes(2);
    expect(mountMermaid.mock.calls[1][2]).toBe(other);
    useThemeStore.setState({ resolved: before });
  });

  it('opens the picture at full size in a dialog', async () => {
    renderBlock('graph A', false);
    await screen.findByRole('img', { name: 'Rendered mermaid' });
    await userEvent.click(screen.getByRole('button', { name: 'Expand' }));
    const dialog = await screen.findByRole('dialog');
    expect(dialog.querySelector('svg text')?.textContent).toBe('graph A');
    await userEvent.keyboard('{Escape}');
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
  });

  it('offers the picture as an SVG file', async () => {
    const downloads: string[] = [];
    const createObjectURL = vi.fn<(blob: Blob) => string>(() => 'blob:picture');
    const revokeObjectURL = vi.fn();
    // jsdom has neither; put back whatever was there afterwards.
    const urlBefore = { createObjectURL: URL.createObjectURL, revokeObjectURL: URL.revokeObjectURL };
    Object.assign(URL, { createObjectURL, revokeObjectURL });
    const click = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(function (this: HTMLAnchorElement) {
      downloads.push(this.download);
    });
    try {
      renderBlock('graph A', false);
      await screen.findByRole('img', { name: 'Rendered mermaid' });
      await userEvent.click(screen.getByRole('button', { name: 'Download SVG' }));
      expect(createObjectURL).toHaveBeenCalledTimes(1);
      expect(createObjectURL.mock.calls[0][0].type).toBe('image/svg+xml');
      expect(downloads).toEqual(['mermaid.svg']);
    } finally {
      click.mockRestore();
      Object.assign(URL, urlBefore);
    }
  });
});
