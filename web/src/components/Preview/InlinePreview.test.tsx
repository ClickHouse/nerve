import { act, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { PreviewBundle } from '../../types/preview';
import type { ContentMessage } from './messages';

interface FakeHost {
  options: {
    onContent: (message: ContentMessage) => void; onFail: (reason: string) => void; onHandOver?: () => void;
  };
  shown: string[];
  sent: unknown[];
  disposed: boolean;
}

const { hosts } = vi.hoisted(() => ({ hosts: [] as FakeHost[] }));

vi.mock('./host', () => ({
  PreviewHost: class {
    options: FakeHost['options'];
    shown: string[] = [];
    sent: unknown[] = [];
    disposed = false;
    constructor(options: FakeHost['options']) {
      this.options = options;
      hosts.push(this);
    }
    start() {}
    show(html: string) { this.shown.push(html); }
    send(message: unknown) { this.sent.push(message); }
    focus() {}
    dispose() { this.disposed = true; }
  },
}));
vi.mock('../../api/client', async (orig) => ({
  ...(await orig<typeof import('../../api/client')>()),
  api: new Proxy({} as Record<string, unknown>, {
    get: (target, key: string) => (target[key] ??= vi.fn(async () => ({}))),
  }),
}));
vi.mock('../../api/websocket', () => ({
  ws: {
    sendMessage: vi.fn(() => 'sent'), switchSession: vi.fn(), send: vi.fn(), connect: vi.fn(),
    subscribeAgent: vi.fn(() => 'sent'), unsubscribeAgent: vi.fn(() => 'sent'), onOpen: vi.fn(() => () => {}),
    answerInteraction: vi.fn(),
  },
}));

const { api } = await import('../../api/client');
const { ApiError } = await import('../../api/hosted');
const { useChatStore } = await import('../../stores/chatStore');
const { usePreviewStore, resetPreviewConfig } = await import('../../stores/previewStore');
const { claimWidgetSlot, liveWidgets, MAX_LIVE_WIDGETS, resetWidgetSlots } = await import('../Chat/widgets/budget');
const { InlinePreview } = await import('./InlinePreview');
const { SendFileBlock } = await import('../Chat/tools/SendFileBlock');
const { MAX_RESTARTS } = await import('./problems');
const { MAX_BUILD_MS } = await import('./document');
const { canPreviewInline, previewTabId } = await import('./open');

const PATH = '/ws/decks/demo/index.html';
const encoder = new TextEncoder();

let entry = '<!doctype html><html><body><h1>Slide one</h1></body></html>';
let fingerprint = 'f1';

function description(): PreviewBundle {
  return {
    path: 'decks/demo/index.html', root: 'decks/demo', root_path: '/ws/decks/demo', kind: 'folder', entry: 'index.html',
    files: [{ path: 'index.html', size: encoder.encode(entry).length, type: 'text/html' }],
    size: 0, fingerprint, modified: Date.now(),
  };
}

const getBundle = () => vi.mocked(api.getPreviewBundle);
const getFile = () => vi.mocked(api.getPreviewFile);
const lastHost = () => hosts[hosts.length - 1];

async function settle() {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(0);
  });
}

async function show(path = PATH) {
  const view = render(<InlinePreview path={path} />);
  await settle();
  return view;
}

beforeEach(() => {
  vi.useFakeTimers({ shouldAdvanceTime: true });
  hosts.length = 0;
  resetWidgetSlots();
  entry = '<!doctype html><html><body><h1>Slide one</h1></body></html>';
  fingerprint = 'f1';
  getBundle().mockImplementation(async () => description());
  getFile().mockImplementation(async () => encoder.encode(entry).slice().buffer);
  usePreviewStore.setState({ enabled: true, maxBytes: 10 * 1024 * 1024 });
});

afterEach(() => {
  vi.useRealTimers();
  getBundle().mockReset();
  getFile().mockReset();
  resetPreviewConfig();
  resetWidgetSlots();
  document.body.innerHTML = '';
});

describe('which files have an inline preview', () => {
  it.each([
    ['/ws/deck/index.html', true], ['/ws/page.HTML', true],
    ['/ws/logo.svg', false], ['/ws/photo.jpeg', false], ['/ws/app.js', false], ['', false],
  ])('%s: %s', (path, inline) => {
    expect(canPreviewInline(path)).toBe(inline);
  });

  it('shows nothing while previews are off', async () => {
    usePreviewStore.setState({ enabled: false });
    const { container } = await show();
    expect(container).toBeEmptyDOMElement();
    expect(getBundle()).not.toHaveBeenCalled();
  });

  it('shows nothing for an image: the card shows the image itself', async () => {
    const { container } = await show('/ws/shots/chart.png');
    expect(container).toBeEmptyDOMElement();
    expect(getBundle()).not.toHaveBeenCalled();
  });
});

describe('the inline preview', () => {
  it('builds the document of the bundle and gives it to the sandbox frame', async () => {
    await show();
    expect(screen.getByRole('figure', { name: 'Preview of index.html' })).toBeInTheDocument();
    expect(getBundle()).toHaveBeenCalledWith(PATH, expect.any(AbortSignal));
    expect(getFile()).toHaveBeenCalledWith(PATH, 'index.html', 'f1', expect.any(AbortSignal));
    expect(hosts).toHaveLength(1);
    expect(lastHost().shown).toHaveLength(1);
    expect(lastHost().shown[0]).toContain('Slide one');
    expect(liveWidgets()).toBe(1);
  });

  it('shows a deck as its first slide that takes no input, and a click opens the Preview tab', async () => {
    useChatStore.setState({ activeSession: 'session-1', panels: [], activePanelId: null, panelVisible: false });
    await show();
    const figure = screen.getByRole('figure', { name: 'Preview of index.html' });
    expect(figure).not.toHaveClass('aspect-video');
    act(() => lastHost().options.onContent({ type: 'ready', kind: 'deck', canPrint: true, slides: 6, slide: 1 }));
    expect(figure).toHaveClass('aspect-video');
    expect(figure.querySelector('[inert]')).not.toBeNull();
    await userEvent.click(screen.getByRole('button', { name: 'Open index.html in the Preview tab' }));
    expect(useChatStore.getState().activePanelId).toBe(previewTabId(PATH));
    expect(useChatStore.getState().panelVisible).toBe(true);
  });

  it('takes a deck that starts on another slide back to its first slide', async () => {
    await show();
    act(() => lastHost().options.onContent({ type: 'ready', kind: 'deck', canPrint: true, slides: 6, slide: 1 }));
    expect(lastHost().sent).toEqual([]);
    act(() => lastHost().options.onContent({ type: 'ready', kind: 'deck', canPrint: true, slides: 6, slide: 3 }));
    expect(lastHost().sent).toEqual([{ type: 'slide', to: 'first' }]);
    // A deck that does not say its slide numbers is sent to its first slide too.
    act(() => lastHost().options.onContent({ type: 'ready', kind: 'deck', canPrint: false }));
    expect(lastHost().sent).toEqual([{ type: 'slide', to: 'first' }, { type: 'slide', to: 'first' }]);
  });

  it('is no deck any more when the frame gets a new document, until the new page says so', async () => {
    await show();
    const figure = screen.getByRole('figure', { name: 'Preview of index.html' });
    act(() => lastHost().options.onContent({ type: 'ready', kind: 'deck', canPrint: true, slides: 6, slide: 1 }));
    expect(figure).toHaveClass('aspect-video');
    act(() => lastHost().options.onHandOver?.());
    expect(figure).not.toHaveClass('aspect-video');
    expect(figure.querySelector('[inert]')).toBeNull();
  });

  it('is no deck when its frame comes back after another frame took the place, until the page says so', async () => {
    await show();
    act(() => lastHost().options.onContent({ type: 'ready', kind: 'deck', canPrint: true, slides: 6, slide: 1 }));
    const figure = () => screen.getByRole('figure', { name: 'Preview of index.html' });
    expect(figure()).toHaveClass('aspect-video');
    for (let i = 1; i < MAX_LIVE_WIDGETS; i++) claimWidgetSlot(() => {});
    act(() => { claimWidgetSlot(() => {}, true); });
    // The place is gone: a placeholder of the height of a page, not a deck.
    expect(figure()).not.toHaveClass('aspect-video');
    await userEvent.click(screen.getByRole('button', { name: 'Load preview' }));
    await settle();
    // The new frame has its document; the proxy has not taken it yet.
    expect(lastHost().shown).toHaveLength(1);
    expect(figure()).not.toHaveClass('aspect-video');
    expect(figure().querySelector('[inert]')).toBeNull();
    expect(screen.queryByRole('button', { name: /in the Preview tab/ })).not.toBeInTheDocument();
  });

  it('keeps a page that is not a deck usable in place', async () => {
    await show();
    act(() => lastHost().options.onContent({ type: 'ready', kind: 'table', canPrint: false }));
    const figure = screen.getByRole('figure', { name: 'Preview of index.html' });
    expect(figure).not.toHaveClass('aspect-video');
    expect(figure.querySelector('[inert]')).toBeNull();
    expect(screen.queryByRole('button', { name: /in the Preview tab/ })).not.toBeInTheDocument();
  });

  it('builds again when a file event names a file of the bundle, and only when the fingerprint changed', async () => {
    await show();
    expect(getFile()).toHaveBeenCalledTimes(1);

    act(() => usePreviewStore.getState().noteFileChange('/ws/elsewhere/index.html'));
    await settle();
    expect(getBundle()).toHaveBeenCalledTimes(1);

    act(() => usePreviewStore.getState().noteFileChange('/ws/decks/demo/index.html'));
    await settle();
    expect(getBundle()).toHaveBeenCalledTimes(2);
    expect(getFile()).toHaveBeenCalledTimes(1);

    entry = '<!doctype html><html><body><h1>Slide two</h1></body></html>';
    fingerprint = 'f2';
    act(() => usePreviewStore.getState().noteFileChange('/ws/decks/demo/index.html'));
    await settle();
    expect(getFile()).toHaveBeenCalledTimes(2);
    expect(lastHost().shown.at(-1)).toContain('Slide two');
  });

  it('says in one line why a file has no preview, frees its place, and tries again on request', async () => {
    getBundle().mockRejectedValueOnce(new ApiError(403, JSON.stringify({
      detail: { code: 'outside_workspace', message: 'The path is outside the workspace.' },
    })));
    await show('/tmp/report.html');
    expect(screen.getByRole('status')).toHaveTextContent('No preview: The path is outside the workspace.');
    expect(screen.queryByRole('figure')).not.toBeInTheDocument();
    expect(liveWidgets()).toBe(0);

    await userEvent.click(screen.getByRole('button', { name: 'Try again' }));
    await settle();
    expect(getBundle()).toHaveBeenCalledTimes(2);
    expect(screen.getByRole('figure', { name: 'Preview of report.html' })).toBeInTheDocument();
    expect(liveWidgets()).toBe(1);
  });

  it('starts again from a new description when the files change while it reads them', async () => {
    const stale = new ApiError(409, JSON.stringify({ detail: { code: 'stale_description', message: 'changed' } }));
    getFile().mockRejectedValueOnce(stale);
    await show();
    expect(getBundle()).toHaveBeenCalledTimes(2);
    expect(lastHost().shown[0]).toContain('Slide one');
  });

  it('says so when the files keep changing', async () => {
    const stale = new ApiError(409, JSON.stringify({ detail: { code: 'stale_description', message: 'changed' } }));
    getFile().mockRejectedValue(stale);
    await show();
    expect(getBundle()).toHaveBeenCalledTimes(MAX_RESTARTS + 1);
    expect(screen.getByRole('status')).toHaveTextContent('No preview: The files change while the preview is read.');
  });

  it('says that a build took too long, and promises no retry that the card does not make', async () => {
    getFile().mockImplementation(() => new Promise<ArrayBuffer>(() => {}));
    await show();
    await act(async () => { await vi.advanceTimersByTimeAsync(MAX_BUILD_MS + 1); });
    expect(screen.getByRole('status').textContent).toBe(
      `No preview: The preview took longer than ${MAX_BUILD_MS / 1000} s to build.Try again`,
    );
    expect(liveWidgets()).toBe(0);
  });

  it('says why when the sandbox fails, and frees its place', async () => {
    await show();
    act(() => lastHost().options.onFail('The preview sandbox does not answer.'));
    expect(screen.getByRole('status')).toHaveTextContent('No preview: The preview sandbox does not answer.');
    expect(liveWidgets()).toBe(0);
  });
});

describe('the frames of the chat', () => {
  it('loads nothing while the card is far from the screen', async () => {
    const observers: Array<(entries: Array<{ isIntersecting: boolean }>) => void> = [];
    vi.stubGlobal('IntersectionObserver', class {
      constructor(callback: (entries: Array<{ isIntersecting: boolean }>) => void) { observers.push(callback); }
      observe() {}
      unobserve() {}
      disconnect() {}
      takeRecords() { return []; }
    });
    try {
      await show();
      expect(getBundle()).not.toHaveBeenCalled();
      expect(hosts).toHaveLength(0);
      expect(liveWidgets()).toBe(0);
      expect(screen.getByRole('figure', { name: 'Preview of index.html' })).toHaveTextContent('Preview');

      act(() => observers.at(-1)!([{ isIntersecting: true }]));
      await settle();
      expect(getBundle()).toHaveBeenCalledTimes(1);
      expect(hosts).toHaveLength(1);
      expect(liveWidgets()).toBe(1);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('waits for a click when every place is held by a frame near the screen', async () => {
    for (let i = 0; i < MAX_LIVE_WIDGETS; i++) claimWidgetSlot(() => {});
    await show();
    expect(getBundle()).not.toHaveBeenCalled();
    expect(hosts).toHaveLength(0);

    await userEvent.click(screen.getByRole('button', { name: 'Load preview' }));
    await settle();
    expect(liveWidgets()).toBe(MAX_LIVE_WIDGETS);
    expect(lastHost().shown[0]).toContain('Slide one');
  });

  it('removes its frame when another frame takes the place, and offers to load it again', async () => {
    await show();
    const first = lastHost();
    // Every other place is taken; the person asks for one more frame.
    for (let i = 1; i < MAX_LIVE_WIDGETS; i++) claimWidgetSlot(() => {});
    act(() => { claimWidgetSlot(() => {}, true); });
    expect(first.disposed).toBe(true);
    expect(screen.getByRole('button', { name: 'Load preview' })).toBeInTheDocument();
  });

  it('takes no place again after a failure when it comes back near the screen, only on Try again', async () => {
    const observers: Array<(entries: Array<{ isIntersecting: boolean }>) => void> = [];
    vi.stubGlobal('IntersectionObserver', class {
      constructor(callback: (entries: Array<{ isIntersecting: boolean }>) => void) { observers.push(callback); }
      observe() {}
      unobserve() {}
      disconnect() {}
      takeRecords() { return []; }
    });
    try {
      getBundle().mockRejectedValueOnce(new ApiError(404, JSON.stringify({
        detail: { code: 'not_found', message: 'The file does not exist.' },
      })));
      await show();
      const near = (on: boolean) => act(() => observers.at(-1)!([{ isIntersecting: on }]));
      near(true);
      await settle();
      expect(screen.getByRole('status')).toHaveTextContent('No preview: The file does not exist.');
      expect(liveWidgets()).toBe(0);

      near(false);
      near(true);
      await settle();
      expect(getBundle()).toHaveBeenCalledTimes(1);
      expect(liveWidgets()).toBe(0);

      await userEvent.click(screen.getByRole('button', { name: 'Try again' }));
      await settle();
      expect(getBundle()).toHaveBeenCalledTimes(2);
      expect(liveWidgets()).toBe(1);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('keeps Load preview when its read fails after another frame took the place', async () => {
    let refuse: (e: unknown) => void = () => {};
    getBundle().mockImplementationOnce(() => new Promise<PreviewBundle>((_resolve, reject) => { refuse = reject; }));
    await show();
    for (let i = 1; i < MAX_LIVE_WIDGETS; i++) claimWidgetSlot(() => {});
    act(() => { claimWidgetSlot(() => {}, true); });
    expect(screen.getByRole('button', { name: 'Load preview' })).toBeInTheDocument();

    refuse(new ApiError(404, JSON.stringify({ detail: { code: 'not_found', message: 'gone' } })));
    await settle();
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Load preview' })).toBeInTheDocument();
  });

  it('stops the build of a place it lost, and that build changes nothing of the next place', async () => {
    let refuse: (e: unknown) => void = () => {};
    let firstSignal: AbortSignal | undefined;
    getBundle().mockImplementationOnce((_path: string, signal?: AbortSignal) => {
      firstSignal = signal;
      return new Promise<PreviewBundle>((_resolve, reject) => { refuse = reject; });
    });
    await show();
    // Another frame takes the place while the first build reads.
    for (let i = 1; i < MAX_LIVE_WIDGETS; i++) claimWidgetSlot(() => {});
    act(() => { claimWidgetSlot(() => {}, true); });
    expect(firstSignal?.aborted).toBe(true);
    // The person loads it again: a new place, a new build.
    await userEvent.click(screen.getByRole('button', { name: 'Load preview' }));
    await settle();
    expect(getBundle()).toHaveBeenCalledTimes(2);
    expect(lastHost().shown[0]).toContain('Slide one');
    const held = liveWidgets();
    // The first build ends now, with an error: nothing changes.
    refuse(new DOMException('The operation was aborted.', 'AbortError'));
    await settle();
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
    expect(screen.getByRole('figure', { name: 'Preview of index.html' })).toBeInTheDocument();
    expect(liveWidgets()).toBe(held);
    expect(lastHost().disposed).toBe(false);
  });

  it('gives its place back when the card goes', async () => {
    const { unmount } = await show();
    expect(liveWidgets()).toBe(1);
    unmount();
    expect(liveWidgets()).toBe(0);
    expect(lastHost().disposed).toBe(true);
  });
});

describe('the card of a sent file', () => {
  const sent = (filePath: string) => ({
    type: 'tool_call' as const, toolUseId: 'tu-1', tool: 'mcp__nerve__send_file', input: { file_path: filePath },
    result: JSON.stringify([{ type: 'text', text: 'Sent file' }]), status: 'complete' as const,
  });

  it('shows the preview of a page under the download card', async () => {
    render(<SendFileBlock block={sent(PATH)} />);
    await settle();
    expect(screen.getByRole('link', { name: /index\.html/ })).toHaveAttribute('download', 'index.html');
    expect(screen.getByRole('button', { name: 'Preview index.html' })).toBeInTheDocument();
    expect(screen.getByRole('figure', { name: 'Preview of index.html' })).toBeInTheDocument();
  });

  it('shows an image as an image, with no inline preview', async () => {
    render(<SendFileBlock block={sent('/ws/shots/chart.png')} />);
    await settle();
    expect(screen.getByRole('img', { name: 'chart.png' })).toBeInTheDocument();
    expect(screen.queryByRole('figure', { name: /^Preview of/ })).not.toBeInTheDocument();
    expect(getBundle()).not.toHaveBeenCalled();
  });
});
