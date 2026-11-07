import { act, render, screen, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { MessageBlock } from '../../types/chat';
import type { PullRequestResult, PullRequestSummary } from '../../types/github';

vi.mock('../../api/client', () => ({ api: { getPullRequests: vi.fn() } }));

const { api } = await import('../../api/client');
const { PullRequestCards } = await import('./PullRequestCards');

const getPullRequests = vi.mocked(api.getPullRequests);

const URL_12 = 'https://github.com/example/repo/pull/12';
const URL_13 = 'https://github.com/example/repo/pull/13';

function pull(overrides: Partial<PullRequestSummary> = {}): PullRequestSummary {
  return {
    repo: 'example/repo', number: 12, title: 'Fix the flaky login test', state: 'open',
    review: null, checks: { passed: 0, failed: 0, running: 0 },
    head_ref: 'alice/fix-login', base_ref: 'main', ...overrides,
  };
}

const ok = (url: string, summary: PullRequestSummary): PullRequestResult => ({ url, pull: summary, error: null });

function answer(...pulls: PullRequestResult[]) {
  getPullRequests.mockResolvedValueOnce({ pulls });
}

const message = (...urls: string[]): MessageBlock[] => [
  { type: 'text', content: `Opened ${urls.join(' and ')}.` },
];

function card(name: string | RegExp) {
  return screen.getByRole('link', { name }).closest('li')!;
}

beforeEach(() => {
  getPullRequests.mockReset();
});

afterEach(() => {
  vi.useRealTimers();
});

describe('PullRequestCards', () => {
  it('renders nothing and reads nothing for a message without pull request links', () => {
    const { container } = render(<PullRequestCards blocks={[{ type: 'text', content: 'https://github.com/example/repo' }]} />);
    expect(container).toBeEmptyDOMElement();
    expect(getPullRequests).not.toHaveBeenCalled();
  });

  it('reads all pull requests of the message in one request', async () => {
    answer(ok(URL_12, pull()), ok(URL_13, pull({ number: 13, title: 'Add a retry' })));
    render(<PullRequestCards blocks={message(URL_12, URL_13)} />);
    expect(await screen.findByRole('link', { name: /Add a retry/ })).toBeInTheDocument();
    expect(getPullRequests).toHaveBeenCalledTimes(1);
    expect(getPullRequests).toHaveBeenCalledWith([URL_12, URL_13]);
    expect(within(screen.getByRole('list', { name: 'Pull requests' })).getAllByRole('listitem')).toHaveLength(2);
  });

  it('shows a draft with its reference, branches and an Open link', async () => {
    answer(ok(URL_12, pull({ state: 'draft' })));
    render(<PullRequestCards blocks={message(URL_12)} />);
    const link = await screen.findByRole('link', { name: /Fix the flaky login test/ });
    expect(link).toHaveAttribute('href', URL_12);
    expect(link).toHaveAttribute('target', '_blank');
    expect(link).toHaveAttribute('rel', expect.stringContaining('noopener'));
    const li = card(/Fix the flaky login test/);
    expect(within(li).getByText('Draft')).toBeInTheDocument();
    expect(within(li).getByText('example/repo#12 · alice/fix-login → main')).toBeInTheDocument();
  });

  it('shows the checks and the review decision of an open pull request', async () => {
    answer(ok(URL_12, pull({
      review: 'changes_requested', checks: { passed: 12, failed: 2, running: 3 },
    })));
    render(<PullRequestCards blocks={message(URL_12)} />);
    await screen.findByText('Open');
    const li = card(/Fix the flaky login test/);
    expect(li).toHaveTextContent('2 failed');
    expect(li).toHaveTextContent('3 running');
    expect(li).toHaveTextContent('12 passed');
    expect(within(li).getByText('Changes requested')).toBeInTheDocument();
  });

  it('leaves out check counts that are zero', async () => {
    answer(ok(URL_12, pull({ state: 'merged', review: 'approved', checks: { passed: 4, failed: 0, running: 0 } })));
    render(<PullRequestCards blocks={message(URL_12)} />);
    await screen.findByText('Merged');
    const li = card(/Fix the flaky login test/);
    expect(li).toHaveTextContent('4 passed');
    expect(li).not.toHaveTextContent('failed');
    expect(li).not.toHaveTextContent('running');
    expect(within(li).getByText('Approved')).toBeInTheDocument();
  });

  it('shows a closed pull request', async () => {
    answer(ok(URL_12, pull({ state: 'closed' })));
    render(<PullRequestCards blocks={message(URL_12)} />);
    expect(await screen.findByText('Closed')).toBeInTheDocument();
  });

  it.each([
    ['no_access', 'No access to details'],
    ['gh_unavailable', 'GitHub CLI not available'],
  ] as const)('falls back to the number and the link for %s', async (error, text) => {
    answer({ url: URL_12, pull: null, error });
    render(<PullRequestCards blocks={message(URL_12)} />);
    expect(await screen.findByText(text)).toBeInTheDocument();
    const link = screen.getByRole('link', { name: 'example/repo#12' });
    expect(link).toHaveAttribute('href', URL_12);
    for (const state of ['Draft', 'Open', 'Merged', 'Closed']) {
      expect(screen.queryByText(state)).not.toBeInTheDocument();
    }
  });

  it('shows the loading state, then the fallback when the request fails', async () => {
    getPullRequests.mockRejectedValueOnce(new Error('offline'));
    render(<PullRequestCards blocks={message(URL_12)} />);
    expect(screen.getByText('Loading details…')).toBeInTheDocument();
    expect(await screen.findByText('Could not load details')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'example/repo#12' })).toHaveAttribute('href', URL_12);
  });
});

describe('refresh', () => {
  const flush = () => act(async () => { await vi.advanceTimersByTimeAsync(0); });
  const wait = (ms: number) => act(async () => { await vi.advanceTimersByTimeAsync(ms); });

  beforeEach(() => {
    vi.useFakeTimers();
  });

  it('reads an open pull request with running checks again every minute until it merges', async () => {
    const running = ok(URL_12, pull({ checks: { passed: 1, failed: 0, running: 2 } }));
    answer(running);
    answer(running);
    answer(ok(URL_12, pull({ state: 'merged', checks: { passed: 3, failed: 0, running: 0 } })));
    render(<PullRequestCards blocks={message(URL_12)} />);
    await flush();
    expect(getPullRequests).toHaveBeenCalledTimes(1);
    await wait(59_000);
    expect(getPullRequests).toHaveBeenCalledTimes(1);
    await wait(1_000);
    expect(getPullRequests).toHaveBeenCalledTimes(2);
    await wait(60_000);
    expect(getPullRequests).toHaveBeenCalledTimes(3);
    expect(screen.getByText('Merged')).toBeInTheDocument();
    await wait(180_000);
    expect(getPullRequests).toHaveBeenCalledTimes(3);
  });

  it('does not read again when no check runs', async () => {
    answer(ok(URL_12, pull({ checks: { passed: 5, failed: 0, running: 0 } })));
    render(<PullRequestCards blocks={message(URL_12)} />);
    await flush();
    await wait(180_000);
    expect(getPullRequests).toHaveBeenCalledTimes(1);
  });

  it('reads again only while the tab shows', async () => {
    const running = ok(URL_12, pull({ checks: { passed: 0, failed: 0, running: 1 } }));
    getPullRequests.mockResolvedValue({ pulls: [running] });
    const hidden = vi.spyOn(document, 'hidden', 'get').mockReturnValue(false);
    const setHidden = (value: boolean) => act(() => {
      hidden.mockReturnValue(value);
      document.dispatchEvent(new Event('visibilitychange'));
    });
    render(<PullRequestCards blocks={message(URL_12)} />);
    await flush();
    expect(getPullRequests).toHaveBeenCalledTimes(1);
    setHidden(true);
    await wait(120_000);
    expect(getPullRequests).toHaveBeenCalledTimes(1);
    setHidden(false);
    await flush();
    expect(getPullRequests).toHaveBeenCalledTimes(2);
    hidden.mockRestore();
  });

  it('keeps the cards and tries again after a failed refresh', async () => {
    const running = ok(URL_12, pull({ checks: { passed: 0, failed: 0, running: 1 } }));
    answer(running);
    getPullRequests.mockRejectedValueOnce(new Error('offline'));
    answer(running);
    render(<PullRequestCards blocks={message(URL_12)} />);
    await flush();
    await wait(60_000);
    expect(getPullRequests).toHaveBeenCalledTimes(2);
    expect(screen.getByText('1 running')).toBeInTheDocument();
    await wait(60_000);
    expect(getPullRequests).toHaveBeenCalledTimes(3);
  });

  it('stops reading when the cards unmount', async () => {
    getPullRequests.mockResolvedValue({ pulls: [ok(URL_12, pull({ checks: { passed: 0, failed: 0, running: 1 } }))] });
    const { unmount } = render(<PullRequestCards blocks={message(URL_12)} />);
    await flush();
    unmount();
    await wait(180_000);
    expect(getPullRequests).toHaveBeenCalledTimes(1);
  });
});

describe('in view', () => {
  let setInView: (inView: boolean) => void;
  const flush = () => act(async () => { await vi.advanceTimersByTimeAsync(0); });
  const wait = (ms: number) => act(async () => { await vi.advanceTimersByTimeAsync(ms); });

  beforeEach(() => {
    vi.useFakeTimers();
    vi.stubGlobal('IntersectionObserver', class {
      constructor(callback: IntersectionObserverCallback) {
        setInView = (inView) => act(() => callback(
          [{ isIntersecting: inView } as IntersectionObserverEntry],
          this as unknown as IntersectionObserver,
        ));
      }
      observe() {}
      disconnect() {}
    });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('reads nothing until the cards come into view', async () => {
    getPullRequests.mockResolvedValue({ pulls: [ok(URL_12, pull())] });
    render(<PullRequestCards blocks={message(URL_12)} />);
    await wait(180_000);
    expect(getPullRequests).not.toHaveBeenCalled();
    expect(screen.getByText('Loading details…')).toBeInTheDocument();
    setInView(true);
    await flush();
    expect(getPullRequests).toHaveBeenCalledTimes(1);
    expect(screen.getByText('Open')).toBeInTheDocument();
  });

  it('refreshes only in view, and not within a minute of the last read', async () => {
    getPullRequests.mockResolvedValue({ pulls: [ok(URL_12, pull({ checks: { passed: 0, failed: 0, running: 1 } }))] });
    render(<PullRequestCards blocks={message(URL_12)} />);
    setInView(true);
    await flush();
    expect(getPullRequests).toHaveBeenCalledTimes(1);
    await wait(30_000);
    setInView(false);
    await wait(180_000);
    expect(getPullRequests).toHaveBeenCalledTimes(1);
    setInView(true);
    await flush();
    expect(getPullRequests).toHaveBeenCalledTimes(2);
    await wait(20_000);
    setInView(false);
    setInView(true);
    await flush();
    expect(getPullRequests).toHaveBeenCalledTimes(2);
    await wait(40_000);
    expect(getPullRequests).toHaveBeenCalledTimes(3);
  });
});

describe('chat end', () => {
  let grow: (height: number) => void;

  beforeEach(() => {
    vi.stubGlobal('ResizeObserver', class {
      constructor(callback: ResizeObserverCallback) {
        grow = (height) => callback([{ contentRect: { height } } as ResizeObserverEntry], this as unknown as ResizeObserver);
      }
      observe() {}
      disconnect() {}
    });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  function renderInScroller() {
    getPullRequests.mockReturnValue(new Promise(() => {}));
    render(
      <div data-testid="scroller" style={{ overflowY: 'auto' }}>
        <PullRequestCards blocks={message(URL_12)} />
      </div>,
    );
    const scroller = screen.getByTestId('scroller');
    let top = 0;
    let height = 1000;
    Object.defineProperties(scroller, {
      clientHeight: { value: 600 },
      scrollHeight: { get: () => height },
      scrollTop: { get: () => top, set: (value: number) => { top = Math.min(value, height - 600); } },
    });
    return {
      scroller,
      setTop: (value: number) => { top = value; },
      setHeight: (value: number) => { height = value; },
    };
  }

  it('keeps the end of the chat in view when the cards grow', () => {
    const { scroller, setTop, setHeight } = renderInScroller();
    grow(100);
    setTop(400);
    setHeight(1100);
    grow(200);
    expect(scroller.scrollTop).toBe(500);
  });

  it('leaves the scroll position alone when the end is out of view', () => {
    const { scroller, setTop, setHeight } = renderInScroller();
    grow(100);
    setTop(150);
    setHeight(1100);
    grow(200);
    expect(scroller.scrollTop).toBe(150);
  });
});
