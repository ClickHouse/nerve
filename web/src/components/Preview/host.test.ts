import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { SANDBOX_PATH } from '../Chat/mcpApps/sandbox';
import { PREVIEW_POLICY } from './document';
import { KeyboardGuard } from '../Chat/mcpApps/keyboard';
import { HELLO_MS, MAX_MESSAGES_PER_SECOND, PREVIEW_SANDBOX, PreviewHost, SANDBOX_WAIT_MS, SETTLE_MS } from './host';
import { ARTIFACT_CHANNEL, type ContentMessage } from './messages';

/**
 * The host of a preview against a fake frame: a jsdom `<iframe>` that loads
 * nothing. The test plays the sandbox proxy and the content page by sending
 * their messages to the page, and reads what the host posts to the frame.
 */

const NONE = 'null';
const PROXY_READY = 'ui/notifications/sandbox-proxy-ready';
const RESOURCE_READY = 'ui/notifications/sandbox-resource-ready';
const PRESSED = 'ui/notifications/sandbox-pressed';
const HTML = `<!DOCTYPE html><html><head><meta http-equiv="Content-Security-Policy" content="${PREVIEW_POLICY}"></head><body>deck</body></html>`;

type Posted = { message: Record<string, unknown>; target: string };

function setup() {
  const place = document.createElement('div');
  document.body.appendChild(place);
  const content: ContentMessage[] = [];
  const failures: string[] = [];
  const handOvers: number[] = [];
  const host = new PreviewHost({
    onContent: m => content.push(m), onFail: r => failures.push(r), onHandOver: () => handOvers.push(handOvers.length),
  });
  host.start(place);
  const frame = place.querySelector('iframe')!;
  const posted: Posted[] = [];
  vi.spyOn(frame.contentWindow!, 'postMessage').mockImplementation(((message: Record<string, unknown>, target: string) => {
    posted.push({ message, target });
  }) as never);
  const say = (data: unknown, from: { origin?: string; source?: MessageEventSource | null } = {}) => {
    window.dispatchEvent(new MessageEvent('message', {
      data, origin: from.origin ?? NONE, source: 'source' in from ? from.source : frame.contentWindow,
    }));
  };
  return {
    host, place, frame, posted, content, failures, say, handOvers,
    proxyReady: () => say({ jsonrpc: '2.0', method: PROXY_READY, params: {} }),
    // The proxy's answer to a question about a press.
    answer: (pressed: boolean | null = false) => say({ jsonrpc: '2.0', method: PRESSED, params: { pressed } }),
    resources: () => posted.filter(p => p.message.method === RESOURCE_READY),
    questions: () => posted.filter(p => p.message.method === PRESSED),
  };
}

beforeEach(() => vi.useFakeTimers());
afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  document.body.innerHTML = '';
});

describe('PreviewHost', () => {
  it('opens the proxy without a sandbox attribute of its own, and with no csp in the address', () => {
    const { frame } = setup();
    expect(frame.hasAttribute('sandbox')).toBe(false);
    expect(frame.getAttribute('src')).toBe(SANDBOX_PATH);
    expect(frame.getAttribute('referrerpolicy')).toBe('origin');
    expect(frame.hasAttribute('allow')).toBe(false);
    expect(frame.hasAttribute('allowfullscreen')).toBe(false);
  });

  it('hands over the document for a frame that may only run scripts', () => {
    const { host, proxyReady, resources } = setup();
    host.show(HTML);
    expect(resources()).toHaveLength(0);
    proxyReady();
    const [resource] = resources();
    expect(resource.target).toBe('*');
    const params = resource.message.params as { html: string; sandbox: string };
    expect(params.sandbox).toBe(PREVIEW_SANDBOX);
    expect(PREVIEW_SANDBOX).toBe('allow-scripts');
    expect(params.sandbox).not.toContain('allow-same-origin');
    expect(params.html).toBe(HTML);
    expect(params).not.toHaveProperty('permissions');
  });

  it('sends a new document to the same proxy', () => {
    const { host, proxyReady, resources } = setup();
    host.show(HTML);
    proxyReady();
    host.show(HTML.replace('deck', 'deck 2'));
    expect(resources()).toHaveLength(2);
    expect((resources()[1].message.params as { html: string }).html).toContain('deck 2');
  });

  it('fails when the proxy loads a second time', () => {
    const { host, proxyReady, failures } = setup();
    host.show(HTML);
    proxyReady();
    proxyReady();
    expect(failures[0]).toMatch(/loaded a second time/);
  });

  it('fails when the sandbox does not answer', () => {
    const { failures } = setup();
    vi.advanceTimersByTime(SANDBOX_WAIT_MS + 1);
    expect(failures).toEqual(['The preview sandbox does not answer.']);
  });

  it('passes on a checked content message from its frame only', () => {
    const { host, proxyReady, answer, say, content } = setup();
    host.show(HTML);
    proxyReady();
    answer();
    const ready = { channel: ARTIFACT_CHANNEL, v: 1, type: 'ready', kind: 'deck', canPrint: false, slides: 4, slide: 1 };
    // Another window, and the right window with an origin: not the proxy.
    say(ready, { source: window });
    say(ready, { origin: window.location.origin });
    expect(content).toEqual([]);
    say({ ...ready, slide: 9 });
    expect(content).toEqual([]);
    say(ready);
    expect(content).toEqual([{ type: 'ready', kind: 'deck', canPrint: false, slides: 4, slide: 1 }]);
  });

  it('takes no message of the page before a new document until the proxy answered its question', () => {
    const { host, proxyReady, answer, say, content, questions, handOvers } = setup();
    const deck = { channel: ARTIFACT_CHANNEL, v: 1, type: 'ready', kind: 'deck', canPrint: true, slides: 4, slide: 1 };
    host.show(HTML);
    proxyReady();
    expect(questions()).toHaveLength(1);
    expect(handOvers).toHaveLength(1);
    answer();
    say(deck);
    expect(content).toHaveLength(1);

    // The next document: the deck's own ready was still on its way.
    host.show(HTML.replace('deck', 'page'));
    expect(questions()).toHaveLength(2);
    expect(handOvers).toHaveLength(2);
    say(deck);
    expect(content).toHaveLength(1);
    answer();
    say({ ...deck, kind: 'table', canPrint: false, slides: undefined, slide: undefined });
    expect(content.at(-1)).toEqual({ type: 'ready', kind: 'table', canPrint: false });
  });

  it('waits for the answer to its own question, not to an earlier one of the keyboard guard', () => {
    const { host, proxyReady, answer, say, content } = setup();
    const answered = vi.spyOn(KeyboardGuard.prototype, 'answer');
    const ready = { channel: ARTIFACT_CHANNEL, v: 1, type: 'ready', kind: 'markdown', canPrint: true };
    host.show(HTML);
    proxyReady();
    answer();
    // The keyboard guard asks, then a new document goes to the proxy before the answer.
    (host as unknown as { ask: (who: string) => void }).ask('keyboard');
    host.show(HTML.replace('deck', 'page'));
    answer(true);
    expect(answered).toHaveBeenCalledTimes(1);
    expect(answered).toHaveBeenLastCalledWith(true);
    say(ready);
    expect(content).toEqual([]);
    answer();
    expect(answered).toHaveBeenCalledTimes(1);
    say(ready);
    expect(content).toEqual([{ type: 'ready', kind: 'markdown', canPrint: true }]);
  });

  it('takes content messages again after a while when the proxy does not answer', () => {
    const { host, proxyReady, say, content } = setup();
    host.show(HTML);
    proxyReady();
    const ready = { channel: ARTIFACT_CHANNEL, v: 1, type: 'ready', kind: 'markdown', canPrint: true };
    say(ready);
    expect(content).toEqual([]);
    vi.advanceTimersByTime(SETTLE_MS + 1);
    say(ready);
    expect(content).toHaveLength(1);
  });

  it('asks a page that said nothing with hello, once', () => {
    const { host, proxyReady, posted } = setup();
    host.show(HTML);
    proxyReady();
    vi.advanceTimersByTime(HELLO_MS + 1);
    const hellos = posted.filter(p => p.message.channel === ARTIFACT_CHANNEL && p.message.type === 'hello');
    expect(hellos).toHaveLength(1);
    expect(hellos[0].target).toBe('*');
  });

  it('sends host messages with the channel', () => {
    const { host, proxyReady, posted } = setup();
    host.show(HTML);
    proxyReady();
    host.send({ type: 'slide', to: 'next' });
    expect(posted.at(-1)!.message).toEqual({ channel: ARTIFACT_CHANNEL, v: 1, type: 'slide', to: 'next' });
  });

  it('stops a frame that floods the page', () => {
    const { host, proxyReady, say, failures } = setup();
    host.show(HTML);
    proxyReady();
    for (let i = 0; i <= MAX_MESSAGES_PER_SECOND; i += 1) say({ any: i });
    expect(failures[0]).toMatch(/without end/);
  });

  it('removes the frame on dispose', () => {
    const { host, place } = setup();
    host.dispose();
    expect(place.querySelector('iframe')).toBeNull();
  });
});
