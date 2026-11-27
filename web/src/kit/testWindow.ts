import { vi } from 'vitest';

/**
 * A window for the specs of the kit: a page in a frame, with a parent that
 * records what the page posts, and a way to deliver a message from a source.
 */
export interface TestWindow {
  win: Window;
  posted: Array<{ message: Record<string, unknown>; target: string }>;
  print: ReturnType<typeof vi.fn>;
  /** Deliver `data` as a message from `source` (default: the parent). */
  say(data: unknown, source?: unknown): void;
  /** The messages of the channel that the page posted, by type. */
  of(type: string): Array<Record<string, unknown>>;
}

export function testWindow(size = { width: 1328, height: 768 }): TestWindow {
  const posted: TestWindow['posted'] = [];
  const parent = { postMessage: vi.fn((message: Record<string, unknown>, target: string) => posted.push({ message, target })) };
  const print = vi.fn();
  const target = new EventTarget();
  const win = Object.assign(target, { parent, innerWidth: size.width, innerHeight: size.height, print }) as unknown as Window;
  return {
    win,
    posted,
    print,
    say(data, source = parent) {
      const event = new Event('message');
      Object.defineProperty(event, 'data', { value: data });
      Object.defineProperty(event, 'source', { value: source });
      target.dispatchEvent(event);
    },
    of: type => posted.map(p => p.message).filter(m => m.type === type),
  };
}
