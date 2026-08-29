import { ClickUIProvider } from '@clickhouse/click-ui';
import { fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { useToastHotkeyShim } from './useToastHotkeyShim';

/**
 * Rendered against the **real** `ClickUIProvider`, not a stand-in.
 *
 * The behaviour under test belongs to a Radix listener three packages down that
 * Click UI mounts unconditionally and configures with nothing. A fake provider
 * would assert that our own hook calls `stopPropagation`, which is not in doubt;
 * what is worth pinning is that doing so is still enough to reach Radix's
 * listener after a Click UI or Radix upgrade moves it.
 *
 * The `shim={false}` case is the negative control. Without it the suite would
 * keep passing if Click UI dropped the toast viewport, if Radix changed the
 * hotkey, or if the harness never dispatched a real F8 — three ways to be green
 * while testing nothing.
 */
function Harness({ shim }: { shim: boolean }) {
  return (
    <ClickUIProvider theme="dark" persistTheme={false}>
      {shim ? <WithShim /> : null}
      <input aria-label="composer" />
    </ClickUIProvider>
  );
}

function WithShim() {
  useToastHotkeyShim();
  return null;
}

function pressF8(target: Element) {
  fireEvent.keyDown(target, { key: 'F8', code: 'F8' });
}

describe('Click UI toast hotkey', () => {
  const listeners: Array<() => void> = [];
  afterEach(() => {
    listeners.splice(0).forEach((off) => off());
  });

  function spyOnDocument() {
    const seen = vi.fn();
    document.addEventListener('keydown', seen);
    listeners.push(() => document.removeEventListener('keydown', seen));
    return seen;
  }

  it('steals focus from a text field when nothing intercepts it', () => {
    // Negative control: with no shim, F8 pulls focus into the toast viewport.
    render(<Harness shim={false} />);
    const input = screen.getByRole('textbox', { name: 'composer' });
    input.focus();
    expect(document.activeElement).toBe(input);

    pressF8(input);

    expect(document.activeElement).not.toBe(input);
    expect(document.activeElement?.tagName).toBe('OL');
  });

  it('leaves focus alone once the shim is mounted', () => {
    render(<Harness shim />);
    const input = screen.getByRole('textbox', { name: 'composer' });
    input.focus();

    pressF8(input);

    expect(document.activeElement).toBe(input);
  });

  it('swallows only F8, and only as far as `document`', () => {
    const seen = spyOnDocument();
    render(<Harness shim />);
    const input = screen.getByRole('textbox', { name: 'composer' });
    input.focus();

    fireEvent.keyDown(input, { key: 'b', code: 'KeyB' });
    fireEvent.keyDown(input, { key: 'Escape', code: 'Escape' });
    expect(seen).toHaveBeenCalledTimes(2);

    pressF8(input);
    expect(seen).toHaveBeenCalledTimes(2);
  });

  it('does not suppress the key itself', () => {
    // The browser and the platform own F8 — this is about one library's
    // listener, so the event must not come back `defaultPrevented`.
    render(<Harness shim />);
    const input = screen.getByRole('textbox', { name: 'composer' });
    const event = new KeyboardEvent('keydown', {
      key: 'F8',
      code: 'F8',
      bubbles: true,
      cancelable: true,
    });
    input.dispatchEvent(event);
    expect(event.defaultPrevented).toBe(false);
  });
});
