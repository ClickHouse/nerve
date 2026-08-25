import { act, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { ChatInput } from './ChatInput';

vi.mock('../../api/client', () => ({
  api: {
    getPromptRewriteStatus: vi.fn(async () => ({ enabled: false })),
    getModels: vi.fn(async () => ({ models: [], default: null })),
    rewritePrompt: vi.fn(),
    uploadFiles: vi.fn(),
  },
}));

/**
 * The composer row wraps on a phone, and `order` decides what lands where.
 *
 * The textarea carries `order-1` and `basis-full` so it takes a line of its own
 * *below* the controls. The run-later kebab used to carry `order-2`, which
 * sorted it after that line — so on a phone it wrapped onto a third line, alone
 * at the left margin under the message box, and its popup (right-aligned to a
 * 40px trigger sitting 16px from the left edge) opened ~114px off the left of
 * the screen. Desktop never showed it: that row is `md:flex-nowrap`, so nothing
 * wraps and `order` is inert.
 *
 * jsdom applies no CSS — `vite.config.ts` sets `test.css: false` — so this is a
 * class assertion rather than a geometry one, and it is deliberately narrow: it
 * pins the two structural facts that caused the bug. The measurements live in
 * the browser sweep, which puts the popup fully on screen at 390 / 768 / 1440.
 */
describe('the composer row on a wrapped (phone) layout', () => {
  /** `act` so the mount effects that probe rewrite/model availability settle. */
  async function renderComposer() {
    await act(async () => {
      render(<ChatInput onSend={vi.fn()} onStop={vi.fn()} isStreaming={false} />);
    });
    return {
      textarea: screen.getByPlaceholderText(/send a message/i),
      kebab: screen.getByRole('button', { name: 'More options' }),
    };
  }

  it('keeps the kebab on the controls line rather than below the message box', async () => {
    const { textarea, kebab } = await renderComposer();

    // The textarea is the one thing that gets a line of its own, and it is last.
    expect(textarea).toHaveClass('basis-full', 'order-1');

    // Anything ordered after it wraps below it. The kebab must not be.
    const wrapper = kebab.parentElement;
    const ordered = [...(wrapper?.classList ?? [])].filter((c) => /^order-/.test(c));
    expect(ordered).toEqual([]);
  });

  it('anchors the popup to the composer, not to the 40px trigger', async () => {
    const { kebab } = await renderComposer();
    const wrapper = kebab.parentElement;

    // A `relative` wrapper would make `right-0` mean "the right edge of the
    // trigger", which is only on screen when the trigger happens to be on the
    // right. The row above owns the positioning instead.
    expect(wrapper).not.toHaveClass('relative');
    expect(wrapper?.parentElement).toHaveClass('relative');
  });
});
