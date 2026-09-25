import { useState } from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import { Modal } from './Modal';

/**
 * The Modal is almost entirely interaction, which is precisely the part
 * `tsc` and `vite build` cannot speak to. These specs pin the behaviours
 * that were the *reason* for extracting the component — Escape ordering
 * against the app's global handlers, the focus trap, the stacking rules,
 * and the mousedown-not-click backdrop — rather than its markup.
 */

/**
 * The backdrop, found by its own class rather than by its position relative to
 * the panel.
 *
 * `getByRole('dialog').parentElement` only holds while the backdrop is the
 * panel's *direct* parent. Insert any wrapper between them (a transition
 * container, a portal's own node) and that lookup silently retargets at the
 * wrapper: the clicks still land on something, `onClose` still isn't called,
 * and the specs pass without testing the backdrop at all.
 */
function backdrop(): HTMLElement {
  const el = document.querySelector<HTMLElement>('.modal-backdrop');
  if (!el) throw new Error('no .modal-backdrop in the document');
  return el;
}

function Basic({
  onClose = () => {},
  ...props
}: Partial<React.ComponentProps<typeof Modal>>) {
  return (
    <Modal open onClose={onClose} title="Test dialog" {...props}>
      <button>first</button>
      <button>second</button>
    </Modal>
  );
}

describe('Modal rendering', () => {
  it('renders nothing when closed', () => {
    render(<Basic open={false} />);
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('portals to document.body rather than the parent node', () => {
    const { container } = render(
      <div data-testid="host">
        <Basic />
      </div>,
    );
    // The host subtree stays empty — that is what makes a dialog immune to
    // an ancestor's overflow/transform/stacking context.
    expect(container.querySelector('[role="dialog"]')).toBeNull();
    expect(document.body).toContainElement(screen.getByRole('dialog'));
  });

  it('exposes dialog semantics with an accessible name', () => {
    render(<Basic />);
    const dialog = screen.getByRole('dialog');
    expect(dialog).toHaveAttribute('aria-modal', 'true');
    expect(dialog).toHaveAccessibleName('Test dialog');
  });

  it('falls back to ariaLabel when there is no visible title', () => {
    render(<Basic title={undefined} ariaLabel="Bare panel" />);
    expect(screen.getByRole('dialog')).toHaveAccessibleName('Bare panel');
  });
});

describe('Modal dismissal', () => {
  it('closes on Escape', async () => {
    const onClose = vi.fn();
    render(<Basic onClose={onClose} />);

    await userEvent.keyboard('{Escape}');

    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('stops Escape from reaching the app-level handlers', async () => {
    // The regression this guards: App.tsx / ChatPage.tsx bind Escape on
    // document to clear search and stop generation. A bubble-phase
    // listener in the Modal would fire *after* them, so dismissing a
    // dialog would also stop a generation running behind it.
    const appHandler = vi.fn();
    document.addEventListener('keydown', appHandler);
    try {
      render(<Basic />);
      await userEvent.keyboard('{Escape}');
      expect(appHandler).not.toHaveBeenCalled();
    } finally {
      document.removeEventListener('keydown', appHandler);
    }
  });

  it('closes on a backdrop click', async () => {
    const onClose = vi.fn();
    render(<Basic onClose={onClose} />);

    await userEvent.click(backdrop());

    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('ignores clicks inside the panel', async () => {
    const onClose = vi.fn();
    render(<Basic onClose={onClose} />);

    await userEvent.click(screen.getByText('first'));

    expect(onClose).not.toHaveBeenCalled();
  });

  it('survives a drag that starts inside and is released on the backdrop', async () => {
    // Selecting text in a textarea and releasing outside the panel fires
    // `click` on the backdrop. Closing on click would bin the dialog
    // mid-interaction; closing on mousedown does not.
    const onClose = vi.fn();
    render(<Basic onClose={onClose} />);

    await userEvent.pointer([
      { target: screen.getByText('first'), keys: '[MouseLeft>]' },
      { target: backdrop(), keys: '[/MouseLeft]' },
    ]);

    expect(onClose).not.toHaveBeenCalled();
  });

  it('does not close on the backdrop when closeOnBackdrop is false', async () => {
    const onClose = vi.fn();
    render(<Basic onClose={onClose} closeOnBackdrop={false} />);

    await userEvent.click(backdrop());

    expect(onClose).not.toHaveBeenCalled();
    // Escape still works — the guard is about stray clicks, not lock-in.
    await userEvent.keyboard('{Escape}');
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('closes from the header button', async () => {
    const onClose = vi.fn();
    render(<Basic onClose={onClose} />);

    await userEvent.click(screen.getByRole('button', { name: 'Close dialog' }));

    expect(onClose).toHaveBeenCalledTimes(1);
  });
});

describe('Modal focus management', () => {
  it('moves focus into the panel on open', async () => {
    render(<Basic />);
    await waitFor(() =>
      expect(screen.getByRole('dialog')).toContainElement(
        document.activeElement as HTMLElement,
      ),
    );
  });

  it('respects a child that claims focus itself', async () => {
    render(
      <Modal open onClose={() => {}} title="Autofocus">
        <input autoFocus aria-label="claims focus" />
        <button>other</button>
      </Modal>,
    );
    await waitFor(() =>
      expect(screen.getByLabelText('claims focus')).toHaveFocus(),
    );
  });

  it('wraps Tab at the end of the panel', async () => {
    render(<Basic />);
    const close = screen.getByRole('button', { name: 'Close dialog' });
    const last = screen.getByText('second');

    last.focus();
    await userEvent.tab();

    expect(close).toHaveFocus();
  });

  it('wraps Shift+Tab at the start of the panel', async () => {
    render(<Basic />);
    const close = screen.getByRole('button', { name: 'Close dialog' });
    const last = screen.getByText('second');

    close.focus();
    await userEvent.tab({ shift: true });

    expect(last).toHaveFocus();
  });

  it('restores focus to the trigger on close', async () => {
    function Harness() {
      const [open, setOpen] = useState(false);
      return (
        <>
          <button onClick={() => setOpen(true)}>open me</button>
          <Modal open={open} onClose={() => setOpen(false)} title="Dialog">
            <button>inside</button>
          </Modal>
        </>
      );
    }
    render(<Harness />);
    const trigger = screen.getByText('open me');

    await userEvent.click(trigger);
    await waitFor(() => expect(screen.getByRole('dialog')).toBeInTheDocument());
    await userEvent.keyboard('{Escape}');

    // Without this, dismissing a dialog drops focus to <body> and keyboard
    // navigation restarts from the top of the document.
    await waitFor(() => expect(trigger).toHaveFocus());
  });
});

describe('Stacked modals', () => {
  function Stack() {
    const [outer, setOuter] = useState(true);
    const [inner, setInner] = useState(true);
    return (
      <>
        <Modal open={outer} onClose={() => setOuter(false)} title="Outer">
          <button>outer body</button>
        </Modal>
        <Modal open={inner} onClose={() => setInner(false)} title="Inner">
          <button>inner body</button>
        </Modal>
      </>
    );
  }

  it('Escape closes only the topmost dialog', async () => {
    render(<Stack />);
    expect(screen.getAllByRole('dialog')).toHaveLength(2);

    await userEvent.keyboard('{Escape}');

    // Both dialogs have a capture-phase listener; without the stack, one
    // keypress would close a confirmation and its parent together.
    await waitFor(() => expect(screen.getAllByRole('dialog')).toHaveLength(1));
    expect(screen.getByRole('dialog')).toHaveAccessibleName('Outer');
  });

  it('keeps body scroll locked until the last dialog closes', async () => {
    render(<Stack />);
    expect(document.body.style.overflow).toBe('hidden');

    await userEvent.keyboard('{Escape}');
    await waitFor(() => expect(screen.getAllByRole('dialog')).toHaveLength(1));

    expect(document.body.style.overflow).toBe('hidden');

    await userEvent.keyboard('{Escape}');
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());

    expect(document.body.style.overflow).not.toBe('hidden');
  });
});

describe('Modal body scroll lock', () => {
  it('locks on open and restores on close', async () => {
    function Harness() {
      const [open, setOpen] = useState(true);
      return (
        <Modal open={open} onClose={() => setOpen(false)} title="Dialog">
          <button>inside</button>
        </Modal>
      );
    }
    render(<Harness />);
    expect(document.body.style.overflow).toBe('hidden');

    await userEvent.keyboard('{Escape}');

    await waitFor(() => expect(document.body.style.overflow).not.toBe('hidden'));
  });
});
