import { useEffect, useRef, useState, type FormEvent, type KeyboardEvent, type ReactNode } from 'react';
import { Link } from 'react-router-dom';
import type { SetupStep } from '../../api/client';
import { Button } from '../ui';
import { ArrowLeft, ArrowRight } from '../ui/icons';
import { useSetupStore } from '../../stores/setupStore';
import { providerDone } from './navigation';
import { Notice } from './Notice';
import { useWizardNav } from './wizardContext';

/**
 * The frame every step renders in: a `<form aria-label>` with the heading,
 * the one-sentence intro, the step's own fields, the error line and the
 * footer (Back · Skip/Undo skip · Continue|Save|Next · Finish now).
 *
 * Continue is the form's submit, so Enter in a single-line field submits.
 * On a `blocked` or read-only step it reads **Next** and sends nothing. A
 * step opened to be edited (`nav.returnTo`) reads **Save** and returns to
 * where it was opened from. Back with unsaved edits asks first, inline, as
 * an `alertdialog`: focus moves to **Keep editing** when it appears, Escape
 * means the same, and focus returns to Back when it closes.
 *
 * Focus is never dropped on `<body>`: the buttons stay enabled while a save
 * is in flight (`aria-busy`, handlers inert) rather than `disabled`, and
 * when a save is refused focus moves to the first control marked
 * `aria-invalid` — or to the error text — so a keyboard user is on the
 * problem and a screen reader hears it.
 */
export function StepFrame({
  step, heading, intro, onSubmit, dirty = false, readOnly = false, canContinue = true,
  continueLabel, children, aside, hideSkip = false,
}: {
  step: SetupStep;
  heading: string;
  intro: string;
  /** Save. Return `false` to stay on the step (a client-side refusal). */
  onSubmit: () => Promise<boolean | void> | boolean | void;
  dirty?: boolean;
  readOnly?: boolean;
  canContinue?: boolean;
  continueLabel?: string;
  children: ReactNode;
  /** Rendered between the fields and the footer (e.g. a pairing widget). */
  aside?: ReactNode;
  /** The primary button already performs the skip; do not offer it twice. */
  hideSkip?: boolean;
}) {
  const nav = useWizardNav();
  const busy = useSetupStore((s) => s.busy);
  const error = useSetupStore((s) => s.error);
  const skip = useSetupStore((s) => s.skip);
  const state = useSetupStore((s) => s.state);
  const [confirmLeave, setConfirmLeave] = useState(false);
  const [refusals, setRefusals] = useState(0);
  const form = useRef<HTMLFormElement>(null);
  const backButton = useRef<HTMLButtonElement>(null);
  const keepEditing = useRef<HTMLButtonElement>(null);

  // The dialog appears inline, so nothing moves focus to it by itself; a
  // screen reader would not announce it and a keyboard user would have to
  // find it. Keep editing is the safe answer, so it takes focus.
  useEffect(() => {
    if (confirmLeave) keepEditing.current?.focus();
  }, [confirmLeave]);

  // A refused submit: the field error has rendered by the time this runs
  // (the refusal and the counter land in the same commit), so the first
  // invalid control is there to focus. Without one, the error text itself.
  useEffect(() => {
    if (refusals === 0) return;
    const target = form.current?.querySelector<HTMLElement>('[aria-invalid="true"]')
      ?? form.current?.querySelector<HTMLElement>('[role="alert"]');
    target?.focus();
  }, [refusals]);

  const stay = () => {
    setConfirmLeave(false);
    backButton.current?.focus();
  };
  const onDialogKeyDown = (e: KeyboardEvent<HTMLDivElement>) => {
    if (e.key !== 'Escape') return;
    e.preventDefault();
    e.stopPropagation();
    stay();
  };

  const locked = readOnly || step.status === 'blocked';
  const saving = busy === step.id;
  const anyBusy = busy !== null;
  const editing = nav.returnTo !== null;
  // After the provider is done the footer offers the short way out; not
  // when Save already returns to the Review.
  const showFinishNow = state !== null && providerDone(state) && nav.returnTo !== '/setup/review';

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    if (anyBusy) return;
    if (locked) { nav.advance(step.id); return; }
    const ok = await onSubmit();
    if (ok === false) { setRefusals((n) => n + 1); return; }
    nav.advance(step.id);
  };

  const goBack = () => {
    if (anyBusy) return;
    if (dirty && !locked) { setConfirmLeave(true); return; }
    nav.back(step.id);
  };

  const skipNow = async () => {
    if (anyBusy) return;
    if (await skip(step.id, true)) nav.advance(step.id);
  };

  const putBack = () => {
    if (anyBusy) return;
    void skip(step.id, false);
  };

  return (
    <form ref={form} onSubmit={(e) => void submit(e)} aria-label={heading} className="flex flex-col min-h-full">
      {/* The bottom padding is the sticky footer's height (44 px targets plus
          its own padding), so the last field's help text is never under it
          at the end of the scroll; the scroller's `scroll-pb` matches it so
          a field tabbed into is scrolled clear of the footer too. */}
      <div className="flex-1 flex flex-col gap-4 pb-20">
        <div>
          <h1 id="setup-step-heading" tabIndex={-1} className="text-xl font-semibold text-text outline-none">
            {heading}
          </h1>
          <p className="text-sm text-text-muted mt-1">{intro}</p>
        </div>

        {step.status === 'blocked' && step.blocked_reason && (
          <Notice tone="locked">{step.blocked_reason}</Notice>
        )}
        {step.status !== 'blocked' && readOnly && step.read_only_reason && (
          <Notice tone="locked">{step.read_only_reason}</Notice>
        )}

        {children}

        {aside}

        {error && <p role="alert" tabIndex={-1} className="text-error text-sm outline-none">{error}</p>}

        {confirmLeave && (
          <div
            role="alertdialog"
            aria-label="Leave without saving?"
            aria-describedby="setup-leave-text"
            onKeyDown={onDialogKeyDown}
            className="flex flex-wrap items-center gap-3 rounded-lg border border-border bg-surface-raised p-3"
          >
            <p id="setup-leave-text" className="text-sm text-text flex-1 min-w-[12rem]">
              Leave without saving? Your changes on this step will be lost.
            </p>
            <Button ref={keepEditing} variant="ghost" size="sm" onClick={stay} className="min-h-[44px] md:min-h-0">
              Keep editing
            </Button>
            <Button
              variant="danger"
              size="sm"
              onClick={() => { setConfirmLeave(false); nav.back(step.id); }}
              className="min-h-[44px] md:min-h-0"
            >
              Discard changes
            </Button>
          </div>
        )}
      </div>

      {/* Sticky inside the scroller, never `fixed`: it must not collide with
          the phone's bottom bar. Every target is at least 44 px tall. The
          buttons are not `disabled` while a write is in flight — a disabled
          control drops focus to <body> — they are inert and `aria-busy`. */}
      <div
        className="sticky bottom-0 -mx-4 lg:-mx-6 px-4 lg:px-6 py-3 bg-bg border-t border-border-subtle
        flex flex-wrap items-center gap-2"
        aria-busy={anyBusy || undefined}
      >
        <Button ref={backButton} variant="ghost" size="md" onClick={goBack} className="min-h-[44px]">
          <ArrowLeft size={14} />Back
        </Button>
        <span className="flex-1" />
        {showFinishNow && (
          <Link to="/setup/review" className="text-sm text-text-muted hover:text-text underline-offset-2 hover:underline px-2 min-h-[44px] inline-flex items-center">
            Finish now
          </Link>
        )}
        {!locked && step.can_skip && step.status !== 'skipped' && !hideSkip && (
          <Button variant="ghost" size="md" onClick={() => void skipNow()} className="min-h-[44px]">
            Skip for now
          </Button>
        )}
        {!locked && step.can_skip && step.status === 'skipped' && (
          <Button variant="ghost" size="md" onClick={putBack} className="min-h-[44px]">
            Undo skip
          </Button>
        )}
        <Button
          type="submit"
          variant="primary"
          size="md"
          disabled={!locked && !canContinue}
          aria-busy={saving || undefined}
          className="min-h-[44px]"
        >
          {saving ? 'Saving…' : locked ? 'Next' : (continueLabel ?? (editing ? 'Save' : 'Continue'))}
          {!saving && <ArrowRight size={14} />}
        </Button>
      </div>
    </form>
  );
}
