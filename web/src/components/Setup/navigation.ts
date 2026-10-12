import type { SetupState, SetupStep, SetupStepId } from '../../api/client';

/** The route segment of the review screen; a frontend route, not a server step. */
export const REVIEW = 'review';

export type WizardStop = SetupStepId | typeof REVIEW;

/** The ordered stops: the server's steps, then Review. */
export function wizardStops(state: SetupState): WizardStop[] {
  return [...state.steps.map((s) => s.id), REVIEW];
}

export function stepOf(state: SetupState, id: string): SetupStep | undefined {
  return state.steps.find((s) => s.id === id);
}

/**
 * Display titles that differ from the wire `title`. The wire says "Claim"
 * (the action); once a person is signed in, the rail, the Overview, the
 * Review and the step page all call it what it now is: their account. One
 * table, so the four surfaces never disagree.
 */
const DISPLAY_TITLES: Partial<Record<SetupStepId, string>> = { claim: 'Account' };

export function stepTitle(step: Pick<SetupStep, 'id' | 'title'>): string {
  return DISPLAY_TITLES[step.id] ?? step.title;
}

/**
 * Whether Back/Continue pass through a step. Blocked steps are skipped, and
 * so is the claim once it is done: it is a read-only card, and Back from
 * Mode — the first step a local first-time user sees — should land on the
 * Overview, exactly as it does in external mode. The rail still lists it.
 */
function passesThrough(step: SetupStep): boolean {
  if (step.status === 'blocked') return false;
  if (step.id === 'claim' && step.status === 'done') return false;
  return true;
}

/** The next stop after `id` that Continue passes through; Review when none is left. */
export function nextStop(state: SetupState, id: WizardStop): WizardStop {
  const stops = wizardStops(state);
  const from = stops.indexOf(id);
  for (let i = from + 1; i < stops.length; i++) {
    const stop = stops[i];
    if (stop === REVIEW) return REVIEW;
    const step = stepOf(state, stop);
    if (step && passesThrough(step)) return stop;
  }
  return REVIEW;
}

/** The previous stop before `id` that Back passes through; `null` means the Overview. */
export function previousStop(state: SetupState, id: WizardStop): WizardStop | null {
  const stops = wizardStops(state);
  const from = stops.indexOf(id);
  for (let i = from - 1; i >= 0; i--) {
    const stop = stops[i];
    const step = stepOf(state, stop);
    if (step && passesThrough(step)) return stop;
  }
  return null;
}

/** Where "Continue" on the Overview goes: the first pending step, else Review. */
export function firstPendingStop(state: SetupState): WizardStop {
  const pending = state.steps.find((s) => s.status === 'pending');
  return pending ? pending.id : REVIEW;
}

/**
 * One-based position of a *step*, for "Step 3 of 9". Review is not a step
 * (it is a frontend route), so it has no position and the Overview's
 * "N of M steps done" counts the same M.
 */
export function stopPosition(state: SetupState, id: WizardStop): { index: number; total: number } | null {
  const index = state.steps.findIndex((s) => s.id === id);
  return index < 0 ? null : { index: index + 1, total: state.steps.length };
}

/** The prefix the server uses for "this step waits on the Mode step". */
const SEQUENCING_PREFIX = 'Choose a mode first';

/**
 * A step that is `blocked` only because Mode has not been chosen yet. That
 * is sequencing, not a permissions problem, and the UI says "After Mode"
 * rather than "Blocked" with a lock.
 */
export function waitsForMode(step: Pick<SetupStep, 'status' | 'blocked_reason'>): boolean {
  return step.status === 'blocked' && (step.blocked_reason ?? '').startsWith(SEQUENCING_PREFIX);
}

export function providerDone(state: SetupState): boolean {
  return stepOf(state, 'provider')?.status === 'done';
}

/** The route of a stop. */
export function stopPath(id: WizardStop): string {
  return `/setup/${id}`;
}
