import type { Plan } from '../../stores/planStore';

/** Where the implementation of an approved plan stands. */
export type ImplState = 'running' | 'parked' | 'waiting' | 'done' | 'failed';

/**
 * The state of a plan's implementation, from the plan status and the session
 * row. Null while the plan is implementing and its session row is gone: then
 * nothing tells the state.
 */
export function implementationState(plan: Plan): ImplState | null {
  const session = plan.impl_session;
  if (plan.status === 'done') return 'done';
  if (plan.status === 'failed' || session?.status === 'error') return 'failed';
  if (!session) return null;
  if (session.awaiting_input) return 'waiting';
  if (session.is_running) return 'running';
  if (session.parked) return 'parked';
  // The turn ended and the task is still open: the agent waits for the person.
  return 'waiting';
}
