import { describe, expect, it } from 'vitest';
import { MODE_LOCK_REASON, WAITS_FOR_MODE, makeState, step } from '../../test/setupFixtures';
import {
  REVIEW, firstPendingStop, nextStop, operatorSteps, ownSteps, previousStop, stepPlace, stopPosition,
} from './navigation';

const CLOSED = 'This instance does not store credentials entered here. Connections are provided by the operator of this deployment.';
const LOCK = 'Configuration is managed from the config repository on this instance; changes arrive by PR and sync.';

describe('stepPlace', () => {
  it('a pending, skipped or done step that the person can change is their own', () => {
    expect(stepPlace(step('provider'))).toBe('own');
    expect(stepPlace(step('memory', { status: 'skipped' }))).toBe('own');
    expect(stepPlace(step('profile', { status: 'done' }))).toBe('own');
    expect(stepPlace(step('claim', { status: 'done' }))).toBe('own');
  });

  it('a step that only waits for Mode is the person\'s own, unless a lockdown keeps Mode closed', () => {
    const waiting = { status: 'blocked' as const, read_only: true, blocked_reason: WAITS_FOR_MODE };
    expect(stepPlace(step('profile', { ...waiting, blocked_kind: 'sequencing' }))).toBe('own');
    expect(stepPlace(step('profile', waiting))).toBe('own');
    expect(stepPlace(step('profile', { ...waiting, blocked_kind: 'lockdown' }))).toBe('hidden');
  });

  it('a step that the policy or a lockdown closed is hidden, also from a backend that sends no kind', () => {
    expect(stepPlace(step('memory', { status: 'blocked', blocked_kind: 'policy', blocked_reason: CLOSED }))).toBe('hidden');
    expect(stepPlace(step('memory', { status: 'blocked', blocked_kind: 'lockdown', blocked_reason: LOCK }))).toBe('hidden');
    expect(stepPlace(step('memory', { status: 'blocked', blocked_reason: CLOSED }))).toBe('hidden');
  });

  it('a done, read-only step is the operator\'s, except the Mode step', () => {
    expect(stepPlace(step('channels', { status: 'done', read_only: true, read_only_reason: CLOSED }))).toBe('operator');
    expect(stepPlace(step('provider', { status: 'done', read_only: true, read_only_reason: LOCK }))).toBe('operator');
    expect(stepPlace(step('mode', { status: 'done', read_only: true, read_only_reason: MODE_LOCK_REASON }))).toBe('own');
  });
});

describe('walking the steps', () => {
  // Memory is the operator's; Channels and Agents are hidden.
  const state = makeState({
    steps: [
      step('claim', { status: 'done' }),
      step('mode', { status: 'done', read_only: true, read_only_reason: MODE_LOCK_REASON }),
      step('provider', { status: 'done' }),
      step('profile'),
      step('memory', { status: 'done', read_only: true, read_only_reason: CLOSED }),
      step('channels', { status: 'blocked', blocked_kind: 'policy', blocked_reason: CLOSED }),
      step('sources'),
      step('automation'),
      step('agents', { status: 'blocked', blocked_kind: 'policy', blocked_reason: CLOSED }),
    ],
  });

  it('splits the steps into the own ones and the operator\'s', () => {
    expect(ownSteps(state).map((s) => s.id)).toEqual(['claim', 'mode', 'provider', 'profile', 'sources', 'automation']);
    expect(operatorSteps(state).map((s) => s.id)).toEqual(['memory']);
  });

  it('Continue and Back pass over the operator\'s and the hidden steps', () => {
    expect(nextStop(state, 'profile')).toBe('sources');
    expect(nextStop(state, 'automation')).toBe(REVIEW);
    expect(previousStop(state, 'sources')).toBe('profile');
    expect(previousStop(state, REVIEW)).toBe('automation');
    // From a step that is no longer the person's, the walk goes on in the server's order.
    expect(nextStop(state, 'memory')).toBe('sources');
    expect(previousStop(state, 'memory')).toBe('profile');
  });

  it('the first pending step and the position count the own steps only', () => {
    expect(firstPendingStop(state)).toBe('profile');
    expect(stopPosition(state, 'sources')).toEqual({ index: 5, total: 6 });
    expect(stopPosition(state, 'memory')).toBeNull();
    expect(stopPosition(state, REVIEW)).toBeNull();
  });
});
