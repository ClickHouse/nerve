import { describe, expect, it } from 'vitest';
import {
  FX_CHILD, FX_CODEX, FX_COMPLETED, FX_FAILED, FX_INTERRUPTED_RESTART, FX_LOST, FX_NOW_MS, FX_OWNER_ACTOR,
  FX_RUNNING, FX_SHELL, FX_STARTING, FX_STOPPED_BY_USER, FX_STOPPING_UNCONFIRMED, FX_WORKFLOW, makeAgentView,
} from '../../../test/agentFixtures';
import {
  agentAriaLabel, agentDetailText, agentElapsedMs, canRelaunch, endReasonText, followUpText, formatCost,
  formatElapsed, formatElapsedLong, formatTokens, formatTokensLong, identityVisual, kindVisual, relaunchText,
  statusVisual, typeLabel,
} from './agentVisuals';

const QUIET = 10;

describe('statusVisual', () => {
  it('maps every status to its icon colour', () => {
    expect(statusVisual('starting', null)).toMatchObject({ className: 'text-text-faint', label: 'starting' });
    expect(statusVisual('running', null)).toMatchObject({ className: 'motion-safe:animate-spin text-accent', label: 'running' });
    expect(statusVisual('running', 'quiet')).toMatchObject({ className: 'text-warning', label: 'quiet' });
    expect(statusVisual('stopping', null)).toMatchObject({ className: 'motion-safe:animate-spin text-text-muted' });
    expect(statusVisual('stopping', 'stop_unconfirmed').label).toBe('stop not confirmed');
    expect(statusVisual('completed', null)).toMatchObject({ className: 'text-success', label: 'completed' });
    expect(statusVisual('failed', 'failed')).toMatchObject({ className: 'text-error', label: 'failed' });
    expect(statusVisual('stopped', null)).toMatchObject({ className: 'text-text-muted', label: 'stopped' });
    expect(statusVisual('interrupted', 'interrupted')).toMatchObject({ className: 'text-warning' });
    expect(statusVisual('lost', 'lost')).toMatchObject({ className: 'text-warning', label: 'lost' });
    expect(statusVisual('running', 'needs_input')).toMatchObject({ className: 'text-info', label: 'needs input' });
  });

  it('keeps every animation behind motion-safe', () => {
    for (const status of ['running', 'stopping'] as const) {
      expect(statusVisual(status, null).className).not.toMatch(/(^|\s)animate-/);
    }
  });
});

describe('identity', () => {
  it('uses the existing identity hues', () => {
    expect(kindVisual('agent', 'general-purpose').className).toBe('text-link');
    expect(kindVisual('agent', 'Explore').className).toBe('text-hue-cyan');
    expect(kindVisual('agent', 'Plan').className).toBe('text-hue-amber');
    expect(kindVisual('workflow', null).className).toBe('text-hue-violet');
    expect(kindVisual('shell', null).className).toBe('text-hue-emerald');
    expect(kindVisual('monitor', null).className).toBe('text-hue-sky');
    expect(kindVisual('teammate', null).className).toBe('text-hue-purple');
    expect(kindVisual('other', null).className).toBe('text-text-muted');
    expect(identityVisual('files').className).toBe('text-hue-teal');
    expect(identityVisual('something-else')).toEqual(identityVisual(null));
  });

  it('labels the type badge', () => {
    expect(typeLabel(FX_RUNNING)).toBe('general');
    expect(typeLabel(FX_FAILED)).toBe('Explore');
    expect(typeLabel(FX_SHELL)).toBe('shell');
    expect(typeLabel(FX_WORKFLOW)).toBe('workflow');
    expect(typeLabel(FX_CODEX)).toBe('worker');
  });
});

describe('formatting', () => {
  it('formats elapsed time compactly and for screen readers', () => {
    expect(formatElapsed(45_000)).toBe('45s');
    expect(formatElapsed(12 * 60_000 + 5_000)).toBe('12m');
    expect(formatElapsed(65 * 60_000)).toBe('1h 5m');
    expect(formatElapsed(null)).toBe('');
    expect(formatElapsedLong(1000)).toBe('1 second');
    expect(formatElapsedLong(4 * 60_000)).toBe('4 minutes');
    expect(formatElapsedLong(65 * 60_000)).toBe('1 hour 5 minutes');
  });

  it('formats tokens as 38k and 1.2M', () => {
    expect(formatTokens(512)).toBe('512');
    expect(formatTokens(1500)).toBe('1.5k');
    expect(formatTokens(38211)).toBe('38k');
    expect(formatTokens(999_600)).toBe('1M');
    expect(formatTokens(1_234_567)).toBe('1.2M');
    expect(formatTokensLong(38211)).toBe('38 thousand tokens');
    expect(formatTokensLong(1_234_567)).toBe('1.2 million tokens');
  });

  it('formats cost as a live estimate', () => {
    expect(formatCost(1.42)).toBe('~$1.42');
    expect(formatCost(0)).toBe('');
    expect(formatCost(0.004)).toBe('<$0.01');
  });
});

describe('copy', () => {
  it('measures elapsed from the run start, to now or to the end', () => {
    expect(agentElapsedMs(FX_RUNNING, FX_NOW_MS)).toBe(FX_NOW_MS - Date.parse('2026-02-27T02:01:01Z'));
    expect(agentElapsedMs(FX_COMPLETED, FX_NOW_MS)).toBe(Date.parse('2026-02-27T02:08:00Z') - Date.parse('2026-02-27T02:01:31Z'));
    expect(agentElapsedMs(FX_STARTING, FX_NOW_MS)).toBeNull();
  });

  it('describes every state with its copy', () => {
    expect(agentDetailText(FX_STARTING, FX_NOW_MS, QUIET)).toBe('Starting…');
    expect(agentDetailText(FX_RUNNING, FX_NOW_MS, QUIET)).toBe('Running the API test suite');
    expect(agentDetailText(FX_STOPPING_UNCONFIRMED, FX_NOW_MS, QUIET)).toBe('Stop not confirmed');
    expect(agentDetailText({ ...FX_STOPPING_UNCONFIRMED, attention: null }, FX_NOW_MS, QUIET)).toBe('Stopping…');
    expect(agentDetailText(FX_COMPLETED, FX_NOW_MS, QUIET)).toBe('4 files changed');
    expect(agentDetailText(FX_FAILED, FX_NOW_MS, QUIET)).toBe('Tests failed: 3 errors');
    expect(agentDetailText(FX_STOPPED_BY_USER, FX_NOW_MS, QUIET)).toMatch(/^Stopped by you · \d{2}:\d{2}$/);
    expect(agentDetailText(FX_INTERRUPTED_RESTART, FX_NOW_MS, QUIET)).toMatch(/^Interrupted: gateway restart · \d{2}:\d{2}$/);
    expect(agentDetailText(FX_LOST, FX_NOW_MS, QUIET)).toBe('Lost track: no final status from the runtime');
    expect(agentDetailText({ ...FX_RUNNING, attention: 'needs_input' }, FX_NOW_MS, QUIET)).toBe('Waiting for your answer');
  });

  it('says how long a quiet agent has been silent', () => {
    const quiet = makeAgentView({ last_activity_at: '2026-02-27T01:58:00Z', run_started_at: '2026-02-27T01:50:00Z' });
    expect(agentDetailText(quiet, FX_NOW_MS, QUIET)).toBe('Quiet 12m · Bash: pytest -q');
  });

  it('never says done for an agent that did not complete', () => {
    for (const reason of ['user', 'stop_all', 'turn_stop', 'orchestrator', 'unknown_stop'] as const) {
      const text = endReasonText({ ...FX_STOPPED_BY_USER, end_reason: reason });
      expect(text).toMatch(/^Stopped/);
      expect(text.toLowerCase()).not.toContain('done');
    }
    for (const reason of ['gateway_restart', 'process_ended', 'force_stop', 'model_switch', 'archived'] as const) {
      expect(endReasonText({ ...FX_INTERRUPTED_RESTART, end_reason: reason })).toMatch(/^Interrupted/);
    }
    expect(endReasonText({ ...FX_STOPPED_BY_USER, end_reason: 'turn_stop' })).toBe('Stopped with the turn');
  });

  it('tells a stop by someone else from the viewer\'s own', () => {
    expect(endReasonText(FX_STOPPED_BY_USER, FX_OWNER_ACTOR)).toMatch(/^Stopped by you/);
    expect(endReasonText(FX_STOPPED_BY_USER, 'act_other')).toMatch(/^Stopped by someone else/);
  });

  it('builds the row\'s accessible name', () => {
    const running = { ...FX_RUNNING, run_started_at: '2026-02-27T02:06:00Z' };
    expect(agentAriaLabel(running, FX_NOW_MS, QUIET))
      .toBe('build-api, running 4 minutes, 38 thousand tokens, Running the API test suite');
    expect(agentAriaLabel(FX_CHILD, FX_NOW_MS, QUIET)).toContain('build-ui-tests, running');
  });

  it('builds the follow-up and relaunch prompts', () => {
    expect(followUpText(FX_RUNNING)).toBe('Use SendMessage to tell `build-api`: ');
    expect(relaunchText(FX_FAILED)).toBe(
      'Relaunch the agent `review-api` (id ag_0000000000a5) with its original brief. Partial work may remain in its working directory.',
    );
    expect(canRelaunch(FX_FAILED)).toBe(true);
    expect(canRelaunch(FX_COMPLETED)).toBe(false);
    expect(canRelaunch(FX_RUNNING)).toBe(false);
  });
});
