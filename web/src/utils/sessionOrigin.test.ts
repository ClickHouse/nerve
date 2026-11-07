import { describe, expect, it } from 'vitest';
import {
  Bot, Clock, GitBranch, Hammer, Mail, MessageSquare, Plug, Send, Slack, Workflow,
} from '../components/ui/icons';
import type { SessionTrigger } from '../types/chat';
import {
  FORK_ICON, ORIGIN_ICONS, originIcon, originLink, showsOrigin, startedByPerson,
} from './sessionOrigin';

describe('origin icons', () => {
  it('has one icon for each trigger', () => {
    const expected: Record<SessionTrigger, unknown> = {
      chat: MessageSquare, api: Plug, slack: Slack, telegram: Send, email: Mail,
      cron: Clock, plan: Hammer, workflow: Workflow, external: Plug, system: Bot,
      unknown: MessageSquare,
    };
    for (const [trigger, icon] of Object.entries(expected)) {
      expect(ORIGIN_ICONS[trigger as SessionTrigger].icon, trigger).toBe(icon);
    }
  });

  it('shows a branch for a fork, whatever the trigger', () => {
    expect(originIcon({ trigger: 'chat', origin_detail: { fork_of: 'p1' } })).toBe(FORK_ICON);
    expect(FORK_ICON.icon).toBe(GitBranch);
  });

  it('falls back to the chat icon without a known trigger', () => {
    expect(originIcon({}).icon).toBe(MessageSquare);
    expect(originIcon({ trigger: 'later' as SessionTrigger }).icon).toBe(MessageSquare);
  });
});

describe('showsOrigin', () => {
  it('tells every origin except a plain web chat', () => {
    expect(showsOrigin({ trigger: 'slack', origin_label: 'Started from Slack' })).toBe(true);
    expect(showsOrigin({ trigger: 'chat', origin_label: 'Started in the web chat' })).toBe(false);
    expect(showsOrigin({
      trigger: 'chat', origin_label: 'Forked from another chat', origin_detail: { fork_of: 'p1' },
    })).toBe(true);
  });

  it('needs a label and a trigger', () => {
    expect(showsOrigin({ trigger: 'slack' })).toBe(false);
    expect(showsOrigin({ origin_label: 'Started from Slack' })).toBe(false);
  });
});

describe('startedByPerson', () => {
  it('names the creator for channel input, the API and Run later only', () => {
    for (const trigger of ['slack', 'telegram', 'email', 'api'] as const) {
      expect(startedByPerson({ trigger }), trigger).toBe(true);
    }
    expect(startedByPerson({ trigger: 'cron', origin_detail: { run_later: '1h' } })).toBe(true);
    for (const trigger of ['cron', 'plan', 'workflow', 'external', 'chat'] as const) {
      expect(startedByPerson({ trigger }), trigger).toBe(false);
    }
  });
});

describe('originLink', () => {
  it('links a plan, a cron job and a workflow run', () => {
    expect(originLink({ origin_detail: { plan_id: 'plan-1', task_id: 't' } }))
      .toEqual({ to: '/plans/plan-1', label: 'Open plan' });
    expect(originLink({ origin_detail: { cron_job: 'daily' } }))
      .toEqual({ to: '/cron', label: 'Open cron jobs' });
    expect(originLink({ origin_detail: { workflow_run: 'wfr-1' } }))
      .toEqual({ to: '/workflow-runs', label: 'Open workflow runs' });
  });

  it('has no link for a channel', () => {
    expect(originLink({ origin_detail: { channel: 'C1', channel_name: 'general' } })).toBeNull();
    expect(originLink({})).toBeNull();
  });
});
