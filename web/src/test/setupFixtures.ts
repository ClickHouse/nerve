import type {
  CredentialSource, SecretOut, SetupAgentsDetail, SetupAutomationDetail, SetupChannelsDetail, SetupClaimDetail,
  SetupMemoryDetail, SetupModeDetail, SetupProfileDetail, SetupProviderDetail,
  SetupSourcesDetail, SetupState, SetupStep, SetupStepDetail, SetupStepId, WorkCatalogItem, WorkSelection,
} from '../api/client';

/** Fixtures for the setup wizard: a personal, local, mid-way instance. */

export const OFF: SecretOut = { configured: false, last4: null };

/**
 * The worker work catalog as the Mode step's detail carries it: display
 * order, groups in sequence, and a `note` that only the alerts item fills.
 */
export const WORK_CATALOG: WorkCatalogItem[] = [
  {
    id: 'review-prs', group: 'code', label: 'Review pull requests',
    description: 'Reads diffs, runs targeted checks, posts review comments.',
    needs: 'github', follow_ups: ['which repositories', 'which account it acts as', 'comments only or approve'], note: '',
  },
  {
    id: 'triage-issues', group: 'code', label: 'Triage issues',
    description: 'Labels, finds duplicates, asks reporters for missing info, routes, flags stale.',
    needs: 'github', follow_ups: ['which repositories', 'label vocabulary', 'may it close'], note: '',
  },
  {
    id: 'fix-issues', group: 'code', label: 'Fix issues and open pull requests',
    description: 'Picks ready issues, implements, opens PRs, follows CI and review.',
    needs: 'github', follow_ups: ['which repositories', 'which issues', 'what needs approval'], note: '',
  },
  {
    id: 'ci-health', group: 'code', label: 'Keep CI healthy',
    description: 'Failures on main, flaky tests, quarantines, perf regressions where benchmarks exist.',
    needs: 'github', follow_ups: ['which repositories', 'which CI system', 'report or fix'], note: '',
  },
  {
    id: 'dependency-updates', group: 'code', label: 'Dependency and security updates',
    description: 'Reviews bot PRs (dependabot/renovate), checks advisories, prepares upgrades.',
    needs: 'github', follow_ups: ['which repositories', 'which bots', 'merge policy'], note: '',
  },
  {
    id: 'alert-response', group: 'operations', label: 'First response to alerts',
    description: 'Reads where alerts land, does the first investigation, posts findings, escalates by rule.',
    needs: 'alerts', follow_ups: ['alert source', 'runbooks', 'escalation path'],
    note: 'Needs an alert source; a Slack channel is the first-class one today.',
  },
  {
    id: 'channel-digests', group: 'communication', label: 'Channel digests and answers',
    description: 'Summarizes busy Slack/Telegram channels, answers recurring questions from docs.',
    needs: 'slack-or-telegram', follow_ups: ['which channels', 'cadence', 'where to post'], note: '',
  },
  {
    id: 'research-reports', group: 'knowledge', label: 'Research and reports',
    description: 'Tracks topics, competitors, releases; periodic reports.',
    needs: 'web', follow_ups: ['topics', 'sources', 'cadence'], note: '',
  },
  {
    id: 'docs-release-notes', group: 'knowledge', label: 'Docs and release notes',
    description: 'Keeps docs in step with merged changes, drafts changelogs.',
    needs: 'github', follow_ups: ['which repositories', 'triggers', 'drafts or direct'], note: '',
  },
];

/** Nothing chosen: what the server sends before a worker's work is saved, and for a personal install. */
export const NO_WORK: WorkSelection = { items: [], custom: '', rules: '' };
/** A configured secret; `source` names who provided it when it was not this wizard. */
export const on = (last4: string, source?: CredentialSource): SecretOut => (
  source ? { configured: true, last4, source } : { configured: true, last4 }
);

export const DETAILS: Record<SetupStepId, SetupStepDetail> = {
  claim: { username: 'alice', passwordless: false } satisfies SetupClaimDetail,
  mode: {
    mode: 'personal',
    templates_present: ['SOUL.md', 'IDENTITY.md', 'USER.md', 'AGENTS.md', 'TOOLS.md', 'MEMORY.md'],
    task_present: false, onboarding: 'not_applicable', onboarding_session_id: null,
    work_catalog: WORK_CATALOG, work: NO_WORK,
  } satisfies SetupModeDetail,
  provider: {
    type: 'anthropic', credential: 'none', anthropic_api_key: OFF, claude_oauth_token: OFF,
    proxy_enabled: false, aws_region: '', aws_profile: '', model: 'claude-opus-5-5', effort: 'high',
    cron_model: 'claude-sonnet-4-6', warning: null, last_test: null,
  } satisfies SetupProviderDetail,
  profile: {
    name: 'Alice', name_source: 'account', display_name_editable: true, writable_fields: null,
    timezone: 'America/New_York', timezone_is_default: true,
    quiet_start: '02:00', quiet_end: '08:00', quiet_enabled: true, quiet_is_default: true,
    language: null, agent_name: null, agent_emoji: null, about_set: false,
    user_md_pristine: { name: true, timezone: true, language: true, quiet_hours: true, work: true },
    identity_md_pristine: { name: true, emoji: true },
  } satisfies SetupProfileDetail,
  memory: { openai_api_key: OFF, embeddings_chosen: false, embed_model: 'text-embedding-3-small' } satisfies SetupMemoryDetail,
  channels: {
    telegram: {
      enabled: false, bot_token: OFF, bot_username: null, live: false, start_error: null,
      dm_policy: 'pairing', allowed_users_count: 0, pairing_available: false, pairing_unavailable_reason: null,
    },
    slack: {
      enabled: false, mode: 'socket', bot_token: OFF, app_token: OFF,
      allow_users: [], allow_direct_messages: false, allow_channels: [],
    },
  } satisfies SetupChannelsDetail,
  sources: {
    github: { enabled: false }, gmail: { enabled: false, accounts: [] },
    telegram: { enabled: false, api_id: OFF, api_hash: OFF, session: { present: false, authorized: false, user: null } },
  } satisfies SetupSourcesDetail,
  automation: {
    file_present: true, timezone: 'America/New_York',
    core: [{ id: 'memory-maintenance', name: 'Memory maintenance', description: 'Keeps memory tidy.', schedule: '0 5 * * *', schedule_text: 'daily at 05:00' }],
    optional: [
      { id: 'inbox-processor', name: 'Inbox processor', description: 'Reads the inbox.', requires: 'At least one sync source connected', requirement_met: false, schedule: '*/30 * * * *', schedule_text: 'every 30 minutes', enabled: true, default: true, default_enabled: true },
      { id: 'task-planner', name: 'Task planner', description: 'Plans the day.', requires: null, requirement_met: true, schedule: '0 7 * * *', schedule_text: 'daily at 07:00', enabled: false, default: true, default_enabled: true },
    ],
  } satisfies SetupAutomationDetail,
  agents: { enabled: [], conflict_policy: 'backup', available: ['codex', 'claude-code'] } satisfies SetupAgentsDetail,
};

/** The channels detail, typed, for tests that replace one half of it. */
export const CHANNELS = DETAILS.channels as SetupChannelsDetail;

const TITLES: Record<SetupStepId, string> = {
  claim: 'Claim', mode: 'Mode', provider: 'Model', profile: 'About you', memory: 'Memory',
  channels: 'Channels', sources: 'Sources', automation: 'Automation', agents: 'Agents',
};

const REQUIRED: SetupStepId[] = ['claim', 'mode', 'provider'];

export function step(id: SetupStepId, overrides: Partial<SetupStep> = {}, detail: Partial<SetupStepDetail> = {}): SetupStep {
  const base: SetupStep = {
    id, title: TITLES[id], status: 'pending', required: REQUIRED.includes(id), can_skip: !REQUIRED.includes(id),
    blocked_reason: null, read_only: false, read_only_reason: null, etag: `${id}-etag-1`,
    summary: '', files: [], detail: { ...DETAILS[id], ...detail } as SetupStepDetail,
  };
  return { ...base, ...overrides, detail: { ...base.detail, ...(overrides.detail ?? {}), ...detail } as SetupStepDetail };
}

export const MODE_LOCK_REASON = 'Mode was chosen when the workspace was created. Changing it means starting a new workspace.';

/** Personal, local: claim and mode done, everything else pending. */
export function makeState(overrides: Partial<SetupState> = {}): SetupState {
  const steps: SetupStep[] = overrides.steps ?? [
    step('claim', { status: 'done', summary: 'alice · password set' }),
    step('mode', { status: 'done', read_only: true, read_only_reason: MODE_LOCK_REASON, summary: 'Personal assistant' }),
    step('provider'),
    step('profile'),
    step('memory'),
    step('channels'),
    step('sources'),
    step('automation'),
    step('agents'),
  ];
  const pending = steps.filter((s) => s.status === 'pending').length;
  return {
    generation: 'G0', auth_mode: 'local', mode: 'personal', lockdown: false, local_secrets: true, writable: true,
    read_only_reason: null, restart_pending: false, restart_reasons: [], restart_mode: 'restart',
    restart_message: 'Restarting stops this process and starts a new one; this page reconnects on its own.',
    finished: false, steps_left: pending, warning: null,
    ...overrides,
    steps,
  };
}

/** The sentence the server puts on a step that waits for the Mode step. */
export const WAITS_FOR_MODE = 'Choose a mode first — the files this step fills in depend on it';

/**
 * Personal, local, straight after the claim — an instance that has never
 * chosen a mode. The steps whose files depend on the mode wait for it
 * (blocked by sequencing), and the server counts them as left to do.
 */
export function makeFreshState(overrides: Partial<SetupState> = {}): SetupState {
  const waiting = (id: SetupStepId) => step(id, {
    status: 'blocked', read_only: true, blocked_reason: WAITS_FOR_MODE, blocked_kind: 'sequencing',
  });
  const steps: SetupStep[] = [
    step('claim', { status: 'done', summary: 'alice · password set' }),
    step('mode', {}, { mode: null, templates_present: [] }),
    step('provider'),
    waiting('profile'),
    step('memory'),
    step('channels'),
    waiting('sources'),
    waiting('automation'),
    waiting('agents'),
  ];
  return makeState({ mode: null, steps, steps_left: 8, ...overrides });
}

/** The same instance behind a front gateway: no claim step, external actor. */
export function makeExternalState(overrides: Partial<SetupState> = {}, base: SetupState = makeState()): SetupState {
  return makeState({
    ...base,
    auth_mode: 'external',
    steps: base.steps.filter((s) => s.id !== 'claim').map((s) => (
      s.id === 'profile'
        ? step('profile', s, { name: 'Alice Example', name_source: 'actor_header', display_name_editable: false })
        : s
    )),
    ...overrides,
  });
}

/** A worker's Operator detail as the Mode step leaves it: no USER.md, quiet hours off (the mode's default). */
export const WORKER_PROFILE: Partial<SetupProfileDetail> = {
  quiet_start: '', quiet_end: '', quiet_enabled: false, quiet_is_default: true,
  user_md_pristine: { name: false, timezone: false, language: false, quiet_hours: false, work: false },
  identity_md_pristine: { name: false, emoji: false },
};

/** A worker's job list: the skill jobs on, the task planner off until onboarding decides. */
export const WORKER_AUTOMATION: Partial<SetupAutomationDetail> = {
  optional: [
    { id: 'skill-reviser', name: 'Skill reviser', description: 'Reviews skills.', requires: null, requirement_met: true, schedule: '0 3 * * 0', schedule_text: 'Sundays at 03:00', enabled: true, default: true, default_enabled: true },
    { id: 'skill-extractor', name: 'Skill extractor', description: 'Proposes skills.', requires: null, requirement_met: true, schedule: '0 */12 * * *', schedule_text: 'every 12 hours', enabled: true, default: true, default_enabled: true },
    { id: 'task-planner', name: 'Task planner', description: 'Plans open tasks.', requires: null, requirement_met: true, schedule: '0 */4 * * *', schedule_text: 'every 4 hours', enabled: false, default: false, default_enabled: false },
  ],
};

/** A worker, local, just after Mode and Model: Operator, Memory, Channels and Automation pending. */
export function makeWorkerState(overrides: Partial<SetupState> = {}): SetupState {
  return makeState({
    mode: 'worker',
    steps: [
      step('claim', { status: 'done', summary: 'alice · password set' }),
      step('mode', { status: 'done', read_only: true, read_only_reason: MODE_LOCK_REASON, summary: 'Worker' }, {
        mode: 'worker', templates_present: ['SOUL.md', 'AGENTS.md', 'TOOLS.md', 'MEMORY.md'],
        task_present: true, onboarding: 'pending',
      }),
      step('provider', { status: 'done' }),
      step('profile', { title: 'Operator' }, WORKER_PROFILE),
      step('memory'),
      step('channels'),
      step('automation', {}, WORKER_AUTOMATION),
    ],
    ...overrides,
  });
}

/** Replace one step in a state, keeping the rest. */
export function withStep(state: SetupState, replacement: SetupStep): SetupState {
  const steps = state.steps.map((s) => (s.id === replacement.id ? replacement : s));
  return { ...state, steps, steps_left: steps.filter((s) => s.status === 'pending').length };
}
