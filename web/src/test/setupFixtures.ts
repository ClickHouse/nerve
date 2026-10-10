import type {
  SecretOut, SetupAgentsDetail, SetupAutomationDetail, SetupChannelsDetail, SetupClaimDetail,
  SetupMemoryDetail, SetupModeDetail, SetupProfileDetail, SetupProviderDetail,
  SetupSourcesDetail, SetupState, SetupStep, SetupStepDetail, SetupStepId,
} from '../api/client';

/** Fixtures for the setup wizard: a personal, local, mid-way instance. */

export const OFF: SecretOut = { configured: false, last4: null };
export const on = (last4: string): SecretOut => ({ configured: true, last4 });

export const DETAILS: Record<SetupStepId, SetupStepDetail> = {
  claim: { username: 'alice', passwordless: false } satisfies SetupClaimDetail,
  mode: {
    mode: 'personal',
    templates_present: ['SOUL.md', 'IDENTITY.md', 'USER.md', 'AGENTS.md', 'TOOLS.md', 'MEMORY.md'],
    task_present: false, onboarding: 'not_applicable', onboarding_session_id: null,
  } satisfies SetupModeDetail,
  provider: {
    type: 'anthropic', credential: 'none', anthropic_api_key: OFF, claude_oauth_token: OFF,
    proxy_enabled: false, aws_region: '', aws_profile: '', model: 'claude-sonnet-4-6', effort: 'high',
    cron_model: 'claude-sonnet-4-6', warning: null, last_test: null,
  } satisfies SetupProviderDetail,
  profile: {
    name: 'Alice', name_source: 'account', display_name_editable: true, writable_fields: null,
    timezone: 'America/New_York', timezone_is_default: true,
    quiet_start: '02:00', quiet_end: '08:00', quiet_is_default: true,
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
    telegram: { enabled: false, api_id: OFF, api_hash: OFF },
  } satisfies SetupSourcesDetail,
  automation: {
    file_present: true, timezone: 'America/New_York',
    core: [{ id: 'memory-maintenance', name: 'Memory maintenance', description: 'Keeps memory tidy.', schedule: '0 5 * * *', schedule_text: 'daily at 05:00' }],
    optional: [
      { id: 'inbox-processor', name: 'Inbox processor', description: 'Reads the inbox.', requires: 'At least one sync source connected', requirement_met: false, schedule: '*/30 * * * *', schedule_text: 'every 30 minutes', enabled: true, default: true },
      { id: 'task-planner', name: 'Task planner', description: 'Plans the day.', requires: null, requirement_met: true, schedule: '0 7 * * *', schedule_text: 'daily at 07:00', enabled: false, default: true },
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
    generation: 'G0', auth_mode: 'local', mode: 'personal', lockdown: false, writable: true,
    read_only_reason: null, restart_pending: false, restart_reasons: [], restart_mode: 'restart',
    restart_message: 'Restarting stops this process and starts a new one; this page reconnects on its own.',
    finished: false, steps_left: pending, warning: null,
    ...overrides,
    steps,
  };
}

/** The same instance behind a front gateway: no claim step, external actor. */
export function makeExternalState(overrides: Partial<SetupState> = {}): SetupState {
  const base = makeState();
  return makeState({
    auth_mode: 'external',
    steps: base.steps.filter((s) => s.id !== 'claim').map((s) => (
      s.id === 'profile'
        ? step('profile', {}, { name: 'Alice Example', name_source: 'actor_header', display_name_editable: false })
        : s
    )),
    ...overrides,
  });
}

/** Replace one step in a state, keeping the rest. */
export function withStep(state: SetupState, replacement: SetupStep): SetupState {
  const steps = state.steps.map((s) => (s.id === replacement.id ? replacement : s));
  return { ...state, steps, steps_left: steps.filter((s) => s.status === 'pending').length };
}
