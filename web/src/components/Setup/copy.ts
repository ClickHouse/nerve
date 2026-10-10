import type { SetupMode, SetupOnboarding, SetupStepId } from '../../api/client';

/**
 * The wizard's words, in one place and without JSX.
 *
 * The API carries data and short labels; every heading, intro, hint and
 * guidance line lives here, so a wording change never crosses the boundary.
 */

export const CLAIM_INTRO =
  'Create your sign-in for this Nerve: a username and password, or keep it passwordless. '
  + 'The rest of setup follows in this browser.';

export const STEP_HEADINGS: Record<SetupStepId, { heading: string; intro: string }> = {
  claim: {
    heading: 'Account',
    intro: 'This instance has been claimed.',
  },
  mode: {
    heading: 'What should Nerve be for you?',
    intro: 'Chosen once. It shapes the assistant\'s starting notes and what it does on its own.',
  },
  provider: {
    heading: 'Connect a model',
    intro: 'Nerve talks to Claude. Pick how this server reaches it; the key stays on the server.',
  },
  profile: {
    heading: 'About you',
    intro: 'A few facts Nerve writes into its notes so it can work on your schedule and in your words.',
  },
  memory: {
    heading: 'Memory',
    intro: 'Nerve remembers what you tell it. An OpenAI key adds vector search; without one, recall still works.',
  },
  channels: {
    heading: 'Talk to Nerve where you already are',
    intro: 'Connect a Telegram bot or Slack so you can message Nerve — and it can reach you — outside this page.',
  },
  sources: {
    heading: 'What should Nerve read?',
    intro: 'Sources are inboxes Nerve checks on a schedule. Each needs its tool set up on the server.',
  },
  automation: {
    heading: 'Scheduled work',
    intro: 'Jobs Nerve runs on its own. The core set is always on; the rest is yours to choose.',
  },
  agents: {
    heading: 'Other coding agents',
    intro: 'Give Codex or Claude Code on this server a line to Nerve, so they can use its memory and tools.',
  },
};

export const PROFILE_HEADING_WORKER = {
  heading: 'Operator',
  intro: 'Who runs this worker, and in which time zone it should plan its day.',
};

export const REVIEW_HEADING = {
  heading: 'Review and finish',
  intro: 'What was decided, what was written where, and whether Nerve needs a restart to pick it up.',
};

export const MODE_CARDS: { value: SetupMode; title: string; description: string; recommended?: boolean }[] = [
  {
    value: 'personal',
    title: 'Personal assistant',
    description: 'Learns about you, keeps notes, answers on your channels, runs your errands.',
    recommended: true,
  },
  {
    value: 'worker',
    title: 'Worker',
    description: 'Owns one job you describe, works it on a schedule and reports back.',
  },
];

export const WORKER_TEMPLATES: { label: string; text: string }[] = [
  {
    label: 'Monitor a build',
    text: 'Watch the nightly build for example/repo. When it fails, find the first bad commit, propose a fix as a pull request, and never merge anything yourself.',
  },
  {
    label: 'Review pull requests',
    text: 'Review every open pull request in example/repo for correctness and test coverage. Leave one consolidated comment per PR; ask before requesting changes.',
  },
  {
    label: 'Watch logs',
    text: 'Read the application logs under /var/log/example every hour. Summarise new errors, group repeats, and open a task for anything that recurs three times.',
  },
];

export const WORKER_FIRST_BOOT =
  'On first boot the worker reads this description, drafts a mission and a plan into TASK.md, '
  + 'and starts an onboarding session you can watch and correct. Nothing runs against the task '
  + 'until that session has written the plan.';

export const PROVIDER_CARDS: { value: 'anthropic' | 'oauth' | 'host_login' | 'bedrock'; title: string; description: string }[] = [
  { value: 'anthropic', title: 'Anthropic API key', description: 'Pay per use. The simplest choice; also powers memory and background jobs.' },
  { value: 'oauth', title: 'Claude subscription', description: 'A Claude Pro or Max login via `claude setup-token`. Chat only; background jobs need an API key.' },
  { value: 'host_login', title: 'Use the Claude Code login on this server', description: 'This server already has a Claude login. Copy it into Nerve.' },
  { value: 'bedrock', title: 'AWS Bedrock', description: 'Claude through your AWS account. Needs model access enabled in the region.' },
];

export const SUBSCRIPTION_NOTICE =
  'Background jobs (memory, titles, cron) also need an API key — add one in the Memory step, '
  + 'or set up the proxy with `nerve init --terminal`.';

export const OAUTH_BACKGROUND_WARNING =
  'The only credential is a subscription token. Chat works; memory, titles and scheduled jobs '
  + 'need an API key, which the Memory step or an ANTHROPIC_API_KEY in the environment can supply.';

export const ENVIRONMENT_CREDENTIAL =
  'Configured from the environment of the server process. Change it there, not here.';

/** Every source was readable and held no login. */
export const HOST_LOGIN_NONE = 'No Claude Code login was found on this server.';
/**
 * A source could not be consulted (on a Mac, the Keychain refuses a
 * background process). Stated as "could not be read", not as a fact about
 * the machine, and routed: look again, paste a key, or use the terminal.
 */
export const HOST_LOGIN_UNREADABLE = 'No Claude Code login could be read on this server.';
export const HOST_LOGIN_UNREADABLE_HINT =
  'On a Mac the server may have been denied Keychain access. Look again, paste an API key, '
  + 'or run `nerve init --terminal` on the server.';
export const HOST_LOGIN_FOUND = (source: string) =>
  `A Claude Code login was found (${source}). You can use it below.`;

/**
 * The live test failed and Continue was pressed anyway. The first press
 * warns; the second saves — the same two-step the sk-ant- check uses.
 */
export const PROVIDER_TEST_FAILED_WARNING =
  'The connection test failed a moment ago, so this key would be saved unverified. '
  + 'Fix the key and test again, or save it anyway.';
/** Appended to a provider summary when the server's last live test failed. */
export const PROVIDER_LAST_TEST_FAILED = 'last connection test failed';

export const PROVIDER_SAVED_PICK_MODEL =
  'Saved. Pick the default model and effort (optional), then Continue.';

export const MODEL_FAMILY_GUIDANCE: Record<string, string> = {
  opus: 'Most capable; best for open-ended work and long sessions.',
  sonnet: 'Fast and strong; a good default when cost matters.',
  haiku: 'Quickest and cheapest; fine for short, well-defined tasks.',
  other: 'A model this build does not know; use it if your account has access.',
};

export const EFFORT_GUIDANCE: Record<string, string> = {
  max: 'Thinks as long as it needs to. Slowest, most thorough.',
  xhigh: 'Very deliberate; for hard problems.',
  high: 'Careful on most questions; a good everyday setting.',
  medium: 'Balanced speed and depth.',
  low: 'Snappy answers; less deliberation.',
};

/** Appended to the guidance of the effort the server reports as current. */
export const EFFORT_SERVER_DEFAULT = ' This is the server\'s current default.';

/** The model picker's empty choice: leave the server's default alone. */
export const MODEL_SERVER_DEFAULT = (model: string | null | undefined) =>
  (model ? `Server default (${model})` : 'Server default');

export const COMMON_AWS_REGIONS = [
  'us-east-1', 'us-east-2', 'us-west-2', 'eu-central-1', 'eu-west-1', 'eu-west-2',
  'eu-west-3', 'eu-north-1', 'ap-northeast-1', 'ap-southeast-1', 'ap-southeast-2',
  'ap-south-1', 'ca-central-1', 'sa-east-1',
];

export const COMMON_LANGUAGES = [
  'English', 'Deutsch', 'Français', 'Español', 'Italiano', 'Português', 'Nederlands',
  'Polski', 'Українська', 'Русский', 'Türkçe', '日本語', '中文', '한국어', 'العربية', 'हिन्दी',
];

export const QUIET_HOURS_PREFILL = { start: '22:00', end: '07:00' };

export const MEMORY_WITH_EMBEDDINGS =
  'Fast vector search over everything Nerve remembers. Needs an OpenAI key; small cost per note.';
export const MEMORY_WITHOUT_EMBEDDINGS =
  'Claude ranks memories at recall time. Works today; slower on very large memories.';

export const TELEGRAM_STEPS = [
  'Open Telegram and message @BotFather.',
  'Send /newbot, pick a name and a username ending in "bot".',
  'Copy the token BotFather sends back and paste it here.',
];

export const SLACK_INTRO =
  'Advanced — needs a Slack app with Socket Mode. Paste its bot token (xoxb-) and app-level token (xapp-).';
export const SLACK_DISCLOSURE = 'Also connect Slack (advanced)';
export const NO_CHANNELS_LABEL = 'No channels for now';

export const AUTOMATION_IDLE_UNTIL_SOURCE =
  'stays idle until a source is connected (Sources step)';

export const SLACK_HOSTED = 'Slack is managed by the channel gateway on this instance; only the switch is yours.';

export const PAIRING_AFTER_RESTART =
  'The bot starts with this token after the restart at the end of setup. Pairing is offered on the ready screen.';

export const SOURCES_IDLE_HINT =
  'Turned on, but stays idle until the tool is set up on the server.';

export const AGENT_CONFLICT_POLICIES: { value: 'backup' | 'skip' | 'merge'; title: string; description: string }[] = [
  { value: 'backup', title: 'Back up and replace', description: 'Keeps a copy of the existing config next to it.' },
  { value: 'merge', title: 'Merge', description: 'Adds the Nerve entry and leaves the rest alone.' },
  { value: 'skip', title: 'Skip', description: 'Leaves an existing config untouched; only writes where none exists.' },
];

/** What Continue does to an agent's config file, by policy, when one exists. */
export const AGENT_WRITE_EXISTING: Record<'backup' | 'skip' | 'merge', string> = {
  backup: 'a backup of the existing file is kept',
  merge: 'the Nerve entry is added and the rest is left alone',
  skip: 'the existing file is left untouched, so nothing is written there',
};
export const AGENTS_WRITE_NOTHING =
  'Nothing is written on this server. Continue records that no agents are connected.';

export const HTTPS_HINT =
  'This page is served over plain HTTP from a non-loopback address. Anyone on the path can read '
  + 'what you type here, including keys. Put Nerve behind HTTPS or reach it through an SSH tunnel.';

export const PASSWORDLESS_LAN_HINT =
  'This instance is passwordless and listens beyond localhost. Anyone who can reach it is the owner. '
  + 'Set a password in Accounts, or bind to localhost.';

export const RESTART_HEADLINE = 'Some of what you changed is read at startup.';
export const RESTART_HEADLINE_FIRST_RUN =
  'One restart finishes a fresh setup; this page reconnects on its own.';
/**
 * What the Restart button will do, in the future tense: the card is read
 * before the click, so the server's present-tense "Restarting. This page
 * reconnects on its own." belongs to the overlay that follows it.
 */
export const RESTART_EXPLAINER =
  'Restarting stops this process and starts a new one; this page reconnects on its own.';

/**
 * A restart that cannot be started from here. With a command, the person
 * sees it; without one (a supervisor this build cannot name), the sentence
 * must not promise a command that is not on the screen.
 */
export const UNSUPPORTED_WITH_COMMAND =
  'This instance is managed by a supervisor. Someone with access to the server must run '
  + 'the command below; this page notices the new process on its own';
export const UNSUPPORTED_WITHOUT_COMMAND =
  'This instance is managed by a supervisor. Someone who runs this instance\'s supervisor '
  + 'must restart Nerve; this page notices the new process on its own';

/**
 * The first-run test: only the Mode write produces `memory` debt, so a
 * reason list made of nothing else is a fresh setup's one restart.
 */
export const isFirstRunRestart = (reasons: string[]): boolean =>
  reasons.length > 0 && reasons.every((r) => /^memory(\.|:)/.test(r));

/**
 * A restart reason in words. The server sends `path: what changed`
 * (`memory: changed`, `timezone: 'A' → 'B'`, `telegram.enabled: False → True`);
 * a first-time user knows none of those paths, so the known ones become a
 * sentence and the raw line stays as secondary text. Unknown paths are
 * returned as they came.
 */
export function describeRestartReason(raw: string): string {
  const colon = raw.indexOf(':');
  const path = (colon > 0 ? raw.slice(0, colon) : raw).trim();
  const rest = colon > 0 ? raw.slice(colon + 1).trim() : '';
  if (path === 'memory' || path.startsWith('memory.')) return 'The mode you chose (its memory layout is read at startup)';
  if (path === 'timezone') {
    const change = rest.match(/^'([^']*)'\s*→\s*'([^']*)'$/);
    return change ? `Time zone: ${change[1]} → ${change[2]}` : 'Time zone';
  }
  if (path.startsWith('telegram.')) return 'Telegram bot settings';
  if (path === 'openai_api_key') return 'Memory embeddings start after a restart';
  if (path.startsWith('slack.') || path.startsWith('channels.')) return 'Slack gateway settings';
  if (path.startsWith('gateway.')) return 'Gateway address or port';
  if (path.startsWith('external_agents') || path.startsWith('mcp_endpoint')) return 'Access for external coding agents';
  if (path === 'cron') return 'Scheduled jobs';
  if (path.startsWith('proxy')) return 'Claude proxy settings';
  if (path === 'workspace') return 'Workspace location';
  return raw;
}

/**
 * The suggested first message. Without a real name (only a login username
 * is known) the "I'm …" clause is dropped rather than introducing the person
 * as "alice".
 */
export const SAY_HELLO = (name: string | null) => (name
  ? `Hi! I'm ${name}. Tell me what you can do and what you already know about me.`
  : 'Hi! Tell me what you can do and what you already know about me.');

/** The name field's help on an instance behind a front gateway, by where the value came from. */
export const PROFILE_NAME_HELP_EXTERNAL = {
  actor_header: 'Prefilled from your organisation\'s sign-in. This only sets what Nerve writes in USER.md.',
  user_md: 'The name Nerve uses in its notes (USER.md), shared by everyone on this instance.',
  signedInAs: (actor: string) => `You are signed in as ${actor}.`,
};

/** Sources are on but the job that reads them is off; the link text follows. */
export const INBOX_JOB_OFF = 'The Inbox processor job is off — it is what reads your sources. Turn it on in';

export const BANNER_STEPS_LEFT = (n: number) =>
  `Finish setting up Nerve — ${n} ${n === 1 ? 'step' : 'steps'} left`;
export const BANNER_RESTART = 'Restart Nerve to apply your setup';

export const CHAT_EMPTY_NO_PROVIDER = 'Add a model provider to start chatting';

/**
 * The first screen. The route and the API keep saying "claim"; the person
 * is creating a sign-in, and the words say that.
 */
export const CLAIM_HEADING = 'Create your sign-in';
export const CLAIM_SUBMIT = 'Create and continue';
export const CLAIM_SUBMIT_BUSY = 'Creating…';
/** What is still missing when the button is pressed too early. */
export const CLAIM_MISSING = (passwordless: boolean) => (passwordless
  ? 'Enter the setup token to continue.'
  : 'Fill in the username, password and setup token to continue.');

/** The Overview's line under the greeting. */
export const OVERVIEW_SUBLINE = 'Three decisions and Nerve is ready: a mode, a model, a few facts about you.';
export const OVERVIEW_SUBLINE_FINISHED = 'Come back here any time to change a step.';
/** The claim row's secondary text; the last word is the link to Accounts. */
export const ACCOUNT_ROW_TEXT = 'Manage people and passwords in';
export const ACCOUNT_ROW_LINK = 'Accounts';

/** Under the still-to-do list on Review while Finish waits for it. */
export const REVIEW_DECIDE_FIRST = 'Open or skip the steps above to finish.';

/** The ready screen when optional steps are still open (another tab, a reload). */
export const READY_STEPS_LEFT = (n: number) =>
  `Setup is complete for now; ${n} optional ${n === 1 ? 'step is' : 'steps are'} still open. The Setup entry brings you back to ${n === 1 ? 'it' : 'them'}.`;

/**
 * A worker's onboarding, in sentences. Never the session id: the link
 * carries it. `ended` is a session that stopped without writing a plan.
 */
export const ONBOARDING_ENDED = 'The onboarding session ended without writing a plan';
export const ONBOARDING_SENTENCES: Record<SetupOnboarding, string> = {
  not_applicable: '',
  pending: 'The onboarding session starts after setup finishes.',
  running: 'Onboarding session running',
  ended: `${ONBOARDING_ENDED} — open it to see why.`,
  done: 'Onboarding done.',
};
export const ONBOARDING_OPEN = 'Open the onboarding session';
export const ONBOARDING_TRY_AGAIN = 'Try again';
