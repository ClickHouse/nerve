import type { SetupMode, SetupOnboarding, SetupStepId } from '../../api/client';
import type { ManagedSource } from './credentialSource';

/**
 * The wizard's words, in one place and without JSX.
 *
 * The API carries data and short labels; every heading, intro, hint and
 * guidance line lives here, so a wording change never crosses the boundary.
 */

/** "Keep this installation passwordless" is the choice's name in the terminal wizard and the docs too. */
export const CLAIM_INTRO =
  'Choose a username and password to sign in with, or keep this installation passwordless. '
  + 'The remaining setup steps follow in this browser.';

export const STEP_HEADINGS: Record<SetupStepId, { heading: string; intro: string }> = {
  claim: {
    heading: 'Account',
    intro: 'The first account on this instance was created at the start of setup.',
  },
  mode: {
    heading: 'Choose a mode',
    intro: 'You choose this once. The mode decides which notes Nerve starts with and which jobs it runs on its own.',
  },
  provider: {
    heading: 'Connect a model',
    intro: 'Choose how this server connects to Claude. Keys and tokens you enter are stored on the server, not in this browser.',
  },
  profile: {
    heading: 'About you',
    intro: 'Nerve saves these details in its notes and uses them for scheduling and in its replies.',
  },
  memory: {
    heading: 'Memory',
    intro: 'Nerve saves facts from your conversations to memory. Choose how it searches them.',
  },
  channels: {
    heading: 'Connect Telegram or Slack',
    intro: 'With a bot, you can message Nerve outside this page and Nerve can message you.',
  },
  sources: {
    heading: 'Choose what Nerve reads',
    intro: 'Nerve checks these inboxes on a schedule. Each card shows whether the server is ready to read it.',
  },
  automation: {
    heading: 'Scheduled jobs',
    intro: 'Core jobs always run. Choose which optional jobs run as well.',
  },
  agents: {
    heading: 'Connect coding agents',
    intro: 'Codex and Claude Code on this server can use Nerve\'s memory and tools. Connecting one adds a Nerve entry to its config file.',
  },
};

export const PROFILE_HEADING_WORKER = {
  heading: 'Operator',
  intro: 'The worker records your name as its operator and runs its jobs in your time zone.',
};

export const REVIEW_HEADING = {
  heading: 'Review and finish',
  intro: 'Most settings apply as soon as a step saves them. Settings read at startup wait for a restart.',
};

export const MODE_CARDS: { value: SetupMode; title: string; description: string; recommended?: boolean }[] = [
  {
    value: 'personal',
    title: 'Personal assistant',
    description: 'An assistant for one person. It keeps notes about you and can read your inboxes.',
    recommended: true,
  },
  {
    value: 'worker',
    title: 'Worker',
    description: 'Works on one task you describe and reports its results.',
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
  + 'and starts an onboarding session you can watch and correct. Work on the task starts '
  + 'only after that session has written the plan.';

export const PROVIDER_CARDS: { value: 'anthropic' | 'oauth' | 'host_login' | 'bedrock'; title: string; description: string }[] = [
  { value: 'anthropic', title: 'Anthropic API key', description: 'Pay per use. Works for chat, memory and background jobs.' },
  { value: 'oauth', title: 'Claude subscription', description: 'Your Claude Pro or Max plan. Chat only; background jobs need an API key.' },
  { value: 'host_login', title: 'Claude Code login on this server', description: 'Nerve copies the Claude login this server already has.' },
  { value: 'bedrock', title: 'AWS Bedrock', description: 'Claude through your AWS account. Needs model access enabled in the region.' },
];

/**
 * Where background jobs get a key when the model step holds a subscription
 * token. Rendered through `CodeText` (a backticked part shows as code).
 *
 * The routes are the ones `resolve_anthropic_api_key()` reads: an API key
 * saved on this step (which replaces the token: the provider PUT clears
 * one when it saves the other), else `ANTHROPIC_API_KEY` in the server's
 * environment, else the proxy, which runs on the subscription. Never the
 * Memory step: it only takes an OpenAI key. Nothing here promises that
 * chat keeps using the subscription: the CLI prefers an API key when it
 * sees both.
 */
export const SUBSCRIPTION_NOTICE =
  'Memory, titles and scheduled jobs cannot use a subscription token. For those, choose Anthropic API key '
  + 'above instead, or set `ANTHROPIC_API_KEY` in the server\'s environment. To run them on the subscription, '
  + 'set up the proxy with `nerve init --terminal`.';

/** The same routes, once a subscription token is the saved credential. */
export const OAUTH_BACKGROUND_WARNING =
  'The only credential is a subscription token. Chat works, but memory, titles and scheduled jobs cannot use it. '
  + 'Replace the token with an Anthropic API key on this step, or set `ANTHROPIC_API_KEY` in the server\'s '
  + 'environment. To run those jobs on the subscription, set up the proxy with `nerve init --terminal`.';

/**
 * A value held outside config.local.yaml: who provides it. The same three
 * sentences everywhere a credential can appear, so a person recognises
 * them from one card to the next.
 */
export const MANAGED_SOURCE_TEXT: Record<ManagedSource, string> = {
  environment: 'Configured by your operator (environment)',
  'config-repo': 'Managed from the configuration repository',
  gateway: 'Managed by the channel gateway',
};
/** Where such a value is changed, since it cannot be changed here. */
export const MANAGED_SOURCE_HINT: Record<ManagedSource, string> = {
  environment: 'Change it where the server process gets its environment, not here.',
  'config-repo': 'Change it in the repository. The change reaches this instance with the next sync.',
  gateway: 'The gateway holds this value; there is nothing to enter here.',
};
/** The short form for a summary line: "Telegram: @bot · from the environment". */
export const MANAGED_SOURCE_SHORT: Record<ManagedSource, string> = {
  environment: 'from the environment',
  'config-repo': 'from the configuration repository',
  gateway: 'held by the channel gateway',
};
/** The sentence inside a line: "configured by your operator (environment)". */
const managedPhrase = (source: ManagedSource) => {
  const text = MANAGED_SOURCE_TEXT[source];
  return `${text.charAt(0).toLowerCase()}${text.slice(1)}`;
};
/** "Token: configured by your operator (environment)". */
export const managedLine = (what: string, source: ManagedSource) => `${what}: ${managedPhrase(source)}`;
/**
 * The GitHub card while the channel gateway polls GitHub and the operator
 * holds the source's switch in the machine's configuration
 * (`detail.github.managed`): in place of the switch, that the source is on
 * and who provides it. The step's reason, the server's sentence, says what
 * decides whether anything arrives.
 */
export const GITHUB_MANAGED_ON = `On · ${managedPhrase('gateway')}`;

/**
 * An instance whose setup page stores no credential (`setup.local_secrets:
 * false`). The server sends these two sentences as the reason of every
 * step it closes and the detail of every write it refuses; the page
 * repeats them where a key could otherwise be entered, and labels the
 * closed steps on the overview. The same string as the server's, so a
 * backend without `blocked_kind` is still recognised by its reason.
 */
export const NO_LOCAL_SECRETS_TEXT =
  'This instance does not store credentials entered here. Connections are provided by the operator of this deployment.';
export const NOT_AVAILABLE_HERE = 'Not available on this instance';

/** Every source was readable and held no login. */
export const HOST_LOGIN_NONE = 'No Claude Code login was found on this server.';
/**
 * A source could not be consulted (on a Mac, the Keychain refuses a
 * background process). Stated as "could not be read", not as a fact about
 * the machine, and routed: look again, paste a key, or use the terminal.
 */
export const HOST_LOGIN_UNREADABLE = 'No Claude Code login could be read on this server.';
/** Rendered through `CodeText` (a backticked part shows as a command). */
export const HOST_LOGIN_UNREADABLE_HINT =
  'On a Mac the server may have been denied Keychain access. Look again, paste an API key, '
  + 'or run `nerve init --terminal` on the server.';
/** The new card appears in the choices above this line, so the line points up. */
export const HOST_LOGIN_FOUND = (source: string) =>
  `A Claude Code login was found (${source}). Choose it above to use it.`;

/**
 * The live test failed and Continue was pressed anyway. The first press
 * warns; the second saves — the same two-step the sk-ant- check uses.
 * "Credential", not "key": Bedrock carries a region, not a key.
 */
export const PROVIDER_TEST_FAILED_WARNING =
  'The last connection test failed, so this credential would be saved unverified. '
  + 'Fix it and test again, or save it anyway.';
/** Appended to a provider summary when the server's last live test failed. */
export const PROVIDER_LAST_TEST_FAILED = 'last connection test failed';

export const PROVIDER_SAVED_PICK_MODEL =
  'Saved. Pick the default model and effort (optional), then Continue.';

export const MODEL_FAMILY_GUIDANCE: Record<string, string> = {
  fable: 'The newest generation; strongest on long, open-ended work.',
  opus: 'Most capable of the classic line; best for open-ended work and long sessions.',
  sonnet: 'Fast and strong; a good default when cost matters.',
  haiku: 'Quickest and cheapest; fine for short, well-defined tasks.',
  other: 'A model this build does not know; use it if your account has access.',
};

/** Under the picker when the instance's model is not Nerve's own default. */
export const MODEL_DEFAULT_LINE = (defaultLabel: string, currentLabel: string) =>
  `Nerve's default is ${defaultLabel}; this instance is set to ${currentLabel}.`;

/** Under the picker: the pick is written as the cron model as well. */
export const MODEL_CRON_LINE = 'Scheduled jobs run on this model too.';

/** Under the picker when agent.cron_model names another model (the installer's default, or one set by hand). */
export const MODEL_CRON_ELSEWHERE = (cronLabel: string) =>
  `Scheduled jobs run on ${cronLabel}; choosing a different model here moves them to it.`;

export const EFFORT_GUIDANCE: Record<string, string> = {
  max: 'The most reasoning and the slowest answers.',
  xhigh: 'Extended reasoning for hard problems.',
  high: 'Careful reasoning for everyday work.',
  medium: 'A balance of speed and reasoning.',
  low: 'Brief reasoning and the fastest answers.',
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

/**
 * The profile step's quiet-hours switch and the sentence under it. Nerve
 * stores the times; the agent is the one that applies them, so the help
 * says that and promises nothing about the server holding messages back.
 */
export const QUIET_HOURS_LABEL = 'Quiet hours';
export const QUIET_HOURS_HELP =
  'Nerve records these for the agent, which keeps non-urgent messages outside them.';
/** The Review's line under the profile row: "Quiet hours: off" or "Quiet hours: 22:00–07:00". */
export const QUIET_HOURS_LINE = (window: { start: string; end: string } | null) =>
  `Quiet hours: ${window ? `${window.start}–${window.end}` : 'off'}`;

export const MEMORY_WITH_EMBEDDINGS =
  'Fast vector search over Nerve\'s memory. Needs an OpenAI API key; OpenAI charges per note.';
export const MEMORY_WITHOUT_EMBEDDINGS =
  'Claude ranks memories when Nerve recalls them. Slower when memory is large.';
export const MEMORY_LOOM =
  'Memory on this server is kept in Loom. Agents read and write it with Loom\'s own tools, and Loom searches it '
  + 'without a key from you, so there is nothing to set up here.';

export const TELEGRAM_STEPS = [
  'Open Telegram and message @BotFather.',
  'Send /newbot, pick a name and a username ending in "bot".',
  'Copy the token BotFather sends back and paste it here.',
];

export const SLACK_INTRO =
  'Needs a Slack app with Socket Mode turned on. Paste its bot token (xoxb-) and its app-level token (xapp-).';
export const SLACK_DISCLOSURE = 'Also connect Slack (advanced)';
/** The primary button on a pending step with nothing entered: it records the skip. */
export const NO_CHANNELS_LABEL = 'Continue without channels';

export const AUTOMATION_IDLE_UNTIL_SOURCE =
  'stays idle until a source is connected (Sources step)';

/** Under a worker's task planner while the mode's default leaves it off. */
export const AUTOMATION_TASK_PLANNER_WORKER =
  'Off for workers until onboarding decides. The onboarding session enables it when this worker '
  + 'should plan through the approval loop.';

/** The Slack card when the shared channel gateway holds the connection: a status line, and only the switch. */
export const SLACK_GATEWAY_LINE = (connected: boolean) =>
  `Managed by your organisation's channel gateway · ${connected ? 'connected' : 'waiting for the gateway'}`;
export const SLACK_GATEWAY_SWITCH = 'The gateway holds the tokens. You can only turn Slack on or off here.';

export const PAIRING_AFTER_RESTART =
  'The bot starts with this token after the restart at the end of setup. You can pair your account on the last setup screen.';

export const SOURCES_IDLE_HINT =
  'Turned on, but stays idle until the tool is set up on the server.';

/** The Sources step's Telegram card: the person's own account, not the bot. */
export const TELEGRAM_SYNC_TITLE = 'Telegram — your own messages';
export const TELEGRAM_SYNC_INTRO =
  'Reads the chats of your own Telegram account. Needs API credentials from my.telegram.org '
  + 'and a one-time sign-in to that account; the sign-in stays on this server.';

/**
 * The sign-in panel. The phone, the code and the password each go to the
 * server once and are not kept; Telegram sends the code to the account's
 * other devices, or by SMS. (Telethon answers the two-step password with a
 * proof derived from it, so the help says the server uses the password,
 * not that it is sent to Telegram.)
 */
export const TELEGRAM_SIGNIN = {
  heading: 'Sign in to your Telegram account',
  saveFirst: 'Save the API credentials first (Continue), then sign in.',
  fallbackPrefix: 'Or on the server:',
  fallbackCommand: 'nerve setup-telegram',
  checking: 'Checking the saved session…',
  busy: 'The sync is using the session right now, so it could not be checked.',
  unreachable: 'The server could not reach Telegram, so the session could not be checked.',
  libraryMissing: 'The server is missing the Telegram library (telethon), so it cannot sign in from here.',
  checkAgain: 'Check again',
  phoneLabel: 'Phone number',
  phoneHelp: 'The number of your Telegram account, with the country code.',
  phoneMissing: 'Enter the phone number of your Telegram account.',
  sendCode: 'Send code',
  sending: 'Sending…',
  codeSent: (phone: string) =>
    `Telegram sent a code to ${phone}. It arrives in the Telegram app on your other devices, or by SMS.`,
  codeValid: (seconds: number) => {
    const minutes = Math.max(1, Math.round(seconds / 60));
    return `The code is valid for ${minutes} ${minutes === 1 ? 'minute' : 'minutes'}.`;
  },
  codeLabel: 'Code',
  codeMissing: 'Enter the code Telegram sent.',
  signIn: 'Sign in',
  signingIn: 'Signing in…',
  passwordLabel: 'Two-step verification password',
  passwordHelp: 'This account has two-step verification. The server uses the password once to sign in and does not keep it.',
  passwordMissing: 'Enter the two-step verification password.',
  startOver: 'Start over',
  signedInAs: (who: string) => `Signed in as ${who}`,
  /** When Telegram names neither a username nor a first name. */
  signedIn: 'Signed in to Telegram',
  signOut: 'Sign out',
  signingOut: 'Signing out…',
  wait: (seconds: number) => `Telegram asks you to wait ${seconds} seconds.`,
  waitUnknown: 'Telegram asks you to wait before trying again.',
  savedSignInNext: 'Saved. Sign in to your Telegram account below, then Continue.',
};

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
  'This page is served over plain HTTP across the network. Anyone on that network can read '
  + 'what you type here, including keys. Put Nerve behind HTTPS or open it through an SSH tunnel.';

export const PASSWORDLESS_LAN_HINT =
  'This instance has no password and accepts connections from other machines. Anyone who can reach it '
  + 'has owner access. Set a password in Accounts, or make Nerve listen on localhost only.';

export const RESTART_HEADLINE = 'Some changes take effect after a restart.';
/** The explainer under it already says the page reconnects; this line does not repeat it. */
export const RESTART_HEADLINE_FIRST_RUN = 'First-time setup needs one restart to finish.';
/**
 * What the Restart button will do, in the future tense: the card is read
 * before the click, so the server's present-tense "Restarting. This page
 * reconnects on its own." belongs to the overlay that follows it.
 */
export const RESTART_EXPLAINER =
  'Restarting stops Nerve and starts it again. This page reconnects by itself.';

/**
 * A restart that cannot be started from here. With a command, the person
 * sees it; without one (a supervisor this build cannot name), the sentence
 * must not promise a command that is not on the screen. No final period:
 * the overlay appends the wait and closes the sentence.
 */
export const UNSUPPORTED_WITH_COMMAND =
  'This instance runs under a supervisor. Someone with access to the server must run '
  + 'the command below. This page reconnects when Nerve is back';
export const UNSUPPORTED_WITHOUT_COMMAND =
  'This instance runs under a supervisor. Whoever manages the supervisor must restart Nerve. '
  + 'This page reconnects when Nerve is back';

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
  if (path === 'memory' || path.startsWith('memory.')) return 'The mode you chose';
  if (path === 'timezone') {
    const change = rest.match(/^'([^']*)'\s*→\s*'([^']*)'$/);
    return change ? `Time zone: ${change[1]} → ${change[2]}` : 'Time zone';
  }
  if (path.startsWith('telegram.')) return 'Telegram bot settings';
  if (path === 'openai_api_key') return 'OpenAI key for memory embeddings';
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
  ? `Hi, I'm ${name}. Tell me what you can do and what you already know about me.`
  : 'Hi. Tell me what you can do and what you already know about me.');

/** The name field's help on an instance behind a front gateway, by where the value came from. */
export const PROFILE_NAME_HELP_EXTERNAL = {
  actor_header: 'Prefilled from your organisation\'s sign-in. This only sets what Nerve writes in USER.md.',
  user_md: 'The name Nerve uses in its notes (USER.md), shared by everyone on this instance.',
  signedInAs: (actor: string) => `You are signed in as ${actor}.`,
};

/** Sources are on but the job that reads them is off; the link text follows. */
export const INBOX_JOB_OFF = 'The Inbox processor job is off. That job reads your sources. Turn it on in';

export const BANNER_STEPS_LEFT = (n: number) =>
  `Finish setting up Nerve — ${n} ${n === 1 ? 'step' : 'steps'} left`;
export const BANNER_RESTART = 'Restart Nerve to apply your setup';

/** The same verb as the Model step's heading, the same noun as the usual empty state. */
export const CHAT_EMPTY_NO_PROVIDER = 'Connect a model to start a conversation';

/**
 * The first screen. The route and the API keep saying "claim"; the person
 * is creating an account, and the words say that, as the "Account" step
 * title does afterwards.
 */
export const CLAIM_HEADING = 'Create your account';
export const CLAIM_SUBMIT = 'Create and continue';
export const CLAIM_SUBMIT_BUSY = 'Creating…';
/** What is still missing when the button is pressed too early. */
export const CLAIM_MISSING = (passwordless: boolean) => (passwordless
  ? 'Enter the setup token to continue.'
  : 'Fill in the username, password and setup token to continue.');

/** The Overview's line under the greeting. */
export const OVERVIEW_SUBLINE = 'The Mode and Model steps are required. You can skip the others and come back to them later.';
export const OVERVIEW_SUBLINE_FINISHED = 'Use Change next to a step to edit it.';
/** The claim row's secondary text; the last word is the link to Accounts. */
export const ACCOUNT_ROW_TEXT = 'Manage people and passwords in';
export const ACCOUNT_ROW_LINK = 'Accounts';

/** Under the still-to-do list on Review while Finish waits for it. */
export const REVIEW_DECIDE_FIRST = 'Open or skip the steps above to finish.';

/** The ready screen's line when the sync is on but nobody has signed in; the link text follows. */
export const READY_TELEGRAM_SYNC_UNSIGNED = 'Telegram message sync: not signed in';
export const READY_TELEGRAM_SYNC_LINK = 'Sign in';

/** The ready screen when optional steps are still open (another tab, a reload). */
export const READY_STEPS_LEFT = (n: number) =>
  `${n} optional ${n === 1 ? 'step is' : 'steps are'} still open. You can finish ${n === 1 ? 'it' : 'them'} later on the setup page.`;

/**
 * A worker's onboarding, in sentences. Never the session id: the link
 * carries it. `ended` is a session that stopped without writing a plan.
 */
export const ONBOARDING_ENDED = 'The onboarding session ended without writing a plan';
export const ONBOARDING_SENTENCES: Record<SetupOnboarding, string> = {
  not_applicable: '',
  pending: 'The onboarding session starts after setup finishes.',
  running: 'Onboarding session running',
  ended: `${ONBOARDING_ENDED}. Open it to see why.`,
  done: 'Onboarding is complete.',
};
export const ONBOARDING_OPEN = 'Open the onboarding session';
export const ONBOARDING_TRY_AGAIN = 'Try again';
