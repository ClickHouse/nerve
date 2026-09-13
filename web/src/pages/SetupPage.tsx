import { useEffect, useState, type FormEvent, type ReactNode } from 'react';
import { Link, useLocation, useNavigate } from 'react-router-dom';
import {
  AlertTriangle, Check, CircleDashed, Lock, ShieldQuestion,
} from '../components/ui/icons';
import { Badge, Button, Checkbox, TextField } from '../components/ui';
import {
  api, setToken, type SetupCron, type SetupStep, type SetupValues,
} from '../api/client';
import { errorDetail } from '../stores/accountStore';
import { actorName, useActorRef, useActorStore } from '../stores/actorStore';
import { useAuthStore } from '../stores/authStore';
import {
  setupIsUnfinished, useSetupStore, type SetupStoreState,
} from '../stores/setupStore';

/**
 * First-run setup: a token-guarded claim, then a checklist.
 *
 * **Step one is the account, and it is the only required one.** Until it is
 * done the instance has no password and everyone who can reach it is signed in
 * as the owner; claiming it ends that. Every claim carries the setup token —
 * there is no exemption for a browser on the machine, because being on the
 * machine says nothing about who wrote the page doing the asking.
 *
 * **Everything after it is a checklist, not a gate.** Each step can be
 * skipped and re-entered, in any order, and leaving halfway is fine: the
 * instance runs on defaults and the nav keeps a "finish setup" hint until the
 * list is answered. That is deliberate — browser wizards get abandoned, and one
 * that leaves a broken install when it is abandoned is worse than no wizard.
 *
 * **It ends with a restart somebody else performs.** The settings it writes
 * (timezone, the Telegram token) are read at startup, so the page names them
 * and prints the command; applying it is `nerve restart` on the server. This
 * page holds no authority over the process it is talking to.
 */
export function SetupPage() {
  const authenticated = useAuthStore((s) => s.authenticated);
  // Unclaimed is the descriptor's answer, not a guess from the token: `none`
  // means the one account has no password, which is the state the claim ends.
  const unclaimed = useAuthStore((s) => s.loginMode === 'none');
  const state = useSetupStore((s: SetupStoreState) => s.state);
  const loading = useSetupStore((s: SetupStoreState) => s.loading);
  const error = useSetupStore((s: SetupStoreState) => s.error);
  const load = useSetupStore((s: SetupStoreState) => s.load);

  useEffect(() => {
    if (authenticated && !unclaimed) void load();
  }, [authenticated, unclaimed, load]);

  return (
    <div className="flex-1 h-full overflow-auto">
      <div className="max-w-2xl w-full mx-auto p-4 lg:p-6">
        <header className="flex items-center gap-3 mb-2 mt-4">
          <ShieldQuestion size={22} className="text-hue-amber" />
          <h1 className="text-xl font-semibold text-text">
            {unclaimed ? 'Set up this instance' : 'Finish setting up'}
          </h1>
        </header>
        <p className="text-sm text-text-muted mb-6">
          {unclaimed
            ? 'This instance has no password yet, so everyone who can reach it '
              + 'is signed in as the owner and no activity can be told apart. '
              + 'Claiming it is the one step that matters; the rest can wait.'
            : 'Nothing here is required. Every step can be skipped, re-entered '
              + 'later, or done from the command line instead.'}
        </p>

        {!unclaimed && error && (
          <p role="alert" className="text-error text-sm mb-4">{error}</p>
        )}

        {!unclaimed && state?.warning && (
          <Notice tone="warning" icon={<AlertTriangle size={14} />}>
            {state.warning}
          </Notice>
        )}

        {!unclaimed && state?.lockdown && (
          <Notice tone="warning" icon={<Lock size={14} />}>
            {state.read_only_reason
              ?? 'This instance is in lockdown: its configuration is '
                + 'fleet-managed, so the checklist is read-only here.'}
          </Notice>
        )}

        {unclaimed && <ClaimCard />}

        {authenticated && !unclaimed && (
          loading && !state
            ? <p className="text-sm text-text-dim">Loading…</p>
            : <Checklist />
        )}
      </div>
    </div>
  );
}

function Notice(
  { tone, icon, children }:
  { tone: 'warning' | 'info'; icon: ReactNode; children: ReactNode },
) {
  return (
    <div
      role="status"
      className="flex gap-2 items-start border border-border rounded-lg
        bg-surface-raised p-3 mb-4"
    >
      <span className={tone === 'warning' ? 'text-hue-amber mt-0.5' : 'text-text-muted mt-0.5'}>
        {icon}
      </span>
      <p className="text-sm text-text-muted">{children}</p>
    </div>
  );
}

/**
 * The one unauthenticated write in the product, as a form.
 *
 * Not in the checklist store: it is guarded by the setup token rather than by
 * a session, so it shares no state with the authenticated steps and the token
 * never leaves this component. It is cleared from form state the moment the
 * response arrives, and it is never persisted anywhere.
 */
function ClaimCard() {
  const navigate = useNavigate();
  const checkAuth = useAuthStore((s) => s.checkAuth);
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [setupToken, setSetupToken] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const claimed = await api.setupClaim({
        username,
        password,
        setup_token: setupToken,
        display_name: displayName.trim() || undefined,
      });
      // Persist only the client session. The setup token remains form state
      // and is discarded as soon as the claim response arrives.
      setSetupToken('');
      setToken(claimed.token);
      await checkAuth();
      await useActorStore.getState().refresh();
      // Stay on this page: the same route is the checklist once the instance
      // is claimed, and the tab that just secured the account is the one with
      // the context for the rest of it. Nothing here is required, and the
      // nav keeps a way back if it is closed.
      navigate('/setup', { replace: true });
    } catch (cause) {
      setError(errorDetail(cause, 'Could not claim this instance'));
    } finally {
      setBusy(false);
    }
  };

  return (
    <form
      onSubmit={submit}
      aria-label="Claim this instance"
      className="border border-border rounded-lg bg-surface-raised p-4 flex flex-col gap-3"
    >
      <div>
        <h2 className="text-sm font-medium text-text">Claim this instance</h2>
        <p className="text-2xs text-text-dim mt-1">
          Choose the username and password for the account this installation
          already created. This names the existing account rather than creating
          a second one, so nothing recorded so far changes hands. The display
          name is optional.
        </p>
      </div>

      {error && <p role="alert" className="text-error text-sm">{error}</p>}

      <TextField
        value={username}
        onChange={(event) => setUsername(event.target.value)}
        placeholder="Username"
        aria-label="Username"
        autoComplete="username"
        autoFocus
        required
      />
      <TextField
        type="password"
        value={password}
        onChange={(event) => setPassword(event.target.value)}
        placeholder="Password"
        aria-label="Password"
        autoComplete="new-password"
        required
      />
      <TextField
        value={displayName}
        onChange={(event) => setDisplayName(event.target.value)}
        placeholder="Display name (optional)"
        aria-label="Display name"
        autoComplete="name"
      />
      <TextField
        type="password"
        value={setupToken}
        onChange={(event) => setSetupToken(event.target.value)}
        placeholder="Setup token"
        aria-label="Setup token"
        autoComplete="one-time-code"
        required
      />

      <p className="text-2xs text-text-dim">
        Read the token with <code>nerve status</code> on the server. For
        remote setup, open this page only over HTTPS or a protected tunnel;
        the setup token is a bearer credential.
      </p>

      <div>
        <Button
          type="submit"
          variant="primary"
          size="md"
          disabled={busy || !username || !password || !setupToken}
        >
          {busy ? 'Claiming…' : 'Claim and sign in'}
        </Button>
      </div>
    </form>
  );
}

function StatusBadge({ status }: { status: SetupStep['status'] }) {
  if (status === 'done') {
    return <Badge tone="success"><Check size={11} className="mr-1" />done</Badge>;
  }
  if (status === 'skipped') return <Badge tone="neutral">skipped</Badge>;
  return <Badge tone="warning"><CircleDashed size={11} className="mr-1" />to do</Badge>;
}

function StepCard(
  { step, children }: { step: SetupStep; children?: ReactNode },
) {
  const skip = useSetupStore((s: SetupStoreState) => s.skip);
  const busy = useSetupStore((s: SetupStoreState) => s.busy);
  const writable = useSetupStore((s: SetupStoreState) => s.state?.writable ?? false);
  const [open, setOpen] = useState(step.status === 'pending');

  return (
    <section
      aria-label={step.title}
      className="border border-border rounded-lg bg-surface-raised p-3"
    >
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div className="min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <h2 className="text-sm font-medium text-text">{step.title}</h2>
            <StatusBadge status={step.status} />
          </div>
          {step.detail && (
            <p className="text-2xs text-text-dim mt-1">{step.detail}</p>
          )}
        </div>
        <div className="flex items-center gap-2">
          {children && (
            <Button variant="ghost" size="sm" onClick={() => setOpen((o) => !o)}>
              {open ? 'Close' : step.status === 'pending' ? 'Set up' : 'Change'}
            </Button>
          )}
          {step.can_skip && (
            <Button
              variant="ghost"
              size="sm"
              // Any write in flight, not just this card's: the server takes
              // one at a time and the checklist it returns is the whole
              // state, so a second submission would be answered with a view
              // that does not include it yet.
              disabled={busy !== null}
              onClick={() => void skip(step.id, step.status !== 'skipped')}
            >
              {step.status === 'skipped' ? 'Put back' : 'Skip'}
            </Button>
          )}
        </div>
      </div>

      {open && children && (
        <div className="mt-3">
          {writable ? children : (
            <p className="text-2xs text-text-dim">
              This instance is read-only here.
            </p>
          )}
        </div>
      )}
    </section>
  );
}

function Checklist() {
  const state = useSetupStore((s: SetupStoreState) => s.state);
  const accountKey = useAuthStore((s) => s.account?.id ?? 'nobody');
  if (!state) return null;

  // Keyed by the account the values describe, so a different person signing
  // in remounts the forms instead of inheriting the last one's name in a
  // field they never typed in — the state a form keeps is `useState`, which
  // no amount of new props resets.
  const forms: Record<string, ReactNode> = {
    provider: <ProviderForm key={accountKey} values={state.values} />,
    profile: <ProfileForm key={accountKey} values={state.values} />,
    channels: <ChannelsForm key={accountKey} values={state.values} />,
    automation: (
      <AutomationForm key={accountKey} crons={state.crons} values={state.values} />
    ),
  };

  return (
    <div className="flex flex-col gap-3">
      <SignedInAs />
      {state.steps.map((step) => (
        <StepCard key={step.id} step={step}>{forms[step.id]}</StepCard>
      ))}
      <RestartNotice />
      <p className="text-2xs text-text-dim mt-2">
        The checklist only decides what a running instance can decide for
        itself. Where the workspace lives, how it is deployed and which
        credential your laptop's keychain holds are not among them — those are{' '}
        <code>nerve init</code> on the machine. Background:{' '}
        <code>docs/setup.md</code>.
      </p>
    </div>
  );
}

/**
 * Who you are signed in as.
 *
 * From the identity the auth store already confirmed and the actor map that
 * already knows every name — not from a second request of its own. The store's
 * binding is the security-relevant one (a session is only bound to an account
 * the server confirmed it belongs to), and re-deriving "who am I" beside it is
 * how the two would disagree.
 */
function SignedInAs() {
  const username = useAuthStore((s) => s.account?.username ?? null);
  const actorId = useAuthStore((s) => s.account?.actor_id ?? null);
  const actor = useActorRef(actorId);

  if (!actorId && !username) return null;
  const name = actor ? actorName(actor) : (username ?? '');
  if (!name) return null;
  return <p className="text-2xs text-text-dim">Signed in as {name}.</p>;
}

function ProviderForm({ values }: { values: SetupValues }) {
  const save = useSetupStore((s: SetupStoreState) => s.save);
  const busy = useSetupStore((s: SetupStoreState) => s.busy);
  const [anthropic, setAnthropic] = useState('');
  const [openai, setOpenai] = useState('');

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    if (await save('provider', () => api.setupProvider({
      anthropic_api_key: anthropic || undefined,
      openai_api_key: openai || undefined,
    }))) {
      setAnthropic('');
      setOpenai('');
    }
  };

  return (
    <form onSubmit={submit} aria-label="Provider credential" className="flex flex-col gap-2">
      <p className="text-2xs text-text-dim">
        Stored in <code>config.local.yaml</code>, which only this user can read.
        A browser cannot reach your laptop's keychain or{' '}
        <code>~/.claude/.credentials.json</code>, so paste a key here or set one
        up with <code>nerve init</code> on the machine instead.
        {(values.has_anthropic_key || values.has_openai_key) && (
          <> A key is already configured; anything you type here replaces it,
            and a field left blank leaves it alone.</>
        )}
      </p>
      <TextField
        type="password"
        value={anthropic}
        onChange={(e) => setAnthropic(e.target.value)}
        placeholder="Anthropic API key"
        aria-label="Anthropic API key"
        autoComplete="off"
      />
      <TextField
        type="password"
        value={openai}
        onChange={(e) => setOpenai(e.target.value)}
        placeholder="OpenAI API key (embeddings, optional)"
        aria-label="OpenAI API key"
        autoComplete="off"
      />
      <div>
        <Button
          type="submit"
          variant="primary"
          size="sm"
          disabled={busy !== null || (!anthropic && !openai)}
        >
          Save
        </Button>
      </div>
    </form>
  );
}

const browserZone = Intl.DateTimeFormat().resolvedOptions().timeZone || '';

function ProfileForm({ values }: { values: SetupValues }) {
  const save = useSetupStore((s: SetupStoreState) => s.save);
  const busy = useSetupStore((s: SetupStoreState) => s.busy);
  // Opened on what the instance says, not on what this browser thinks: the
  // timezone here is the one scheduled work runs in, and a form that opens on
  // the browser's guess submits that guess every time somebody edits their
  // name.
  const [timezone, setTimezone] = useState(values.timezone);
  const [displayName, setDisplayName] = useState(values.display_name ?? '');

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    // Only what changed. Each field is written independently on the server,
    // so an untouched one is genuinely untouched.
    const body: { timezone?: string; display_name?: string } = {};
    if (timezone && timezone !== values.timezone) body.timezone = timezone;
    if (displayName !== (values.display_name ?? '')) body.display_name = displayName;
    if (!body.timezone && body.display_name === undefined) return;
    await save('profile', () => api.setupProfile(body));
  };

  const changed = (
    (!!timezone && timezone !== values.timezone)
    || displayName !== (values.display_name ?? '')
  );

  return (
    <form onSubmit={submit} aria-label="Timezone and name" className="flex flex-col gap-2">
      <p className="text-2xs text-text-dim">
        The time zone is shared configuration — it decides when scheduled work
        runs, so it is left alone unless you change it here. The display name
        is yours, and is what the chat shows beside your messages.
        {browserZone && browserZone !== values.timezone && (
          <>
            {' '}This browser is in{' '}
            <button
              type="button"
              className="underline"
              onClick={() => setTimezone(browserZone)}
            >
              {browserZone}
            </button>.
          </>
        )}
      </p>
      <TextField
        value={timezone}
        onChange={(e) => setTimezone(e.target.value)}
        placeholder="Europe/Berlin"
        aria-label="Time zone"
        autoComplete="off"
      />
      <TextField
        value={displayName}
        onChange={(e) => setDisplayName(e.target.value)}
        placeholder="Display name"
        aria-label="Your display name"
        autoComplete="name"
      />
      <div>
        <Button
          type="submit"
          variant="primary"
          size="sm"
          disabled={busy !== null || !changed}
        >
          Save
        </Button>
      </div>
    </form>
  );
}

function ChannelsForm({ values }: { values: SetupValues }) {
  const save = useSetupStore((s: SetupStoreState) => s.save);
  const busy = useSetupStore((s: SetupStoreState) => s.busy);
  const [token, setToken] = useState('');

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    if (await save('channels', () => api.setupChannels({
      telegram_bot_token: token,
    }))) setToken('');
  };

  return (
    <form onSubmit={submit} aria-label="Telegram" className="flex flex-col gap-2">
      <p className="text-2xs text-text-dim">
        A bot token from <code>@BotFather</code>. Stored with the other secrets;
        who may talk to the bot is decided afterwards by pairing
        (<code>nerve pair</code>), not here — saving a token leaves the people
        already paired alone.
        {values.has_telegram_token && <> A token is already configured.</>}
      </p>
      <TextField
        type="password"
        value={token}
        onChange={(e) => setToken(e.target.value)}
        placeholder="Telegram bot token"
        aria-label="Telegram bot token"
        autoComplete="off"
      />
      <div>
        <Button
          type="submit"
          variant="primary"
          size="sm"
          disabled={busy !== null || !token.trim()}
        >
          Save
        </Button>
      </div>
    </form>
  );
}

function AutomationForm(
  { crons, values }: { crons: SetupCron[]; values: SetupValues },
) {
  const save = useSetupStore((s: SetupStoreState) => s.save);
  const busy = useSetupStore((s: SetupStoreState) => s.busy);
  const [enabled, setEnabled] = useState<string[]>(
    () => crons.filter((c) => c.enabled).map((c) => c.id),
  );
  // Hydrated, so re-entering this step to change a cron does not submit
  // switched-off sources somebody configured on another screen.
  const [github, setGithub] = useState(values.sync_github);
  const [gmail, setGmail] = useState(values.sync_gmail);
  const [telegram, setTelegram] = useState(values.sync_telegram);
  // Telegram's inbox needs its own API credentials (my.telegram.org), which
  // are not the bot token: the bot is how Nerve talks *as* you, these are how
  // it reads your own messages.
  const [apiId, setApiId] = useState('');
  const [apiHash, setApiHash] = useState('');

  const toggle = (id: string) => setEnabled((ids) => (
    ids.includes(id) ? ids.filter((x) => x !== id) : [...ids, id]
  ));

  const before = crons.filter((c) => c.enabled).map((c) => c.id).sort().join(',');
  const cronsChanged = [...enabled].sort().join(',') !== before;

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    // Only what changed: the server leaves an omitted field exactly as it is,
    // and that is the whole point of the shape.
    const body: {
      crons?: string[]; github?: boolean; gmail?: boolean; telegram?: boolean;
      telegram_api_id?: number; telegram_api_hash?: string;
    } = {};
    if (cronsChanged) body.crons = enabled;
    if (github !== values.sync_github) body.github = github;
    if (gmail !== values.sync_gmail) body.gmail = gmail;
    if (telegram !== values.sync_telegram) body.telegram = telegram;
    // Sent whenever they were typed, whether or not the switch moved: they
    // are what makes the source work, and re-entering the step to supply
    // them is exactly what somebody does after turning it on.
    if (apiId.trim()) body.telegram_api_id = Number(apiId.trim());
    if (apiHash.trim()) body.telegram_api_hash = apiHash.trim();
    if (await save('automation', () => api.setupAutomation(body))) {
      setApiId('');
      setApiHash('');
    }
  };

  return (
    <form onSubmit={submit} aria-label="Automation" className="flex flex-col gap-2">
      <p className="text-2xs text-text-dim">
        Scheduled work the agent does on its own. Each one is off until you say
        otherwise, and can be changed later on the cron screen.
      </p>
      {crons.length === 0 && (
        <p className="text-2xs text-text-dim">
          This install has no optional crons to offer.
        </p>
      )}
      {crons.map((cron) => (
        <Checkbox
          key={cron.id}
          checked={enabled.includes(cron.id)}
          onChange={() => toggle(cron.id)}
          label={<span title={cron.description}>{cron.name}</span>}
          labelSize="sm"
        />
      ))}
      <p className="text-2xs text-text-dim mt-2">
        Sources to poll. Each still needs its own credentials before it can
        sync, which the sources screen collects.
      </p>
      <Checkbox
        checked={github}
        onChange={(e) => setGithub(e.target.checked)}
        label="GitHub"
        labelSize="sm"
      />
      <Checkbox
        checked={gmail}
        onChange={(e) => setGmail(e.target.checked)}
        label="Gmail"
        labelSize="sm"
      />
      <Checkbox
        checked={telegram}
        onChange={(e) => setTelegram(e.target.checked)}
        label="Telegram"
        labelSize="sm"
      />
      {telegram && (
        <div className="flex flex-col gap-2 pl-5">
          <p className="text-2xs text-text-dim">
            Reading your Telegram messages needs its own API credentials from{' '}
            <code>my.telegram.org</code> — not the bot token above. Leave these
            blank if they are already configured.
          </p>
          <TextField
            value={apiId}
            onChange={(e) => setApiId(e.target.value)}
            placeholder="API id"
            aria-label="Telegram API id"
            inputMode="numeric"
            autoComplete="off"
          />
          <TextField
            type="password"
            value={apiHash}
            onChange={(e) => setApiHash(e.target.value)}
            placeholder="API hash"
            aria-label="Telegram API hash"
            autoComplete="off"
          />
        </div>
      )}
      <div>
        <Button type="submit" variant="primary" size="sm" disabled={busy !== null}>
          Save
        </Button>
      </div>
    </form>
  );
}

/**
 * What is still waiting on a restart, and who performs it.
 *
 * There is no button. Restarting the daemon is an operator action on the
 * server: a browser that could do it would hold process control over the box
 * for the sake of a convenience, and the checklist is just as usable when it
 * names the command instead. The page picks the result up on its next read,
 * because "pending" is a comparison between what is on disk and what the
 * process is running rather than a flag anybody has to clear.
 */
function RestartNotice() {
  const state = useSetupStore((s: SetupStoreState) => s.state);
  if (!state) return null;
  const waiting = [
    ...state.restart_pending_paths,
    ...state.restart_pending_reasons,
  ];

  return (
    <section
      aria-label="Restart to apply"
      className="border border-border rounded-lg bg-surface-raised p-3"
    >
      <h2 className="text-sm font-medium text-text">Restart to apply</h2>
      {state.restart_pending ? (
        <>
          <p className="text-2xs text-text-dim mt-1">
            Waiting on a restart: {waiting.join('; ')}.
          </p>
          <p className="text-2xs text-text-dim mt-2">
            Run <code>{state.restart_command}</code> on the server, then reload
            this page. You stay signed in — a restart does not end your session.
          </p>
        </>
      ) : (
        <p className="text-2xs text-text-dim mt-1">
          Nothing is waiting on a restart.
        </p>
      )}
      {state.finished && (
        <p className="text-2xs text-text-dim mt-2">
          Setup is finished. <Link to="/chat" className="underline">Go to chat</Link>.
        </p>
      )}
    </section>
  );
}

/**
 * The "finish setup" affordance, for an abandoned checklist.
 *
 * An overlay rather than a bar in the layout, like the notification toast it
 * sits beside: every page in this app is a full-height flex box, and a strip
 * inserted above them would cost each one the strip's height.
 *
 * It follows the checklist, not the claim. Hiding it the moment a password
 * exists is how an abandoned *post-claim* checklist becomes invisible —
 * which is the state the affordance is for, since an unclaimed instance
 * already routes to this page by itself. Gone for good once the server says
 * the list is finished, and never on the page it points at.
 */
export function SetupReminder() {
  const unclaimed = useAuthStore((s) => s.loginMode === 'none');
  const unfinished = useSetupStore(setupIsUnfinished);
  const { pathname } = useLocation();
  if ((!unclaimed && !unfinished) || pathname.startsWith('/setup')) return null;
  return (
    <Link
      to="/setup"
      className="fixed bottom-4 left-4 z-40 max-w-xs flex items-start gap-2 p-3
        rounded-lg border border-border-subtle bg-surface-raised shadow-xl
        text-2xs text-text-muted hover:text-text"
    >
      <AlertTriangle size={14} className="text-hue-amber shrink-0 mt-0.5" />
      <span>
        {unclaimed
          ? 'This instance has no password — anyone who can reach it is signed '
            + 'in as the owner. '
          : 'Setup is not finished on this instance. '}
        <span className="text-text underline">Finish setup</span>.
      </span>
    </Link>
  );
}
