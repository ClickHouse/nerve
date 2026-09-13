import { useEffect, useState, type FormEvent, type ReactNode } from 'react';
import { Link, useLocation } from 'react-router-dom';
import {
  AlertTriangle, Check, CircleDashed, Lock, RefreshCw, ShieldQuestion,
} from '../components/ui/icons';
import { Badge, Button, Checkbox, TextField } from '../components/ui';
import { api, type SetupCron, type SetupStep } from '../api/client';
import { actorName, useActorRef } from '../stores/actorStore';
import { useAuthStore } from '../stores/authStore';
import { useSetupStore, type SetupStoreState } from '../stores/setupStore';

/**
 * The first-run setup wizard: a guarded claim, then a checklist.
 *
 * **Step one is the account, and it is the only required one.** Until it is
 * done the instance has no password and everyone who can reach it is signed in
 * as the owner; claiming it ends that. A browser on the machine needs nothing
 * more, one anywhere else needs the setup token from the server log — which is
 * why the token field is here and optional, rather than a separate screen.
 *
 * **Everything after it is a checklist, not a gate.** Each step can be
 * skipped and re-entered, in any order, and leaving halfway is fine: the
 * instance runs on defaults and the nav keeps a "finish setup" hint until the
 * list is answered. That is deliberate — browser wizards get abandoned, and one
 * that leaves a broken install when it is abandoned is worse than no wizard.
 *
 * **It ends in a restart**, because the settings it writes (timezone, the
 * Telegram token) are read at startup. The page waits for the new process and
 * stays signed in: the session token survives, since the signing secret is
 * pinned and persisted and nothing here rotates it.
 */
export function SetupPage() {
  const authenticated = useAuthStore((s) => s.authenticated);
  const state = useSetupStore((s: SetupStoreState) => s.state);
  const loading = useSetupStore((s: SetupStoreState) => s.loading);
  const error = useSetupStore((s: SetupStoreState) => s.error);
  const reconnecting = useSetupStore((s: SetupStoreState) => s.reconnecting);
  const load = useSetupStore((s: SetupStoreState) => s.load);
  // Before the claim the instance is passwordless, so the browser is signed in
  // anyway and the checklist loads; a tab that arrived with a dead token is the
  // case where it cannot, and there the claim form is the whole page.
  const setupPending = useAuthStore((s) => s.setupPending);

  useEffect(() => { if (authenticated) void load(); }, [authenticated, load]);

  const unclaimed = state ? state.setup_pending : setupPending;

  if (reconnecting) return <Reconnecting />;

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

        {error && (
          <p role="alert" className="text-error text-sm mb-4">{error}</p>
        )}

        {state?.lockdown && (
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

        {!authenticated && (
          <p className="text-2xs text-text-dim mt-8">
            The rest of the checklist appears once this instance has been
            claimed and you are signed in.
          </p>
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

/** The one unauthenticated write in the product, as a form. */
function ClaimCard() {
  const claim = useSetupStore((s: SetupStoreState) => s.claim);
  const busy = useSetupStore((s: SetupStoreState) => s.busy);
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [token, setToken] = useState('');

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    await claim({
      username,
      password,
      display_name: displayName || undefined,
      setup_token: token || undefined,
    });
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
          Choose the username and password you will sign in with. This names the
          account the install already has rather than creating a second one, so
          nothing recorded so far changes hands.
        </p>
      </div>

      <TextField
        value={username}
        onChange={(e) => setUsername(e.target.value)}
        placeholder="username"
        aria-label="Username"
        autoComplete="username"
        autoFocus
        required
      />
      <TextField
        type="password"
        value={password}
        onChange={(e) => setPassword(e.target.value)}
        placeholder="password"
        aria-label="Password"
        autoComplete="new-password"
        required
      />
      <TextField
        value={displayName}
        onChange={(e) => setDisplayName(e.target.value)}
        placeholder="Display name (optional)"
        aria-label="Display name"
        autoComplete="name"
      />

      <div>
        <TextField
          value={token}
          onChange={(e) => setToken(e.target.value)}
          placeholder="Setup token (only from another machine)"
          aria-label="Setup token"
          autoComplete="off"
        />
        <p className="text-2xs text-text-dim mt-1">
          A browser running on the machine itself needs no token. From anywhere
          else, take it from the server log — <code>nerve logs</code>, or{' '}
          <code>docker logs</code> for a container — or from{' '}
          <code>nerve status</code> on the machine.
        </p>
      </div>

      <div>
        <Button
          type="submit"
          variant="primary"
          size="md"
          disabled={busy === 'account' || !username || !password}
        >
          {busy === 'account' ? 'Claiming…' : 'Claim and sign in'}
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
              disabled={busy === step.id}
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
  if (!state) return null;

  const forms: Record<string, ReactNode> = {
    provider: <ProviderForm />,
    profile: <ProfileForm />,
    channels: <ChannelsForm />,
    automation: <AutomationForm crons={state.crons} />,
  };

  return (
    <div className="flex flex-col gap-3">
      <SignedInAs />
      {state.steps.map((step) => (
        <StepCard key={step.id} step={step}>{forms[step.id]}</StepCard>
      ))}
      <RestartCard />
      <p className="text-2xs text-text-dim mt-2">
        The wizard only decides what a running instance can decide for itself.
        Where the workspace lives, how it is deployed and which credential your
        laptop's keychain holds are not among them — those are{' '}
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
 * how the two would disagree. `GET /api/auth/me` answers the same question in
 * one call for a caller that has neither; see the handoff.
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

function ProviderForm() {
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
          disabled={busy === 'provider' || (!anthropic && !openai)}
        >
          Save
        </Button>
      </div>
    </form>
  );
}

function ProfileForm() {
  const save = useSetupStore((s: SetupStoreState) => s.save);
  const busy = useSetupStore((s: SetupStoreState) => s.busy);
  const [timezone, setTimezone] = useState(
    () => Intl.DateTimeFormat().resolvedOptions().timeZone || '',
  );
  const [displayName, setDisplayName] = useState('');

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    await save('profile', () => api.setupProfile({
      timezone: timezone || undefined,
      display_name: displayName || undefined,
    }));
  };

  return (
    <form onSubmit={submit} aria-label="Timezone and name" className="flex flex-col gap-2">
      <p className="text-2xs text-text-dim">
        The time zone is shared configuration — it decides when scheduled work
        runs. The display name is yours, and is what the chat shows beside your
        messages.
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
          disabled={busy === 'profile' || (!timezone && !displayName)}
        >
          Save
        </Button>
      </div>
    </form>
  );
}

function ChannelsForm() {
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
        (<code>nerve pair</code>), not here.
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
          disabled={busy === 'channels' || !token}
        >
          Save
        </Button>
      </div>
    </form>
  );
}

function AutomationForm({ crons }: { crons: SetupCron[] }) {
  const save = useSetupStore((s: SetupStoreState) => s.save);
  const busy = useSetupStore((s: SetupStoreState) => s.busy);
  const [enabled, setEnabled] = useState<string[]>(
    () => crons.filter((c) => c.enabled).map((c) => c.id),
  );
  const [github, setGithub] = useState(false);
  const [gmail, setGmail] = useState(false);

  const toggle = (id: string) => setEnabled((ids) => (
    ids.includes(id) ? ids.filter((x) => x !== id) : [...ids, id]
  ));

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    await save('automation', () => api.setupAutomation({
      crons: enabled, github, gmail,
    }));
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
      <div>
        <Button type="submit" variant="primary" size="sm" disabled={busy === 'automation'}>
          Save
        </Button>
      </div>
    </form>
  );
}

function RestartCard() {
  const state = useSetupStore((s: SetupStoreState) => s.state);
  const restart = useSetupStore((s: SetupStoreState) => s.restart);
  const busy = useSetupStore((s: SetupStoreState) => s.busy);
  if (!state) return null;

  return (
    <section
      aria-label="Restart"
      className="border border-border rounded-lg bg-surface-raised p-3"
    >
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div className="min-w-0">
          <h2 className="text-sm font-medium text-text">Restart to apply</h2>
          <p className="text-2xs text-text-dim mt-1">
            {state.restart_pending
              ? `Waiting on a restart: ${state.restart_pending_paths.join(', ')}.`
              : 'Nothing is waiting on a restart.'}
          </p>
        </div>
        <Button
          variant={state.restart_pending ? 'primary' : 'ghost'}
          size="sm"
          disabled={busy === 'restart'}
          onClick={() => void restart()}
        >
          <RefreshCw size={14} className="mr-1" />
          Restart now
        </Button>
      </div>
      {state.finished && (
        <p className="text-2xs text-text-dim mt-2">
          Setup is finished. <Link to="/chat" className="underline">Go to chat</Link>.
        </p>
      )}
    </section>
  );
}

function Reconnecting() {
  return (
    <div className="flex-1 h-full flex items-center justify-center p-6">
      <div className="max-w-sm text-center">
        <RefreshCw size={22} className="text-text-muted mx-auto mb-3 animate-spin" />
        <h1 className="text-sm font-medium text-text" role="status">
          Restarting this instance…
        </h1>
        <p className="text-2xs text-text-dim mt-2">
          This page reconnects on its own and you stay signed in. It can take a
          few seconds while the database is re-opened.
        </p>
      </div>
    </div>
  );
}

/**
 * The "finish setup" affordance, for an abandoned wizard.
 *
 * An overlay rather than a bar in the layout, like the notification toast it
 * sits beside: every page in this app is a full-height flex box, and a strip
 * inserted above them would cost each one the strip's height. It shows only
 * while the instance is *unclaimed* — the one state that is a real exposure.
 * A half-finished checklist beyond that is not worth a permanent banner; the
 * setup page is a link away and says what is left.
 */
export function SetupReminder() {
  const setupPending = useAuthStore((s) => s.setupPending);
  const { pathname } = useLocation();
  if (!setupPending || pathname.startsWith('/setup')) return null;
  return (
    <Link
      to="/setup"
      className="fixed bottom-4 left-4 z-40 max-w-xs flex items-start gap-2 p-3
        rounded-lg border border-border-subtle bg-surface-raised shadow-xl
        text-2xs text-text-muted hover:text-text"
    >
      <AlertTriangle size={14} className="text-hue-amber shrink-0 mt-0.5" />
      <span>
        This instance has no password — anyone who can reach it is signed in as
        the owner. <span className="text-text underline">Finish setup</span>.
      </span>
    </Link>
  );
}
