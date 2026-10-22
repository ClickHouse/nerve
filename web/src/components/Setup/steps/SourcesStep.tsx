import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import {
  api, type SecretOut, type SetupAutomationDetail, type SetupSourceId, type SetupSourcesDetail, type SetupState,
  type SetupStep,
} from '../../../api/client';
import { unsavedFor, useSetupStore } from '../../../stores/setupStore';
import { Button, Checkbox, TextField } from '../../ui';
import { Github, Mail, MessageCircle, RefreshCw, X } from '../../ui/icons';
import {
  GITHUB_MANAGED_ON, INBOX_JOB_OFF, MANAGED_SOURCE_TEXT, SOURCES_IDLE_HINT, STEP_HEADINGS, TELEGRAM_SIGNIN,
  TELEGRAM_SYNC_INTRO, TELEGRAM_SYNC_TITLE, managedLine,
} from '../copy';
import { managedSource, secretManaged, type ManagedSource } from '../credentialSource';
import { CodeText, Field } from '../Field';
import { stepOf, stopPath } from '../navigation';
import { Notice } from '../Notice';
import { secretPayload } from '../payload';
import { SecretField } from '../SecretField';
import { StepFrame } from '../StepFrame';
import { TelegramSyncSignIn } from '../TelegramSyncSignIn';

const EMAIL_RE = /^[^\s@-][^\s@]*@[^\s@]+\.[^\s@]+$/;
/** The optional job that reads the sources; off by default on a first run with none connected. */
const INBOX_JOB = 'inbox-processor';
/** What an older backend, which says nothing about availability, offers. */
const ALL_SOURCES: SetupSourceId[] = ['github', 'gmail', 'telegram'];
/** The sources as the button that switches them off names them. */
const SOURCE_NAMES: Record<SetupSourceId, string> = { github: 'GitHub', gmail: 'Gmail', telegram: 'Telegram' };
/** Secondary controls reach the 44 px phone target without changing the desktop density. */
const TOUCH = 'min-h-[44px] md:min-h-0';

/** What the form holds; a refused save keeps it for the form remounted after it (`unsavedFor`). */
interface SourcesForm {
  github: boolean;
  gmail: boolean;
  telegram: boolean;
  accounts: string[];
  newAccount: string;
  apiId: string;
  apiHash: string | null;
}

/** A value the operator provided, in the place its field would be. */
function ManagedValue({ label, secret, source }: { label: string; secret?: SecretOut; source: ManagedSource }) {
  return (
    <div className="flex flex-col gap-1">
      <span className="text-sm font-medium text-text">{label}</span>
      <p className="text-sm text-text-muted">
        {MANAGED_SOURCE_TEXT[source]}
        {secret?.last4 ? <> · <span className="font-mono">…{secret.last4}</span></> : null}
      </p>
    </div>
  );
}

/**
 * GitHub, Gmail and Telegram message sync as toggle cards with the server's
 * preflight beside each. Always sends all three booleans — "none of these"
 * is a decision too.
 *
 * The Telegram sync needs more than the API id and hash: a one-time sign-in
 * of the person's own account, which creates the session file on the
 * server. Once the credentials are saved the card shows the sign-in panel;
 * a Continue that saves new credentials therefore **stays** on the step
 * (with a notice) rather than moving on with a sync that cannot read
 * anything yet. The Ready screen points back here when it is still unsigned.
 *
 * A credential the operator provided (environment, configuration
 * repository) renders as managed: the line says so, there is no field. So
 * does a GitHub source whose switch the operator holds in the machine's
 * configuration while the channel gateway polls GitHub
 * (`detail.github.managed`): the card says it is on and who provides it,
 * with no switch, and the step's reason says what decides. A source held
 * off that way is not shown; the reason says so.
 *
 * On an instance that keeps no credentials on disk (`local_secrets: false`)
 * the server says which sources the page may switch (`detail.available`):
 * a GitHub token from the environment keeps the GitHub toggle alone on
 * offer, with the server's reason above it; the others are not shown unless
 * the operator switched one on, and then only as a disabled card. The save
 * carries the offered booleans only — the server refuses the rest.
 */
export function SourcesStep({ step, state }: { step: SetupStep; state: SetupState }) {
  const detail = step.detail as SetupSourcesDetail;
  const available = detail.available ?? ALL_SOURCES;
  const offers = (id: SetupSourceId) => available.includes(id);
  const fieldError = useSetupStore((s) => s.fieldError);
  // A refused switch has no field to stand under (a form opened before the
  // operator took the GitHub switch over still sends it), so it gets a line
  // of its own, as the other steps give theirs.
  const switchError = fieldError && (ALL_SOURCES as string[]).includes(fieldError.field) ? fieldError.detail : null;
  // Said once: the frame already shows the sentence when it is the step's
  // own read-only or blocked reason, and the refusal when it is the same.
  const partial = available.length > 0 && available.length < ALL_SOURCES.length;
  const unavailableReason = partial && detail.unavailable_reason
    && detail.unavailable_reason !== step.read_only_reason && detail.unavailable_reason !== step.blocked_reason
    && detail.unavailable_reason !== switchError
    ? detail.unavailable_reason : null;
  const save = useSetupStore((s) => s.save);
  const load = useSetupStore((s) => s.load);
  const preflight = useSetupStore((s) => s.preflight);
  const loadPreflight = useSetupStore((s) => s.loadPreflight);
  const savedNotice = useSetupStore((s) => s.savedNotice);
  const noteSaved = useSetupStore((s) => s.noteSaved);
  const locked = step.read_only || step.status === 'blocked';
  // A Telegram sync the operator switched on where the page may not: shown, not editable.
  const telegramLocked = locked || !offers('telegram');

  const githubManaged = managedSource(detail.github.source);
  // The operator holds the GitHub switch: never offered, so never in the save.
  const githubSwitchManaged = detail.github.managed === true;
  const gmailManaged = managedSource(detail.gmail.source);
  const apiIdManaged = secretManaged(detail.telegram.api_id);
  const apiHashManaged = secretManaged(detail.telegram.api_hash);
  const credentialsSaved = detail.telegram.api_id.configured && detail.telegram.api_hash.configured;

  // A save the server refused re-reads the state, which remounts this form
  // when the step changed (GitHub no longer on offer, say). The form starts
  // from what the person had chosen wherever it still lets them change it,
  // so the refusal does not also discard their other changes.
  const [kept] = useState(() => unsavedFor(step) as Partial<SourcesForm> | null);
  const keep = <T,>(editable: boolean, mine: T | undefined, server: T): T => (editable && mine !== undefined ? mine : server);
  const [github, setGithub] = useState(() => keep(offers('github'), kept?.github, detail.github.enabled));
  const [gmail, setGmail] = useState(() => keep(offers('gmail'), kept?.gmail, detail.gmail.enabled));
  const [telegram, setTelegram] = useState(() => keep(offers('telegram'), kept?.telegram, detail.telegram.enabled));
  const [accounts, setAccounts] = useState<string[]>(
    () => keep(offers('gmail') && !gmailManaged, kept?.accounts, detail.gmail.accounts),
  );
  const [newAccount, setNewAccount] = useState(() => keep(offers('gmail') && !gmailManaged, kept?.newAccount, ''));
  const [apiId, setApiId] = useState(() => keep(offers('telegram') && !apiIdManaged, kept?.apiId, ''));
  const [apiHash, setApiHash] = useState<string | null>(() => keep(offers('telegram') && !apiHashManaged, kept?.apiHash, ''));
  const [clientError, setClientError] = useState<{ field: string; detail: string } | null>(null);
  const [rechecking, setRechecking] = useState(false);

  useEffect(() => { if (kept) useSetupStore.getState().dropUnsaved(); }, [kept]);
  useEffect(() => { void loadPreflight(); }, [loadPreflight]);

  // After credentials are saved the server's answer remounts this form and
  // the sign-in panel is what comes next, so its heading takes focus.
  useEffect(() => {
    if (savedNotice === 'sources') document.getElementById('setup-telegram-signin-heading')?.focus();
  }, [savedNotice]);

  const errFor = (field: string) =>
    clientError?.field === field ? clientError.detail
      : fieldError?.field === field ? fieldError.detail : null;

  const addAccount = () => {
    const address = newAccount.trim();
    if (!address) return;
    if (!EMAIL_RE.test(address)) { setClientError({ field: 'gmail_accounts', detail: 'That is not an email address.' }); return; }
    if (!accounts.includes(address)) setAccounts([...accounts, address]);
    setNewAccount('');
    setClientError(null);
  };

  const submit = async () => {
    setClientError(null);
    const body: Record<string, unknown> = {};
    if (offers('github')) body.github = github;
    if (offers('gmail')) {
      body.gmail = gmail;
      if (gmail && !gmailManaged) {
        const list = newAccount.trim() && EMAIL_RE.test(newAccount.trim()) && !accounts.includes(newAccount.trim())
          ? [...accounts, newAccount.trim()] : accounts;
        if (list.length === 0) { setClientError({ field: 'gmail_accounts', detail: 'Add at least one Gmail address.' }); return false; }
        body.gmail_accounts = list;
      }
    }
    if (offers('telegram')) {
      body.telegram = telegram;
      if (telegram) {
        const id = apiId.trim();
        if (id) {
          const n = Number(id);
          if (!Number.isInteger(n) || n <= 0) { setClientError({ field: 'telegram_api_id', detail: 'The API id is a positive number.' }); return false; }
          body.telegram_api_id = n;
        } else if (!detail.telegram.api_id.configured) {
          setClientError({ field: 'telegram_api_id', detail: 'Telegram sync needs the API id from my.telegram.org.' }); return false;
        }
        const hash = secretPayload(apiHash);
        if (hash) body.telegram_api_hash = hash;
        else if (!detail.telegram.api_hash.configured) {
          setClientError({ field: 'telegram_api_hash', detail: 'Telegram sync needs the API hash from my.telegram.org.' }); return false;
        }
      }
    }
    const credentialsSent = body.telegram_api_id !== undefined || body.telegram_api_hash !== undefined;
    const form: SourcesForm = { github, gmail, telegram, accounts, newAccount, apiId, apiHash };
    const ok = await save('sources', () => api.setupStep('sources', body), form);
    if (!ok) return false;
    setApiId('');
    setApiHash('');
    if (telegram && credentialsSent) {
      // Stay: the sync still needs the sign-in, and the panel for it has
      // appeared below. (The server's answer usually remounts this form;
      // the notice lives in the store, so it survives that.)
      const latest = useSetupStore.getState().state;
      const after = latest ? stepOf(latest, 'sources')?.detail as SetupSourcesDetail | undefined : undefined;
      const saved = !!after?.telegram.api_id.configured && !!after?.telegram.api_hash.configured;
      if (saved && after?.telegram.session?.authorized !== true) {
        noteSaved('sources');
        return false;
      }
    }
    return true;
  };

  const recheck = async () => {
    setRechecking(true);
    try { await loadPreflight(); } finally { setRechecking(false); }
  };

  // Hosted GitHub is polled by the channel gateway: nothing to install or sign in here.
  const githubHosted = preflight?.github.hosted === true;
  const githubOk = preflight ? githubHosted || (preflight.github.cli && preflight.github.authenticated) : null;
  const gmailOk = preflight ? preflight.gmail.cli : null;

  // A first run turns the inbox job off because nothing was connected; a
  // source switched on here would then be read by no job, and nothing else
  // points back at Automation. Say so, with the way there.
  const automation = stepOf(state, 'automation')?.detail as SetupAutomationDetail | undefined;
  const inboxJobOff = automation?.optional.some((job) => job.id === INBOX_JOB && !job.enabled) ?? false;
  const inboxWarning = (github || gmail || telegram) && inboxJobOff;
  // "All" only while every source is on offer: one the operator keeps on (a
  // GitHub held in the machine's configuration) stays on, so the button names
  // the ones it switches.
  const turnOffLabel = available.length === ALL_SOURCES.length
    ? 'Turn all off' : `Turn ${available.map((id) => SOURCE_NAMES[id]).join(' and ')} off`;
  const dirty = github !== detail.github.enabled || gmail !== detail.gmail.enabled
    || telegram !== detail.telegram.enabled || accounts.join() !== detail.gmail.accounts.join()
    || newAccount !== '' || apiId !== '' || (apiHash ?? '') !== '';

  const card = 'flex flex-col gap-2 rounded-lg border border-border bg-surface-raised p-4';

  return (
    <StepFrame
      step={step}
      heading={STEP_HEADINGS.sources.heading}
      intro={STEP_HEADINGS.sources.intro}
      onSubmit={submit}
      readOnly={locked}
      dirty={dirty}
    >
      <div className="flex items-center justify-between gap-2">
        <p className="text-xs text-text-dim">
          {preflight ? `Checked on the server at ${new Date(preflight.checked_at).toLocaleTimeString()}` : 'Checking the server…'}
        </p>
        <Button variant="ghost" size="xs" onClick={() => void recheck()} disabled={rechecking} className={TOUCH}>
          <RefreshCw size={12} className={rechecking ? 'animate-spin' : undefined} />Re-check
        </Button>
      </div>

      {unavailableReason && <Notice tone="locked">{unavailableReason}</Notice>}

      {savedNotice === 'sources' && <Notice tone="info">{TELEGRAM_SIGNIN.savedSignInNext}</Notice>}

      {inboxWarning && (
        <Notice tone="warning">
          {INBOX_JOB_OFF}{' '}
          <Link to={stopPath('automation')} className="underline underline-offset-2 text-text">Automation</Link>.
        </Notice>
      )}

      {(offers('github') || detail.github.enabled) && (
        <section aria-label="GitHub" className={card}>
          {githubSwitchManaged ? (
            <p className="flex items-center gap-2 text-sm text-text"><Github size={16} />GitHub — notifications, PRs, issues</p>
          ) : (
            <Checkbox
              checked={github}
              onChange={(e) => setGithub(e.target.checked)}
              disabled={locked || !offers('github')}
              label={<span className="flex items-center gap-2 text-text"><Github size={16} />GitHub — notifications, PRs, issues</span>}
            />
          )}
          <p className="text-xs text-text-dim" role="status">
            {githubOk === null ? 'Checking…'
              : githubHosted ? preflight?.github.hint
                : githubOk ? `gh is installed and signed in${preflight?.github.login ? ` as ${preflight.github.login}` : ''}`
                  : preflight?.github.hint ?? 'gh is not signed in on the server'}
          </p>
          {githubSwitchManaged && <p className="text-xs text-text-muted">{GITHUB_MANAGED_ON}</p>}
          {githubManaged && <p className="text-xs text-text-muted">{managedLine('Token', githubManaged)}</p>}
          {github && githubOk === false && <Notice tone="warning">{SOURCES_IDLE_HINT}</Notice>}
        </section>
      )}

      {(offers('gmail') || detail.gmail.enabled) && (
        <section aria-label="Gmail" className={card}>
          <Checkbox
            checked={gmail}
            onChange={(e) => setGmail(e.target.checked)}
            disabled={locked || !offers('gmail')}
            label={<span className="flex items-center gap-2 text-text"><Mail size={16} />Gmail — your inbox, via gog</span>}
          />
          <p className="text-xs text-text-dim" role="status">
            {gmailOk === null ? 'Checking…' : gmailOk ? 'gog is installed on the server' : preflight?.gmail.hint ?? 'gog is not installed on the server'}
          </p>
          {gmailManaged && <p className="text-xs text-text-muted">{managedLine('Accounts', gmailManaged)}</p>}
          {gmail && (
            <>
              {gmailOk === false && <Notice tone="warning">{SOURCES_IDLE_HINT}</Notice>}
              {!gmailManaged && offers('gmail') && (
                <>
                  <Field label="Gmail addresses" help={<CodeText text="Run `gog gmail setup <address>` on the server for each address." />} error={errFor('gmail_accounts')}>
                    <TextField
                      value={newAccount}
                      onChange={(e) => { setNewAccount(e.target.value); setClientError(null); }}
                      onKeyDown={(e) => { if (e.key === 'Enter') { e.preventDefault(); addAccount(); } }}
                      placeholder="alice@example.com"
                      autoComplete="off"
                      type="email"
                      disabled={locked}
                    />
                  </Field>
                  <div>
                    <Button variant="secondary" size="xs" onClick={addAccount} disabled={locked || !newAccount.trim()} className={TOUCH}>Add address</Button>
                  </div>
                </>
              )}
              {accounts.length > 0 && (
                <ul className="flex flex-wrap gap-1.5" aria-label="Gmail addresses to sync">
                  {accounts.map((a) => (
                    <li key={a} className="inline-flex items-center gap-1 rounded-full border border-border px-2 py-0.5 text-xs text-text">
                      {a}
                      {!gmailManaged && offers('gmail') && (
                        <button
                          type="button"
                          aria-label={`Remove ${a}`}
                          // A larger hit area on phones without growing the chip.
                          className="text-text-dim hover:text-text inline-flex items-center justify-center p-3 -m-3 md:p-0 md:m-0"
                          onClick={() => setAccounts(accounts.filter((x) => x !== a))}
                          disabled={locked}
                        >
                          <X size={12} />
                        </button>
                      )}
                    </li>
                  ))}
                </ul>
              )}
            </>
          )}
        </section>
      )}

      {(offers('telegram') || detail.telegram.enabled) && (
        <section aria-label="Telegram sync" className={card}>
          <Checkbox
            checked={telegram}
            onChange={(e) => setTelegram(e.target.checked)}
            disabled={telegramLocked}
            label={<span className="flex items-center gap-2 text-text"><MessageCircle size={16} />{TELEGRAM_SYNC_TITLE}</span>}
          />
          <p className="text-xs text-text-dim">{TELEGRAM_SYNC_INTRO}</p>
          {telegram && (
            <>
              <div className="grid gap-3 sm:grid-cols-2">
                {apiIdManaged ? (
                  <ManagedValue label="API id" secret={detail.telegram.api_id} source={apiIdManaged} />
                ) : (
                  <Field label="API id" error={errFor('telegram_api_id')} help={detail.telegram.api_id.configured ? 'Configured — leave blank to keep' : undefined}>
                    <TextField value={apiId} onChange={(e) => { setApiId(e.target.value); setClientError(null); }} inputMode="numeric" autoComplete="off" disabled={telegramLocked} />
                  </Field>
                )}
                <SecretField
                  label="API hash"
                  secret={detail.telegram.api_hash}
                  value={apiHash}
                  onChange={(v) => { setApiHash(v); setClientError(null); }}
                  error={errFor('telegram_api_hash')}
                  removable={false}
                  disabled={telegramLocked || apiHashManaged !== null}
                />
              </div>
              {credentialsSaved ? (
                <TelegramSyncSignIn known={detail.telegram.session} disabled={telegramLocked} onChanged={() => void load()} />
              ) : (
                <div className="flex flex-col gap-1">
                  <p className="text-sm text-text-muted" role="status">{TELEGRAM_SIGNIN.saveFirst}</p>
                  <p className="text-xs text-text-dim">
                    {TELEGRAM_SIGNIN.fallbackPrefix} <code className="font-mono">{TELEGRAM_SIGNIN.fallbackCommand}</code>.
                  </p>
                </div>
              )}
            </>
          )}
        </section>
      )}

      {!locked && available.length > 1 && (
        <div>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => {
              if (offers('github')) setGithub(false);
              if (offers('gmail')) setGmail(false);
              if (offers('telegram')) setTelegram(false);
            }}
            className={TOUCH}
          >
            {turnOffLabel}
          </Button>
        </div>
      )}
      {switchError && <p role="alert" tabIndex={-1} className="text-sm text-error outline-none">{switchError}</p>}
      {state.mode === 'worker' && <Notice tone="info">Sources do not apply to a worker instance.</Notice>}
    </StepFrame>
  );
}
