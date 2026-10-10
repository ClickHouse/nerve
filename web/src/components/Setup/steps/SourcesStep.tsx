import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import {
  api, type SetupAutomationDetail, type SetupSourcesDetail, type SetupState, type SetupStep,
} from '../../../api/client';
import { useSetupStore } from '../../../stores/setupStore';
import { Button, Checkbox, TextField } from '../../ui';
import { Github, Mail, MessageCircle, RefreshCw, X } from '../../ui/icons';
import { INBOX_JOB_OFF, SOURCES_IDLE_HINT, STEP_HEADINGS } from '../copy';
import { Field } from '../Field';
import { stepOf, stopPath } from '../navigation';
import { Notice } from '../Notice';
import { secretPayload } from '../payload';
import { SecretField } from '../SecretField';
import { StepFrame } from '../StepFrame';

const EMAIL_RE = /^[^\s@-][^\s@]*@[^\s@]+\.[^\s@]+$/;
/** The optional job that reads the sources; off by default on a first run with none connected. */
const INBOX_JOB = 'inbox-processor';
/** Secondary controls reach the 44 px phone target without changing the desktop density. */
const TOUCH = 'min-h-[44px] md:min-h-0';

/**
 * GitHub, Gmail and Telegram message sync as toggle cards with the server's
 * preflight beside each. Always sends all three booleans — "none of these"
 * is a decision too.
 */
export function SourcesStep({ step, state }: { step: SetupStep; state: SetupState }) {
  const detail = step.detail as SetupSourcesDetail;
  const save = useSetupStore((s) => s.save);
  const fieldError = useSetupStore((s) => s.fieldError);
  const preflight = useSetupStore((s) => s.preflight);
  const loadPreflight = useSetupStore((s) => s.loadPreflight);
  const locked = step.read_only || step.status === 'blocked';

  const [github, setGithub] = useState(detail.github.enabled);
  const [gmail, setGmail] = useState(detail.gmail.enabled);
  const [telegram, setTelegram] = useState(detail.telegram.enabled);
  const [accounts, setAccounts] = useState<string[]>(detail.gmail.accounts);
  const [newAccount, setNewAccount] = useState('');
  const [apiId, setApiId] = useState('');
  const [apiHash, setApiHash] = useState<string | null>('');
  const [clientError, setClientError] = useState<{ field: string; detail: string } | null>(null);
  const [rechecking, setRechecking] = useState(false);

  useEffect(() => { void loadPreflight(); }, [loadPreflight]);

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
    const body: Record<string, unknown> = { github, gmail, telegram };
    if (gmail) {
      const list = newAccount.trim() && EMAIL_RE.test(newAccount.trim()) && !accounts.includes(newAccount.trim())
        ? [...accounts, newAccount.trim()] : accounts;
      if (list.length === 0) { setClientError({ field: 'gmail_accounts', detail: 'Add at least one Gmail address.' }); return false; }
      body.gmail_accounts = list;
    }
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
    const ok = await save('sources', () => api.setupStep('sources', body));
    if (ok) { setApiId(''); setApiHash(''); }
    return ok;
  };

  const recheck = async () => {
    setRechecking(true);
    try { await loadPreflight(); } finally { setRechecking(false); }
  };

  const githubOk = preflight ? preflight.github.cli && preflight.github.authenticated : null;
  const gmailOk = preflight ? preflight.gmail.cli : null;

  // A first run turns the inbox job off because nothing was connected; a
  // source switched on here would then be read by no job, and nothing else
  // points back at Automation. Say so, with the way there.
  const automation = stepOf(state, 'automation')?.detail as SetupAutomationDetail | undefined;
  const inboxJobOff = automation?.optional.some((job) => job.id === INBOX_JOB && !job.enabled) ?? false;
  const inboxWarning = (github || gmail || telegram) && inboxJobOff;
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

      {inboxWarning && (
        <Notice tone="warning">
          {INBOX_JOB_OFF}{' '}
          <Link to={stopPath('automation')} className="underline underline-offset-2 text-text">Automation</Link>.
        </Notice>
      )}

      <section aria-label="GitHub" className={card}>
        <Checkbox
          checked={github}
          onChange={(e) => setGithub(e.target.checked)}
          disabled={locked}
          label={<span className="flex items-center gap-2 text-text"><Github size={16} />GitHub — notifications, PRs, issues</span>}
        />
        <p className="text-xs text-text-dim" role="status">
          {githubOk === null ? 'Preflight pending'
            : githubOk ? `gh is installed and signed in${preflight?.github.login ? ` as ${preflight.github.login}` : ''}`
              : preflight?.github.hint ?? 'gh is not signed in on the server'}
        </p>
        {github && githubOk === false && <Notice tone="warning">{SOURCES_IDLE_HINT}</Notice>}
      </section>

      <section aria-label="Gmail" className={card}>
        <Checkbox
          checked={gmail}
          onChange={(e) => setGmail(e.target.checked)}
          disabled={locked}
          label={<span className="flex items-center gap-2 text-text"><Mail size={16} />Gmail — your inbox, via gog</span>}
        />
        <p className="text-xs text-text-dim" role="status">
          {gmailOk === null ? 'Preflight pending' : gmailOk ? 'gog is installed on the server' : preflight?.gmail.hint ?? 'gog is not installed on the server'}
        </p>
        {gmail && (
          <>
            {gmailOk === false && <Notice tone="warning">{SOURCES_IDLE_HINT}</Notice>}
            <Field label="Gmail addresses" help="Each needs `gog gmail setup <address>` on the server." error={errFor('gmail_accounts')}>
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
            {accounts.length > 0 && (
              <ul className="flex flex-wrap gap-1.5" aria-label="Gmail addresses to sync">
                {accounts.map((a) => (
                  <li key={a} className="inline-flex items-center gap-1 rounded-full border border-border px-2 py-0.5 text-xs text-text">
                    {a}
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
                  </li>
                ))}
              </ul>
            )}
          </>
        )}
      </section>

      <section aria-label="Telegram sync" className={card}>
        <Checkbox
          checked={telegram}
          onChange={(e) => setTelegram(e.target.checked)}
          disabled={locked}
          label={<span className="flex items-center gap-2 text-text"><MessageCircle size={16} />Telegram — your own messages</span>}
        />
        <p className="text-xs text-text-dim">
          Reads your messages with API credentials from my.telegram.org — not the bot token.
          {detail.telegram.api_id.configured && detail.telegram.api_hash.configured && ' Credentials are configured.'}
        </p>
        {telegram && (
          <div className="grid gap-3 sm:grid-cols-2">
            <Field label="API id" error={errFor('telegram_api_id')} help={detail.telegram.api_id.configured ? 'Configured — leave blank to keep' : undefined}>
              <TextField value={apiId} onChange={(e) => { setApiId(e.target.value); setClientError(null); }} inputMode="numeric" autoComplete="off" disabled={locked} />
            </Field>
            <SecretField
              label="API hash"
              secret={detail.telegram.api_hash}
              value={apiHash}
              onChange={(v) => { setApiHash(v); setClientError(null); }}
              error={errFor('telegram_api_hash')}
              removable={false}
              disabled={locked}
            />
          </div>
        )}
      </section>

      {!locked && (
        <div>
          <Button variant="ghost" size="sm" onClick={() => { setGithub(false); setGmail(false); setTelegram(false); }} className={TOUCH}>
            None of these
          </Button>
        </div>
      )}
      {state.mode === 'worker' && <Notice tone="info">Sources do not apply to a worker instance.</Notice>}
    </StepFrame>
  );
}
