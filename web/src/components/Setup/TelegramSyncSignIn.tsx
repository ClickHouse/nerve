import { useCallback, useEffect, useState, type KeyboardEvent } from 'react';
import {
  api, type TelegramSyncSession, type TelegramSyncSessionSummary, type TelegramSyncUser,
} from '../../api/client';
import { ApiError } from '../../api/hosted';
import { errorDetail } from '../../stores/accountStore';
import { fieldErrorOf } from '../../stores/setupStore';
import { Button, TextField } from '../ui';
import { Check, Loader2 } from '../ui/icons';
import { TELEGRAM_SIGNIN } from './copy';
import { Field } from './Field';

type Phase = 'checking' | 'phone' | 'code' | 'password' | 'signed_in';
type Busy = 'check' | 'start' | 'code' | 'password' | 'cancel' | 'logout' | null;

/** Secondary controls reach the 44 px phone target without changing the desktop density. */
const TOUCH = 'min-h-[44px] md:min-h-0';

/** The seconds Telegram asks for, out of a 429 (`0` when it names none); `null` for any other error. */
function retryAfterOf(e: unknown): number | null {
  if (!(e instanceof ApiError) || e.status !== 429) return null;
  const body = e.message.slice(e.message.indexOf(':') + 1).trim();
  try {
    const parsed: unknown = JSON.parse(body);
    if (parsed && typeof parsed === 'object' && 'retry_after' in parsed && typeof parsed.retry_after === 'number') {
      return Math.max(1, Math.round(parsed.retry_after));
    }
  } catch {
    // Not JSON.
  }
  return 0;
}

function phaseOf(s: TelegramSyncSession): Phase {
  if (s.authorized === true) return 'signed_in';
  if (s.pending === 'code') return 'code';
  if (s.pending === 'password') return 'password';
  return 'phone';
}

/** "@alice_example", else the first name, else `null` ("Signed in to Telegram", never "as your account"). */
function whoIs(user: TelegramSyncUser | null): string | null {
  if (!user) return null;
  return user.username ? `@${user.username}` : (user.first_name || null);
}

/**
 * The one-time sign-in of the person's own Telegram account, for the
 * message sync. API credentials alone do nothing: the sync needs a user
 * session file on the server, which only a sign-in creates.
 *
 * Phone → **Send code** → code → (two-step password when the account has
 * one) → "Signed in as @name" with **Sign out**. Every step is a request to
 * the server, which talks to Telegram; the phone, the code and the password
 * are sent once and not kept here. A `400 {detail, field}` lands under its
 * field; a `429` says how long Telegram asks to wait; anything else is the
 * server's sentence.
 *
 * On mount the saved session is checked (the checklist's own summary shows
 * meanwhile). `authorized: null` means it could not be: the running sync
 * holds the file (`busy`) or Telegram did not answer (`unreachable`); the
 * panel says which and offers to check again. Enter in a field acts on
 * this panel, not on the step's form around it.
 */
export function TelegramSyncSignIn({ known, disabled = false, onChanged }: {
  /** What the checklist already says (`detail.telegram.session`), shown until the live check answers. */
  known?: TelegramSyncSessionSummary | null;
  disabled?: boolean;
  /** The signed-in state changed (sign-in complete, sign-out); the parent re-reads the checklist. */
  onChanged?: () => void;
}) {
  const knownAuthorized = known?.authorized === true;
  const [phase, setPhase] = useState<Phase>(() => (knownAuthorized ? 'signed_in' : 'checking'));
  const [session, setSession] = useState<TelegramSyncSession | null>(null);
  const [user, setUser] = useState<TelegramSyncUser | null>(known?.user ?? null);
  const [phone, setPhone] = useState('');
  const [code, setCode] = useState('');
  const [password, setPassword] = useState('');
  const [phoneMasked, setPhoneMasked] = useState<string | null>(null);
  const [expiresIn, setExpiresIn] = useState<number | null>(null);
  const [busy, setBusy] = useState<Busy>(null);
  const [error, setError] = useState<{ field: string | null; detail: string } | null>(null);

  const applySession = useCallback((s: TelegramSyncSession) => {
    setSession(s);
    setUser(s.user);
    setPhoneMasked(s.phone_masked);
    setPhase(phaseOf(s));
  }, []);

  useEffect(() => {
    let alive = true;
    api.setupTelegramSession()
      .then((s) => { if (alive) applySession(s); })
      .catch((e: unknown) => {
        if (!alive) return;
        // The check could not run; the panel still works from what the
        // checklist said, and a sign-in attempt reports its own answer.
        setError({ field: null, detail: errorDetail(e, 'Could not check the Telegram session.') });
        setPhase(knownAuthorized ? 'signed_in' : 'phone');
      });
    return () => { alive = false; };
  }, [applySession, knownAuthorized]);

  const fail = (e: unknown) => {
    const wait = retryAfterOf(e);
    if (wait !== null) {
      setError({ field: null, detail: wait > 0 ? TELEGRAM_SIGNIN.wait(wait) : TELEGRAM_SIGNIN.waitUnknown });
      return;
    }
    const field = fieldErrorOf(e);
    setError(field ? { field: field.field, detail: field.detail } : { field: null, detail: errorDetail(e, 'The request failed. Try again.') });
  };

  const run = async (what: Exclude<Busy, null>, action: () => Promise<void>) => {
    setBusy(what);
    setError(null);
    try {
      await action();
    } catch (e) {
      fail(e);
    } finally {
      setBusy(null);
    }
  };

  const check = () => run('check', async () => {
    applySession(await api.setupTelegramSession());
  });

  const sendCode = () => {
    const number = phone.trim();
    if (!number) { setError({ field: 'phone', detail: TELEGRAM_SIGNIN.phoneMissing }); return; }
    void run('start', async () => {
      const started = await api.setupTelegramSessionStart({ phone: number });
      setPhoneMasked(started.phone_masked);
      setExpiresIn(started.expires_in);
      setCode('');
      setPhase('code');
    });
  };

  const submitCode = () => {
    const value = code.trim();
    if (!value) { setError({ field: 'code', detail: TELEGRAM_SIGNIN.codeMissing }); return; }
    void run('code', async () => {
      const answer = await api.setupTelegramSessionCode({ code: value });
      setCode('');
      applySession(answer);
      if (answer.authorized === true) onChanged?.();
    });
  };

  const submitPassword = () => {
    if (!password) { setError({ field: 'password', detail: TELEGRAM_SIGNIN.passwordMissing }); return; }
    void run('password', async () => {
      const answer = await api.setupTelegramSessionPassword({ password });
      setPassword('');
      applySession(answer);
      if (answer.authorized === true) onChanged?.();
    });
  };

  const cancel = () => run('cancel', async () => {
    const answer = await api.setupTelegramSessionCancel();
    setCode('');
    setPassword('');
    applySession(answer);
    setPhase('phone');
  });

  const signOut = () => run('logout', async () => {
    const answer = await api.setupTelegramSessionLogout();
    applySession(answer);
    setPhase('phone');
    onChanged?.();
  });

  // Enter acts on this panel; the step's own form must not submit.
  const onEnter = (action: () => void) => (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key !== 'Enter') return;
    e.preventDefault();
    if (!busy && !disabled) action();
  };

  const off = disabled || busy !== null;
  const err = (field: string) => (error?.field === field ? error.detail : null);
  const general = error && error.field === null ? error.detail : null;
  const unchecked = session?.authorized === null
    && (session.reason === 'busy' || session.reason === 'unreachable');
  const reasonLine = unchecked
    ? (session.reason === 'busy' ? TELEGRAM_SIGNIN.busy : TELEGRAM_SIGNIN.unreachable)
    : session?.reason === 'telethon_missing' ? TELEGRAM_SIGNIN.libraryMissing : null;
  const who = whoIs(user);
  const spinner = <Loader2 size={14} className="animate-spin" />;

  return (
    <div className="flex flex-col gap-3 rounded-lg border border-border bg-surface-raised p-3" aria-busy={busy !== null || undefined}>
      <h3 id="setup-telegram-signin-heading" tabIndex={-1} className="text-sm font-medium text-text outline-none">
        {TELEGRAM_SIGNIN.heading}
      </h3>

      {phase === 'checking' && (
        <p role="status" className="text-xs text-text-dim flex items-center gap-2">
          <Loader2 size={12} className="animate-spin" />{TELEGRAM_SIGNIN.checking}
        </p>
      )}

      {reasonLine && (
        <p role="status" className="text-sm text-text-muted flex flex-wrap items-center gap-x-2">
          <span>{reasonLine}</span>
          {unchecked && (
            <Button variant="link" size="sm" onClick={() => void check()} disabled={off} className={TOUCH}>
              {TELEGRAM_SIGNIN.checkAgain}
            </Button>
          )}
        </p>
      )}

      {phase === 'signed_in' && (
        <div className="flex flex-wrap items-center gap-2" role="status">
          <Check size={14} className="text-success shrink-0" />
          <span className="text-sm text-text">{who ? TELEGRAM_SIGNIN.signedInAs(who) : TELEGRAM_SIGNIN.signedIn}</span>
          <Button variant="link" size="sm" onClick={() => void signOut()} disabled={off} className={TOUCH}>
            {busy === 'logout' ? TELEGRAM_SIGNIN.signingOut : TELEGRAM_SIGNIN.signOut}
          </Button>
        </div>
      )}

      {phase === 'phone' && (
        <div className="flex flex-col gap-2">
          <Field label={TELEGRAM_SIGNIN.phoneLabel} help={TELEGRAM_SIGNIN.phoneHelp} error={err('phone')}>
            <TextField
              value={phone}
              onChange={(e) => { setPhone(e.target.value); setError(null); }}
              onKeyDown={onEnter(sendCode)}
              type="tel"
              inputMode="tel"
              autoComplete="tel"
              placeholder="+1 555 0100"
              disabled={off}
            />
          </Field>
          <div>
            <Button variant="primary" size="sm" onClick={sendCode} disabled={off} className={TOUCH}>
              {busy === 'start' ? spinner : null}
              {busy === 'start' ? TELEGRAM_SIGNIN.sending : TELEGRAM_SIGNIN.sendCode}
            </Button>
          </div>
        </div>
      )}

      {phase === 'code' && (
        <div className="flex flex-col gap-2">
          <p role="status" className="text-sm text-text-muted">
            {TELEGRAM_SIGNIN.codeSent(phoneMasked ?? 'your phone')}
            {expiresIn ? ` ${TELEGRAM_SIGNIN.codeValid(expiresIn)}` : ''}
          </p>
          <Field label={TELEGRAM_SIGNIN.codeLabel} error={err('code')}>
            <TextField
              value={code}
              onChange={(e) => { setCode(e.target.value); setError(null); }}
              onKeyDown={onEnter(submitCode)}
              inputMode="numeric"
              autoComplete="one-time-code"
              disabled={off}
            />
          </Field>
          <div className="flex flex-wrap items-center gap-2">
            <Button variant="primary" size="sm" onClick={submitCode} disabled={off} className={TOUCH}>
              {busy === 'code' ? spinner : null}
              {busy === 'code' ? TELEGRAM_SIGNIN.signingIn : TELEGRAM_SIGNIN.signIn}
            </Button>
            <Button variant="ghost" size="sm" onClick={() => void cancel()} disabled={off} className={TOUCH}>
              {TELEGRAM_SIGNIN.startOver}
            </Button>
          </div>
        </div>
      )}

      {phase === 'password' && (
        <div className="flex flex-col gap-2">
          <Field label={TELEGRAM_SIGNIN.passwordLabel} help={TELEGRAM_SIGNIN.passwordHelp} error={err('password')}>
            <TextField
              type="password"
              value={password}
              onChange={(e) => { setPassword(e.target.value); setError(null); }}
              onKeyDown={onEnter(submitPassword)}
              autoComplete="current-password"
              disabled={off}
            />
          </Field>
          <div className="flex flex-wrap items-center gap-2">
            <Button variant="primary" size="sm" onClick={submitPassword} disabled={off} className={TOUCH}>
              {busy === 'password' ? spinner : null}
              {busy === 'password' ? TELEGRAM_SIGNIN.signingIn : TELEGRAM_SIGNIN.signIn}
            </Button>
            <Button variant="ghost" size="sm" onClick={() => void cancel()} disabled={off} className={TOUCH}>
              {TELEGRAM_SIGNIN.startOver}
            </Button>
          </div>
        </div>
      )}

      {general && <p role="alert" className="text-sm text-error">{general}</p>}

      <p className="text-xs text-text-dim">
        {TELEGRAM_SIGNIN.fallbackPrefix} <code className="font-mono">{TELEGRAM_SIGNIN.fallbackCommand}</code>.
      </p>
    </div>
  );
}
