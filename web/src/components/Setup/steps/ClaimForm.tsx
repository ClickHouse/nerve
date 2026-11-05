import { useRef, useState, type FormEvent } from 'react';
import { useNavigate } from 'react-router-dom';
import { api, setToken } from '../../../api/client';
import { ApiError } from '../../../api/hosted';
import { errorDetail } from '../../../stores/accountStore';
import { useActorStore } from '../../../stores/actorStore';
import { useAuthStore } from '../../../stores/authStore';
import { useSetupStore } from '../../../stores/setupStore';
import { Button, Checkbox, IconButton, TextField } from '../../ui';
import { Eye, EyeOff, ShieldQuestion } from '../../ui/icons';
import { CLAIM_HEADING, CLAIM_INTRO, CLAIM_MISSING, CLAIM_SUBMIT, CLAIM_SUBMIT_BUSY } from '../copy';
import { Field } from '../Field';
import { PasswordStrength } from '../PasswordStrength';

/** The HTTP status the claim answers for a wrong setup token. */
const TOKEN_REFUSED = 403;

type Missing = 'username' | 'password' | 'setup_token';

const MISSING_TEXT: Record<Missing, string> = {
  username: 'Enter a username.',
  password: 'Enter a password.',
  setup_token: 'Enter the setup token.',
};

function statusOf(cause: unknown): number | null {
  if (cause instanceof ApiError) return cause.status;
  const message = cause instanceof Error ? cause.message : '';
  const prefix = message.match(/^(\d{3}):/);
  return prefix ? Number(prefix[1]) : null;
}

/**
 * Step 0, local mode only: the first-account claim. The one unauthenticated
 * write in the product. The setup token lives in form state alone, is sent
 * in the JSON body and nowhere else, and is cleared on success — and on a
 * refusal, so a wrong token is never resubmitted by habit. Success signs
 * the tab in and lands on the Mode step — not on chat.
 *
 * The route and the API say "claim"; the person is creating an account, so
 * the words on the screen say that. The button is never disabled: pressing
 * it early names the first missing field (under the field, focused) rather
 * than leaving a keyboard user to guess why Tab skipped the button.
 *
 * Every input has a visible label (the later steps all do; the first
 * screen must not be the least accessible one), with its hint underneath.
 */
export function ClaimForm() {
  const navigate = useNavigate();
  const checkAuth = useAuthStore((state) => state.checkAuth);
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [passwordless, setPasswordless] = useState(false);
  const [displayName, setDisplayName] = useState('');
  const [setupToken, setSetupToken] = useState('');
  const [showToken, setShowToken] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [missing, setMissing] = useState<Missing | null>(null);
  const usernameInput = useRef<HTMLInputElement>(null);
  const passwordInput = useRef<HTMLInputElement>(null);
  const tokenInput = useRef<HTMLInputElement>(null);

  const firstMissing = (): Missing | null => {
    if (!passwordless && !username.trim()) return 'username';
    if (!passwordless && !password) return 'password';
    if (!setupToken) return 'setup_token';
    return null;
  };

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (busy) return;
    const gap = firstMissing();
    if (gap) {
      setMissing(gap);
      setError(CLAIM_MISSING(passwordless));
      ({ username: usernameInput, password: passwordInput, setup_token: tokenInput })[gap].current?.focus();
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const claimed = await api.setupClaim({
        username: username.trim() || undefined,
        ...(passwordless ? { passwordless: true } : { password }),
        setup_token: setupToken,
        display_name: displayName.trim() || undefined,
      });
      // Keep the setup token in form state; store only the returned session.
      setSetupToken('');
      setToken(claimed.token);
      await checkAuth();
      await useActorStore.getState().refresh();
      void useSetupStore.getState().load();
      navigate('/setup/mode', { replace: true });
    } catch (cause) {
      setError(errorDetail(cause, 'Could not create the account'));
      if (statusOf(cause) === TOKEN_REFUSED) {
        // The token was wrong: clear it and put the cursor back there.
        setSetupToken('');
        tokenInput.current?.focus();
      }
    } finally {
      setBusy(false);
    }
  };

  const clearMissing = (field: Missing) => {
    if (missing === field) { setMissing(null); setError(null); }
  };

  return (
    <div className="flex-1 h-full overflow-auto flex items-start justify-center p-6">
      {/* `noValidate`: the handler names the missing field in the page, in
          every browser and for every assistive technology, instead of a
          native bubble that vanishes. The `required` attributes stay for
          what they tell a screen reader. */}
      <form
        onSubmit={submit}
        noValidate
        aria-label={CLAIM_HEADING}
        className="max-w-xl w-full mt-10 border border-border rounded-lg
          bg-surface p-5 flex flex-col gap-4"
      >
        <div className="flex items-center gap-3">
          <ShieldQuestion size={22} className="text-hue-amber" />
          <h1 className="text-xl font-semibold text-text">{CLAIM_HEADING}</h1>
        </div>

        <p className="text-sm text-text-muted">{CLAIM_INTRO}</p>

        {error && <p role="alert" className="text-error text-sm">{error}</p>}

        <Field
          label="Username"
          help={passwordless ? 'Optional without a password.' : undefined}
          error={missing === 'username' ? MISSING_TEXT.username : undefined}
        >
          <TextField
            ref={usernameInput}
            value={username}
            onChange={(event) => { setUsername(event.target.value); clearMissing('username'); }}
            autoComplete="username"
            autoFocus
            required={!passwordless}
          />
        </Field>
        {!passwordless && (
          <div className="flex flex-col gap-1.5">
            <Field label="Password" error={missing === 'password' ? MISSING_TEXT.password : undefined}>
              <TextField
                ref={passwordInput}
                type="password"
                value={password}
                onChange={(event) => { setPassword(event.target.value); clearMissing('password'); }}
                autoComplete="new-password"
                required
              />
            </Field>
            <PasswordStrength password={password} />
          </div>
        )}
        <Checkbox
          checked={passwordless}
          onChange={(event) => {
            setPasswordless(event.target.checked);
            if (event.target.checked && (missing === 'username' || missing === 'password')) { setMissing(null); setError(null); }
          }}
          label="Keep this installation passwordless"
          labelSize="sm"
          labelTone="secondary"
        />
        {passwordless && (
          <p role="note" className="text-sm text-hue-amber">
            Without a password, anyone who can reach this address is the owner
            and can use Nerve, its tools and its stored credentials. Keep Nerve
            on localhost or on a protected network. You can set a password later
            in Accounts.
          </p>
        )}
        <Field label="Display name" help="Optional. The name Nerve and other people see. You can change it later.">
          <TextField
            value={displayName}
            onChange={(event) => setDisplayName(event.target.value)}
            autoComplete="name"
          />
        </Field>
        <Field
          label="Setup token"
          help={(
            <>
              Run <code>nerve status</code> on the server to see it. Anyone with
              this token can create the owner account, so for remote setup open
              this page only over HTTPS or a protected tunnel.
            </>
          )}
          error={missing === 'setup_token' ? MISSING_TEXT.setup_token : undefined}
          trailing={(
            <IconButton label={showToken ? 'Hide setup token' : 'Show setup token'} onClick={() => setShowToken((s) => !s)}>
              {showToken ? <EyeOff size={16} /> : <Eye size={16} />}
            </IconButton>
          )}
        >
          <TextField
            ref={tokenInput}
            type={showToken ? 'text' : 'password'}
            value={setupToken}
            onChange={(event) => { setSetupToken(event.target.value); clearMissing('setup_token'); }}
            autoComplete="one-time-code"
            spellCheck={false}
            required
          />
        </Field>

        <div>
          <Button
            type="submit"
            variant="primary"
            size="md"
            aria-busy={busy || undefined}
          >
            {busy ? CLAIM_SUBMIT_BUSY : CLAIM_SUBMIT}
          </Button>
        </div>
      </form>
    </div>
  );
}
