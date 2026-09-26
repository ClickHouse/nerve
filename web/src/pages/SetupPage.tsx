import { useState, type FormEvent } from 'react';
import { Navigate, useNavigate } from 'react-router-dom';
import { ShieldQuestion } from '../components/ui/icons';
import { Button, Checkbox, TextField } from '../components/ui';
import { api, setToken } from '../api/client';
import { errorDetail } from '../stores/accountStore';
import { useActorStore } from '../stores/actorStore';
import { useAuthStore } from '../stores/authStore';

/** Complete setup of the installation's initial account. */
export function SetupPage() {
  const navigate = useNavigate();
  const loginMode = useAuthStore((state) => state.loginMode);
  const checkAuth = useAuthStore((state) => state.checkAuth);
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [passwordless, setPasswordless] = useState(false);
  const [displayName, setDisplayName] = useState('');
  const [setupToken, setSetupToken] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  if (loginMode !== 'setup') return <Navigate to="/chat" replace />;

  const submit = async (event: FormEvent) => {
    event.preventDefault();
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
      navigate('/chat', { replace: true });
    } catch (cause) {
      setError(errorDetail(cause, 'Could not claim this instance'));
    } finally {
      setBusy(false);
    }
  };

  const credentialsReady = passwordless || (!!username && !!password);

  return (
    <div className="flex-1 h-full overflow-auto flex items-start justify-center p-6">
      <form
        onSubmit={submit}
        aria-label="Claim this instance"
        className="max-w-xl w-full mt-10 border border-border rounded-lg
          bg-surface-raised p-5 flex flex-col gap-4"
      >
        <div className="flex items-center gap-3">
          <ShieldQuestion size={22} className="text-hue-amber" />
          <h1 className="text-xl font-semibold text-text">Claim this instance</h1>
        </div>

        <p className="text-sm text-text-muted">
          Choose the username and password for the account this installation
          already created, or keep the installation passwordless. The display
          name is optional.
        </p>

        {error && <p role="alert" className="text-error text-sm">{error}</p>}

        <TextField
          value={username}
          onChange={(event) => setUsername(event.target.value)}
          placeholder={passwordless ? 'Username (optional)' : 'Username'}
          aria-label="Username"
          autoComplete="username"
          autoFocus
          required={!passwordless}
        />
        {!passwordless && (
          <TextField
            type="password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            placeholder="Password"
            aria-label="Password"
            autoComplete="new-password"
            required
          />
        )}
        <Checkbox
          checked={passwordless}
          onChange={(event) => setPasswordless(event.target.checked)}
          label="Keep this installation passwordless"
          labelSize="sm"
          labelTone="secondary"
        />
        {passwordless && (
          <p role="note" className="text-sm text-hue-amber">
            Without a password, anyone who can reach this address is the owner:
            they can use the agent, its tools and its credentials. Keep the
            gateway on localhost or behind a protected network. You can set a
            password later.
          </p>
        )}
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
            disabled={busy || !credentialsReady || !setupToken}
          >
            {busy ? 'Claiming…' : 'Claim and sign in'}
          </Button>
        </div>
      </form>
    </div>
  );
}
