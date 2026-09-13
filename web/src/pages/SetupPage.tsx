import { useState, type FormEvent } from 'react';
import { Navigate, useNavigate } from 'react-router-dom';
import { ShieldQuestion } from '../components/ui/icons';
import { Button, TextField } from '../components/ui';
import { api, setToken } from '../api/client';
import { errorDetail } from '../stores/accountStore';
import { useActorStore } from '../stores/actorStore';
import { useAuthStore } from '../stores/authStore';

/** Secure the account created by installation; no other setup lives here. */
export function SetupPage() {
  const navigate = useNavigate();
  const loginMode = useAuthStore((state) => state.loginMode);
  const checkAuth = useAuthStore((state) => state.checkAuth);
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [setupToken, setSetupToken] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  if (loginMode !== 'none') return <Navigate to="/accounts" replace />;

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
      navigate('/chat', { replace: true });
    } catch (cause) {
      setError(errorDetail(cause, 'Could not claim this instance'));
    } finally {
      setBusy(false);
    }
  };

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
          already created. The display name is optional.
        </p>

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
    </div>
  );
}
