import { useEffect, useState, type FormEvent } from 'react';
import { useAuthStore } from '../../stores/authStore';
import { Button, TextField } from '../ui';

/**
 * Password prompt shown *over* the running app when a session expires.
 *
 * The app underneath stays mounted, so re-authenticating returns you to the
 * exact chat, scroll position and half-written prompt you had. This replaces
 * a `window.location.reload()` that used to fire from any background request
 * the moment a token aged out — reliably destroying whatever was in the
 * composer at the time.
 *
 * **It unlocks one account: the one whose app is on screen.** Everything
 * underneath — drafts, which notifications have been read, the loaded session
 * list — belongs to that person, and a form that took any username would let
 * somebody else walk up to a colleague's expired tab, sign in, and inherit all
 * of it (and then send a draft they never wrote, under their own name). So the
 * username is shown rather than asked for, and switching accounts goes through
 * "log out", which is the action that purges.
 */
export function SessionExpiredOverlay() {
  const [password, setPassword] = useState('');
  const {
    login, loading, error, logout, loginMode, refreshStatus, account,
  } = useAuthStore();

  // The session has been open for a while; the instance may have gained its
  // second account in that time. Here that only decides what to display — the
  // account is not in question.
  useEffect(() => { void refreshStatus(); }, [refreshStatus]);

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();
    // Always this account, never something typed. On a single-account install
    // there is no username to send and the server resolves the only account
    // there is, which is the same one.
    login(password, account?.username ?? undefined);
  };

  // Which account this was cannot always be established — the call that reads
  // it can fail. Rather than fall back to an open form over somebody's mounted
  // session, offer the one action that is safe without knowing.
  const unknownAccount = account === null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-bg/80 backdrop-blur-sm"
      role="dialog"
      aria-modal="true"
      aria-labelledby="session-expired-title"
    >
      <form
        onSubmit={handleSubmit}
        className="bg-surface-raised p-8 rounded-lg border border-border-subtle w-80 shadow-xl"
      >
        <h1 id="session-expired-title" className="text-xl font-semibold mb-2 text-center">
          Session expired
        </h1>
        {unknownAccount ? (
          <p className="text-sm text-text-muted mb-6 text-center">
            This session cannot be confirmed. Log out and sign in again.
          </p>
        ) : (
          <>
            <p className="text-sm text-text-muted mb-6 text-center">
              Your work is still here — log back in to continue.
            </p>
            {account.username && (
              <TextField
                type="text"
                value={account.username}
                aria-label="Signed in as"
                readOnly
                disabled
                className="mb-3"
              />
            )}
            <TextField
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="Password"
              aria-label="Password"
              autoComplete="current-password"
              autoFocus
              className="mb-4"
            />
            {error && <p className="text-error text-sm mb-3">{error}</p>}
            <Button
              type="submit"
              variant="primary"
              size="md"
              fullWidth
              disabled={loading || loginMode === null}
            >
              {loading ? '...' : 'Unlock'}
            </Button>
          </>
        )}
        {/* The way to use a different account, and deliberately the only way:
            it is what discards this one's unsent drafts and read state before
            anybody else gets in. `ghost` says "the quieter of the two" when
            there are two. */}
        <Button
          variant={unknownAccount ? 'primary' : 'ghost'}
          fullWidth
          onClick={logout}
          className="mt-3"
        >
          {unknownAccount ? 'Log out' : 'Log out and discard unsent drafts'}
        </Button>
      </form>
    </div>
  );
}
