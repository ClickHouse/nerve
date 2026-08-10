import { useState, type FormEvent } from 'react';
import { useAuthStore } from '../../stores/authStore';

/**
 * Password prompt shown *over* the running app when a session expires.
 *
 * The app underneath stays mounted, so re-authenticating returns you to the
 * exact chat, scroll position and half-written prompt you had. This replaces
 * a `window.location.reload()` that used to fire from any background request
 * the moment a token aged out — reliably destroying whatever was in the
 * composer at the time.
 */
export function SessionExpiredOverlay() {
  const [password, setPassword] = useState('');
  const { login, loading, error, logout } = useAuthStore();

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();
    login(password);
  };

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
        <p className="text-sm text-text-muted mb-6 text-center">
          Your work is still here — log back in to continue.
        </p>
        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          placeholder="Password"
          autoFocus
          className="w-full px-3 py-2 bg-surface-raised border border-border-subtle rounded text-text outline-none focus:border-accent mb-4"
        />
        {error && <p className="text-hue-red text-sm mb-3">{error}</p>}
        <button
          type="submit"
          disabled={loading}
          className="w-full py-2 bg-accent hover:bg-accent-hover text-white rounded font-medium disabled:opacity-50 cursor-pointer"
        >
          {loading ? '...' : 'Unlock'}
        </button>
        {/* Explicit escape hatch. Unlike the overlay this *does* clear unsent
            drafts, so it is deliberately the secondary action. */}
        <button
          type="button"
          onClick={logout}
          className="w-full mt-3 text-xs text-text-muted hover:text-text cursor-pointer"
        >
          Log out and discard unsent drafts
        </button>
      </form>
    </div>
  );
}
