import { TextField } from '../ui';
import type { LoginKind } from '../../api/client';

interface Props {
  /** `null` while the descriptor has never been read — see the loading case. */
  loginMode: LoginKind | null;
  username: string;
  password: string;
  onUsername: (value: string) => void;
  onPassword: (value: string) => void;
}

/**
 * The fields a login form collects, which depend on the instance.
 *
 * The username input is rendered only when the server says a username is
 * required — two or more accounts. An install that has one account (the
 * overwhelming majority, and every install that just upgraded) shows exactly
 * what it showed before: a password box. Nobody has to learn that they do not
 * have a username.
 *
 * Which of the two is a *fact about the server*, not a guess, so while it has
 * never been answered this renders a loading line instead of picking one. Once
 * answered it stays answered: a later failed read keeps the last value rather
 * than dropping back to the placeholder, and a read that has never succeeded
 * falls back to asking for both.
 *
 * Shared by the full-page login and the session-expired overlay so the two
 * cannot drift into asking for different things.
 */
export function CredentialFields({
  loginMode, username, password, onUsername, onPassword,
}: Props) {
  if (loginMode === null) {
    return (
      <p role="status" className="text-sm text-text-dim mb-4 text-center">
        Checking how to sign in…
      </p>
    );
  }

  const needsUsername = loginMode === 'username_password';
  return (
    <>
      {needsUsername && (
        <TextField
          type="text"
          value={username}
          onChange={(e) => onUsername(e.target.value)}
          placeholder="Username"
          aria-label="Username"
          autoComplete="username"
          autoFocus
          className="mb-3"
        />
      )}
      <TextField
        type="password"
        value={password}
        onChange={(e) => onPassword(e.target.value)}
        placeholder="Password"
        aria-label="Password"
        autoComplete="current-password"
        autoFocus={!needsUsername}
        className="mb-4"
      />
    </>
  );
}
