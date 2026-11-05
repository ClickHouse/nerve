import type { SetupProfileDetail, SetupState } from '../../api/client';

/**
 * Whose name goes into the Ready screen's "Hi, I'm …".
 *
 * It must be the *viewer's* own name: USER.md (`detail.name`) is shared
 * instance state, so on an instance behind a front gateway Alice would
 * otherwise introduce herself as the Bob somebody else saved there. Order:
 * the server's `actor_name` (the header name, or the local account's display
 * name) → the viewer's display name → locally only, the USER.md name →
 * nothing. A bare login username is never used; the caller drops the clause
 * instead.
 */
export function personNameFor(
  state: Pick<SetupState, 'auth_mode'>,
  profile: Pick<SetupProfileDetail, 'name' | 'actor_name'> | undefined,
  viewer: { display_name?: string | null } | null,
): string | null {
  const own = profile?.actor_name?.trim() || viewer?.display_name?.trim() || '';
  if (own) return own;
  if (state.auth_mode === 'external') return null;
  return profile?.name?.trim() || null;
}
