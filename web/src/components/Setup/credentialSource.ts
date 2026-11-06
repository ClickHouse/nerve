import type { CredentialSource, SecretOut, SetupProviderDetail } from '../../api/client';

/**
 * Where a configured value lives, when that is somewhere other than the
 * wizard's own `config.local.yaml`.
 *
 * An operator can provide a connection before anyone opens this page: an
 * environment variable, the tracked `settings.yaml` of a configuration
 * repository, or the shared channel gateway. Such a value is shown as
 * configured and managed — no Replace, no Remove, no field to type into —
 * and the server refuses to overwrite it (409). An older backend sends no
 * `source` at all, which reads as the normal file case.
 */
export type ManagedSource = Exclude<CredentialSource, 'file'>;

export function managedSource(source: CredentialSource | null | undefined): ManagedSource | null {
  return source === 'environment' || source === 'config-repo' || source === 'gateway' ? source : null;
}

/** The managed source of a configured secret; `null` for a file value or nothing. */
export function secretManaged(secret: SecretOut | null | undefined): ManagedSource | null {
  return secret?.configured ? managedSource(secret.source) : null;
}

/**
 * The provider's credential: the server's word for the whole provider when
 * it gives one, else `credential: 'environment'`, else the saved secret's.
 */
export function providerManaged(detail: SetupProviderDetail): ManagedSource | null {
  const declared = managedSource(detail.credential_source);
  if (declared) return declared;
  if (detail.credential === 'environment') return 'environment';
  if (detail.credential === 'api_key') return secretManaged(detail.anthropic_api_key);
  if (detail.credential === 'oauth_token') return secretManaged(detail.claude_oauth_token);
  return null;
}
