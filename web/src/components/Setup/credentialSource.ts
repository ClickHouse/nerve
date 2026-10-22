import type {
  CredentialSource, SecretOut, SetupChannelsDetail, SetupMemoryDetail, SetupProviderDetail,
  SetupSourcesDetail, SetupStep,
} from '../../api/client';

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

/** The first managed credential a step carries, for "Already configured" and the summaries. */
export function stepManaged(step: SetupStep): ManagedSource | null {
  switch (step.id) {
    case 'provider':
      return providerManaged(step.detail as SetupProviderDetail);
    case 'memory':
      return secretManaged((step.detail as SetupMemoryDetail).openai_api_key);
    case 'channels': {
      const { telegram, slack } = step.detail as SetupChannelsDetail;
      return secretManaged(telegram.bot_token) ?? secretManaged(slack.bot_token)
        ?? secretManaged(slack.app_token) ?? (slack.mode === 'hosted' ? 'gateway' : null);
    }
    case 'sources': {
      const { github, gmail, telegram } = step.detail as SetupSourcesDetail;
      // A GitHub the operator switched on in the machine's configuration
      // arrives through the channel gateway.
      return managedSource(github.source) ?? (github.managed && github.enabled ? 'gateway' : null)
        ?? managedSource(gmail.source) ?? secretManaged(telegram.api_id) ?? secretManaged(telegram.api_hash);
    }
    default:
      return null;
  }
}
