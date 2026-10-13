/**
 * The display name for a source.
 *
 * A source is named `type` or `type:account` — `getRendererType` documents that
 * shape, and for `gmail:user@example.com` the account is the identity worth
 * showing, since the transport is already carried by the row's icon.
 *
 * A channel source adds a third shape, `type:qualifier`. `slack:observed` names
 * the drain that feeds the inbox from watched channels, and the qualifier is
 * load-bearing rather than decorative: `telegram` alone is already the Telethon
 * pull source, so the drain cannot share the bare transport name.
 */
const CHANNEL_SOURCE_SUFFIX = 'observed';

export function sourceLabel(source: string): string {
  const sep = source.indexOf(':');
  if (sep < 0) return source;
  const type = source.slice(0, sep);
  const rest = source.slice(sep + 1);
  // A qualifier describes the transport's feed, so it has to be read together
  // with the transport. Stripping it the way an account is stripped leaves
  // "observed", which names neither this source nor anything else.
  if (rest === CHANNEL_SOURCE_SUFFIX) return `${type} (${rest})`;
  // For gmail:<account>, show just the account
  return rest.length > 20 ? rest.slice(0, 18) + '..' : rest;
}
