/** The last turn's usage, as the `done` event and the session's last usage
 *  carry it. */
export interface ContextUsage {
  input_tokens: number;
  output_tokens: number;
  cache_creation_input_tokens: number;
  cache_read_input_tokens: number;
  // The 5m and 1h ephemeral cache TTL split, from usage.cache_creation of
  // the Anthropic API. Both are 0 when the API does not send the split
  // (older responses) or when no cache was written.
  cache_creation_5m_input_tokens?: number;
  cache_creation_1h_input_tokens?: number;
  max_context_tokens: number;
  num_turns: number;
  /** The context use the runtime measured after a compact, or the input
   *  tokens of the turn's last model call, when the backend reports them
   *  (Codex). */
  context_tokens?: number;
}

export const COMPACT_CONFIRM_TEXT =
  'Summarizes earlier messages to free context. The full transcript stays in Nerve.';

/** Fill colour of a meter by how full it is. These land in an inline
 *  `backgroundColor`, so they reference the theme variables directly. */
export function meterColor(pct: number): string {
  if (pct > 80) return 'var(--theme-error)';
  if (pct > 60) return 'var(--theme-warning)';
  return 'var(--theme-info)';
}

export function formatTokens(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return String(n);
}

/** All input tokens of the turn, over all its model calls. */
export function turnInputTokens(usage: ContextUsage): number {
  return usage.input_tokens + usage.cache_read_input_tokens + usage.cache_creation_input_tokens;
}

/** Model calls of the turn. */
export function turnCalls(usage: ContextUsage): number {
  return usage.num_turns
    || Math.max(1, Math.ceil(turnInputTokens(usage) / usage.max_context_tokens));
}

/**
 * The context use: reported by the backend (`exact`: measured after a
 * compact, or the last model call's input on Codex), else estimated from the
 * last turn's usage.
 *
 * The SDK's ResultMessage.usage adds up the tokens of all model calls of a
 * turn (each tool use starts a new call), so cache_read can be N times the
 * context window. The estimate of the context of one call is the turn's
 * input tokens divided by its calls (num_turns). Output tokens do not fill
 * the context window, so they are not in the estimate.
 */
export function contextTokens(usage: ContextUsage): { tokens: number; exact: boolean } {
  if (usage.context_tokens != null) return { tokens: usage.context_tokens, exact: true };
  return { tokens: Math.round(turnInputTokens(usage) / turnCalls(usage)), exact: false };
}
