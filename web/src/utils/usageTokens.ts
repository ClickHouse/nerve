/**
 * Token fields of one usage aggregate row from `/api/diagnostics`
 * (`usage.daily`, `usage.by_model`, `usage.by_source`, `usage.by_cron_job`).
 */
export interface UsageTokens {
  input_tokens?: number | null;
  cache_read?: number | null;
  cache_creation?: number | null;
}

/**
 * All input tokens of a usage row.
 *
 * `input_tokens` is only the uncached part of the prompt. The API reports
 * cache reads and cache writes in separate fields, and both are input that
 * the model read and that is billed. With prompt caching, the uncached part
 * is often a tiny fraction of the total, so showing it alone reads as if
 * almost no input was sent.
 */
export function totalInputTokens(row: UsageTokens): number {
  return (row.input_tokens || 0) + (row.cache_read || 0) + (row.cache_creation || 0);
}
