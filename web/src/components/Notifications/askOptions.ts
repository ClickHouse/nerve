/**
 * The options of an ask_user call or a question notification, by the rule of
 * the server's ask_user handler: a list as it is, then a string that holds a
 * JSON array, then a comma-separated string. Each option is trimmed and empty
 * ones are dropped.
 */
export function parseAskOptions(raw: unknown): string[] {
  const clean = (items: unknown[]) => items.map(item => String(item).trim()).filter(Boolean);
  if (Array.isArray(raw)) return clean(raw);
  if (typeof raw !== 'string' || !raw.trim()) return [];
  try {
    const parsed: unknown = JSON.parse(raw);
    if (Array.isArray(parsed)) return clean(parsed);
  } catch {
    // Not JSON: a comma-separated string.
  }
  return clean(raw.split(','));
}
