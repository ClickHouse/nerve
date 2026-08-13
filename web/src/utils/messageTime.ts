/**
 * Compact local datetime for a chat message's `created_at`,
 * e.g. "Aug 13, 2:41 AM" (locale-aware; 24h where the locale uses it).
 * Returns '' for missing/invalid input so callers can guard on falsy.
 */
export function formatMessageTime(input?: string): string {
  if (!input) return '';
  const date = new Date(input);
  if (Number.isNaN(date.getTime())) return '';
  return date.toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  });
}
