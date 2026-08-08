/**
 * Parse a server timestamp into a Date. Handles both ISO 8601 strings and
 * legacy SQLite "YYYY-MM-DD HH:MM:SS" strings (which are UTC but lack a
 * timezone marker).
 */
export function parseTimestamp(input: string): Date {
  return new Date(input.includes('T') ? input : input.replace(' ', 'T') + 'Z');
}

/**
 * Assign a session to a coarse recency bucket for the sidebar.
 *
 *   < 1h                    → "Last hour"
 *   1–3h                    → "Last 3 hours"
 *   same calendar day       → "Today"
 *   within the last 7 days  → "This week"
 *   older                   → "Other"
 *
 * The first two buckets are purely relative (elapsed time), so they stay
 * correct across a midnight boundary. Items arrive sorted by updated_at DESC,
 * so Map insertion order in groupByDate yields the correct top-to-bottom
 * sequence, and empty buckets are never created (so they aren't rendered).
 */
export function getDateGroup(updatedAt: string): string {
  if (!updatedAt) return 'Other';
  const now = new Date();
  const date = parseTimestamp(updatedAt);
  const hoursAgo = (now.getTime() - date.getTime()) / 3600000;

  if (hoursAgo < 1) return 'Last hour';
  if (hoursAgo < 3) return 'Last 3 hours';

  const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  if (date >= todayStart) return 'Today';

  const daysDiff = Math.floor((todayStart.getTime() - date.getTime()) / 86400000);
  if (daysDiff < 7) return 'This week';

  return 'Other';
}

/**
 * Compact relative-time formatter for inline labels: "2m ago", "3h ago", "5d ago".
 * Falls back to a short date for anything older than ~30 days.
 */
export function formatTimeAgo(input: string): string {
  if (!input) return '';
  const date = parseTimestamp(input);
  const diffMs = Date.now() - date.getTime();
  if (diffMs < 0) return 'just now';
  const minutes = Math.floor(diffMs / 60000);
  if (minutes < 1) return 'just now';
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days}d ago`;
  return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

/**
 * Group items by date label. Preserves the order items arrive in
 * (most-recent-first from the API), so groups appear top-to-bottom
 * from newest to oldest without needing a hardcoded order list.
 */
export function groupByDate<T extends { updated_at: string }>(
  items: T[],
): { group: string; items: T[] }[] {
  const groups = new Map<string, T[]>();
  for (const item of items) {
    const group = getDateGroup(item.updated_at);
    if (!groups.has(group)) groups.set(group, []);
    groups.get(group)!.push(item);
  }
  return Array.from(groups.entries()).map(([group, groupItems]) => ({
    group,
    items: groupItems,
  }));
}
