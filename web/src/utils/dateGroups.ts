/**
 * Parse a server timestamp into a Date. Handles both ISO 8601 strings and
 * legacy SQLite "YYYY-MM-DD HH:MM:SS" strings (which are UTC but lack a
 * timezone marker).
 */
export function parseTimestamp(input: string): Date {
  return new Date(input.includes('T') ? input : input.replace(' ', 'T') + 'Z');
}

/** Date buckets, newest first — also the sidebar's top-to-bottom order. */
export const DATE_GROUPS = ['Last hour', 'Today', 'Yesterday', 'This week', 'Older'] as const;

/** Buckets the sidebar starts collapsed. */
export const DEFAULT_COLLAPSED_GROUPS = ['This week', 'Older'];

const COLLAPSED_GROUPS_KEY = 'nerve_sidebar_collapsed_groups';
/** Bump to fold a new default-collapsed bucket into already-stored prefs. */
const COLLAPSED_GROUPS_VERSION = 2;

const asStrings = (v: unknown): string[] =>
  Array.isArray(v) ? v.filter((x): x is string => typeof x === 'string') : [];

/**
 * Which sidebar groups are collapsed, from localStorage.
 *
 * A stored preference wins, except once: any payload written before
 * ``COLLAPSED_GROUPS_VERSION`` gets the current defaults folded in and
 * rewritten, so a new default-collapsed bucket reaches existing users instead
 * of being masked forever by their old value.
 */
export function loadCollapsedGroups(): Set<string> {
  try {
    const raw = localStorage.getItem(COLLAPSED_GROUPS_KEY);
    if (!raw) return new Set(DEFAULT_COLLAPSED_GROUPS);
    const parsed = JSON.parse(raw);
    // Legacy payload was a bare array; anything not at the current version is migrated by union with the defaults.
    const stored = Array.isArray(parsed) ? asStrings(parsed) : asStrings(parsed?.groups);
    if (Array.isArray(parsed) || parsed?.v !== COLLAPSED_GROUPS_VERSION) {
      const merged = new Set([...stored, ...DEFAULT_COLLAPSED_GROUPS]);
      saveCollapsedGroups(merged);
      return merged;
    }
    return new Set(stored);
  } catch {
    return new Set(DEFAULT_COLLAPSED_GROUPS);
  }
}

export function saveCollapsedGroups(groups: Set<string>): void {
  try {
    localStorage.setItem(
      COLLAPSED_GROUPS_KEY,
      JSON.stringify({ v: COLLAPSED_GROUPS_VERSION, groups: [...groups] }),
    );
  } catch { /* quota exceeded / disabled — keep the in-memory state only */ }
}

const EXPANDED_PARENTS_KEY = 'nerve_sidebar_expanded_parents';

/**
 * Which parent sessions are expanded (their children shown), from localStorage.
 * Empty by default — a parent starts collapsed and the user clicks its chevron
 * to reveal its children. Keyed by parent session id.
 */
export function loadExpandedParents(): Set<string> {
  try {
    const raw = localStorage.getItem(EXPANDED_PARENTS_KEY);
    if (!raw) return new Set();
    const parsed = JSON.parse(raw);
    return new Set(asStrings(Array.isArray(parsed) ? parsed : parsed?.ids));
  } catch {
    return new Set();
  }
}

export function saveExpandedParents(ids: Set<string>): void {
  try {
    localStorage.setItem(EXPANDED_PARENTS_KEY, JSON.stringify({ ids: [...ids] }));
  } catch { /* quota exceeded / disabled — keep the in-memory state only */ }
}

/**
 * Assign a session to a coarse recency bucket for the sidebar.
 *
 *   < 1h                       → "Last hour"
 *   same calendar day          → "Today"
 *   previous calendar day      → "Yesterday"
 *   within the last 7 days     → "This week"
 *   older                      → "Older"
 *
 * "Last hour" is purely relative (elapsed time), so it stays correct across a
 * midnight boundary; the rest are calendar-day based, so "Yesterday" means the
 * day before today rather than "24h ago". Items arrive sorted by updated_at
 * DESC, so Map insertion order in groupByDate yields the correct sequence, and
 * empty buckets are never created — an empty group is never rendered.
 */
export function getDateGroup(updatedAt: string): string {
  if (!updatedAt) return 'Older';
  const now = new Date();
  const date = parseTimestamp(updatedAt);
  const hoursAgo = (now.getTime() - date.getTime()) / 3600000;

  if (hoursAgo < 1) return 'Last hour';

  const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  if (date >= todayStart) return 'Today';

  const yesterdayStart = new Date(todayStart.getTime() - 86400000);
  if (date >= yesterdayStart) return 'Yesterday';

  const daysDiff = Math.floor((todayStart.getTime() - date.getTime()) / 86400000);
  if (daysDiff < 7) return 'This week';

  return 'Older';
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
