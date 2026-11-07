import type { Notification } from '../../stores/notificationStore';

/** An approval option that has its own icon and colour. */
export type OptionMark = 'approve' | 'decline' | 'snooze';

/** One answer option, as a row of `OptionList` shows it. */
export interface OptionItem {
  /** The answer it sends: the option as the agent wrote it, or an approval's value. */
  value: string;
  /** The row text: the value without "(recommended)", or an approval's label. */
  label: string;
  /** The agent marked it "(recommended)": it shows first, with a tag. */
  recommended: boolean;
  /** An approval value with its own icon and colour. The icon shows where other rows show a letter. */
  mark?: OptionMark;
}

const RECOMMENDED = /\s*\(recommended\)\s*$/i;

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

/** The question's id in the result of the server's ask_user handler: "Question sent (ask-1a2b3c4d). …". */
const SENT_ID = /Question sent \((ask-[\w-]+)\)/;

/** The notification id of an ask_user call, from its tool result; null when the question was not sent. */
export function questionIdFromResult(result: string | undefined): string | null {
  return (result && SENT_ID.exec(result)?.[1]) || null;
}

/** The rows for a question's options: the recommended ones first, each group in its own order. */
export function questionOptions(values: string[]): OptionItem[] {
  const items = values.map(value => {
    const recommended = RECOMMENDED.test(value);
    const label = recommended ? value.replace(RECOMMENDED, '') || value : value;
    return { value, label, recommended };
  });
  return [...items.filter(item => item.recommended), ...items.filter(item => !item.recommended)];
}

const APPROVAL_DEFAULT_LABELS: Record<string, string> = {
  approve: 'Approve',
  decline: 'Decline',
  snooze_24h: 'Snooze 24h',
};

/** Keyed by the value, not the label: an approval keeps its colours when the agent gives its own labels. */
const APPROVAL_MARKS: Record<string, OptionMark> = {
  approve: 'approve',
  decline: 'decline',
  snooze_24h: 'snooze',
};

/** An approval's label map (value to label), from the live event or the row's metadata. */
function approvalLabels(notif: Notification): Record<string, string> {
  if (notif.option_labels) return notif.option_labels;
  try {
    const meta: unknown = typeof notif.metadata === 'string' ? JSON.parse(notif.metadata) : notif.metadata;
    const labels = meta && typeof meta === 'object' ? (meta as Record<string, unknown>).option_labels : null;
    if (labels && typeof labels === 'object') return labels as Record<string, string>;
  } catch {
    // Malformed metadata: the default labels apply.
  }
  return {};
}

/** The label of an approval value: the agent's label, a default, or the value in title case. */
function approvalLabel(value: string, labels: Record<string, string>): string {
  return labels[value] || APPROVAL_DEFAULT_LABELS[value]
    || value.split('_').map(part => part.charAt(0).toUpperCase() + part.slice(1)).join(' ');
}

/** The option rows of a question or an approval notification. */
export function notificationOptions(notif: Notification): OptionItem[] {
  const values = parseAskOptions(notif.options);
  if (notif.type !== 'approval') return questionOptions(values);
  const labels = approvalLabels(notif);
  return values.map(value => ({
    value,
    label: approvalLabel(value, labels),
    recommended: false,
    mark: APPROVAL_MARKS[value],
  }));
}
