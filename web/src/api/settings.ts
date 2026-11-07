/**
 * Types of the Settings API (`/api/settings/<section>`, see `docs/api.md`).
 * `PUT` takes a partial body of `{path: value}` and answers with the whole
 * section again.
 */

export type SettingState = 'editable' | 'machine' | 'managed' | 'read_only';

export interface SettingField<T> {
  value: T;
  state: SettingState;
  /** `settings.yaml`, `config.yaml` or `config.local.yaml`; null when the page cannot change it. */
  file: string | null;
  reason: string | null;
}

/** A channel gets every item, or questions, approvals, high and urgent only. */
export type NotificationLevel = 'all' | 'important';

export interface NotificationSettingFields {
  'notifications.channels': SettingField<string[]>;
  'notifications.levels.slack': SettingField<NotificationLevel>;
  'notifications.levels.telegram': SettingField<NotificationLevel>;
  'notifications.slack_channel_id': SettingField<string | null>;
  'notifications.telegram_chat_id': SettingField<number | null>;
  'notifications.default_expiry_hours': SettingField<number>;
  'notifications.max_redeliveries': SettingField<number>;
  /** `HH:MM`, or `''` for both while quiet hours are off. */
  quiet_start: SettingField<string>;
  quiet_end: SettingField<string>;
}

export type NotificationSettingPath = keyof NotificationSettingFields;

/** A `PUT` body: the fields to change. A text field may send its raw text. */
export type NotificationSettingValues = Partial<Record<NotificationSettingPath, unknown>>;

export interface NotificationSettings {
  /** Why nothing here can change (lockdown), or null. */
  read_only_reason: string | null;
  fields: NotificationSettingFields;
  slack: {
    /** Tokens are set (or the channel gateway holds them) and Slack is on. */
    connected: boolean;
    /** The name of the configured channel, when Slack answered. */
    channel_name: string | null;
  };
  telegram: { connected: boolean };
  /** The time zone of the quiet hours (the instance's `timezone`). */
  quiet_hours: { timezone: string };
  /** `PUT` only: `{part: error}` for each part that did not reload. */
  reload_errors?: Record<string, string>;
}

export interface NotificationTestResult {
  notification_id: string;
  /** A silence rule matched the test, so only the panel shows it. */
  silenced: boolean;
  /** One entry per channel in `notifications.channels`, and `webpush` when a browser has push on. */
  results: { channel: string; delivered: boolean }[];
}
