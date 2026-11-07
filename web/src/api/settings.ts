/**
 * Types of the Settings API (`/api/settings/<section>`).
 *
 * Each field of a section is a dotted configuration path. `GET` gives each
 * field with its value, its state and the file a change goes to; `PUT` takes
 * a partial body of `{path: value}` and answers with the whole section again.
 */

/**
 * - `editable`: in `settings.yaml`, or not stated anywhere.
 * - `machine`: stated in `config.yaml` or `config.local.yaml` on this machine;
 *   a change is written there.
 * - `managed`: the environment or the control plane sets it; `reason` says which.
 * - `read_only`: nothing on the page can change (lockdown); `reason` says why.
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
    /** Nerve Cloud: the channel gateway posts the cards. */
    hosted: boolean;
    /** The name of the configured channel, when Slack answered. */
    channel_name: string | null;
  };
  telegram: { connected: boolean };
  /** `PUT` only: `{part: error}` for each part that did not reload. */
  reload_errors?: Record<string, string>;
}

export interface NotificationTestResult {
  notification_id: string;
  /** A silence rule matched the test, so only the panel shows it. */
  silenced: boolean;
  /** One entry per channel in `notifications.channels`. */
  results: { channel: string; delivered: boolean }[];
}
