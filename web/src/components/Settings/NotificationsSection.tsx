import { useEffect, useId, type ReactNode } from 'react';
import { Link } from 'react-router-dom';
import type {
  NotificationSettingFields, NotificationSettingPath, NotificationSettings,
} from '../../api/settings';
import { useSettingsStore } from '../../stores/settingsStore';
import { Badge, Button, Switch } from '../ui';
import { BellOff, Check, ChevronRight, Monitor, Moon, Send, Slack, X } from '../ui/icons';
import { QUIET_HOURS_PREFILL } from '../Setup/copy';
import { Notice } from '../Setup/Notice';
import { canEdit } from './sections';
import { CommitField, FileNote, LevelChoice, ManagedNote, SettingRow, SettingsGroup } from './SettingsRows';
import { ThisBrowserRow } from './ThisBrowserRow';

const CHANNELS = 'notifications.channels' as const;
const SETUP_CHANNELS = '/setup/channels';
const SILENCES_PATH = '/notifications?silences=1';
const HOSTED_SLACK_DESTINATION = 'Cards go to the notification channel that an admin sets in the control plane. '
  + 'With no channel set there, Slack gets none.';

/** One field of the section, with its save state and a save for it. */
function useSetting<P extends NotificationSettingPath>(data: NotificationSettings, path: P) {
  const busy = useSettingsStore((s) => s.saving.includes(path));
  const saveError = useSettingsStore((s) => s.saveError);
  const save = useSettingsStore((s) => s.saveNotifications);
  const field: NotificationSettingFields[P] = data.fields[path];
  return {
    field,
    busy,
    error: saveError?.field === path ? saveError.detail : null,
    editable: canEdit(field),
    save: (value: unknown) => save({ [path]: value }),
  };
}

/**
 * Settings > Notifications: where notifications go and how much goes to each
 * place, a test, the silences, and the question limits.
 *
 * Every control saves when it changes (a text field on Enter or when it
 * loses focus). The server answers with the whole section, so a row always
 * shows the saved state.
 */
export function NotificationsSection() {
  const data = useSettingsStore((s) => s.notifications);
  const loadError = useSettingsStore((s) => s.loadError);
  const load = useSettingsStore((s) => s.loadNotifications);

  useEffect(() => { void load(); }, [load]);

  return (
    <section aria-labelledby="settings-notifications-heading" className="flex flex-col gap-6">
      <SectionHeading />
      {data ? <SectionBody data={data} /> : loadError ? (
        <div className="flex flex-col items-start gap-2">
          <p role="alert" className="text-sm text-error">{loadError}</p>
          <Button variant="secondary" size="sm" onClick={() => void load()}>Retry</Button>
        </div>
      ) : (
        <p role="status" className="text-sm text-text-dim">Loading…</p>
      )}
    </section>
  );
}

function SectionHeading() {
  const saved = useSettingsStore((s) => s.saved);
  return (
    <div className="flex items-start justify-between gap-3">
      <div>
        <h2 id="settings-notifications-heading" className="text-xl font-semibold text-text">Notifications</h2>
        <p className="mt-1 text-sm text-text-muted">Where notifications go, and how much goes to each place.</p>
      </div>
      <p role="status" className="mt-1.5 flex shrink-0 items-center gap-1 text-xs text-text-dim">
        {saved && <><Check size={12} aria-hidden="true" />Saved</>}
      </p>
    </div>
  );
}

function SectionBody({ data }: { data: NotificationSettings }) {
  const saveError = useSettingsStore((s) => s.saveError);
  const reloadErrors = useSettingsStore((s) => s.reloadErrors);
  const notReloaded = Object.entries(reloadErrors);
  return (
    <>
      {data.read_only_reason && <Notice tone="locked">{data.read_only_reason}</Notice>}
      {saveError && saveError.field === null && <Notice tone="error">{saveError.detail}</Notice>}
      {notReloaded.length > 0 && (
        <Notice tone="warning">
          Saved, but these parts did not reload: {notReloaded.map(([part, error]) => `${part} (${error})`).join('; ')}
        </Notice>
      )}

      <SettingsGroup
        title="Where notifications go"
        description="Questions, approvals and messages from the agent."
        footer={<TestFooter />}
      >
        <SettingRow
          icon={<Monitor size={16} />}
          title="Nerve panel"
          description="Every notification. Answer questions here."
          control={<Badge tone="neutral">Always on</Badge>}
        >
          <TestResult channel="web" />
        </SettingRow>
        <ThisBrowserRow />
        {chatChannels(data).map((channel) => (
          <ChatChannelRow key={channel.name} data={data} channel={channel} />
        ))}
      </SettingsGroup>

      <QuietHoursGroup data={data} />

      <SilencesLink />

      <details className="group flex flex-col">
        <summary className="flex min-h-[44px] cursor-pointer list-none items-center gap-1.5 text-sm font-semibold text-text [&::-webkit-details-marker]:hidden">
          <ChevronRight size={14} className="transition-transform group-open:rotate-90" aria-hidden="true" />
          Advanced
        </summary>
        <QuestionsGroup data={data} />
      </details>
    </>
  );
}

/** A chat channel's row: what it needs from the section, per channel. */
interface ChatChannel {
  name: 'slack' | 'telegram';
  title: string;
  icon: ReactNode;
  description: string;
  connected: boolean;
  level: 'notifications.levels.slack' | 'notifications.levels.telegram';
  destination: 'notifications.slack_channel_id' | 'notifications.telegram_chat_id';
  label: string;
  placeholder: string;
  help: string;
  /** The destination as text, for a destination set elsewhere. */
  shown: string;
  /** One read-only line in place of the destination, for a destination this page does not choose. */
  fixedDestination?: string;
  inputMode?: 'numeric';
}

/**
 * The Slack and Telegram rows. A hosted instance has no Telegram, and shows
 * Slack only while the control plane has a Slack app connected; the control
 * plane also chooses where its cards go.
 */
function chatChannels(data: NotificationSettings): ChatChannel[] {
  const slackName = data.slack.channel_name;
  const slackId = data.fields['notifications.slack_channel_id'].value;
  const chatId = data.fields['notifications.telegram_chat_id'].value;
  const [slack, telegram]: ChatChannel[] = [
    {
      name: 'slack', title: 'Slack', icon: <Slack size={16} />, connected: data.slack.connected,
      description: 'Cards in a Slack channel, with answer buttons.',
      level: 'notifications.levels.slack', destination: 'notifications.slack_channel_id',
      label: 'Channel', placeholder: 'C0123ABCD',
      help: slackName ? `Posts in #${slackName}.` : 'A channel ID. Empty: the first channel ID in the Slack allow list.',
      shown: slackName ? `#${slackName}` : slackId ?? 'The first channel ID in the Slack allow list',
    },
    {
      name: 'telegram', title: 'Telegram', icon: <Send size={16} />, connected: data.telegram.connected,
      description: 'Messages from your bot, with answer buttons.',
      level: 'notifications.levels.telegram', destination: 'notifications.telegram_chat_id',
      label: 'Chat', placeholder: 'Your direct messages', inputMode: 'numeric',
      help: 'A chat ID, for example of a group. Empty: direct messages with the first paired person.',
      shown: chatId === null ? 'Your direct messages' : String(chatId),
    },
  ];
  if (!data.hosted) return [slack, telegram];
  return data.slack.connected ? [{ ...slack, fixedDestination: HOSTED_SLACK_DESTINATION }] : [];
}

/**
 * Slack or Telegram: a switch in `notifications.channels`, then the
 * destination and the level while it is on. A channel that is not
 * connected links to Setup.
 */
function ChatChannelRow({ data, channel }: { data: NotificationSettings; channel: ChatChannel }) {
  const titleId = useId();
  const channels = useSetting(data, CHANNELS);
  const level = useSetting(data, channel.level);
  const target = useSetting(data, channel.destination);
  if (!channel.connected) {
    return (
      <SettingRow
        icon={channel.icon}
        title={channel.title}
        description="Not connected."
        control={(
          <Link to={SETUP_CHANNELS} className="text-xs font-medium text-link hover:text-link-hover hover:underline">
            Connect {channel.title} in Setup
          </Link>
        )}
      />
    );
  }
  const list = channels.field.value;
  const on = list.includes(channel.name);
  const toggle = (next: boolean) => {
    if (!channels.busy) void channels.save(next ? [...list, channel.name] : list.filter((c) => c !== channel.name));
  };
  const value = target.field.value === null ? '' : String(target.field.value);
  const fixed = channel.fixedDestination;
  return (
    <SettingRow
      icon={channel.icon}
      title={channel.title}
      titleId={titleId}
      description={channel.description}
      control={(
        <Switch
          checked={on}
          aria-labelledby={titleId}
          aria-busy={channels.busy || undefined}
          disabled={!channels.editable}
          onChange={toggle}
        />
      )}
    >
      <TestResult channel={channel.name} />
      {on && fixed && <p className="text-xs text-text-dim">{fixed}</p>}
      {on && !fixed && (target.field.state === 'managed' ? (
        <div className="flex flex-col gap-1">
          <span className="text-xs font-medium text-text-muted">{channel.label}</span>
          <span className="text-sm text-text">{channel.shown}</span>
          <ManagedNote field={target.field} />
        </div>
      ) : (
        <CommitField
          key={value}
          label={channel.label}
          value={value}
          placeholder={channel.placeholder}
          help={channel.help}
          error={target.error}
          disabled={!target.editable}
          busy={target.busy}
          inputMode={channel.inputMode}
          onCommit={(text) => void target.save(text)}
        />
      ))}
      {on && (
        <LevelChoice
          value={level.field.value}
          disabled={!level.editable}
          busy={level.busy}
          onChange={(next) => void level.save(next)}
        />
      )}
      {on && <ManagedNote field={level.field} />}
      {channels.error && <p role="alert" className="text-xs text-error">{channels.error}</p>}
      <ManagedNote field={channels.field} />
      <FileNote fields={[channels.field, ...(on ? [...(fixed ? [] : [target.field]), level.field] : [])]} />
    </SettingRow>
  );
}

/** "Test arrived" or "Test did not arrive" on a row, after a test. */
function TestResult({ channel }: { channel: string }) {
  const entry = useSettingsStore((s) => s.testResult?.results.find((r) => r.channel === channel));
  if (!entry) return null;
  return (
    <div>
      {entry.delivered
        ? <Badge tone="success"><Check size={11} aria-hidden="true" />Test arrived</Badge>
        : <Badge tone="danger"><X size={11} aria-hidden="true" />Test did not arrive</Badge>}
    </div>
  );
}

function TestFooter() {
  const testing = useSettingsStore((s) => s.testing);
  const result = useSettingsStore((s) => s.testResult);
  const error = useSettingsStore((s) => s.testError);
  const sendTest = useSettingsStore((s) => s.sendTest);
  let line = 'Sends one notification to each place that is on.';
  if (testing) line = 'Sending…';
  else if (error) line = error;
  else if (result?.silenced) line = 'A silence rule matched the test, so only the panel shows it.';
  else if (result) line = 'Sent. Each place shows whether it arrived.';
  return (
    <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
      <Button variant="secondary" size="sm" aria-busy={testing || undefined} onClick={() => void sendTest()}>
        <Send size={13} aria-hidden="true" />
        Send a test
      </Button>
      <p role={error ? 'alert' : 'status'} className={`text-xs ${error ? 'text-error' : 'text-text-dim'}`}>{line}</p>
    </div>
  );
}

/**
 * Quiet hours: one switch for both times (on fills in the times that Setup
 * offers, off empties both), then a time field for each. The times are in
 * the instance's time zone, which can differ from the browser's.
 */
function QuietHoursGroup({ data }: { data: NotificationSettings }) {
  const titleId = useId();
  const start = useSetting(data, 'quiet_start');
  const end = useSetting(data, 'quiet_end');
  const save = useSettingsStore((s) => s.saveNotifications);
  const on = Boolean(start.field.value && end.field.value);
  const busy = start.busy || end.busy;
  const toggle = (next: boolean) => {
    if (busy) return;
    void save(next
      ? { quiet_start: QUIET_HOURS_PREFILL.start, quiet_end: QUIET_HOURS_PREFILL.end }
      : { quiet_start: '', quiet_end: '' });
  };
  const time = (setting: typeof start, label: string) => (
    <CommitField
      key={`${label}-${setting.field.value}`}
      type="time"
      label={label}
      value={setting.field.value}
      error={setting.error}
      disabled={!setting.editable}
      busy={setting.busy}
      onCommit={(text) => void setting.save(text)}
    />
  );
  return (
    <SettingsGroup title="Quiet hours" description="Notifications still arrive, without sound. Slack follows its own settings.">
      <SettingRow
        icon={<Moon size={16} />}
        title="No sound except urgent"
        titleId={titleId}
        description={`Every day, in ${data.quiet_hours.timezone} time.`}
        control={(
          <>
            {on && data.quiet_hours.active && <Badge tone="info">Quiet now</Badge>}
            <Switch
              checked={on}
              aria-labelledby={titleId}
              aria-busy={busy || undefined}
              disabled={!start.editable || !end.editable}
              onChange={toggle}
            />
          </>
        )}
      >
        {on && <div className="flex flex-wrap gap-3">{time(start, 'Start')}{time(end, 'End')}</div>}
        {!on && (start.error || end.error) && <p role="alert" className="text-xs text-error">{start.error ?? end.error}</p>}
        <ManagedNote field={start.field.state === 'managed' ? start.field : end.field} />
        <FileNote fields={[start.field, end.field]} />
      </SettingRow>
    </SettingsGroup>
  );
}

function SilencesLink() {
  return (
    <Link
      to={SILENCES_PATH}
      className="flex min-h-[44px] items-center gap-3 rounded-lg border border-border bg-surface-raised px-4 py-3 transition-colors hover:bg-surface-hover"
    >
      <BellOff size={16} className="shrink-0 text-text-muted" aria-hidden="true" />
      <span className="min-w-0 flex-1">
        <span className="block text-sm font-medium text-text">Silences</span>
        <span className="block text-xs text-text-dim">Rules that hold back known alerts. Manage them on the Notifications page.</span>
      </span>
      <ChevronRight size={16} className="shrink-0 text-text-dim" aria-hidden="true" />
    </Link>
  );
}

function QuestionsGroup({ data }: { data: NotificationSettings }) {
  const expiry = useSetting(data, 'notifications.default_expiry_hours');
  const redeliveries = useSetting(data, 'notifications.max_redeliveries');
  const number = (text: string) => (/^\d+$/.test(text) ? Number(text) : text);
  return (
    <div className="mt-2">
      <SettingsGroup title="Questions" description="Applies to questions and approvals from the agent.">
        <li className="flex flex-col gap-4 px-4 py-3">
          <CommitField
            key={`expiry-${expiry.field.value}`}
            label="Close an unanswered question after"
            value={String(expiry.field.value)}
            unit="hours"
            error={expiry.error}
            disabled={!expiry.editable}
            busy={expiry.busy}
            inputMode="numeric"
            onCommit={(text) => void expiry.save(number(text))}
          />
          <ManagedNote field={expiry.field} />
          <CommitField
            key={`redeliveries-${redeliveries.field.value}`}
            label="Send a snoozed question again at most"
            value={String(redeliveries.field.value)}
            unit="times"
            error={redeliveries.error}
            disabled={!redeliveries.editable}
            busy={redeliveries.busy}
            inputMode="numeric"
            onCommit={(text) => void redeliveries.save(number(text))}
          />
          <ManagedNote field={redeliveries.field} />
          <FileNote fields={[expiry.field, redeliveries.field]} />
        </li>
      </SettingsGroup>
    </div>
  );
}
