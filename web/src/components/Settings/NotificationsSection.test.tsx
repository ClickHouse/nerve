import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { NotificationSettings, SettingField } from '../../api/settings';

vi.mock('../../api/client', () => ({
  api: {
    settingsNotifications: vi.fn(),
    saveSettingsNotifications: vi.fn(),
    testSettingsNotifications: vi.fn(),
  },
  getToken: vi.fn(() => 'session-token'),
  setToken: vi.fn(),
  clearToken: vi.fn(),
  setUnauthorizedHandler: vi.fn(),
}));

const client = await import('../../api/client');
const { NotificationsSection } = await import('./NotificationsSection');
const { useSettingsStore } = await import('../../stores/settingsStore');

const api = client.api as unknown as Record<string, ReturnType<typeof vi.fn>>;

function field<T>(value: T, over: Partial<SettingField<T>> = {}): SettingField<T> {
  return { value, state: 'editable', file: 'settings.yaml', reason: null, ...over };
}

function settings(over: Partial<NotificationSettings> = {}, fields: Partial<NotificationSettings['fields']> = {}): NotificationSettings {
  return {
    read_only_reason: null,
    hosted: false,
    fields: {
      'notifications.channels': field(['web', 'slack', 'telegram']),
      'notifications.levels.slack': field('all' as const),
      'notifications.levels.telegram': field('important' as const),
      'notifications.slack_channel_id': field<string | null>('C0123ABCD'),
      'notifications.telegram_chat_id': field<number | null>(null),
      'notifications.default_expiry_hours': field(48),
      'notifications.max_redeliveries': field(3),
      ...fields,
    },
    slack: { connected: true, channel_name: 'alerts' },
    telegram: { connected: true },
    ...over,
  };
}

async function renderSection(body: NotificationSettings = settings()) {
  api.settingsNotifications.mockResolvedValue(body);
  render(<MemoryRouter><NotificationsSection /></MemoryRouter>);
  await screen.findByRole('heading', { name: 'Where notifications go' });
}

const row = (title: string) => screen.getByText(title, { selector: 'p' }).closest('li') as HTMLElement;
const refusal = (status: number, body: object) => new Error(`${status}: ${JSON.stringify(body)}`);

beforeEach(() => {
  vi.clearAllMocks();
  useSettingsStore.setState(useSettingsStore.getInitialState(), true);
});

describe('NotificationsSection', () => {
  it('switches a channel off by saving the list without it', async () => {
    api.saveSettingsNotifications.mockResolvedValue({
      ...settings({}, { 'notifications.channels': field(['web', 'telegram']) }), reload_errors: {},
    });
    await renderSection();
    const slack = screen.getByRole('switch', { name: 'Slack' });
    expect(slack).toHaveAttribute('aria-checked', 'true');

    await userEvent.click(slack);

    expect(api.saveSettingsNotifications).toHaveBeenCalledWith({ 'notifications.channels': ['web', 'telegram'] });
    await waitFor(() => expect(screen.getByRole('switch', { name: 'Slack' })).toHaveAttribute('aria-checked', 'false'));
    expect(screen.getByText('Saved')).toBeInTheDocument();
    // An off channel hides its destination and level.
    expect(within(row('Slack')).queryByLabelText('Channel')).not.toBeInTheDocument();
  });

  it('saves a level from its radio group', async () => {
    api.saveSettingsNotifications.mockResolvedValue({ ...settings(), reload_errors: {} });
    await renderSection();
    const choice = within(row('Slack')).getByRole('radio', { name: 'Questions and important' });

    await userEvent.click(choice);

    expect(api.saveSettingsNotifications).toHaveBeenCalledWith({ 'notifications.levels.slack': 'important' });
    expect(within(row('Telegram')).getByRole('radio', { name: 'Questions and important' }))
      .toHaveAttribute('aria-checked', 'true');
  });

  it('saves a text field on Enter and shows a refusal under it', async () => {
    api.saveSettingsNotifications.mockRejectedValue(refusal(400, {
      detail: 'Use a numeric chat ID, such as -1001234567890.', field: 'notifications.telegram_chat_id',
    }));
    await renderSection();
    const chat = within(row('Telegram')).getByLabelText('Chat');

    await userEvent.type(chat, 'group{Enter}');

    expect(api.saveSettingsNotifications).toHaveBeenCalledWith({ 'notifications.telegram_chat_id': 'group' });
    expect(await screen.findByRole('alert')).toHaveTextContent('Use a numeric chat ID');
    expect(chat).toHaveAttribute('aria-invalid', 'true');
    expect(chat).toHaveValue('group');
  });

  it('shows the channel name and the file a row saves to', async () => {
    await renderSection(settings({}, {
      'notifications.levels.slack': field('all' as const, { state: 'machine', file: 'config.yaml' }),
    }));
    expect(within(row('Slack')).getByText('Posts in #alerts.')).toBeInTheDocument();
    expect(within(row('Slack')).getByText('Saved in settings.yaml and config.yaml on this machine')).toBeInTheDocument();
  });

  it('links to Setup for a channel that is not connected', async () => {
    await renderSection(settings({ slack: { connected: false, channel_name: null } }));
    expect(screen.queryByRole('switch', { name: 'Slack' })).not.toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Connect Slack in Setup' })).toHaveAttribute('href', '/setup/channels');
  });

  it('shows a destination the environment sets, with no field for it', async () => {
    const reason = 'This value is configured by your operator from the environment and cannot be changed here.';
    await renderSection(settings(
      { slack: { connected: true, channel_name: null } },
      { 'notifications.slack_channel_id': field<string | null>('C0456DEFG', { state: 'managed', file: null, reason }) },
    ));
    const slack = row('Slack');
    expect(within(slack).queryByLabelText('Channel')).not.toBeInTheDocument();
    expect(within(slack).getByText('C0456DEFG')).toBeInTheDocument();
    expect(within(slack).getByText(reason)).toBeInTheDocument();
    expect(screen.getByRole('switch', { name: 'Slack' })).toBeEnabled();
  });

  describe('on a hosted instance', () => {
    const managed = (reason: string) => ({ state: 'managed' as const, file: null, reason });
    const hosted = (connected: boolean) => settings(
      { hosted: true, slack: { connected, channel_name: null } },
      {
        'notifications.slack_channel_id': field<string | null>(null, managed('Set by an admin in the control plane.')),
        'notifications.levels.telegram': field('all' as const, managed('Telegram is not available on this hosted instance.')),
        'notifications.telegram_chat_id': field<number | null>(null, managed('Telegram is not available on this hosted instance.')),
      },
    );

    it('shows Slack with its switch and level, and a line in place of the destination', async () => {
      await renderSection(hosted(true));
      const slack = row('Slack');
      expect(screen.getByRole('switch', { name: 'Slack' })).toBeEnabled();
      expect(within(slack).getByRole('radio', { name: 'Everything' })).toHaveAttribute('aria-checked', 'true');
      expect(within(slack).getByText(
        'Cards go to the notification channel that an admin sets in the control plane. With no channel set there, Slack gets none.',
      )).toBeInTheDocument();
      expect(within(slack).queryByLabelText('Channel')).not.toBeInTheDocument();
      expect(within(slack).queryByText('Set by an admin in the control plane.')).not.toBeInTheDocument();
      expect(within(slack).getByText('Saved in settings.yaml')).toBeInTheDocument();
    });

    it('shows no Telegram row', async () => {
      await renderSection(hosted(true));
      expect(screen.queryByText('Telegram', { selector: 'p' })).not.toBeInTheDocument();
      expect(screen.queryByRole('switch', { name: 'Telegram' })).not.toBeInTheDocument();
      expect(screen.queryByRole('link', { name: 'Connect Telegram in Setup' })).not.toBeInTheDocument();
    });

    it('shows no Slack row while the control plane has no Slack app connected', async () => {
      await renderSection(hosted(false));
      expect(screen.queryByText('Slack', { selector: 'p' })).not.toBeInTheDocument();
      expect(screen.queryByRole('link', { name: 'Connect Slack in Setup' })).not.toBeInTheDocument();
      expect(screen.getByText('Nerve panel')).toBeInTheDocument();
    });
  });

  it('is read-only under lockdown, with one line that says why', async () => {
    const reason = 'Configuration is managed from the config repository on this instance; changes arrive by PR and sync.';
    const locked = settings({ read_only_reason: reason });
    for (const key of Object.keys(locked.fields) as (keyof typeof locked.fields)[]) {
      Object.assign(locked.fields[key], { state: 'read_only', file: null, reason });
    }
    await renderSection(locked);
    expect(screen.getByText(reason)).toBeInTheDocument();
    for (const control of screen.getAllByRole('switch')) expect(control).toBeDisabled();
    expect(within(row('Telegram')).getByLabelText('Chat')).toBeDisabled();
    expect(screen.queryByText(/^Saved in/)).not.toBeInTheDocument();
    // A test is not a change: it stays available.
    expect(screen.getByRole('button', { name: 'Send a test' })).toBeEnabled();
  });

  it('marks each channel with the result of a test', async () => {
    api.testSettingsNotifications.mockResolvedValue({
      notification_id: 'notif-1', silenced: false,
      results: [
        { channel: 'web', delivered: true },
        { channel: 'slack', delivered: true },
        { channel: 'telegram', delivered: false },
      ],
    });
    await renderSection();

    await userEvent.click(screen.getByRole('button', { name: 'Send a test' }));

    expect(await within(row('Slack')).findByText('Test arrived')).toBeInTheDocument();
    expect(within(row('Telegram')).getByText('Test did not arrive')).toBeInTheDocument();
    expect(screen.getByText('Sent. Each place shows whether it arrived.')).toBeInTheDocument();
  });

  it('says which parts did not reload after a save', async () => {
    api.saveSettingsNotifications.mockResolvedValue({
      ...settings(), reload_errors: { cron: 'the cron file is not valid' },
    });
    await renderSection();
    await userEvent.click(within(row('Slack')).getByRole('radio', { name: 'Questions and important' }));
    expect(await screen.findByText(/did not reload: cron \(the cron file is not valid\)/)).toBeInTheDocument();
  });

  it('links to the silences manager', async () => {
    await renderSection();
    expect(screen.getByRole('link', { name: /Silences/ })).toHaveAttribute('href', '/notifications?silences=1');
  });
});
