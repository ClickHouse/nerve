import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { MOBILE_QUERY } from '../../hooks/useMediaQuery';
import { useBrowserAlertStore } from '../../stores/browserAlertStore';
import { useNotificationStore, type Notification } from '../../stores/notificationStore';
import { stubBrowser } from '../../test/browserAlertFixtures';
import { BrowserAlertOffer, BrowserAlertsLink } from './BrowserAlertOffer';
import { NotificationToast } from './NotificationToast';

function notif(id: string, type: Notification['type']): Notification {
  return {
    id, session_id: 's1', session_title: null, type, title: `Item ${id}`, body: '', priority: 'normal',
    status: 'pending', options: type === 'notify' ? null : ['Yes', 'No'], answer: null, answered_by: null,
    answered_at: null, created_at: '2026-01-01T00:00:00Z', expires_at: null,
  };
}

function renderToasts(...queue: Notification[]) {
  useNotificationStore.setState({ toastQueue: queue });
  render(<MemoryRouter><NotificationToast /></MemoryRouter>);
}

beforeEach(() => {
  localStorage.clear();
  useBrowserAlertStore.setState({ enabled: false, level: 'important', offerDone: false });
});

afterEach(() => vi.unstubAllGlobals());

describe('BrowserAlertOffer', () => {
  it('shows on the first toast that waits for an answer only', () => {
    stubBrowser();
    const queue = [notif('n1', 'notify'), notif('q1', 'question'), notif('q2', 'question')];
    useNotificationStore.setState({ toastQueue: queue });
    const offers = queue.map((n) => render(<MemoryRouter><BrowserAlertOffer notif={n} /></MemoryRouter>).container);
    expect(offers.map((c) => within(c).queryByRole('button', { name: 'Turn on' }) !== null)).toEqual([false, true, false]);
  });

  it('is part of the toast', () => {
    stubBrowser();
    renderToasts(notif('q1', 'question'));
    expect(screen.getByText('Alert me when this tab is in the background?')).toBeInTheDocument();
  });

  it('turns alerts on and says so', async () => {
    const request = stubBrowser();
    renderToasts(notif('q1', 'question'));

    await userEvent.click(screen.getByRole('button', { name: 'Turn on' }));

    expect(request).toHaveBeenCalled();
    expect(useBrowserAlertStore.getState().enabled).toBe(true);
    expect(screen.getByRole('status')).toHaveTextContent('Alerts are on. Change them in Settings.');
    expect(screen.getByRole('link', { name: 'Settings' })).toHaveAttribute('href', '/settings/notifications');
  });

  it('says when the browser blocks alerts', async () => {
    stubBrowser({ answer: 'denied' });
    renderToasts(notif('q1', 'approval'));
    await userEvent.click(screen.getByRole('button', { name: 'Turn on' }));
    expect(screen.getByRole('status')).toHaveTextContent('The browser blocked alerts.');
  });

  it('does not come back after "Not now"', async () => {
    stubBrowser();
    renderToasts(notif('q1', 'question'));
    await userEvent.click(screen.getByRole('button', { name: 'Not now' }));
    expect(screen.queryByRole('button', { name: 'Turn on' })).not.toBeInTheDocument();
    expect(useBrowserAlertStore.getState().offerDone).toBe(true);
  });

  it.each([
    ['without HTTPS', { secure: false }],
    ['when the browser blocks alerts', { permission: 'denied' as const }],
  ])('is not shown %s', (_name, browser) => {
    stubBrowser(browser);
    renderToasts(notif('q1', 'question'));
    expect(screen.queryByRole('button', { name: 'Turn on' })).not.toBeInTheDocument();
  });
});

describe('BrowserAlertsLink', () => {
  it('links to the setting with the state of this browser, also on a phone', () => {
    const stub = window.matchMedia;
    const matchMedia = vi.spyOn(window, 'matchMedia').mockImplementation(
      (query: string) => ({ ...stub(query), matches: query === MOBILE_QUERY }),
    );
    stubBrowser({ permission: 'granted' });
    useBrowserAlertStore.setState({ enabled: true });
    render(<MemoryRouter><BrowserAlertsLink /></MemoryRouter>);
    expect(screen.getByRole('link', { name: 'Browser alerts are on' })).toHaveAttribute('href', '/settings/notifications');
    matchMedia.mockRestore();
  });
});
