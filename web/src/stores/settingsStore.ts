import { create } from 'zustand';
import { api } from '../api/client';
import type {
  NotificationSettingPath, NotificationSettingValues, NotificationSettings, NotificationTestResult,
} from '../api/settings';
import { errorDetail } from './accountStore';
import { fieldErrorOf } from './setupStore';

/** A refused save. `field` is null when the refusal is about the whole body. */
export interface SettingsSaveError {
  field: string | null;
  detail: string;
}

export interface SettingsStoreState {
  notifications: NotificationSettings | null;
  loading: boolean;
  loadError: string | null;
  /** The fields of the save in flight, so only their controls show busy. */
  saving: NotificationSettingPath[];
  /** Set after a save that went through; cleared by the next one. */
  saved: boolean;
  saveError: SettingsSaveError | null;
  /** `{part: error}` for each part that did not reload after the last save. */
  reloadErrors: Record<string, string>;
  testing: boolean;
  testResult: NotificationTestResult | null;
  testError: string | null;
  loadNotifications: () => Promise<void>;
  /** Sends the changed fields; true when the server took them. */
  saveNotifications: (values: NotificationSettingValues) => Promise<boolean>;
  /** Sends a test once the saves in flight are done. */
  sendTest: () => Promise<void>;
}

const INITIAL = {
  notifications: null,
  loading: false,
  loadError: null,
  saving: [],
  saved: false,
  saveError: null,
  reloadErrors: {},
  testing: false,
  testResult: null,
  testError: null,
} satisfies Partial<SettingsStoreState>;

/** The save in flight: the next one starts when it ends. */
let saves: Promise<unknown> = Promise.resolve();

/**
 * The Settings page's sections. Saves run one after another, in the order
 * the person made the changes, so a click while a text field saves on blur
 * is not lost. A test waits for them, so it uses the saved configuration,
 * and a change clears the result of the last test.
 */
export const useSettingsStore = create<SettingsStoreState>((set, get) => ({
  ...INITIAL,

  loadNotifications: async () => {
    // A new visit starts clean: no "Saved", no old refusal or test result.
    set({ ...INITIAL, notifications: get().notifications, loading: true });
    try {
      set({ notifications: await api.settingsNotifications(), loading: false });
    } catch (e) {
      set({ loading: false, loadError: errorDetail(e, 'Settings could not be loaded.') });
    }
  },

  saveNotifications: (values) => {
    set({ testResult: null, testError: null });
    const run = saves.then(async () => {
      set({ saving: Object.keys(values) as NotificationSettingPath[], saved: false, saveError: null });
      try {
        const { reload_errors: reloadErrors = {}, ...notifications } = await api.saveSettingsNotifications(values);
        set({ notifications, saving: [], saved: true, reloadErrors });
        return true;
      } catch (e) {
        const field = fieldErrorOf(e);
        set({ saving: [], saveError: field ?? { field: null, detail: errorDetail(e, 'Not saved.') } });
        return false;
      }
    });
    saves = run;
    return run;
  },

  sendTest: async () => {
    if (get().testing) return;
    set({ testing: true, testResult: null, testError: null });
    await saves;
    try {
      set({ testing: false, testResult: await api.testSettingsNotifications() });
    } catch (e) {
      set({ testing: false, testError: errorDetail(e, 'The test was not sent.') });
    }
  },
}));
