import { create } from 'zustand';
import { api } from '../api/client';

/**
 * Whether this Nerve shows the Preview tab (`GET /api/preview/config`), and
 * the file events that a preview reads its bundle again for.
 *
 * Off until the answer is here, and off when the route does not answer: a
 * Nerve without the feature shows no preview action. In local mode and in
 * hosted mode alike; this is not the switch of MCP Apps and widgets
 * (`mcpAppsStore.ts`), which stay off in hosted mode.
 */
interface PreviewState {
  enabled: boolean;
  /** The most bytes that the files of one preview may have together. */
  maxBytes: number;
  /** The newest `file_changed` event of the open chat: its path, and a counter. */
  fileChange: { path: string; seq: number } | null;
  load: () => Promise<void>;
  noteFileChange: (path: string) => void;
}

/** The limit until the server says its own. */
export const DEFAULT_PREVIEW_MAX_BYTES = 10 * 1024 * 1024;

const CONFIG_DEADLINE_MS = 10_000;

let reading: Promise<void> | null = null;

export const usePreviewStore = create<PreviewState>((set, get) => ({
  enabled: false,
  maxBytes: DEFAULT_PREVIEW_MAX_BYTES,
  fileChange: null,

  load: () => {
    reading ??= (async () => {
      try {
        const config = await api.getPreviewConfig(AbortSignal.timeout?.(CONFIG_DEADLINE_MS));
        const maxBytes = Number.isSafeInteger(config.max_bytes) && config.max_bytes > 0
          ? config.max_bytes : DEFAULT_PREVIEW_MAX_BYTES;
        set({ enabled: config.enabled === true, maxBytes });
      } catch {
        reading = null;
        set({ enabled: false });
      }
    })();
    return reading;
  },

  noteFileChange: (path: string) => {
    set({ fileChange: { path, seq: (get().fileChange?.seq ?? 0) + 1 } });
  },
}));

/** Forget the read, for tests. */
export function resetPreviewConfig(): void {
  reading = null;
  usePreviewStore.setState({ enabled: false, maxBytes: DEFAULT_PREVIEW_MAX_BYTES, fileChange: null });
}
