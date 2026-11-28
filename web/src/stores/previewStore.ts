import { create } from 'zustand';
import type { ViewerPreview } from '../types/preview';

/**
 * Whether this Nerve shows the Preview tab, and the file events that a
 * preview reads its bundle again for.
 *
 * The answer of `GET /api/auth/me`, which the web UI reads at start, has a
 * `preview` member only while the gateway serves previews
 * (`applyViewerPreview`). Without it the tab is off and the web UI sends no
 * preview request. In local mode and in hosted mode alike; this is not the
 * switch of MCP Apps and widgets (`mcpAppsStore.ts`), which stay off in
 * hosted mode.
 */
interface PreviewState {
  enabled: boolean;
  /** The most bytes that the files of one preview may have together. */
  maxBytes: number;
  /** The newest `file_changed` event of the open chat: its path, and a counter. */
  fileChange: { path: string; seq: number } | null;
  noteFileChange: (path: string) => void;
}

/** The limit until the server says its own. */
export const DEFAULT_PREVIEW_MAX_BYTES = 10 * 1024 * 1024;

export const usePreviewStore = create<PreviewState>((set, get) => ({
  enabled: false,
  maxBytes: DEFAULT_PREVIEW_MAX_BYTES,
  fileChange: null,

  noteFileChange: (path: string) => {
    set({ fileChange: { path, seq: (get().fileChange?.seq ?? 0) + 1 } });
  },
}));

/** Take the `preview` member of `GET /api/auth/me` (absent: the tab is off). */
export function applyViewerPreview(preview: ViewerPreview | null | undefined): void {
  const limit = preview?.max_bytes;
  usePreviewStore.setState({
    enabled: !!preview && typeof preview === 'object',
    maxBytes: typeof limit === 'number' && Number.isSafeInteger(limit) && limit > 0 ? limit : DEFAULT_PREVIEW_MAX_BYTES,
  });
}

/** Forget the switch, for tests. */
export function resetPreviewConfig(): void {
  usePreviewStore.setState({ enabled: false, maxBytes: DEFAULT_PREVIEW_MAX_BYTES, fileChange: null });
}
