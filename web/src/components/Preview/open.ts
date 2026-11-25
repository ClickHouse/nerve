import { useChatStore } from '../../stores/chatStore';

/**
 * Which files have a preview, and how a preview tab opens.
 *
 * The same rules as the bundles of the server (`nerve/artifacts/bundle.py`):
 * an HTML file (a file named `index.html` shows its whole folder), and an
 * image. The server decides; this only says where to offer the action.
 */
const PREVIEW_FILE = /\.(html|png|jpe?g|gif|webp|avif|svg)$/i;

/** True for a file path that has a preview. */
export function canPreview(path: string | null | undefined): boolean {
  return typeof path === 'string' && PREVIEW_FILE.test(path.trim());
}

/** The id of the preview tab of a path. */
export function previewTabId(path: string): string {
  return `preview:${path}`;
}

/** Open the preview tab of `path` in the side panel of the open chat, or focus it. */
export function openPreview(path: string): void {
  useChatStore.getState().openPanelTab({
    id: previewTabId(path),
    type: 'preview',
    label: 'Preview',
    subagentType: 'preview',
    description: path,
    content: null,
    prompt: '',
    streaming: false,
    status: 'complete',
    startedAt: Date.now(),
    blocks: [],
    previewPath: path,
  });
}
