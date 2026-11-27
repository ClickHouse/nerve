import { api } from '../../api/client';
import type { PreviewBundle } from '../../types/preview';
import { downloadBlob } from '../Chat/rich/export';
import { DOWNLOAD_FILES } from './messages';

/**
 * The answer to `download` from a table or Markdown page, in the Preview tab
 * and in the inline preview of the chat: the source file of the bundle,
 * which has a fixed name for the kind (`DOWNLOAD_FILES`), read with the
 * fingerprint of that bundle and saved under the name of the workspace file.
 * The kind of the bundle decides, never the kind that the page states: a
 * bundle of another kind gets nothing. The caller decides that `bundle` is
 * the bundle of the document in the frame, and that the document said
 * `ready`.
 */
export async function downloadSource(path: string, bundle: PreviewBundle): Promise<void> {
  const file = DOWNLOAD_FILES[bundle.kind];
  if (!file || !bundle.files.some(f => f.path === file)) return;
  const bytes = await api.getPreviewFile(path, file, bundle.fingerprint);
  const type = bundle.files.find(f => f.path === file)?.type ?? 'application/octet-stream';
  const name = bundle.path.split('/').pop() || file;
  downloadBlob(new Blob([bytes], { type }), name);
}
