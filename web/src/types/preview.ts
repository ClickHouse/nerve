/**
 * The Preview tab: what `GET /api/preview/*` answers (`nerve/gateway/routes/preview.py`).
 */

/** `GET /api/preview/config`. */
export interface PreviewConfig {
  enabled: boolean;
  /** The most bytes that the files of one preview may have together. */
  max_bytes: number;
}

/** What the path of a bundle names. */
export type PreviewBundleKind = 'folder' | 'page' | 'image';

/** One file of a bundle. */
export interface PreviewBundleFile {
  /** Relative to the root of the bundle, with `/` between the names. */
  path: string;
  size: number;
  /** The type that the server gives the file, from its extension. */
  type: string;
}

/** `GET /api/preview/bundle?path=`: the description of the bundle of a workspace path. */
export interface PreviewBundle {
  /** The path that was asked for, relative to the workspace. */
  path: string;
  /** The folder of the bundle, relative to the workspace (`''` for the root). */
  root: string;
  /** The same folder as an absolute path. */
  root_path: string;
  kind: PreviewBundleKind;
  /** The path of the entry file, relative to the root. */
  entry: string;
  files: PreviewBundleFile[];
  size: number;
  /** Changes when a file of the bundle changes. */
  fingerprint: string;
  /** The newest modification time of a file, in milliseconds since the epoch. */
  modified: number;
}
