import type { ReadOnlyReason, WorkspaceEntry } from '../api/client';

export interface FileNode {
  name: string;
  path: string;
  type: 'file' | 'directory';
  size?: number;
  modified?: string;
  // The server's answer, not the client's guess: only memory markdown is
  // writable, and on a locked instance not even all of that.
  readOnly?: boolean;
  readOnlyReason?: ReadOnlyReason;
}

/** One loaded directory of the workspace tree. */
export interface DirListing {
  entries: FileNode[];
  total: number;
  truncated: boolean;
}

export function toFileNode(e: WorkspaceEntry): FileNode {
  return {
    name: e.name,
    path: e.path,
    type: e.type,
    size: e.size,
    modified: e.modified,
    readOnly: e.read_only,
    readOnlyReason: e.read_only_reason ?? null,
  };
}
