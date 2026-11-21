import { create } from 'zustand';
import { api, type ReadOnlyReason } from '../api/client';
import { toFileNode, type DirListing } from '../utils/fileTree';

interface OpenFile {
  path: string;
  name: string;
  content: string;
  originalContent: string;
  modified: boolean;
  readOnly: boolean;
  readOnlyReason?: ReadOnlyReason;
  /** Not shown as text: binary, or over `files.max_file_bytes`. */
  unreadable?: 'binary' | 'too_large';
  size?: number;
}

interface FilesState {
  /** Loaded directories by workspace path; `''` is the root. */
  dirs: Record<string, DirListing>;
  /** Directories whose last listing failed, by path. */
  dirErrors: Record<string, string>;
  openFiles: OpenFile[];
  activeFile: string | null;
  loading: boolean;
  saving: boolean;

  /** Reload the root and forget every other loaded directory. */
  loadTree: () => Promise<void>;
  /** Load one directory, once; `force` reloads it. */
  loadDir: (path: string, force?: boolean) => Promise<void>;
  openFile: (path: string) => Promise<void>;
  closeFile: (path: string) => void;
  setActiveFile: (path: string) => void;
  updateContent: (path: string, content: string) => void;
  saveFile: (path: string) => Promise<void>;
}

export const useFilesStore = create<FilesState>((set, get) => ({
  dirs: {},
  dirErrors: {},
  openFiles: [],
  activeFile: null,
  loading: false,
  saving: false,

  loadTree: async () => {
    set({ dirs: {}, dirErrors: {} });
    await get().loadDir('', true);
  },

  loadDir: async (path: string, force = false) => {
    if (!force && get().dirs[path]) return;
    try {
      const listing = await api.listWorkspaceDir(path);
      set(s => {
        const dirErrors = { ...s.dirErrors };
        delete dirErrors[path];
        return {
          dirs: {
            ...s.dirs,
            [path]: {
              entries: listing.entries.map(toFileNode),
              total: listing.total,
              truncated: listing.truncated,
            },
          },
          dirErrors,
        };
      });
    } catch (e) {
      console.error('Failed to load directory:', e);
      set(s => ({ dirErrors: { ...s.dirErrors, [path]: 'Failed to load' } }));
    }
  },

  openFile: async (path: string) => {
    const existing = get().openFiles.find(f => f.path === path);
    if (existing) {
      set({ activeFile: path });
      return;
    }

    set({ loading: true });
    try {
      const file = await api.readWorkspaceFile(path);
      const name = path.split('/').pop() || path;
      const unreadable = file.binary ? 'binary' as const
        : file.too_large ? 'too_large' as const : undefined;
      set(s => ({
        openFiles: [...s.openFiles, {
          path, name, content: file.content, originalContent: file.content, modified: false,
          // A file shown as a notice has no text to save.
          readOnly: !!file.read_only || !!unreadable,
          readOnlyReason: file.read_only_reason,
          unreadable,
          size: file.size,
        }],
        activeFile: path,
        loading: false,
      }));
    } catch (e) {
      console.error('Failed to open file:', e);
      set({ loading: false });
    }
  },

  closeFile: (path: string) => {
    set(s => {
      const openFiles = s.openFiles.filter(f => f.path !== path);
      let activeFile = s.activeFile;
      if (activeFile === path) {
        activeFile = openFiles.length > 0 ? openFiles[openFiles.length - 1].path : null;
      }
      return { openFiles, activeFile };
    });
  },

  setActiveFile: (path: string) => set({ activeFile: path }),

  updateContent: (path: string, content: string) => {
    set(s => ({
      openFiles: s.openFiles.map(f =>
        f.path === path && !f.readOnly
          ? { ...f, content, modified: content !== f.originalContent }
          : f
      ),
    }));
  },

  saveFile: async (path: string) => {
    const file = get().openFiles.find(f => f.path === path);
    // Only memory markdown has a write route; everything else is read-only.
    if (!file || file.readOnly) return;

    set({ saving: true });
    try {
      await api.writeMemoryFile(path, file.content);
      set(s => ({
        openFiles: s.openFiles.map(f =>
          f.path === path ? { ...f, originalContent: file.content, modified: false } : f
        ),
        saving: false,
      }));
    } catch (e) {
      console.error('Failed to save file:', e);
      set({ saving: false });
    }
  },
}));
