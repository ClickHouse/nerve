import { create } from 'zustand';
import { api } from '../api/client';
import { ApiError } from '../api/hosted';
import { buildFileTree, type FileNode } from '../utils/fileTree';

interface OpenFile {
  path: string;
  name: string;
  content: string;
  originalContent: string;
  modified: boolean;
  readOnly: boolean;
  revision: string;
  saveError: string | null;
}

interface FilesState {
  tree: FileNode[];
  openFiles: OpenFile[];
  activeFile: string | null;
  loading: boolean;
  saving: boolean;

  loadTree: () => Promise<void>;
  openFile: (path: string) => Promise<void>;
  closeFile: (path: string) => void;
  setActiveFile: (path: string) => void;
  updateContent: (path: string, content: string) => void;
  saveFile: (path: string) => Promise<void>;
}

export const useFilesStore = create<FilesState>((set, get) => ({
  tree: [],
  openFiles: [],
  activeFile: null,
  loading: false,
  saving: false,

  loadTree: async () => {
    try {
      const { files } = await api.listMemoryFiles();
      const tree = buildFileTree(files);
      set({ tree });
    } catch (e) {
      console.error('Failed to load file tree:', e);
    }
  },

  openFile: async (path: string) => {
    const existing = get().openFiles.find(f => f.path === path);
    if (existing) {
      set({ activeFile: path });
      // A clean cached tab may have changed while another agent was writing.
      // Refresh it on re-selection, but never replace local edits or a tab
      // that changed during the asynchronous read.
      if (!existing.modified && !get().saving) {
        try {
          const { content, revision, read_only } = await api.readMemoryFile(path);
          set(state => ({
            openFiles: state.openFiles.map(tab =>
              tab.path === path && !tab.modified && tab.revision === existing.revision && !state.saving
                ? { ...tab, content, originalContent: content, revision,
                    readOnly: !!read_only, saveError: null }
                : tab
            ),
          }));
        } catch (e) {
          console.error('Failed to refresh file:', e);
        }
      }
      return;
    }

    set({ loading: true });
    try {
      const { content, revision, read_only } = await api.readMemoryFile(path);
      const name = path.split('/').pop() || path;
      set(s => ({
        openFiles: [...s.openFiles, {
          path, name, content, originalContent: content, modified: false,
          readOnly: !!read_only, revision, saveError: null,
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
    // A pending save must not commit its response to a newly opened tab.
    if (get().saving) return;
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
        f.path === path
          ? { ...f, content, modified: content !== f.originalContent }
          : f
      ),
    }));
  },

  saveFile: async (path: string) => {
    const file = get().openFiles.find(f => f.path === path);
    if (!file || get().saving) return;

    set({ saving: true });
    try {
      const result = await api.writeMemoryFile(path, file.content, file.revision);
      set(s => ({
        openFiles: s.openFiles.map(f =>
          f.path === path
            ? { ...f, originalContent: file.content,
                modified: f.content !== file.content, revision: result.revision, saveError: null }
            : f
        ),
        saving: false,
      }));
    } catch (e) {
      console.error('Failed to save file:', e);
      set(s => ({
        openFiles: s.openFiles.map(f => f.path === path
          ? { ...f, saveError: e instanceof ApiError && e.status === 409
              ? 'This file changed on disk. Your edits were not saved. Copy your changes before reloading.'
              : 'Save failed. Your edits are still here; try again.' }
          : f),
        saving: false,
      }));
    }
  },
}));
