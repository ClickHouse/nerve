import { create } from 'zustand';
import { api } from '../api/client';
import { buildFileTree, type FileNode } from '../utils/fileTree';

interface OpenFile {
  path: string;
  name: string;
  content: string;
  originalContent: string;
  modified: boolean;
  readOnly: boolean;
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
      return;
    }

    set({ loading: true });
    try {
      const { content, read_only } = await api.readMemoryFile(path);
      const name = path.split('/').pop() || path;
      set(s => ({
        openFiles: [...s.openFiles, {
          path, name, content, originalContent: content, modified: false,
          readOnly: !!read_only,
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
        f.path === path
          ? { ...f, content, modified: content !== f.originalContent }
          : f
      ),
    }));
  },

  saveFile: async (path: string) => {
    const file = get().openFiles.find(f => f.path === path);
    if (!file) return;

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
