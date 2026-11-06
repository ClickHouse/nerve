import { useEffect, useState, type ReactNode } from 'react';
import { useFilesStore } from '../stores/filesStore';
import { FileTree } from '../components/Files/FileTree';
import { EditorTabBar } from '../components/Files/EditorTabBar';
import { FileEditor } from '../components/Files/FileEditor';
import { FolderOpen, X } from '../components/ui/icons';
import { IconButton } from '../components/ui';
import { PageHeader } from '../components/ui/PageHeader';
import { PaneToggle } from '../components/ui/PaneToggle';
import { Drawer } from '../components/ui/Drawer';
import { useIsMobile } from '../hooks/useMediaQuery';

/** The "Workspace" heading and the file tree under it. */
function TreePane({ inDrawer = false, onClose, children }: {
  inDrawer?: boolean;
  /** Drawer mode only: the visible close control in the heading row. */
  onClose?: () => void;
  children: ReactNode;
}) {
  return (
    <div className={inDrawer
      // The drawer supplies the width, the background and the edge.
      ? 'w-full flex-1 min-h-0 flex flex-col'
      : 'w-64 bg-bg border-r border-border-subtle flex flex-col shrink-0'}>
      <div className="flex items-center gap-2 p-3 border-b border-border-subtle">
        <FolderOpen size={16} className="text-accent" />
        <span className="text-sm font-medium text-text-muted">Workspace</span>
        {onClose && (
          <IconButton label="Close file list" size="xs" onClick={onClose} className="ml-auto">
            <X size={14} />
          </IconButton>
        )}
      </div>
      <div className="flex-1 overflow-y-auto">
        {children}
      </div>
    </div>
  );
}

export function FilesPage() {
  const {
    tree, openFiles, activeFile, loading, saving,
    loadTree, openFile, closeFile, setActiveFile, updateContent, saveFile,
  } = useFilesStore();

  useEffect(() => { loadTree(); }, []);

  // On a phone the tree is a left drawer: a 256px column leaves the editor
  // too narrow to read. It starts open when no file is open, because the
  // editor pane has nothing to show until a file is picked.
  const isMobile = useIsMobile();
  const [treeOpen, setTreeOpen] = useState(() => activeFile === null);

  // Retire the drawer when the layout leaves the phone breakpoint, so a later
  // resize back down does not arrive with an overlay already open. Adjusted
  // during render rather than in an effect: an effect paints the stale state
  // for a frame first.
  const [lastIsMobile, setLastIsMobile] = useState(isMobile);
  if (lastIsMobile !== isMobile) {
    setLastIsMobile(isMobile);
    setTreeOpen(false);
  }

  // `FileTree` calls `onSelect` for files only. A directory row expands or
  // collapses in place, so the drawer stays open while the user browses.
  const openFromDrawer = (path: string) => {
    openFile(path);
    setTreeOpen(false);
  };

  const currentFile = openFiles.find(f => f.path === activeFile);

  return (
    <div className="h-full flex flex-col">
      {isMobile && (
        <PageHeader
          leading={<PaneToggle open={treeOpen} onToggle={() => setTreeOpen(o => !o)} label="file list" />}
          title="Files"
        />
      )}

      <div className="flex-1 flex min-h-0">
        {isMobile ? (
          <Drawer open={treeOpen} onClose={() => setTreeOpen(false)} side="left" label="Workspace files">
            <TreePane inDrawer onClose={() => setTreeOpen(false)}>
              <FileTree tree={tree} selectedPath={activeFile} onSelect={openFromDrawer} />
            </TreePane>
          </Drawer>
        ) : (
          <TreePane>
            <FileTree tree={tree} selectedPath={activeFile} onSelect={openFile} />
          </TreePane>
        )}

        {/* Editor area */}
        <div className="flex-1 flex flex-col min-w-0">
          <EditorTabBar
            files={openFiles}
            activePath={activeFile}
            onSelect={setActiveFile}
            onClose={closeFile}
          />

          {currentFile ? (
            <FileEditor
              path={currentFile.path}
              content={currentFile.content}
              modified={currentFile.modified}
              readOnly={currentFile.readOnly}
              saving={saving}
              onContentChange={(c) => updateContent(currentFile.path, c)}
              onSave={() => saveFile(currentFile.path)}
            />
          ) : (
            <div className="flex-1 flex items-center justify-center text-text-faint">
              {loading ? 'Loading...' : 'Select a file to edit'}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
