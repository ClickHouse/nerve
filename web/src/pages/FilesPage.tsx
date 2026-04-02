import { useEffect } from 'react';
import { useFilesStore } from '../stores/filesStore';
import { FileTree } from '../components/Files/FileTree';
import { EditorTabBar } from '../components/Files/EditorTabBar';
import { FileEditor } from '../components/Files/FileEditor';
import { FolderOpen } from 'lucide-react';

export function FilesPage() {
  const {
    tree, openFiles, activeFile, loading, saving,
    loadTree, openFile, closeFile, setActiveFile, updateContent, saveFile,
  } = useFilesStore();

  useEffect(() => { loadTree(); }, []);

  const currentFile = openFiles.find(f => f.path === activeFile);

  return (
    <div className="h-full flex">
      {/* File tree sidebar */}
      <div className="w-64 bg-bg border-r border-border-subtle flex flex-col shrink-0">
        <div className="flex items-center gap-2 p-3 border-b border-border-subtle">
          <FolderOpen size={16} className="text-[#6366f1]" />
          <span className="text-sm font-medium text-text-muted">Workspace</span>
        </div>
        <div className="flex-1 overflow-y-auto">
          <FileTree tree={tree} selectedPath={activeFile} onSelect={openFile} />
        </div>
      </div>

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
  );
}
