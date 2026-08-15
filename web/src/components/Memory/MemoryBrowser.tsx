import { useEffect, useState } from 'react';
import { api } from '../../api/client';

interface MemFile {
  path: string;
  name: string;
  size: number;
  modified: string;
}

export function MemoryBrowser() {
  const [files, setFiles] = useState<MemFile[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [content, setContent] = useState('');
  const [editing, setEditing] = useState(false);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    api.listMemoryFiles().then(({ files }) => setFiles(files)).catch(console.error);
  }, []);

  const openFile = async (path: string) => {
    try {
      const { content } = await api.readMemoryFile(path);
      setSelected(path);
      setContent(content);
      setEditing(false);
    } catch (e) {
      console.error('Failed to read file:', e);
    }
  };

  const saveFile = async () => {
    if (!selected) return;
    setSaving(true);
    try {
      await api.writeMemoryFile(selected, content);
      setEditing(false);
    } catch (e) {
      console.error('Failed to save:', e);
    } finally {
      setSaving(false);
    }
  };

  const formatSize = (bytes: number) => {
    if (bytes < 1024) return `${bytes}B`;
    return `${(bytes / 1024).toFixed(1)}KB`;
  };

  return (
    <div className="flex h-full">
      {/* File list */}
      <div className="w-60 border-r border-border-subtle overflow-y-auto">
        <div className="p-3 border-b border-border-subtle text-sm font-medium text-text-muted">
          Memory Files
        </div>
        {files.map((f) => (
          <div
            key={f.path}
            onClick={() => openFile(f.path)}
            className={`px-3 py-1.5 text-sm cursor-pointer hover:bg-surface-raised truncate ${
              f.path === selected ? 'bg-surface-raised border-l-2 border-accent' : ''
            }`}
          >
            <div className="truncate">{f.name}</div>
            <div className="text-xs text-text-faint">{formatSize(f.size)}</div>
          </div>
        ))}
      </div>

      {/* Content */}
      <div className="flex-1 flex flex-col">
        {selected ? (
          <>
            <div className="flex items-center justify-between p-3 border-b border-border-subtle">
              <span className="text-sm font-medium">{selected}</span>
              <div className="flex gap-2">
                {editing ? (
                  <>
                    <button
                      onClick={saveFile}
                      disabled={saving}
                      className="text-xs px-2 py-1 bg-accent rounded text-on-accent cursor-pointer"
                    >
                      {saving ? 'Saving...' : 'Save'}
                    </button>
                    <button
                      onClick={() => { setEditing(false); openFile(selected); }}
                      className="text-xs px-2 py-1 bg-surface-raised border border-border-subtle rounded text-text-muted cursor-pointer"
                    >
                      Cancel
                    </button>
                  </>
                ) : (
                  <button
                    onClick={() => setEditing(true)}
                    className="text-xs px-2 py-1 bg-surface-raised border border-border-subtle rounded text-text-muted hover:text-text cursor-pointer"
                  >
                    Edit
                  </button>
                )}
              </div>
            </div>
            {editing ? (
              <textarea
                value={content}
                onChange={(e) => setContent(e.target.value)}
                className="flex-1 p-4 bg-bg text-text font-mono text-sm outline-none resize-none"
              />
            ) : (
              <pre className="flex-1 p-4 overflow-auto text-sm font-mono whitespace-pre-wrap">
                {content}
              </pre>
            )}
          </>
        ) : (
          <div className="flex-1 flex items-center justify-center text-text-faint">
            Select a file
          </div>
        )}
      </div>
    </div>
  );
}
