import { X } from 'lucide-react';

interface OpenFile {
  path: string;
  name: string;
  modified: boolean;
}

export function EditorTabBar({ files, activePath, onSelect, onClose }: {
  files: OpenFile[];
  activePath: string | null;
  onSelect: (path: string) => void;
  onClose: (path: string) => void;
}) {
  if (files.length === 0) return null;

  return (
    <div className="flex border-b border-border-subtle bg-surface overflow-x-auto">
      {files.map(f => (
        <div
          key={f.path}
          className={`flex items-center gap-1.5 px-3 py-2 text-[13px] cursor-pointer border-r border-border-subtle shrink-0
            ${f.path === activePath
              ? 'bg-bg text-text border-b-2 border-b-[#6366f1]'
              : 'text-text-muted hover:text-text-secondary hover:bg-surface-raised'
            }`}
          onClick={() => onSelect(f.path)}
        >
          <span>{f.name}</span>
          {f.modified && <span className="w-1.5 h-1.5 rounded-full bg-[#6366f1]" />}
          <button
            onClick={(e) => { e.stopPropagation(); onClose(f.path); }}
            className="p-0.5 hover:bg-border-subtle rounded cursor-pointer"
          >
            <X size={12} />
          </button>
        </div>
      ))}
    </div>
  );
}
