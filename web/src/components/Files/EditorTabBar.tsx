import { IconButton } from '../ui';
import { X } from '../ui/icons';

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
          className={`flex items-center gap-1.5 px-3 py-2 text-sm cursor-pointer border-r border-border-subtle shrink-0
            ${f.path === activePath
              ? 'bg-bg text-text border-b-2 border-b-accent'
              : 'text-text-muted hover:text-text-secondary hover:bg-surface-raised'
            }`}
          onClick={() => onSelect(f.path)}
        >
          <span>{f.name}</span>
          {f.modified && <span className="w-1.5 h-1.5 rounded-full bg-accent" />}
          {/* Named per tab: a row of identical "Close" buttons tells a screen
              reader nothing about which file each one closes. */}
          <IconButton
            label={`Close ${f.name}`}
            size="xs"
            onClick={(e) => { e.stopPropagation(); onClose(f.path); }}
          >
            <X size={12} />
          </IconButton>
        </div>
      ))}
    </div>
  );
}
