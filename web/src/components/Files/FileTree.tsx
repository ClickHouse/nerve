import { useState } from 'react';
import { ChevronRight, ChevronDown, File, Folder, FolderOpen } from 'lucide-react';
import type { FileNode } from '../../utils/fileTree';

function FileTreeNode({ node, depth, selectedPath, onSelect }: {
  node: FileNode;
  depth: number;
  selectedPath: string | null;
  onSelect: (path: string) => void;
}) {
  const [expanded, setExpanded] = useState(depth < 2);

  if (node.type === 'directory') {
    return (
      <div>
        <button
          onClick={() => setExpanded(!expanded)}
          className="flex items-center gap-1.5 w-full text-left px-2 py-1 text-[13px] text-text-muted hover:bg-surface-raised cursor-pointer rounded"
          style={{ paddingLeft: depth * 16 + 8 }}
        >
          {expanded
            ? <ChevronDown size={12} className="shrink-0 text-text-faint" />
            : <ChevronRight size={12} className="shrink-0 text-text-faint" />
          }
          {expanded
            ? <FolderOpen size={14} className="shrink-0 text-[#6366f1]" />
            : <Folder size={14} className="shrink-0 text-[#6366f1]" />
          }
          <span className="truncate">{node.name}</span>
        </button>
        {expanded && node.children?.map(child => (
          <FileTreeNode
            key={child.path}
            node={child}
            depth={depth + 1}
            selectedPath={selectedPath}
            onSelect={onSelect}
          />
        ))}
      </div>
    );
  }

  const isSelected = selectedPath === node.path;
  return (
    <button
      onClick={() => onSelect(node.path)}
      className={`flex items-center gap-1.5 w-full text-left px-2 py-1 text-[13px] cursor-pointer rounded
        ${isSelected ? 'bg-[#1f1f2f] text-text' : 'text-text-muted hover:bg-surface-raised hover:text-text-secondary'}`}
      style={{ paddingLeft: depth * 16 + 20 }}
    >
      <File size={13} className="shrink-0 text-text-dim" />
      <span className="truncate">{node.name}</span>
    </button>
  );
}

export function FileTree({ tree, selectedPath, onSelect }: {
  tree: FileNode[];
  selectedPath: string | null;
  onSelect: (path: string) => void;
}) {
  return (
    <div className="py-1">
      {tree.map(node => (
        <FileTreeNode
          key={node.path}
          node={node}
          depth={0}
          selectedPath={selectedPath}
          onSelect={onSelect}
        />
      ))}
    </div>
  );
}
