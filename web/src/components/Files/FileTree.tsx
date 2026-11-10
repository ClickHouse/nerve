import { useState } from 'react';
import type { DirListing, FileNode } from '../../utils/fileTree';
import { ChevronRight, ChevronDown, File, Folder, FolderOpen, Lock } from '../ui/icons';

interface TreeProps {
  /** Loaded directories by workspace path; `''` is the root. */
  dirs: Record<string, DirListing>;
  dirErrors?: Record<string, string>;
  selectedPath: string | null;
  onSelect: (path: string) => void;
  /** Called when a directory is expanded, so its children can be loaded. */
  onExpand: (path: string) => void;
}

function lockTitle(node: FileNode): string {
  if (!node.readOnly) return node.name;
  return node.readOnlyReason === 'lockdown'
    ? `${node.name} — read-only (lockdown)`
    : `${node.name} — read-only`;
}

/** The children of `path`: loaded entries, or a placeholder row. */
function Children({ path, depth, ...props }: TreeProps & { path: string; depth: number }) {
  const listing = props.dirs[path];
  const pad = { paddingLeft: depth * 16 + 20 };
  if (!listing) {
    const error = props.dirErrors?.[path];
    return (
      <div className="px-2 py-1 text-xs text-text-faint" style={pad}>
        {error ?? 'Loading…'}
      </div>
    );
  }
  return (
    <>
      {listing.entries.map(child => (
        <FileTreeNode key={child.path} node={child} depth={depth} {...props} />
      ))}
      {listing.entries.length === 0 && (
        <div className="px-2 py-1 text-xs text-text-faint" style={pad}>Empty</div>
      )}
      {listing.truncated && (
        <div className="px-2 py-1 text-xs text-text-faint" style={pad}>
          {listing.total - listing.entries.length} more not shown
        </div>
      )}
    </>
  );
}

function FileTreeNode({ node, depth, ...props }: TreeProps & { node: FileNode; depth: number }) {
  const [expanded, setExpanded] = useState(false);

  if (node.type === 'directory') {
    const toggle = () => {
      if (!expanded) props.onExpand(node.path);
      setExpanded(!expanded);
    };
    return (
      <div>
        {/* Native, like the app's other list rows. `Button`'s `xs`/`sm` sizes
            pin the label to `text-xs`, and Tailwind emits `.text-xs` after
            `.text-sm`, so a call site cannot ask for a 14px tree row; the one
            size that does carry `text-sm` is `md`, whose `py-2` doubles the
            height of a row in a tree that is often 200 deep. */}
        <button
          onClick={toggle}
          aria-expanded={expanded}
          className="flex items-center gap-1.5 w-full text-left px-2 py-1 text-sm text-text-muted hover:bg-surface-raised cursor-pointer rounded"
          style={{ paddingLeft: depth * 16 + 8 }}
        >
          {expanded
            ? <ChevronDown size={12} className="shrink-0 text-text-faint" />
            : <ChevronRight size={12} className="shrink-0 text-text-faint" />
          }
          {expanded
            ? <FolderOpen size={14} className="shrink-0 text-accent" />
            : <Folder size={14} className="shrink-0 text-accent" />
          }
          <span className="truncate">{node.name}</span>
        </button>
        {expanded && <Children path={node.path} depth={depth + 1} {...props} />}
      </div>
    );
  }

  const isSelected = props.selectedPath === node.path;
  return (
    <button
      onClick={() => props.onSelect(node.path)}
      className={`flex items-center gap-1.5 w-full text-left px-2 py-1 text-sm cursor-pointer rounded
        ${isSelected ? 'bg-accent/10 text-text' : 'text-text-muted hover:bg-surface-raised hover:text-text-secondary'}`}
      style={{ paddingLeft: depth * 16 + 20 }}
      title={lockTitle(node)}
    >
      {/* The lock marks a file a locked instance refuses to save. Plain
          workspace files are read-only too, but that is the norm here, so
          they keep the plain file icon. */}
      {node.readOnly && node.readOnlyReason === 'lockdown'
        ? <Lock size={13} className="shrink-0 text-text-faint" />
        : <File size={13} className={`shrink-0 ${node.readOnly ? 'text-text-faint' : 'text-text-dim'}`} />
      }
      <span className="truncate">{node.name}</span>
    </button>
  );
}

export function FileTree(props: TreeProps) {
  return (
    <div className="py-1">
      <Children path="" depth={0} {...props} />
    </div>
  );
}
