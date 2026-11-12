import { useState } from 'react';
import { Folder, VSCode } from '../ui/icons';
import type { SessionWorkspace } from '../../types/chat';
import { copyToClipboard } from '../../utils/clipboard';
import { vscodeFolderUrl } from '../../utils/vscodeLinks';
import { isHosted } from '../../api/hosted';

const moved = (w: SessionWorkspace) => w.exists && w.current_branch !== w.branch;

/** The git checkout the session last changed: folder above branch; a click copies its path. */
export function WorkspaceChip({ workspaces }: { workspaces: SessionWorkspace[] }) {
  const [copied, setCopied] = useState(false);
  const latest = workspaces[0];
  const folder = latest.path.split('/').filter(Boolean).pop() ?? latest.path;
  const title = [
    ...workspaces.map(w => {
      const now = moved(w) ? `, now on ${w.current_branch ?? 'detached HEAD'}` : '';
      return `${w.path} (${w.branch ?? 'detached HEAD'}${now}${w.exists ? '' : ', removed'})`;
    }),
    'Click to copy the path',
  ].join('\n');
  return (
    <span className="hidden md:inline-flex items-center gap-1 min-w-0 max-w-[12rem]">
      <button
        type="button"
        title={title}
        onClick={async () => {
          if (await copyToClipboard(latest.path)) {
            setCopied(true);
            setTimeout(() => setCopied(false), 1500);
          }
        }}
        className="inline-flex items-center gap-1.5 min-w-0 overflow-hidden px-1.5 rounded text-text-faint hover:bg-surface-hover cursor-pointer"
      >
        <Folder size={12} className="shrink-0" />
        {copied ? (
          <span className="text-xs">Path copied</span>
        ) : (
          <span className={`flex flex-col min-w-0 text-left text-2xs leading-3 ${latest.exists ? '' : 'line-through'}`}>
            <span className="truncate text-text-secondary">{folder}</span>
            <span className={`truncate font-mono ${moved(latest) ? 'text-warning' : ''}`}>{latest.branch ?? 'detached'}</span>
          </span>
        )}
        {workspaces.length > 1 && <span className="shrink-0 text-2xs">+{workspaces.length - 1}</span>}
      </button>
      {latest.exists && !isHosted() && (
        <a
          href={vscodeFolderUrl(latest.path)}
          title={`Open ${folder} in VS Code`}
          aria-label={`Open ${folder} in VS Code`}
          className="shrink-0 p-0.5 rounded text-text-faint hover:text-hue-blue hover:bg-surface-hover"
        >
          <VSCode size={13} />
        </a>
      )}
    </span>
  );
}
