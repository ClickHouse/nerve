import { PatchDiff, MultiFileDiff } from '@pierre/diffs/react';
import { useDiffOptions } from './diffTheme';
import { MAX_DIFF_LINES } from '../../types/chat';
import type { FileDiff } from '../../types/chat';

function Notice({ children }: { children: React.ReactNode }) {
  return (
    <div className="px-4 py-6 text-center text-[13px] text-text-faint">
      {children}
    </div>
  );
}

// ------------------------------------------------------------------ //
//  DiffView — backend-computed file diff, rendered from a patch string //
// ------------------------------------------------------------------ //

export function DiffView({ diff, wrap }: { diff: FileDiff; wrap?: boolean }) {
  const options = useDiffOptions({ wrap });

  if (diff.binary) {
    return <Notice>Binary file — diff not available</Notice>;
  }

  if (diff.status === 'unchanged' || !diff.patch) {
    return <Notice>No changes</Notice>;
  }

  return (
    <div className="diff-view">
      <PatchDiff patch={diff.patch} options={options} />
      {diff.truncated && (
        <div className="text-center py-3 text-[11px] text-text-faint bg-bg border-t border-border-subtle">
          Diff truncated at {MAX_DIFF_LINES} lines
        </div>
      )}
    </div>
  );
}

// ------------------------------------------------------------------ //
//  EditDiff — before/after diff for an Edit tool call (old/new text)  //
// ------------------------------------------------------------------ //

export function EditDiff({
  fileName,
  oldString,
  newString,
}: {
  fileName: string;
  oldString: string;
  newString: string;
}) {
  const options = useDiffOptions();
  return (
    <div className="diff-view">
      <MultiFileDiff
        oldFile={{ name: fileName, contents: oldString }}
        newFile={{ name: fileName, contents: newString }}
        options={options}
      />
    </div>
  );
}
