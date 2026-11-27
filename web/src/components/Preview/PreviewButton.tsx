import { usePreviewStore } from '../../stores/previewStore';
import { IconButton } from '../ui';
import { Monitor } from '../ui/icons';
import { canPreview, openPreview } from './open';

/**
 * The action that opens the preview of a file in the side panel. Nothing
 * while the Preview tab is off, and nothing for a file that has no preview.
 * `onOpen` runs after the tab opens (the Files page goes to the chat).
 */
export function PreviewButton({ path, onOpen, className }: {
  path: string;
  onOpen?: () => void;
  className?: string;
}) {
  const enabled = usePreviewStore(s => s.enabled);
  if (!enabled || !canPreview(path)) return null;
  const name = path.split('/').pop() || path;
  return (
    <IconButton
      label={`Preview ${name}`}
      size="xs"
      className={className}
      onClick={(event) => {
        event.stopPropagation();
        openPreview(path);
        onOpen?.();
      }}
    >
      <Monitor size={13} />
    </IconButton>
  );
}
