import { Badge } from '../ui';
import { Lock } from '../ui/icons';

/** Marks an MCP server that the organization provides through the MCP gateway. */
export function ManagedBadge({ className }: { className?: string }) {
  return (
    <Badge
      tone="info"
      className={className}
      title="Managed by your organization through the MCP gateway. Read-only."
    >
      <Lock size={10} aria-hidden="true" />
      Managed by organization
    </Badge>
  );
}
