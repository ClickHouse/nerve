import { Badge } from '../ui';
import { Globe } from '../ui/icons';
import { SettingRow } from './SettingsRows';

/**
 * The "This browser" row of the Notifications section: system alerts in
 * this browser while a panel tab is open. Kept per browser, not on the
 * server, so the row reads and writes nothing of the section's fields.
 */
export function ThisBrowserRow() {
  return (
    <SettingRow
      icon={<Globe size={16} />}
      title="This browser"
      description="System alerts while a tab with the panel is open."
      control={<Badge tone="neutral">Not available yet</Badge>}
    />
  );
}
