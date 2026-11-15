import type { SettingField } from '../../api/settings';
import { Bell, type Icon } from '../ui/icons';

/** One section of the Settings page: `/settings/<id>`. */
export interface SettingsSection {
  id: string;
  label: string;
  /** One line under the label in the phone's section list. */
  summary: string;
  icon: Icon;
}

/** The sections, in the order of the section list. The first one opens on a desktop. */
export const SETTINGS_SECTIONS: readonly SettingsSection[] = [
  { id: 'notifications', label: 'Notifications', summary: 'Where they go and how much', icon: Bell },
];

/** The person may change the field on this page. */
export function canEdit(field: SettingField<unknown>): boolean {
  return field.state === 'editable' || field.state === 'machine';
}
