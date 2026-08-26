import { useThemeStore } from '../../stores/themeStore';
import { IconButton } from '../ui';
import { Sun, Moon, Monitor } from '../ui/icons';

const THEME_ICONS = {
  system: Monitor,
  light: Sun,
  dark: Moon,
} as const;

const THEME_LABELS = {
  system: 'System theme',
  light: 'Light mode',
  dark: 'Dark mode',
} as const;

export function ThemeToggle() {
  const preference = useThemeStore((s) => s.preference);
  const cycleTheme = useThemeStore((s) => s.cycleTheme);
  const Icon = THEME_ICONS[preference];

  // The label names the *current* preference, which is what the cycle
  // dark → light → system needs to expose: the glyph alone cannot say which of
  // the three states you are in. IconButton spends it as both `title` and
  // `aria-label`, so the control has an accessible name.
  return (
    <IconButton label={THEME_LABELS[preference]} size="md" onClick={cycleTheme}>
      <Icon size={16} />
    </IconButton>
  );
}
