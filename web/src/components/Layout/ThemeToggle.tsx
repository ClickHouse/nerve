import { Sun, Moon, Monitor } from 'lucide-react';
import { useThemeStore } from '../../stores/themeStore';

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

  return (
    <button
      onClick={cycleTheme}
      className="w-10 h-10 rounded-lg flex items-center justify-center text-text-faint hover:text-text-muted hover:bg-surface-hover cursor-pointer transition-colors"
      title={THEME_LABELS[preference]}
    >
      <Icon size={16} />
    </button>
  );
}
