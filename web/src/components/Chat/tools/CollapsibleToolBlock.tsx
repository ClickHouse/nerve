import { useState, type ReactNode } from 'react';
import { ChevronRight, ChevronDown, Loader2 } from 'lucide-react';
import type { LucideIcon } from 'lucide-react';

interface CollapsibleToolBlockProps {
  /** Whether the tool is currently running. */
  isRunning: boolean;
  /** Whether the tool resulted in an error. */
  isError?: boolean;
  /** Primary icon shown in collapsed state. */
  icon: LucideIcon;
  /** CSS class for the icon (color). Falls back to text-[#888]. */
  iconClassName?: string;
  /** Short label next to the icon (e.g. "List Tasks"). */
  label: string;
  /** CSS class for the label text. Falls back to text-[#ccc]. */
  labelClassName?: string;
  /** Extra elements rendered between label and chevron. */
  headerExtra?: ReactNode;
  /** Border + background theme. Defaults to neutral gray. */
  theme?: 'default' | 'amber' | 'purple' | 'cyan';
  /** Start expanded? Defaults to false. */
  defaultExpanded?: boolean;
  /** Content shown when expanded. */
  children: ReactNode;
}

const THEMES = {
  default: {
    border: 'border-[#2a2a2a]',
    bg: 'bg-[#141414]',
    hover: 'hover:bg-[#1a1a1a]',
    divider: 'border-[#2a2a2a]',
    spinner: 'text-[#6366f1]',
  },
  amber: {
    border: 'border-amber-500/20',
    bg: 'bg-[#141411]',
    hover: 'hover:bg-[#1a1a18]',
    divider: 'border-amber-500/10',
    spinner: 'text-amber-400',
  },
  purple: {
    border: 'border-purple-500/20',
    bg: 'bg-[#141418]',
    hover: 'hover:bg-[#1a1a20]',
    divider: 'border-purple-500/10',
    spinner: 'text-purple-400',
  },
  cyan: {
    border: 'border-cyan-500/20',
    bg: 'bg-[#141416]',
    hover: 'hover:bg-[#1a1a1e]',
    divider: 'border-cyan-500/10',
    spinner: 'text-cyan-400',
  },
} as const;

export function CollapsibleToolBlock({
  isRunning,
  isError,
  icon: Icon,
  iconClassName = 'text-[#888]',
  label,
  labelClassName = 'text-[#ccc]',
  headerExtra,
  theme = 'default',
  defaultExpanded = false,
  children,
}: CollapsibleToolBlockProps) {
  const [expanded, setExpanded] = useState(defaultExpanded);
  const t = THEMES[theme];

  return (
    <div className={`my-1.5 border ${t.border} rounded-lg ${t.bg} overflow-hidden`}>
      <button
        onClick={() => setExpanded(!expanded)}
        className={`flex items-center gap-2 w-full px-3 py-2 text-left cursor-pointer ${t.hover} transition-colors`}
      >
        {isRunning
          ? <Loader2 size={14} className={`${t.spinner} animate-spin shrink-0`} />
          : <Icon size={14} className={`shrink-0 ${isError ? 'text-red-400' : iconClassName}`} />
        }
        <span className={`text-[13px] font-medium ${labelClassName}`}>{label}</span>
        {headerExtra}
        <div className="ml-auto shrink-0">
          {expanded ? <ChevronDown size={14} className="text-[#555]" /> : <ChevronRight size={14} className="text-[#555]" />}
        </div>
      </button>

      {expanded && (
        <div className={`border-t ${t.divider}`}>
          {children}
        </div>
      )}
    </div>
  );
}
