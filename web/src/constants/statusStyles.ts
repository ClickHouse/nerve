/** Compact badge style — no border, used inside tool blocks. */
export const TASK_STATUS_COLORS: Record<string, string> = {
  pending: 'bg-yellow-500/15 text-yellow-400',
  'in-progress': 'bg-blue-500/15 text-blue-400',
  'in_progress': 'bg-blue-500/15 text-blue-400',
  done: 'bg-green-500/15 text-green-400',
  completed: 'bg-green-500/15 text-green-400',
  deferred: 'bg-[#333] text-[#888]',
};

/** Bordered pill style — used on pages and cards. */
export const TASK_STATUS_STYLES: Record<string, string> = {
  pending: 'bg-yellow-400/10 text-yellow-400 border-yellow-400/20',
  in_progress: 'bg-blue-400/10 text-blue-400 border-blue-400/20',
  done: 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20',
  deferred: 'bg-[#333]/50 text-[#888] border-[#333]',
};

/** Compact badge style for plan statuses in tool blocks. */
export const PLAN_STATUS_COLORS: Record<string, string> = {
  pending: 'bg-yellow-500/15 text-yellow-400',
  approved: 'bg-green-500/15 text-green-400',
  implementing: 'bg-blue-500/15 text-blue-400',
  declined: 'bg-red-500/15 text-red-400',
  superseded: 'bg-[#333] text-[#888]',
};

/** Bordered pill style for plan statuses on pages. */
export const PLAN_STATUS_STYLES: Record<string, string> = {
  pending: 'bg-yellow-400/10 text-yellow-400 border-yellow-400/20',
  approved: 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20',
  implementing: 'bg-blue-400/10 text-blue-400 border-blue-400/20',
  declined: 'bg-red-400/10 text-red-400 border-red-400/20',
  superseded: 'bg-[#333]/50 text-[#888] border-[#333]',
  failed: 'bg-red-400/10 text-red-400 border-red-400/20',
};

/** Plan type styles (skill-create, skill-update). */
export const PLAN_TYPE_STYLES: Record<string, { label: string; className: string }> = {
  'skill-create': { label: 'Skill', className: 'bg-purple-400/10 text-purple-400 border-purple-400/20' },
  'skill-update': { label: 'Skill Update', className: 'bg-purple-400/10 text-purple-300 border-purple-400/20' },
};

/** Bordered pill style for notification statuses. */
export const NOTIFICATION_STATUS_STYLES: Record<string, string> = {
  pending: 'bg-yellow-400/10 text-yellow-400 border-yellow-400/20',
  answered: 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20',
  expired: 'bg-[#333]/50 text-[#888] border-[#333]',
  dismissed: 'bg-[#333]/50 text-[#666] border-[#333]',
};

/** Text-only status colors for inline use (e.g. task lists). */
export const TASK_STATUS_TEXT_COLORS: Record<string, string> = {
  pending: 'text-yellow-400',
  in_progress: 'text-blue-400',
  done: 'text-green-400',
  deferred: 'text-[#888]',
};
