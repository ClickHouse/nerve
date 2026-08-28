import {
  MessageSquare, FolderOpen, CheckSquare, Inbox, Activity, Brain, Clock,
  Lightbulb, Sparkles, Bell, Plug, Workflow, Rocket, type Icon,
} from '../ui/icons';

export type NavItem = {
  path: string;
  /** The destination's glyph. `Icon` is the shape every icon in `ui/icons`
   *  has, so NavRail and BottomNav can both render it as `<Icon size={n}/>`. */
  icon: Icon;
  label: string;
  /** Hidden unless the named feature is enabled. */
  feature?: 'ultracode';
};

export const NAV_ITEMS: NavItem[] = [
  { path: '/chat', icon: MessageSquare, label: 'Chat' },
  { path: '/notifications', icon: Bell, label: 'Notifs' },
  { path: '/files', icon: FolderOpen, label: 'Files' },
  { path: '/tasks', icon: CheckSquare, label: 'Tasks' },
  { path: '/plans', icon: Lightbulb, label: 'Plans' },
  { path: '/skills', icon: Sparkles, label: 'Skills' },
  { path: '/mcp', icon: Plug, label: 'MCP' },
  { path: '/ultracode', icon: Workflow, label: 'Ultra', feature: 'ultracode' },
  { path: '/workflow-runs', icon: Rocket, label: 'Runs' },
  { path: '/sources', icon: Inbox, label: 'Sources' },
  { path: '/cron', icon: Clock, label: 'Cron' },
  { path: '/memory', icon: Brain, label: 'Memory' },
  { path: '/diagnostics', icon: Activity, label: 'Diag' },
];

/**
 * The four destinations that keep a permanent slot in the phone's bottom bar;
 * everything else lives behind "More".
 *
 * These are the ones you open to *decide* something — read what the agent
 * said, check a task, approve a plan, answer a blocking question. Notifs
 * earns its slot by carrying the pending-question badge, which is the whole
 * reason to open the panel on a phone and is useless if it is hidden behind
 * a menu. The rest (files, skills, MCP, cron, memory, diagnostics, sources)
 * are configuration and inspection surfaces — reached deliberately, rarely
 * in a hurry.
 */
export const PRIMARY_PATHS = ['/chat', '/tasks', '/notifications', '/plans'];
