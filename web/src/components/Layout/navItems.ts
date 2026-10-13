import {
  MessageSquare, FolderOpen, CheckSquare, Inbox, Activity, Brain, Clock,
  Lightbulb, Sparkles, Bell, Plug, Workflow, Rocket, Users, ShieldCheck, type Icon,
} from '../ui/icons';

export type NavItem = {
  path: string;
  /** The destination's glyph. `Icon` is the shape every icon in `ui/icons`
   *  has, so NavRail and BottomNav can both render it as `<Icon size={n}/>`. */
  icon: Icon;
  label: string;
  /** Hidden unless the named feature is enabled. `accounts` is enabled in
   *  local mode only: hosted Nerve has no local accounts. `setup` is enabled
   *  in both modes once the setup checklist has been read; hidden only while
   *  it is unknown (an older backend, or before the first read answers). */
  feature?: 'ultracode' | 'accounts' | 'setup';
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
  { path: '/accounts', icon: Users, label: 'Accounts', feature: 'accounts' },
  { path: '/setup', icon: ShieldCheck, label: 'Setup', feature: 'setup' },
  { path: '/diagnostics', icon: Activity, label: 'Diag' },
];

/** What decides which feature-gated entries show. */
export interface NavVisibility {
  hosted: boolean;
  ultracodeEnabled: boolean;
  /** The setup store has read the checklist at least once. */
  setupKnown: boolean;
}

/**
 * The entries to render, pure in its inputs. Callers subscribe to the stores
 * (`useHostedStore`, `useSetupStore`) and pass the values in — never read a
 * store with `getState()` inside a render-time filter, which would not
 * re-render when the answer changes.
 */
export function visibleNavItems(items: NavItem[], { hosted, ultracodeEnabled, setupKnown }: NavVisibility): NavItem[] {
  return items.filter((item) => {
    if (item.feature === 'ultracode' && !ultracodeEnabled) return false;
    if (item.feature === 'accounts' && hosted) return false;
    if (item.feature === 'setup' && !setupKnown) return false;
    return true;
  });
}

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
 * in a hurry. Accounts is the same kind of thing: opened once when somebody
 * joins or leaves, never in a hurry. Setup likewise.
 */
export const PRIMARY_PATHS = ['/chat', '/tasks', '/notifications', '/plans'];
