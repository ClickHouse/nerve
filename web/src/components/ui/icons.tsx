import { createLucideIcon, Star, type LucideProps } from 'lucide-react';

/**
 * The app's icon set.
 *
 * Every icon comes from here rather than from `lucide-react` directly. The
 * indirection costs nothing and gives the set one seam: a glyph can be swapped
 * for a different set, or drawn locally, without touching the 75 files that
 * render it.
 *
 * Call sites render icons as `<Name size={14} className="…"/>`. `size` is
 * pixels, `className` lands on the `<svg>` itself, and colour comes from
 * `currentColor`, so a `text-hue-red` on the call site or on any ancestor
 * carries.
 */

export type { LucideIcon as Icon } from 'lucide-react';

export {
  Activity, AlertCircle, AlertTriangle, Archive, ArchiveRestore, ArrowLeft,
  ArrowRight, ArrowUp, ArrowUpRight, Ban, BarChart3, Bell, BellOff, BellRing, BookOpen, Bot, Brain,
  Calendar, Check, CheckCheck, CheckCircle, CheckCircle2, CheckSquare,
  ChevronDown, ChevronLeft, ChevronRight, ChevronUp, Circle, CircleCheck, Cloud,
  KeyRound, ListChecks, ListTree, LocateFixed, PartyPopper,
  CircleDashed, CircleDollarSign, CircleHelp, CircleX, Clock, Clock3,
  Columns3, Copy, CornerDownLeft, Cpu, Database, DollarSign, Download,
  Edit3, ExternalLink, Eye, EyeOff, File, FileCheck, FileDiff, FileEdit,
  FilePlus, FileText, FileX, Files, Filter, Folder, FolderOpen, GitBranch,
  GitMerge, GitPullRequest, GitPullRequestClosed, GitPullRequestDraft,
  Globe, GripVertical, Hammer, HardDrive, HelpCircle, History,
  Hourglass, Inbox, Lightbulb, List, ListTodo, Loader2, Lock, LogOut, Mail,
  Maximize2, MessageCircle, MessageCircleQuestion, MessageSquare, MessagesSquare, Minus, Monitor,
  Moon, MoreHorizontal, OctagonX, PanelLeftClose, PanelLeftOpen, Paperclip,
  Pause, Pencil, Play, Plug, Plus, Radio, RefreshCw, Repeat, Reply, Rocket,
  RotateCcw, RotateCw, Save, Search, SearchCheck, Send, Server, Settings,
  ShieldCheck, ShieldQuestion, SlidersHorizontal, Sparkle, Sparkles,
  Square, SquareTerminal, Star, StickyNote, Sun, Tag, Terminal, Timer,
  User, Users,
  Trash2, TrendingDown, TrendingUp, Unlink, Workflow, WrapText, Wrench, X,
  XCircle, Zap
} from 'lucide-react';

// The rendered code blocks' figure toolbar and full-size view (Chat/rich).
export { CodeXml, ZoomIn, ZoomOut } from 'lucide-react';

// The send_image card's failure and missing-file states.
export { ImageOff } from 'lucide-react';

/*
 * The icon nodes of Github and Slack are from lucide-react 0.575.0.
 * License: ISC. Copyright (c) for portions of Lucide are held by Cole Bemis
 * 2013-2026 as part of Feather (MIT). All other copyright (c) for Lucide are
 * held by Lucide Contributors 2026.
 */
// eslint-disable-next-line react-refresh/only-export-components -- createLucideIcon returns a component.
export const Github = createLucideIcon({
  name: 'github',
  node: [
    [
      'path',
      {
        d: 'M15 22v-4a4.8 4.8 0 0 0-1-3.5c3 0 6-2 6-5.5.08-1.25-.27-2.48-1-3.5.28-1.15.28-2.35 0-3.5 0 0-1 0-3 1.5-2.64-.5-5.36-.5-8 0C6 2 5 2 5 2c-.3 1.15-.3 2.35 0 3.5A5.403 5.403 0 0 0 4 9c0 3.5 3 5.5 6 5.5-.39.49-.68 1.05-.85 1.65-.17.6-.22 1.23-.15 1.85v4',
        key: 'tonef',
      },
    ],
    ['path', { d: 'M9 18c-4.51 2-5-2-7-2', key: '9comsn' }],
  ],
});

// eslint-disable-next-line react-refresh/only-export-components -- createLucideIcon returns a component.
export const Slack = createLucideIcon({
  name: 'slack',
  node: [
    ['rect', { width: '3', height: '8', x: '13', y: '2', rx: '1.5', key: 'diqz80' }],
    ['path', { d: 'M19 8.5V10h1.5A1.5 1.5 0 1 0 19 8.5', key: '183iwg' }],
    ['rect', { width: '3', height: '8', x: '8', y: '14', rx: '1.5', key: 'hqg7r1' }],
    ['path', { d: 'M5 15.5V14H3.5A1.5 1.5 0 1 0 5 15.5', key: '76g71w' }],
    ['rect', { width: '8', height: '3', x: '14', y: '13', rx: '1.5', key: '1kmz0a' }],
    ['path', { d: 'M15.5 19H14v1.5a1.5 1.5 0 1 0 1.5-1.5', key: 'jc4sz0' }],
    ['rect', { width: '8', height: '3', x: '2', y: '8', rx: '1.5', key: '1omvl4' }],
    ['path', { d: 'M8.5 5H10V3.5A1.5 1.5 0 1 0 8.5 5', key: '16f3cl' }],
  ],
});

/**
 * A star with its interior filled, for a session that is starred.
 *
 * lucide ships the outline only. Filling from `currentColor` rather than a
 * fixed colour keeps one `text-*` class on the call site in charge of both the
 * stroke and the fill.
 */
export function StarFilled(props: LucideProps) {
  return <Star fill="currentColor" {...props} />;
}
