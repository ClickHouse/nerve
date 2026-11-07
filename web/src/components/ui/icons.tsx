import { Star, type LucideProps } from 'lucide-react';

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
  ArrowRight, ArrowUp, ArrowUpRight, Ban, BarChart3, Bell, BellOff, BookOpen, Bot, Brain,
  Calendar, Check, CheckCheck, CheckCircle, CheckCircle2, CheckSquare,
  ChevronDown, ChevronLeft, ChevronRight, ChevronUp, Circle, CircleCheck, Cloud,
  KeyRound, ListChecks, ListTree, LocateFixed, PartyPopper,
  CircleDashed, CircleDollarSign, CircleHelp, CircleX, Clock, Clock3,
  Columns3, Copy, CornerDownLeft, Cpu, Database, DollarSign, Download,
  Edit3, ExternalLink, Eye, EyeOff, File, FileCheck, FileDiff, FileEdit,
  FilePlus, FileText, FileX, Files, Filter, Folder, FolderOpen, GitBranch,
  Github, Globe, GripVertical, Hammer, HardDrive, HelpCircle, History,
  Hourglass, Inbox, Lightbulb, List, ListTodo, Loader2, Lock, LogOut, Mail,
  Maximize2, MessageCircle, MessageCircleQuestion, MessageSquare, Monitor,
  Moon, MoreHorizontal, OctagonX, PanelLeftClose, PanelLeftOpen, Paperclip,
  Pause, Pencil, Play, Plug, Plus, Radio, RefreshCw, Repeat, Reply, Rocket,
  RotateCcw, RotateCw, Save, Search, SearchCheck, Send, Server, Settings,
  ShieldCheck, ShieldQuestion, Slack, SlidersHorizontal, Sparkle, Sparkles,
  Square, SquareTerminal, Star, StickyNote, Sun, Tag, Terminal, Timer,
  User, Users,
  Trash2, TrendingDown, TrendingUp, Unlink, Workflow, WrapText, Wrench, X,
  XCircle, Zap
} from 'lucide-react';

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

// The `vscode` glyph of @vscode/codicons, © Microsoft, CC BY 4.0.
export function VSCode({ size = 16, className }: { size?: number; className?: string }) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="currentColor" aria-hidden="true" className={className}>
      <path d="M15.434 1.72887L12.14 0.144875C12.002 0.078875 11.855 0.046875 11.709 0.046875C11.353 0.046875 11.18 0.211875 11.155 0.228875C11.073 0.270875 11.005 0.337875 11.004 0.338875L4.698 6.08888L1.951 4.00488C1.832 3.91388 1.69 3.86987 1.548 3.86987C1.387 3.86987 1.226 3.92788 1.1 4.04288L0.219 4.84387C0.074 4.97587 0.001 5.15688 0.001 5.33687C0.001 5.51687 0.073 5.69688 0.218 5.82888L2.6 8.00088L0.217 10.1719C0.072 10.3039 0 10.4839 0 10.6639C0 10.8439 0.073 11.0249 0.218 11.1569L1.099 11.9579C1.226 12.0729 1.386 12.1309 1.547 12.1309C1.688 12.1309 1.83 12.0859 1.95 11.9959L4.697 9.91187L11.003 15.6619C11.003 15.6619 11.072 15.7299 11.155 15.7719C11.179 15.7889 11.353 15.9529 11.709 15.9529C11.855 15.9529 12.003 15.9209 12.141 15.8549L15.435 14.2709C15.781 14.1049 16.001 13.7539 16.001 13.3699V2.62888C16.001 2.24487 15.781 1.89488 15.435 1.72787L15.434 1.72887ZM7.217 7.99988L12.002 4.36987V11.6299L7.217 7.99988Z" />
    </svg>
  );
}
