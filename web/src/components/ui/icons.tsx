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
  ArrowRight, Ban, BarChart3, Bell, BellOff, BookOpen, Bot, Brain,
  Calendar, Check, CheckCheck, CheckCircle, CheckCircle2, CheckSquare,
  ChevronDown, ChevronLeft, ChevronRight, Circle, CircleCheck,
  CircleDashed, CircleDollarSign, CircleHelp, CircleX, Clock, Clock3,
  Columns3, Copy, CornerDownLeft, Cpu, Database, DollarSign, Download,
  Edit3, ExternalLink, Eye, EyeOff, File, FileCheck, FileDiff, FileEdit,
  FilePlus, FileText, FileX, Files, Filter, Folder, FolderOpen, GitBranch,
  Github, Globe, GripVertical, Hammer, HardDrive, HelpCircle, History,
  Hourglass, Inbox, Lightbulb, List, ListTodo, Loader2, Lock, LogOut, Mail,
  Maximize2, MessageCircle, MessageCircleQuestion, MessageSquare, Monitor,
  Moon, MoreHorizontal, OctagonX, PanelLeftClose, PanelLeftOpen, Paperclip,
  Pause, Pencil, Play, Plug, Plus, Radio, RefreshCw, Repeat, Rocket,
  RotateCcw, RotateCw, Save, Search, SearchCheck, Send, Server,
  ShieldCheck, ShieldQuestion, SlidersHorizontal, Sparkle, Sparkles,
  Square, SquareTerminal, Star, StickyNote, Sun, Tag, Terminal, Timer,
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
