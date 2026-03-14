import { useEffect } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { MessageSquare, FolderOpen, CheckSquare, Inbox, Activity, Brain, LogOut, Clock, Lightbulb, Sparkles, Bell, Plug } from 'lucide-react';
import { useAuthStore } from '../../stores/authStore';
import { useNotificationStore } from '../../stores/notificationStore';
import { ws } from '../../api/websocket';

const NAV_ITEMS = [
  { path: '/chat', icon: MessageSquare, label: 'Chat' },
  { path: '/files', icon: FolderOpen, label: 'Files' },
  { path: '/tasks', icon: CheckSquare, label: 'Tasks' },
  { path: '/plans', icon: Lightbulb, label: 'Plans' },
  { path: '/skills', icon: Sparkles, label: 'Skills' },
  { path: '/mcp', icon: Plug, label: 'MCP' },
  { path: '/notifications', icon: Bell, label: 'Notifs' },
  { path: '/sources', icon: Inbox, label: 'Inbox' },
  { path: '/cron', icon: Clock, label: 'Cron' },
  { path: '/memory', icon: Brain, label: 'Memory' },
  { path: '/diagnostics', icon: Activity, label: 'Diag' },
];

export function NavRail() {
  const location = useLocation();
  const navigate = useNavigate();
  const { logout } = useAuthStore();
  const pendingCount = useNotificationStore(s => s.pendingCount);
  const loadNotifications = useNotificationStore(s => s.loadNotifications);

  // Load notification count on mount
  useEffect(() => { loadNotifications(); }, []);

  return (
    <div className="w-14 bg-[#141414] border-r border-[#2a2a2a] flex flex-col items-center py-3 shrink-0">
      <div className="text-[#6366f1] font-bold text-xs mb-4 tracking-wider">N</div>

      <div className="flex-1 flex flex-col gap-1">
        {NAV_ITEMS.map(({ path, icon: Icon, label }) => {
          const active = location.pathname.startsWith(path);
          const isNotifs = path === '/notifications';
          return (
            <button
              key={path}
              onClick={() => navigate(path)}
              className={`relative w-10 h-10 rounded-lg flex flex-col items-center justify-center gap-0.5 cursor-pointer transition-colors
                ${active
                  ? 'bg-[#6366f1]/15 text-[#6366f1]'
                  : 'text-[#666] hover:text-[#999] hover:bg-[#1f1f1f]'
                }`}
              title={label}
            >
              <Icon size={18} />
              <span className="text-[9px]">{label}</span>
              {isNotifs && pendingCount > 0 && (
                <span className="absolute -top-0.5 -right-0.5 w-4 h-4 bg-red-500 rounded-full text-[9px] text-white flex items-center justify-center font-medium">
                  {pendingCount > 9 ? '9+' : pendingCount}
                </span>
              )}
            </button>
          );
        })}
      </div>

      <div className="flex flex-col items-center gap-2">
        <div className={`w-2 h-2 rounded-full ${ws.connected ? 'bg-emerald-400' : 'bg-red-400'}`}
             title={ws.connected ? 'Connected' : 'Disconnected'} />
        <button
          onClick={logout}
          className="w-10 h-10 rounded-lg flex items-center justify-center text-[#555] hover:text-[#888] hover:bg-[#1f1f1f] cursor-pointer"
          title="Logout"
        >
          <LogOut size={16} />
        </button>
      </div>
    </div>
  );
}
