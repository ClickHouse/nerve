import { useEffect, useMemo } from 'react';
import { Routes, Route, Navigate, useNavigate, useLocation } from 'react-router-dom';
import type { Location } from 'react-router-dom';
import { useAuthStore } from './stores/authStore';
import { ws } from './api/websocket';
import { useChatStore } from './stores/chatStore';
import { useUIStore } from './stores/uiStore';
import { useKeyboardShortcuts } from './hooks/useKeyboardShortcuts';
import type { ShortcutDef } from './utils/keyboard';
import { LoginPage } from './components/Auth/LoginPage';
import { AppShell } from './components/Layout/AppShell';
import { ChatPage } from './pages/ChatPage';
import { FilesPage } from './pages/FilesPage';
import { TasksPage } from './pages/TasksPage';
import { TaskDetailPage } from './pages/TaskDetailPage';
import { TaskDetailModal } from './components/Tasks/TaskDetailModal';
import { DiagnosticsPage } from './pages/DiagnosticsPage';
import { MemuPage } from './pages/MemuPage';
import { SourcesPage } from './pages/SourcesPage';
import { CronPage } from './pages/CronPage';
import { PlansPage } from './pages/PlansPage';
import { PlanDetailPage } from './pages/PlanDetailPage';
import { SkillsPage } from './pages/SkillsPage';
import { SkillDetailPage } from './pages/SkillDetailPage';
import { McpServersPage } from './pages/McpServersPage';
import { UltracodePage } from './pages/UltracodePage';
import { WorkflowRunsPage } from './pages/WorkflowRunsPage';
import { McpServerDetailPage } from './pages/McpServerDetailPage';
import { NotificationsPage } from './pages/NotificationsPage';
import { NotificationToast } from './components/Notifications/NotificationToast';
import { ShortcutsModal } from './components/ShortcutsModal';

function App() {
  const { authenticated, checking, checkAuth } = useAuthStore();
  const { handleWSMessage, loadSessions } = useChatStore();
  // Above the early returns — hooks can't run conditionally.
  const location = useLocation();

  useEffect(() => { checkAuth(); }, []);

  useEffect(() => {
    if (!authenticated) return;
    ws.connect();
    const unsub = ws.onMessage(handleWSMessage);
    loadSessions();
    return () => { unsub(); ws.disconnect(); };
  }, [authenticated]);

  if (checking) return null;
  if (!authenticated) return <LoginPage />;

  // Background-location routing: when a route is entered with a
  // `background` location in history state, render *that* location's route
  // tree and overlay the real one as a modal. Reaching the same URL
  // directly — a cold load, a refresh, a shared link — carries no such
  // state and renders the ordinary full page. Used by the task board so
  // opening a card doesn't tear the board down.
  const background = (location.state as { background?: Location } | null)?.background;

  return (
    <>
      <GlobalShortcuts />
      <Routes location={background ?? location}>
        <Route element={<AppShell />}>
          <Route path="/" element={<Navigate to="/chat" replace />} />
          <Route path="/chat/:sessionId?" element={<ChatPage />} />
          <Route path="/files/*" element={<FilesPage />} />
          <Route path="/tasks" element={<TasksPage />} />
          <Route path="/tasks/:taskId" element={<TaskDetailPage />} />
          <Route path="/plans" element={<PlansPage />} />
          <Route path="/plans/:planId" element={<PlanDetailPage />} />
          <Route path="/skills" element={<SkillsPage />} />
          <Route path="/skills/:skillId" element={<SkillDetailPage />} />
          <Route path="/ultracode" element={<UltracodePage />} />
          <Route path="/workflow-runs" element={<WorkflowRunsPage />} />
          <Route path="/mcp" element={<McpServersPage />} />
          <Route path="/mcp/:serverName" element={<McpServerDetailPage />} />
          <Route path="/notifications" element={<NotificationsPage />} />
          <Route path="/sources" element={<SourcesPage />} />
          <Route path="/cron" element={<CronPage />} />
          <Route path="/memory" element={<MemuPage />} />
          <Route path="/diagnostics" element={<DiagnosticsPage />} />
        </Route>
      </Routes>

      {background && (
        <Routes>
          <Route path="/tasks/:taskId" element={<TaskDetailModal />} />
        </Routes>
      )}

      <NotificationToast />
      <ShortcutsModal />
    </>
  );
}

/**
 * Global keyboard shortcuts — work on every page. Page-scoped chat shortcuts
 * live in ChatPage so they only activate while the chat view is mounted.
 *
 * Esc behavior is intentionally cascaded:
 *   1. ShortcutsModal swallows Esc first via capture-phase listener.
 *   2. SessionSidebar's own listener clears search when active.
 *   3. This handler stops generation only if streaming and nothing else
 *      is claiming Esc (modal closed, search empty).
 */
function GlobalShortcuts() {
  const navigate = useNavigate();

  const shortcuts = useMemo<ShortcutDef[]>(() => [
    {
      id: 'global-new-chat',
      combo: { mod: true, shift: true, key: 'o' },
      description: 'New chat',
      section: 'global',
      action: () => {
        navigate('/chat');
        void useChatStore.getState().createSession();
      },
    },
    {
      id: 'global-focus-search',
      combo: { mod: true, key: 'k' },
      description: 'Focus session search',
      section: 'global',
      action: () => {
        const focusNow = () => {
          const store = useChatStore.getState();
          if (store.sidebarCollapsed) store.toggleSidebar();
          // The sidebar search input is unmounted until something asks for it.
          // requestSearchFocus bumps a nonce the sidebar subscribes to.
          store.requestSearchFocus();
        };
        if (!window.location.pathname.startsWith('/chat')) {
          navigate('/chat');
          // Wait one tick for ChatPage + SessionSidebar to mount.
          setTimeout(focusNow, 0);
        } else {
          focusNow();
        }
      },
    },
    {
      id: 'global-shortcuts-modal',
      combo: { mod: true, key: '/' },
      description: 'Show keyboard shortcuts',
      section: 'global',
      allowInInput: true,
      action: () => useUIStore.getState().toggleShortcutsModal(),
    },
    {
      id: 'global-esc-stop',
      combo: { key: 'Escape' },
      description: 'Stop generation',
      section: 'global',
      // Only fire when nothing else is claiming Esc:
      // - modal handles its own Esc in capture phase
      // - sidebar handles Esc only while searching
      when: () => {
        if (useUIStore.getState().shortcutsModalOpen) return false;
        if (!useChatStore.getState().isStreaming) return false;
        return true;
      },
      action: () => useChatStore.getState().stopSession(),
    },
  ], [navigate]);

  useKeyboardShortcuts(shortcuts);
  return null;
}

export default App;
