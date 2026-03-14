import { useEffect } from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import { useAuthStore } from './stores/authStore';
import { ws } from './api/websocket';
import { useChatStore } from './stores/chatStore';
import { LoginPage } from './components/Auth/LoginPage';
import { AppShell } from './components/Layout/AppShell';
import { ChatPage } from './pages/ChatPage';
import { FilesPage } from './pages/FilesPage';
import { TasksPage } from './pages/TasksPage';
import { TaskDetailPage } from './pages/TaskDetailPage';
import { DiagnosticsPage } from './pages/DiagnosticsPage';
import { MemuPage } from './pages/MemuPage';
import { SourcesPage } from './pages/SourcesPage';
import { CronPage } from './pages/CronPage';
import { PlansPage } from './pages/PlansPage';
import { PlanDetailPage } from './pages/PlanDetailPage';
import { SkillsPage } from './pages/SkillsPage';
import { SkillDetailPage } from './pages/SkillDetailPage';
import { McpServersPage } from './pages/McpServersPage';
import { McpServerDetailPage } from './pages/McpServerDetailPage';
import { NotificationsPage } from './pages/NotificationsPage';
import { NotificationToast } from './components/Notifications/NotificationToast';

function App() {
  const { authenticated, checkAuth } = useAuthStore();
  const { handleWSMessage, loadSessions } = useChatStore();

  useEffect(() => { checkAuth(); }, []);

  useEffect(() => {
    if (!authenticated) return;
    ws.connect();
    const unsub = ws.onMessage(handleWSMessage);
    loadSessions();
    return () => { unsub(); ws.disconnect(); };
  }, [authenticated]);

  if (!authenticated) return <LoginPage />;

  return (
    <>
      <Routes>
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
          <Route path="/mcp" element={<McpServersPage />} />
          <Route path="/mcp/:serverName" element={<McpServerDetailPage />} />
          <Route path="/notifications" element={<NotificationsPage />} />
          <Route path="/sources" element={<SourcesPage />} />
          <Route path="/cron" element={<CronPage />} />
          <Route path="/memory" element={<MemuPage />} />
          <Route path="/diagnostics" element={<DiagnosticsPage />} />
        </Route>
      </Routes>
      <NotificationToast />
    </>
  );
}

export default App;
